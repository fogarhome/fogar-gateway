"""Auditoría 3.0 ronda 2, E-r2-1: a server that accepts (`ready`) and closes at once must
not drive a reconnect loop with no wait (~1000 connections/s before the fix). A connection
that dies younger than `initial_backoff` waits a growing backoff; one that lasts reconnects
straight away. Real `websockets` servers on 127.0.0.1, port 0."""

import asyncio
import itertools
import json
import time
from unittest.mock import AsyncMock

import pytest
import websockets

from fogar_gateway.services.reconnection import (
    STABLE_CONNECTION_S,
    ConnectionState,
    ServiceHealth,
    reconnect_with_backoff,
)
from fogar_gateway.services.speech.ports import SttCallbacks, SttParams
from fogar_gateway.services.transcriber_reconnecting import ReconnectingTranscriberClient


def _wrap(port: int, **kw) -> ReconnectingTranscriberClient:
    cbs = SttCallbacks(
        on_transcription=AsyncMock(), on_warning=AsyncMock(),
        on_status=AsyncMock(), on_incomplete=AsyncMock(),
    )
    return ReconnectingTranscriberClient(
        url=f"127.0.0.1:{port}", client_id="c1", callbacks=cbs,
        params=SttParams(language="es", vad_threshold=0.0), **kw,
    )


async def _run_against(handler, seconds: float, **kw) -> list[float]:
    """Connection times (monotonic) seen by `handler` while the wrapper runs `seconds`."""
    times: list[float] = []

    async def counting(ws):
        times.append(time.monotonic())
        await handler(ws, len(times))

    async with websockets.serve(counting, "127.0.0.1", 0) as server:
        w = _wrap(server.sockets[0].getsockname()[1], **kw)
        await w.start()
        run = asyncio.create_task(w.run())
        try:
            await asyncio.sleep(seconds)
            assert not run.done()  # never DEGRADED within max_duration
        finally:
            await w.close()
            await asyncio.wait([run], timeout=2.0)
    return times


@pytest.mark.parametrize(("code", "hold"), [(1000, 0), (1001, 0), (1011, 0), (1001, 0.25)])
async def test_flapping_server_gets_growing_waits(code, hold):
    """`hold` > initial_backoff: a server cutting just after the first wait (review 1) still
    counts as a flap, since the bar is STABLE_CONNECTION_S, not initial_backoff."""

    async def handler(ws, _n):
        await ws.recv()  # config handshake
        await ws.send(json.dumps({"type": "ready", "session_id": "s"}))
        await asyncio.sleep(hold)
        await ws.close(code)

    times = await _run_against(handler, 2.0, initial_backoff=0.2, max_backoff=1.0)

    # Waits 0.2, 0.4, 0.8, 1.0 (+hold) -> connections near 0, 0.2, 0.6, 1.4 s (thousands before).
    assert 2 + (hold == 0) <= len(times) <= 6
    gaps = [b - a for a, b in itertools.pairwise(times)]
    assert gaps[0] >= 0.15
    assert all(later > earlier for earlier, later in itertools.pairwise(gaps))


async def test_connection_that_lasts_reconnects_at_once(monkeypatch):
    monkeypatch.setattr("fogar_gateway.services.reconnection.STABLE_CONNECTION_S", 0.3)
    closed_at: list[float] = []

    async def handler(ws, n):
        await ws.recv()
        await ws.send(json.dumps({"type": "ready", "session_id": f"s{n}"}))
        if n == 1:
            await asyncio.sleep(0.4)  # outlives STABLE_CONNECTION_S
            closed_at.append(time.monotonic())
            await ws.close(1001)
        else:
            await ws.wait_closed()

    times = await _run_against(handler, 0.8, initial_backoff=0.2, max_backoff=1.0)

    assert len(times) == 2
    assert times[1] - closed_at[0] < 0.15  # no flap wait


async def test_lasting_connection_resets_the_flap_backoff(monkeypatch):
    sleeps: list[float] = []

    async def fake_sleep(d):
        sleeps.append(d)

    monkeypatch.setattr("fogar_gateway.services.reconnection._sleep", fake_sleep)
    h = ServiceHealth("x", ConnectionState.RECONNECTING)
    h.mark_connected()

    async def ok() -> None:
        return None

    async def reconnect() -> None:
        await reconnect_with_backoff(
            ok, h, initial_backoff=1.0, max_backoff=4.0, max_duration=None, label="x"
        )

    for _ in range(4):  # each reconnect "dies" at once: mark_connected was just now
        await reconnect()
    assert sleeps == [1.0, 2.0, 4.0, 4.0]

    h.connected_mono = time.monotonic() - STABLE_CONNECTION_S - 1  # this one lasted
    await reconnect()
    h.connected_mono = time.monotonic()
    await reconnect()
    assert sleeps == [1.0, 2.0, 4.0, 4.0, 1.0]


async def test_reopen_long_after_a_flap_starts_clean(monkeypatch):
    """Review 2: a healthy reopen outside the loop (ElevenLabs per phrase, OpenClaw connect())
    must not leave a later early drop with the doubled wait."""
    sleeps: list[float] = []

    async def fake_sleep(d):
        sleeps.append(d)

    monkeypatch.setattr("fogar_gateway.services.reconnection._sleep", fake_sleep)
    h = ServiceHealth("x", ConnectionState.RECONNECTING)
    h.mark_connected()

    async def ok() -> None:
        return None

    async def reconnect() -> None:
        await reconnect_with_backoff(
            ok, h, initial_backoff=1.0, max_backoff=8.0, max_duration=None, label="x"
        )

    for _ in range(3):
        await reconnect()
    assert sleeps == [1.0, 2.0, 4.0]
    assert h.flap_backoff == 8.0  # the connect right after a flap wait keeps it

    h.flap_at = time.monotonic() - STABLE_CONNECTION_S - 1
    h.mark_connected()  # direct reopen, long after the last flap
    await reconnect()
    assert sleeps == [1.0, 2.0, 4.0, 1.0]


async def test_flap_wait_counts_towards_max_duration(monkeypatch):
    """Review 3: with the flap wait as long as max_duration, one failed attempt degrades."""
    monkeypatch.setattr("fogar_gateway.services.reconnection._sleep", asyncio.sleep)
    h = ServiceHealth("x", ConnectionState.RECONNECTING)
    h.mark_connected()

    async def fail() -> None:
        raise OSError

    ok = await reconnect_with_backoff(
        fail, h, initial_backoff=0.2, max_backoff=1.0, max_duration=0.2, label="x"
    )
    assert ok is False
    assert h.state == ConnectionState.DEGRADED
    assert h.reconnect_attempts == 1


async def test_wake_during_flap_wait_resets_it():
    """Review 4: a wake (trigger_reconnect, direct connect) ends the wait and resets it."""
    h = ServiceHealth("x", ConnectionState.RECONNECTING)
    h.mark_connected()
    h.flap_backoff = 30.0
    wake = asyncio.Event()

    async def ok() -> None:
        return None

    task = asyncio.create_task(reconnect_with_backoff(
        ok, h, initial_backoff=1.0, max_backoff=60.0, max_duration=None, label="x", wake=wake
    ))
    await asyncio.sleep(0.05)
    assert not task.done()
    wake.set()
    assert await asyncio.wait_for(task, 1.0) is True
    assert h.flap_backoff == 0.0
