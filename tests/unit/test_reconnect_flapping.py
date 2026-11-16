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


@pytest.mark.parametrize("code", [1000, 1001, 1011])
async def test_flapping_server_gets_growing_waits(code):
    async def handler(ws, _n):
        await ws.recv()  # config handshake
        await ws.send(json.dumps({"type": "ready", "session_id": "s"}))
        await ws.close(code)

    times = await _run_against(handler, 2.0, initial_backoff=0.2, max_backoff=1.0)

    # Waits 0.2, 0.4, 0.8, 1.0 -> connections near 0, 0.2, 0.6, 1.4 s (thousands before).
    assert 3 <= len(times) <= 6
    gaps = [b - a for a, b in itertools.pairwise(times)]
    assert gaps[0] >= 0.15
    assert all(later > earlier for earlier, later in itertools.pairwise(gaps))


async def test_connection_that_lasts_reconnects_at_once():
    closed_at: list[float] = []

    async def handler(ws, n):
        await ws.recv()
        await ws.send(json.dumps({"type": "ready", "session_id": f"s{n}"}))
        if n == 1:
            await asyncio.sleep(0.4)  # outlives initial_backoff
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

    h.connected_mono = time.monotonic() - 5  # this one lasted
    await reconnect()
    h.connected_mono = time.monotonic()
    await reconnect()
    assert sleeps == [1.0, 2.0, 4.0, 4.0, 1.0]
