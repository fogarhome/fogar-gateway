"""Auditoría 3.0 ronda 2, E-r2-1: a server that accepts (`ready`) and closes at once must
not drive a reconnect loop with no wait (~1000 connections/s before the fix). A connection
that dies younger than `STABLE_CONNECTION_S` waits a growing backoff from the second such
drop in a row (ronda 3, E-r3-1: the first one reconnects at once, so a lone cut does not
lose the phrase in flight); one that lasts reconnects straight away. Real `websockets`
servers on 127.0.0.1, port 0."""

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
    """The first early drop reconnects at once, the next ones wait a growing backoff.
    `hold` > initial_backoff: a server cutting just after a wait (review 1) still counts as
    a flap, since the bar is STABLE_CONNECTION_S, not initial_backoff."""

    async def handler(ws, _n):
        await ws.recv()  # config handshake
        await ws.send(json.dumps({"type": "ready", "session_id": "s"}))
        await asyncio.sleep(hold)
        await ws.close(code)

    times = await _run_against(handler, 2.0, initial_backoff=0.2, max_backoff=1.0)

    # Waits 0, 0.2, 0.4, 0.8 (+hold) -> connections near 0, 0, 0.2, 0.6, 1.4 s (thousands
    # before the fix).
    assert 3 <= len(times) <= 6
    gaps = [b - a for a, b in itertools.pairwise(times)]
    assert gaps[1] >= 0.15 + hold
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

    for _ in range(5):  # each reconnect "dies" at once: mark_connected was just now
        await reconnect()
    assert sleeps == [1.0, 2.0, 4.0, 4.0]  # the first early drop does not wait (E-r3-1)

    h.connected_mono = time.monotonic() - STABLE_CONNECTION_S - 1  # this one lasted
    await reconnect()
    h.connected_mono = time.monotonic()
    await reconnect()
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

    for _ in range(4):
        await reconnect()
    assert sleeps == [1.0, 2.0, 4.0]
    assert h.flap_backoff == 8.0  # the connect right after a flap wait keeps it

    h.flap_at = time.monotonic() - STABLE_CONNECTION_S - 1
    h.mark_connected()  # direct reopen, long after the last flap
    await reconnect()
    await reconnect()
    assert sleeps == [1.0, 2.0, 4.0, 1.0]


async def test_flap_wait_counts_towards_max_duration(monkeypatch):
    """Review 3: with the flap wait as long as max_duration, one failed attempt degrades."""
    monkeypatch.setattr("fogar_gateway.services.reconnection._sleep", asyncio.sleep)
    h = ServiceHealth("x", ConnectionState.RECONNECTING)
    h.mark_connected()
    h.flap_backoff = 0.2  # a second early drop in a row

    async def fail() -> None:
        raise OSError

    ok = await reconnect_with_backoff(
        fail, h, initial_backoff=0.2, max_backoff=1.0, max_duration=0.2, label="x"
    )
    assert ok is False
    assert h.state == ConnectionState.DEGRADED
    assert h.reconnect_attempts == 1


async def test_wake_during_flap_wait_resets_it():
    """Review 4: a wake (trigger_reconnect, direct connect) ends the wait; the next early drop
    still waits initial_backoff (no two free reconnects against a server closing at once)."""
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
    assert h.flap_backoff == 1.0




async def _until(cond, timeout=3.0):
    async with asyncio.timeout(timeout):
        while not cond():
            await asyncio.sleep(0.01)


def _fogar_handler(script):
    """fogar-transcriber: `ready`, a partial on the first audio, and on `end` the final + close
    1000. `script(n)` says how connection n behaves: "cut_on_audio", "cut_on_end",
    "cut_idle" (closes 1011 right after `ready`), "slow_ready" (`ready` after 0.3 s),
    "incomplete" (final with complete=false) or None (normal)."""
    state = {"conns": 0, "audio": {}, "connected_at": [], "cut_at": []}

    async def handler(ws):
        state["conns"] += 1
        n = state["conns"]
        state["connected_at"].append(time.monotonic())
        state["audio"][n] = 0
        await ws.recv()  # config handshake
        if script(n) == "slow_ready":
            await asyncio.sleep(0.3)
        await ws.send(json.dumps({"type": "ready", "session_id": f"s{n}"}))
        if script(n) == "cut_idle":
            await asyncio.sleep(0.05)
            await ws.close(1011)
            return
        async for msg in ws:
            if isinstance(msg, bytes):
                if script(n) == "cut_on_audio":
                    state["cut_at"].append(time.monotonic())
                    await ws.close(1011)  # transient server error mid-phrase
                    return
                state["audio"][n] += 1
                if state["audio"][n] == 1:
                    await ws.send(json.dumps(
                        {"type": "transcription", "text": "parcial", "is_final": False}
                    ))
            elif json.loads(msg).get("type") == "end":
                if script(n) == "cut_on_end":
                    await ws.close(1011)
                    return
                final = {"type": "transcription", "text": f"frase {n}", "is_final": True}
                if script(n) == "incomplete":
                    final |= {"complete": False, "reason": "gpu_saturated"}
                await ws.send(json.dumps(final))
                await ws.close(1000)
                return

    return handler, state


async def _phrase(w, frames: int) -> None:
    for _ in range(frames):  # 20 ms frames, then end
        await w.send_audio(b"\0" * 640)
        await asyncio.sleep(0.02)
    await w.send_end()


def _finals(w) -> list[str]:
    return [c.args[0] for c in w._callbacks.on_transcription.await_args_list if c.args[1]]


async def test_cut_mid_phrase_withholds_the_truncated_final():
    """E-r3-1 and its review: one 1011 while the user speaks, healthy server afterwards, default
    backoffs. The reconnect is immediate, so the tail of the phrase reaches the new connection,
    but its final lacks the head ("no enciendas…" -> "enciendas…"): it is withheld and the
    client is told to repeat. The next phrase goes through normally."""
    handler, st = _fogar_handler({1: "cut_on_audio", 2: "incomplete"}.get)
    async with websockets.serve(handler, "127.0.0.1", 0) as server:
        w = _wrap(server.sockets[0].getsockname()[1])
        await w.start()
        run = asyncio.create_task(w.run())
        try:
            await _phrase(w, 40)
            await _until(lambda: w._callbacks.on_warning.await_count)
            w._callbacks.on_warning.assert_awaited_once_with("phrase_interrupted", None)
            # One incomplete per phrase: the final's own complete=false is not reported twice.
            w._callbacks.on_incomplete.assert_awaited_once_with("phrase_interrupted")
            assert st["audio"][2] >= 20  # the reconnect did not wait: the tail got through
            w._callbacks.on_transcription.assert_not_awaited()  # nor its partials

            await _until(lambda: st["conns"] == 3 and w._client._is_ready)  # quiet reopen
            await _phrase(w, 5)
            await _until(lambda: _finals(w))
            assert _finals(w) == ["frase 3"]
            assert w._callbacks.on_warning.await_count == 1
        finally:
            await w.close()
            await asyncio.wait([run], timeout=2.0)


async def test_cut_after_end_reports_the_phrase_at_once():
    """Cut after our `end`, before the final: nothing of the phrase reaches the new connection,
    so the warning goes out right away and the next phrase is not withheld."""
    handler, st = _fogar_handler(lambda n: "cut_on_end" if n == 1 else None)
    async with websockets.serve(handler, "127.0.0.1", 0) as server:
        w = _wrap(server.sockets[0].getsockname()[1])
        await w.start()
        run = asyncio.create_task(w.run())
        try:
            await _phrase(w, 5)
            await _until(lambda: w._callbacks.on_warning.await_count and st["conns"] == 2
                         and w.state == ConnectionState.CONNECTED)
            w._callbacks.on_warning.assert_awaited_once_with("phrase_interrupted", None)
            await _phrase(w, 5)
            await _until(lambda: _finals(w))
            assert _finals(w) == ["frase 2"]
        finally:
            await w.close()
            await asyncio.wait([run], timeout=2.0)



async def test_end_dropped_in_the_gap_closes_the_phrase():
    """Review 2, case 1: send_audio sees the dead socket before the listen loop does and the
    `end` lands in that gap. The `end` is dropped, so the warning goes out at once and nothing
    is left armed: the next good phrase is delivered."""
    handler, st = _fogar_handler(lambda n: "cut_on_audio" if n == 1 else None)
    async with websockets.serve(handler, "127.0.0.1", 0) as server:
        w = _wrap(server.sockets[0].getsockname()[1])
        await w.start()
        await w.send_audio(b"\0" * 640)  # the server cuts on it
        await _until(lambda: w._client.ws.state.name == "CLOSED")
        await w.send_audio(b"\0" * 640)  # sees the dead socket first
        await w.send_end()  # before run() notices the drop
        w._callbacks.on_warning.assert_awaited_once_with("phrase_interrupted", None)
        run = asyncio.create_task(w.run())
        try:
            await _until(lambda: st["conns"] == 2 and w.state == ConnectionState.CONNECTED)
            await _phrase(w, 5)
            await _until(lambda: _finals(w))
            assert _finals(w) == ["frase 2"]
            assert w._callbacks.on_warning.await_count == 1
        finally:
            await w.close()
            await asyncio.wait([run], timeout=2.0)


async def test_reconnect_landing_mid_phrase_withholds_its_final():
    """Review 2, case 2: cut while idle, and the reconnect completes while the user speaks.
    The frames sent before it were dropped, so the final lacks its head: withheld. The next
    phrase is delivered."""
    handler, st = _fogar_handler({1: "cut_idle", 2: "slow_ready"}.get)
    async with websockets.serve(handler, "127.0.0.1", 0) as server:
        w = _wrap(server.sockets[0].getsockname()[1])
        await w.start()
        run = asyncio.create_task(w.run())
        try:
            await _until(lambda: st["conns"] == 2)  # reconnecting, `ready` still 0.3 s away
            await _phrase(w, 30)
            await _until(lambda: w._callbacks.on_warning.await_count)
            w._callbacks.on_warning.assert_awaited_once_with("phrase_interrupted", None)
            assert st["audio"][2] >= 5  # part of the phrase did reach the new connection
            w._callbacks.on_transcription.assert_not_awaited()

            await _until(lambda: st["conns"] == 3 and w._client._is_ready)
            await _phrase(w, 5)
            await _until(lambda: _finals(w))
            assert _finals(w) == ["frase 3"]
        finally:
            await w.close()
            await asyncio.wait([run], timeout=2.0)


async def test_lone_cuts_between_normal_phrases_do_not_add_up():
    """Review point 2: cut -> normal phrase (closed 1000 after our end) -> cut. The phrase in
    between resets the flap backoff, so the second cut also reconnects at once (1 s before)."""
    handler, st = _fogar_handler(lambda n: "cut_on_audio" if n in (1, 3) else None)
    async with websockets.serve(handler, "127.0.0.1", 0) as server:
        w = _wrap(server.sockets[0].getsockname()[1])
        await w.start()
        run = asyncio.create_task(w.run())
        try:
            await w.send_audio(b"\0" * 640)  # cut 1
            await _until(lambda: st["conns"] == 2 and w.state == ConnectionState.CONNECTED)
            await _phrase(w, 3)  # its final is withheld (truncated), then the quiet reopen
            await _until(lambda: st["conns"] == 3)
            await w.send_audio(b"\0" * 640)  # cut 2
            await _until(lambda: st["conns"] == 4)
            assert st["connected_at"][3] - st["cut_at"][1] < 0.5
        finally:
            await w.close()
            await asyncio.wait([run], timeout=2.0)


async def test_server_closing_after_ready_still_backs_off():
    """E-r2-1: `ready` and a close 0.1 s later. The second connect is immediate, then waits of
    0.1 s, 0.2 s, 0.4 s... (≈1000 connections/s without protection)."""

    async def handler(ws, _n):
        await ws.recv()
        await ws.send(json.dumps({"type": "ready", "session_id": "s"}))
        await asyncio.sleep(0.1)
        await ws.close(1011)

    times = await _run_against(handler, 2.0, initial_backoff=0.1)

    # Connections near 0, 0.1, 0.3, 0.6, 1.1, 1.9 s.
    assert 4 <= len(times) <= 7
    gaps = [b - a for a, b in itertools.pairwise(times)]
    assert gaps[0] < 0.18  # hold only, no wait
    assert all(later > earlier for earlier, later in itertools.pairwise(gaps))
