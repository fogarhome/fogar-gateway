"""Auditoría 3.0, E-1: a server-side close with 1000/1001 (ConnectionClosedOK) ends the
`async for` without an exception. It must still count as a drop: the wrapper leaves
CONNECTED and reconnects, and the bridge's silence watchdog does not close the session.

Real `websockets` servers on port 0: fogar-transcriber sends 1001 when it restarts.
"""

import asyncio
import json
import time
from unittest.mock import AsyncMock, patch

import pytest
import websockets

from fogar_gateway.models.schemas import ClientConfig
from fogar_gateway.services.reconnection import ConnectionState
from fogar_gateway.services.speech.ports import SttCallbacks, SttParams
from fogar_gateway.services.transcriber_client import TranscriberClient
from fogar_gateway.services.transcriber_reconnecting import ReconnectingTranscriberClient
from tests.fakes.bridge import sent


def _wrap(port: int, **kw) -> ReconnectingTranscriberClient:
    cbs = SttCallbacks(
        on_transcription=AsyncMock(), on_warning=AsyncMock(),
        on_status=AsyncMock(), on_incomplete=AsyncMock(),
    )
    return ReconnectingTranscriberClient(
        url=f"127.0.0.1:{port}", client_id="c1", callbacks=cbs,
        params=SttParams(language="es", vad_threshold=0.0), **kw,
    )


async def _until(cond, timeout=2.0):
    async with asyncio.timeout(timeout):
        while not cond():
            await asyncio.sleep(0.01)


@pytest.mark.parametrize("code", [1000, 1001])
async def test_server_clean_close_reconnects(code):
    conns = 0

    async def handler(ws):
        nonlocal conns
        conns += 1
        await ws.recv()  # config handshake
        await ws.send(json.dumps({"type": "ready", "session_id": f"s{conns}"}))
        if conns == 1:
            await ws.close(code)
        else:
            await ws.wait_closed()

    async with websockets.serve(handler, "127.0.0.1", 0) as server:
        w = _wrap(server.sockets[0].getsockname()[1], initial_backoff=0.01, max_backoff=0.01)
        await w.start()
        seen: list[ConnectionState] = []
        w.on_state_change = seen.append
        run = asyncio.create_task(w.run())
        try:
            await _until(lambda: conns == 2 and w.state == ConnectionState.CONNECTED)
            assert ConnectionState.RECONNECTING in seen
            assert not run.done()  # listening on the new connection
        finally:
            await w.close()
            await asyncio.wait([run], timeout=2.0)  # close() may cancel it


async def test_own_close_does_not_reconnect():
    conns = 0

    async def handler(ws):
        nonlocal conns
        conns += 1
        await ws.recv()
        await ws.send(json.dumps({"type": "ready", "session_id": "s1"}))
        await ws.wait_closed()

    async with websockets.serve(handler, "127.0.0.1", 0) as server:
        w = _wrap(server.sockets[0].getsockname()[1])
        await w.start()
        run = asyncio.create_task(w.run())
        await asyncio.sleep(0.05)
        # The socket closed by us (not by cancelling run()): the loop ends, no reconnect.
        await w._client.close()
        await asyncio.wait_for(run, 2.0)
        assert w.state == ConnectionState.CONNECTED
    assert conns == 1
    assert not w._client._dropped_unexpectedly


async def test_bridge_does_not_close_session_on_clean_transcriber_drop(make_bridge):
    """With the bug the state stayed CONNECTED after the 1001 and the watchdog, seeing audio
    without transcriptions, closed the session for silence_timeout."""
    conns = 0

    async def handler(ws):
        nonlocal conns
        conns += 1
        await ws.recv()
        if conns == 1:
            await ws.send(json.dumps({"type": "ready", "session_id": "s1"}))
        await ws.close(1001)  # the restart: later attempts fail until it is back

    async with websockets.serve(handler, "127.0.0.1", 0) as server:
        w = _wrap(server.sockets[0].getsockname()[1], initial_backoff=10, max_backoff=10)
        await w.start()
        bridge, _ = make_bridge(
            config=ClientConfig(silence_timeout_s=1, max_silence_turns=1),
            input_mode="audio",
            transcriber=w,
        )
        w.on_state_change = bridge._on_transcriber_state_change
        bridge._audio_burst_start = time.monotonic() - 2
        bridge._last_audio_at = float("inf")  # the client keeps streaming audio
        bridge.close_all = AsyncMock()
        run = asyncio.create_task(w.run())
        try:
            await _until(lambda: run.done() or w.state != ConnectionState.CONNECTED)
            with patch(
                "fogar_gateway.services.bridge._sleep", new=lambda _s: asyncio.sleep(0.01)
            ):
                with pytest.raises(TimeoutError):  # the watchdog keeps waiting, no close
                    await asyncio.wait_for(bridge._transcription_watchdog(), 0.3)
            bridge.close_all.assert_not_awaited()
            msgs = await sent(bridge)
            assert {"type": "status", "service": "transcriber", "state": "reconnecting"} in [
                {k: m[k] for k in ("type", "service", "state")} for m in msgs
            ]
        finally:
            await w.close()
            await asyncio.wait([run], timeout=2.0)  # close() may cancel it


async def test_drop_seen_first_by_send_audio_still_reconnects():
    """Streaming audio: send_audio can see the closed socket (and lower _is_ready) before
    the listen loop resumes. That must not pass for our own close()."""
    async def handler(ws):
        await ws.recv()
        await ws.send(json.dumps({"type": "ready", "session_id": "s1"}))
        await ws.close(1001)

    async with websockets.serve(handler, "127.0.0.1", 0) as server:
        client = TranscriberClient(url=f"127.0.0.1:{server.sockets[0].getsockname()[1]}", client_id="c1")
        await client.connect()
        await _until(lambda: client.ws.state.name == "CLOSED")
        await client.send_audio(b"\0\0")
        assert not client._is_ready
        await asyncio.wait_for(client.listen_loop(AsyncMock()), 2.0)
    assert client._dropped_unexpectedly
