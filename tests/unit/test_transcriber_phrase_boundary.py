"""Auditoría 3.0 ronda 4, E-r4-1/2/3 (#358): audio around the phrase boundary.

fogar-transcriber's handleEnd stops reading the socket at our `end`, runs the final, sends it
and closes 1000. Audio sent from our `end` until the reopened connection's `ready` belongs to
the next phrase: it is held and replayed there, in order. Real `websockets` servers on port 0;
each frame carries its id and the final lists the ids the server read.
"""

import asyncio
import json
from unittest.mock import AsyncMock

import websockets

from fogar_gateway.services.reconnection import ConnectionState
from fogar_gateway.services.speech.ports import SttCallbacks, SttParams
from fogar_gateway.services.transcriber_reconnecting import ReconnectingTranscriberClient

INFERENCE_S = 0.2


def _wrap(port: int, **kw) -> ReconnectingTranscriberClient:
    cbs = SttCallbacks(
        on_transcription=AsyncMock(), on_warning=AsyncMock(),
        on_status=AsyncMock(), on_incomplete=AsyncMock(),
    )
    return ReconnectingTranscriberClient(
        url=f"127.0.0.1:{port}", client_id="c1", callbacks=cbs,
        params=SttParams(language="es", vad_threshold=0.0), **kw,
    )


def _handle_end(script=lambda n: None):
    """`script(n)`: None (normal), "cut_on_audio" (1011 on the first frame), "empty" (final
    with no text) or "refuse" (closes before `ready`)."""
    st = {"conns": 0}

    async def handler(ws):
        st["conns"] += 1
        n = st["conns"]
        await ws.recv()  # config handshake
        if script(n) == "refuse":
            await ws.close(1011)
            return
        await ws.send(json.dumps({"type": "ready", "session_id": f"s{n}"}))
        ids: list[str] = []
        async for msg in ws:
            if isinstance(msg, bytes):
                if script(n) == "cut_on_audio":
                    await ws.close(1011)
                    return
                ids.append(msg.decode())
            elif json.loads(msg).get("type") == "end":
                break  # handleEnd: stops reading here
        await asyncio.sleep(INFERENCE_S)
        text = "" if script(n) == "empty" else " ".join(ids)
        await ws.send(json.dumps({"type": "transcription", "text": text, "is_final": True}))
        await ws.close(1000)

    return handler, st


async def _until(cond, timeout=3.0):
    async with asyncio.timeout(timeout):
        while not cond():
            await asyncio.sleep(0.01)


async def _frames(w, tag: str, n: int) -> None:
    for i in range(n):  # 20 ms frames
        await w.send_audio(f"{tag}{i}".encode())
        await asyncio.sleep(0.02)


def _ids(tag: str, n: int) -> str:
    return " ".join(f"{tag}{i}" for i in range(n))


def _finals(w) -> list[str]:
    return [c.args[0] for c in w._callbacks.on_transcription.await_args_list if c.args[1]]


async def _session(script, body, **kw) -> tuple[ReconnectingTranscriberClient, dict]:
    handler, st = _handle_end(script)
    async with websockets.serve(handler, "127.0.0.1", 0) as server:
        w = _wrap(server.sockets[0].getsockname()[1], **kw)
        await w.start()
        run = asyncio.create_task(w.run())
        try:
            await body(w, st)
        finally:
            await w.close()
            await asyncio.wait([run], timeout=2.0)
    return w, st


async def test_next_phrase_starting_during_the_final_arrives_whole():
    """head_silent: phrase b starts while a's final is being computed. Before the fix its
    head went to the old socket, which no longer reads, and b arrived without it, unflagged."""
    async def body(w, _st):
        await _frames(w, "a", 5)
        await w.send_end()
        await asyncio.sleep(INFERENCE_S / 2)
        await _frames(w, "b", 15)  # outlasts the rest of the inference and the reopen
        await w.send_end()
        await _until(lambda: len(_finals(w)) == 2)

    w, _ = await _session(lambda n: None, body)
    assert _finals(w) == [_ids("a", 5), _ids("b", 15)]
    w._callbacks.on_warning.assert_not_awaited()


async def test_back_to_back_phrases_arrive_whole():
    async def body(w, _st):
        await _frames(w, "a", 5)
        await w.send_end()
        await _frames(w, "b", 20)  # straight after a's end, its own end after the reopen
        assert not w._client._end_sent
        await w.send_end()
        await _until(lambda: len(_finals(w)) == 2)

    w, _ = await _session(lambda n: None, body)
    assert _finals(w) == [_ids("a", 5), _ids("b", 20)]
    w._callbacks.on_warning.assert_not_awaited()


async def test_end_during_the_replay_goes_after_the_held_frames():
    """`ready` came but the held frames are still being replayed: the `end` waits behind them
    instead of cutting the phrase."""
    async def body(w, _st):
        await _frames(w, "a", 3)
        await w.send_end()
        await _frames(w, "b", 2)  # held
        inner = w._client.send_audio

        async def slow(frame):
            await asyncio.sleep(0.05)
            await inner(frame)

        w._client.send_audio = slow
        await _until(lambda: not w._client._end_sent)  # `ready`: the replay is under way
        assert w._held
        await w.send_end()
        await _until(lambda: len(_finals(w)) == 2)

    w, _ = await _session(lambda n: None, body)
    assert _finals(w) == [_ids("a", 3), _ids("b", 2)]
    w._callbacks.on_warning.assert_not_awaited()


async def test_whole_phrase_inside_the_reopen_is_reported():
    """A phrase spoken and ended before the new `ready` is not kept: the client repeats it."""
    async def body(w, _st):
        await _frames(w, "a", 3)
        await w.send_end()
        await _frames(w, "b", 2)
        await w.send_end()  # still before `ready`
        await w.send_end()  # an end with no audio does nothing
        await _until(lambda: _finals(w) and w._client._is_ready)
        await _frames(w, "c", 3)
        await w.send_end()
        await _until(lambda: len(_finals(w)) == 2)

    w, _ = await _session(lambda n: None, body)
    assert _finals(w) == [_ids("a", 3), _ids("c", 3)]
    w._callbacks.on_warning.assert_awaited_once_with("phrase_interrupted", None)
    w._callbacks.on_incomplete.assert_awaited_once_with("phrase_interrupted")


async def test_late_trailing_frames_raise_no_false_warning():
    """trailing_late: a couple of frames right after the `end` are not a cut phrase."""
    async def body(w, _st):
        await _frames(w, "a", 5)
        await w.send_end()
        await w.send_audio(b"t0")
        await w.send_audio(b"t1")
        await _until(lambda: _finals(w) and w._client._is_ready)
        await asyncio.sleep(INFERENCE_S)
        await _frames(w, "c", 3)
        await w.send_end()
        await _until(lambda: len(_finals(w)) == 2)

    w, _ = await _session(lambda n: None, body)
    assert _finals(w) == [_ids("a", 5), "t0 t1 " + _ids("c", 3)]
    w._callbacks.on_warning.assert_not_awaited()


async def test_cut_phrase_with_an_empty_final_is_reported():
    """drop_empty_tail (E-r4-2): a cut loses the head, the tail's final comes back empty and
    the server closes 1000 after our end. The cut phrase is still reported."""
    async def body(w, st):
        await w.send_audio(b"x0")  # the server cuts on it
        await _until(lambda: st["conns"] == 2 and w.state == ConnectionState.CONNECTED)
        await _frames(w, "x", 3)
        await w.send_end()
        await _until(lambda: st["conns"] == 3 and w._client._is_ready)

    w, _ = await _session({1: "cut_on_audio", 2: "empty"}.get, body)
    w._callbacks.on_warning.assert_awaited_once_with("phrase_interrupted", None)
    assert _finals(w) == []


async def test_degraded_after_an_end_raises_no_warnings():
    """E-r4-3: the reopen after a's final never comes back (DEGRADED): later phrases are not
    reported as interrupted, the client already has `unavailable`."""
    async def body(w, _st):
        await _frames(w, "a", 2)
        await w.send_end()
        await _until(lambda: w.state == ConnectionState.DEGRADED)
        for _ in range(2):
            await _frames(w, "b", 2)
            await w.send_end()

    w, _ = await _session(
        lambda n: "refuse" if n > 1 else None, body,
        initial_backoff=0.01, max_backoff=0.01, max_duration=0.1,
    )
    assert _finals(w) == [_ids("a", 2)]
    w._callbacks.on_warning.assert_not_awaited()
    w._callbacks.on_incomplete.assert_not_awaited()


async def test_degraded_mid_phrase_raises_no_warnings():
    async def body(w, _st):
        await w.send_audio(b"x0")  # the server cuts on it and never comes back
        await _until(lambda: w.state == ConnectionState.DEGRADED)
        for _ in range(2):
            await _frames(w, "b", 2)
            await w.send_end()

    w, _ = await _session(
        lambda n: "cut_on_audio" if n == 1 else "refuse", body,
        initial_backoff=0.01, max_backoff=0.01, max_duration=0.1,
    )
    w._callbacks.on_warning.assert_not_awaited()
    w._callbacks.on_incomplete.assert_not_awaited()
