"""Auditoría 3.0 ronda 4, E-r4-1/2/3 (#358): audio around the phrase boundary.

fogar-transcriber's handleEnd stops reading the socket at our `end`, runs the final, sends it
and closes 1000. Audio sent from our `end` until the reopened connection's `ready` belongs to
the next phrase: it is held and replayed there, in order. Real `websockets` servers on port 0;
each frame carries its id and the final lists the ids the server read.
"""

import asyncio
import json
from unittest.mock import AsyncMock

import pytest
import websockets
from websockets.exceptions import ConnectionClosedError

from fogar_gateway.services.reconnection import ConnectionState
from fogar_gateway.services.speech.ports import SttCallbacks, SttParams
from fogar_gateway.services.transcriber_client import TranscriberClient
from fogar_gateway.services.transcriber_reconnecting import (
    HELD_MAX_BYTES,
    ReconnectingTranscriberClient,
)

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
    with no text), "cut_before_final" (1011 between our end and the final) or "refuse"
    (closes before `ready`)."""
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
        if script(n) == "cut_before_final":
            await ws.close(1011)
            return
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
    assert not w._held and w._held_bytes == 0  # E-r5-2: nothing is kept for a reopen that won't come


async def test_whole_phrase_in_the_gap_keeps_the_cut_previous_phrase_withheld():
    """E-r5-1 case 1: a loses its head to a 1011 and b falls whole in the gap before a's
    final. b's end used to clear a's mark and a's truncated final went out as an order."""
    async def body(w, st):
        await w.send_audio(b"a0")  # the server cuts on it
        await _until(lambda: st["conns"] == 2 and w.state == ConnectionState.CONNECTED)
        await _frames(w, "a", 3)
        await w.send_end()
        await _frames(w, "b", 2)
        await w.send_end()  # before a's final
        await _until(lambda: st["conns"] == 3 and w._client._is_ready)

    w, _ = await _session({1: "cut_on_audio"}.get, body, initial_backoff=0.01)
    assert _finals(w) == []
    assert w._callbacks.on_warning.await_count == 2  # a and b
    assert w._callbacks.on_incomplete.await_count == 2


async def test_whole_phrase_in_the_gap_then_a_cut_before_the_final_reports_both():
    """E-r5-1 case 2: b falls whole in the gap, then a 1011 lands before a's final. b's end
    used to clear a's open mark and a was lost without a warning."""
    async def body(w, st):
        await _frames(w, "a", 3)
        await w.send_end()
        await _frames(w, "b", 2)
        await w.send_end()
        await _until(lambda: st["conns"] == 2 and w._client._is_ready)

    w, _ = await _session({1: "cut_before_final"}.get, body, initial_backoff=0.01)
    assert _finals(w) == []
    assert w._callbacks.on_warning.await_count == 2
    assert w._callbacks.on_incomplete.await_count == 2


async def test_degraded_holds_no_audio_without_an_end():
    """E-r5-2: after a's `end` the reopen never comes (DEGRADED, _end_sent stays up): audio
    with no closing `end` is not held for the rest of the session."""
    async def body(w, _st):
        await _frames(w, "a", 2)
        await w.send_end()
        await _until(lambda: w.state == ConnectionState.DEGRADED)
        for i in range(5):
            await w.send_audio(f"b{i}|".encode().ljust(HELD_MAX_BYTES // 10))
        assert not w._held and w._held_bytes == 0

    await _session(
        lambda n: "refuse" if n > 1 else None, body,
        initial_backoff=0.01, max_backoff=0.01, max_duration=0.1,
    )


async def test_degrading_drops_a_whole_held_phrase():
    """E-r5-2: a whole phrase (with its `end`) is held when the reconnect gives up: the
    buffer is emptied, no reopen will ever replay it."""
    handler, _st = _handle_end(lambda n: "refuse")
    async with websockets.serve(handler, "127.0.0.1", 0) as server:
        w = _wrap(server.sockets[0].getsockname()[1], initial_backoff=0.01, max_backoff=0.01,
                  max_duration=0.1)
        w._health.set_state(ConnectionState.RECONNECTING)
        w._held.extend([b"c0", None])
        w._held_bytes = 2
        await asyncio.wait_for(w.run(), timeout=3.0)
    assert w.state == ConnectionState.DEGRADED
    assert not w._held and w._held_bytes == 0


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


class _SlowInner:
    """Writes each frame to the wire at once, then yields like websockets' drain."""

    def __init__(self, drain: float) -> None:
        self.wire: list[bytes | str] = []
        self.drain = drain
        self._is_ready = True
        self._end_sent = False

    async def send_audio(self, frame: bytes) -> None:
        self.wire.append(frame)
        await asyncio.sleep(self.drain)

    async def send_end(self) -> None:  # _end_sent late on purpose: the wrapper must not rely on it
        self.wire.append("END")
        await asyncio.sleep(self.drain)
        self._end_sent = True


def _slow(drain: float) -> tuple[ReconnectingTranscriberClient, _SlowInner]:
    w = _wrap(1)
    w._health.set_state(ConnectionState.CONNECTED)
    inner = _SlowInner(drain)
    w._client = inner  # type: ignore[assignment]
    inner._end_sent = True  # our `end` went out; the reopen is under way
    return w, inner


async def test_overflow_during_the_replay_marks_the_phrase_lost():
    """Review 1: a full buffer mid-replay drops an unsent frame, never the one in flight, and
    the gap is flagged (final withheld) instead of passing silently."""
    w, inner = _slow(0.05)
    size = HELD_MAX_BYTES // 10
    for i in range(10):
        await w.send_audio(f"b{i}|".encode().ljust(size))
    assert w._held_bytes == HELD_MAX_BYTES
    inner._end_sent = False  # `ready`
    replay = asyncio.create_task(w._replay_held())
    await asyncio.sleep(0)
    for i in range(5):
        await w.send_audio(f"n{i}|".encode().ljust(size))
    assert w._held_bytes <= HELD_MAX_BYTES
    inner.drain = 0
    await replay
    assert w._lost
    sent = [f.split(b"|")[0] for f in inner.wire if isinstance(f, bytes)]
    assert sent == [f"b{i}".encode() for i in range(10)] + [b"n0"]  # the new frames went


async def test_overflow_drops_the_new_frame_never_a_queued_end():
    """E-r5-3: a full buffer drops the new frame, never a queued `end` (which would merge two
    phrases), and the cut is marked on the phrase that lost it, not on the one replaying."""
    w, inner = _slow(0.01)
    third = HELD_MAX_BYTES // 3
    for i in range(3):
        await w.send_audio(f"b{i}|".encode().ljust(third))
    inner._end_sent = False
    replay = asyncio.create_task(w._replay_held())
    await asyncio.sleep(0)
    await w.send_end()  # queued behind b's frames
    for i in range(3):  # c: the first fits, the rest don't
        await w.send_audio(f"c{i}|".encode().ljust(third))
    await replay  # stops at b's replayed `end`
    assert [f if isinstance(f, str) else f.split(b"|")[0] for f in inner.wire] == [
        b"b0", b"b1", b"b2", "END"]
    assert not w._lost  # b is whole
    inner._end_sent = False  # the next `ready`
    await w._replay_held()
    assert w._lost  # c is the one cut
    assert w._held_bytes == 0


async def test_frames_after_a_replayed_end_wait_for_the_next_ready():
    """Review: audio arriving while the replayed `end` drains belongs to the next phrase: it
    is held, not written after the `end` the server no longer reads."""
    w, inner = _slow(0.01)
    for i in range(3):
        await w.send_audio(f"b{i}".encode())
    inner._end_sent = False
    replay = asyncio.create_task(w._replay_held())
    await asyncio.sleep(0)
    await w.send_end()  # queued behind the frames
    await asyncio.sleep(0.035)  # the replay is draining its `end`
    for i in range(3):
        await w.send_audio(f"c{i}".encode())
    await replay
    assert inner.wire == [b"b0", b"b1", b"b2", "END"]
    assert list(w._held) == [b"c0", b"c1", b"c2"]


class _Ws:
    def __init__(self, client: TranscriberClient, fail: bool) -> None:
        self.client, self.fail, self.end_sent_at_send = client, fail, None

    async def send(self, _data) -> None:
        self.end_sent_at_send = self.client._end_sent
        if self.fail:
            raise ConnectionClosedError(None, None)


@pytest.mark.parametrize("fail", [False, True])
async def test_end_sent_is_raised_before_the_send(fail):
    """Review 2: audio arriving while `end` drains must already see _end_sent; a failed send
    lowers it again (the phrase is reported by the wrapper)."""
    client = TranscriberClient(url="x:1", client_id="c1")
    client._is_ready = True
    client.ws = ws = _Ws(client, fail)  # type: ignore[assignment]
    await client.send_end()
    assert ws.end_sent_at_send is True
    assert client._end_sent is (not fail)
    assert client._is_ready is (not fail)


async def test_whole_phrase_in_the_gap_drops_only_itself():
    """E-r5-1: a replayed `end` stops the replay with c (whole, with its `end`) still queued;
    d then falls whole before the next `ready`. Only d is dropped and reported; c goes out."""
    w, inner = _slow(0.01)
    await w.send_audio(b"b0")
    inner._end_sent = False  # `ready`
    replay = asyncio.create_task(w._replay_held())
    await asyncio.sleep(0)
    await w.send_end()  # b's
    await w.send_audio(b"c0")
    await w.send_end()
    await w.send_audio(b"d0")
    await replay  # stops at b's `end`
    assert list(w._held) == [b"c0", None, b"d0"]
    await w.send_end()  # d's, before the next `ready`
    assert list(w._held) == [b"c0", None]
    w._callbacks.on_warning.assert_awaited_once_with("phrase_interrupted", None)
    inner._end_sent = False
    await w._replay_held()
    assert inner.wire[-2:] == [b"c0", "END"]


async def test_listen_loop_crash_drops_the_held_audio():
    """E-r5-2: a crash in the listen loop degrades for good: the held audio is dropped too."""
    w, inner = _slow(0)

    async def crash(*_a):
        raise RuntimeError("bug")

    inner._dropped_unexpectedly = False  # type: ignore[attr-defined]
    inner.listen_loop = crash  # type: ignore[attr-defined]
    w._held.extend([b"c0", None, b"d0"])
    w._held_bytes = 4
    await w.run()
    assert w.state == ConnectionState.DEGRADED
    assert not w._held and w._held_bytes == 0
