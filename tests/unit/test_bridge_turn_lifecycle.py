"""Turn lifecycle in FogarBridge (#215, #201).

G-08 (#201) cancel race, G-09 turn created during close_all, G-10 one turn_end per
turn on every path, G-11 turn_seq wraparound, G-18 a gone client stops receiving (5.0: it no longer aborts
the channel's turn, decision 2).
The M6 exit-gate mutants live with their topic: the `_closed` guard of close_all in
test_bridge_close_all.py, the `>=` boundary of barge_in_min_chars in test_bridge_barge_in.py.
"""

import asyncio
import json
import time
from unittest.mock import AsyncMock

from fogar_gateway.core.config import settings
from fogar_gateway.services.outbound import QueuedSender
from tests.fakes.bridge import (
    RecWS,
    blocking,
    enqueue_push,
    push,
    push_reply,
    run_turn,
    sent,
    start_turn,
    tokens,
    wait_until,
)
from tests.fakes.orchestrator import OrchestratorEvent

# ── G-08 (#201) ──────────────────────────────────────────────────────────────


async def test_concurrent_cancels_do_not_lose_a_new_turn(make_bridge):
    """Two cancels race over one slow-to-cancel turn; a new turn started right after the
    first returns must survive the second (it cancels nothing)."""
    bridge, ws = make_bridge()
    gate = asyncio.Event()
    started = asyncio.Event()

    async def old(*a, **k):
        started.set()
        try:
            await asyncio.sleep(3600)
        except asyncio.CancelledError:
            await gate.wait()  # slow cancellation: both callers end up waiting on it
            raise
        yield OrchestratorEvent(type="token", content="x")  # pragma: no cover

    bridge.orchestrator.stream_response = old
    await start_turn(bridge, "one")
    await asyncio.wait_for(started.wait(), 2)
    new_task = None

    async def caller_a():
        nonlocal new_task
        result = await bridge.turns.cancel("cancelled")
        # another path starts a new turn right after A's cancel returns
        bridge.orchestrator.stream_response = blocking()
        new_task = await start_turn(bridge, "two")
        return result

    async def caller_b():
        return await bridge.turns.cancel("cancelled")

    ta = asyncio.create_task(caller_a())
    tb = asyncio.create_task(caller_b())
    await asyncio.sleep(0.02)
    gate.set()
    results = await asyncio.gather(ta, tb)

    try:
        assert sorted(r[0] for r in results) == [False, True], "the turn was cancelled twice"
        assert bridge.turns.user_task is new_task, "the new turn's reference was lost"
        assert not new_task.done()
        await wait_until(lambda: bridge.channel.current_turn is not None)
    finally:
        await bridge.turns.cancel("cancelled")  # 5.0 PR 2b: the run of "two", queued behind
        await asyncio.wait({new_task}, timeout=2)
    await sent(bridge)
    assert [m["turn_id"] for m in ws.of("turn_end")] == ["t-1", "t-2"]


# ── G-09 ─────────────────────────────────────────────────────────────────────


async def test_send_during_close_all_does_not_start_or_replace_a_turn(make_bridge, monkeypatch):
    monkeypatch.setattr(settings, "SESSION_TEARDOWN_S", 5.0)
    bridge, ws = make_bridge()
    release = asyncio.Event()
    calls = []

    async def stream(*a, **k):
        calls.append(1)
        await release.wait()
        yield OrchestratorEvent(type="token", content="x")

    bridge.orchestrator.stream_response = stream
    first = await start_turn(bridge, "one")
    await wait_until(lambda: len(calls) == 1)

    closer = asyncio.create_task(bridge.close_all())
    await asyncio.sleep(0)  # close_all has started (_closing is set)
    ws.inbox.put_nowait({"type": "websocket.message", "text": json.dumps({"type": "send", "text": "two"})})
    ws.inbox.put_nowait({"type": "websocket.disconnect"})
    await bridge._client_input_loop()
    await asyncio.wait_for(closer, timeout=2.0)

    assert len(calls) == 1, "a turn was created while the session was closing"
    # 7d (#223): closing the session's own hub closes its channel (5.0 PR 2b: its runs end
    # there); the send neither replaced the turn nor started another one.
    assert bridge.turns._last_sent is first
    assert first.cancelled()
    await sent(bridge)
    assert ws.of("turn_end") == [{"type": "turn_end", "turn_id": "t-1", "reason": "closed"}]


# ── G-10 ─────────────────────────────────────────────────────────────────────


async def test_normal_turn_sends_exactly_one_turn_end(make_bridge):
    bridge, ws = make_bridge()
    bridge.orchestrator.stream_response = tokens("a", "b")
    await run_turn(bridge)
    await sent(bridge)
    assert ws.types() == ["turn_start", "token", "token", "turn_end"]
    assert bridge.turns.last_ended.end_reason == "done"
    assert bridge.turns.last_ended.origin == "user"


async def test_runtime_error_turn_sends_error_and_one_turn_end(make_bridge):
    bridge, ws = make_bridge()

    async def s(*a, **k):
        yield OrchestratorEvent(type="error", content="orch down")

    bridge.orchestrator.stream_response = s
    await run_turn(bridge)
    await sent(bridge)
    assert ws.types() == ["turn_start", "error", "turn_end"]
    assert bridge.turns.last_ended.end_reason == "error"


async def test_unexpected_exception_turn_sends_error_and_turn_end(make_bridge):
    bridge, ws = make_bridge()

    async def s(*a, **k):
        raise ValueError("secret user text")
        yield  # pragma: no cover

    bridge.orchestrator.stream_response = s
    await run_turn(bridge)

    await sent(bridge)
    assert ws.types() == ["turn_start", "error", "turn_end"]
    err = ws.of("error")[0]
    assert err["code"] == "TURN_ERROR" and err["fatal"] is False and err["turn_id"] == "t-1"
    assert "secret" not in json.dumps(err)


async def test_cancelled_turn_sends_turn_end(make_bridge):
    bridge, ws = make_bridge()
    started = asyncio.Event()
    bridge.orchestrator.stream_response = blocking(started)
    await start_turn(bridge)
    await asyncio.wait_for(started.wait(), 2)

    assert await bridge.turns.cancel("cancelled") == (True, "t-1")

    await sent(bridge)
    assert ws.types().count("turn_end") == 1
    assert ws.of("turn_end")[0]["turn_id"] == "t-1"
    assert bridge.turns.last_ended.end_reason == "cancelled"


async def test_cancel_message_sends_turn_end(make_bridge):
    bridge, ws = make_bridge()
    started = asyncio.Event()
    bridge.orchestrator.stream_response = blocking(started)
    await start_turn(bridge)
    await asyncio.wait_for(started.wait(), 2)
    ws.inbox.put_nowait({"type": "websocket.message", "text": json.dumps({"type": "cancel"})})
    ws.inbox.put_nowait({"type": "websocket.disconnect"})

    await bridge._client_input_loop()

    await sent(bridge)
    assert ws.types().count("turn_end") == 1


async def test_send_that_interrupts_a_turn_closes_it_with_one_turn_end_each(make_bridge):
    """5.0 PR 2b: only a `send` with `interrupt` ends the turn in flight (`interrupted`); a
    plain `send` waits in OpenClaw's queue (tests/unit/test_channel_queue.py)."""
    bridge, ws = make_bridge()
    started = asyncio.Event()
    bridge.orchestrator.stream_response = blocking(started)
    await start_turn(bridge)
    await asyncio.wait_for(started.wait(), 2)
    bridge.orchestrator.stream_response = tokens("new")
    send = {"type": "send", "text": "y", "interrupt": True}
    ws.inbox.put_nowait({"type": "websocket.message", "text": json.dumps(send)})
    ws.inbox.put_nowait({"type": "websocket.disconnect"})

    await bridge._client_input_loop()
    replaced = bridge.turns.last_ended
    await asyncio.wait_for(bridge.turns.user_task, 2)
    assert (replaced.turn_id, replaced.end_reason) == ("t-1", "interrupted")

    await sent(bridge)
    ends = ws.of("turn_end")
    assert [e["turn_id"] for e in ends] == ["t-1", "t-2"]
    # t-1 closes before t-2 opens
    order = [(m["type"], m.get("turn_id")) for m in ws.sent if isinstance(m, dict)
             if m["type"] in ("turn_start", "turn_end")]
    assert order == [("turn_start", "t-1"), ("turn_end", "t-1"), ("turn_start", "t-2"), ("turn_end", "t-2")]


async def test_final_transcription_cancelling_a_turn_sends_turn_end(make_bridge):
    bridge, ws = make_bridge()
    started = asyncio.Event()
    bridge.orchestrator.stream_response = blocking(started)
    await start_turn(bridge)
    await asyncio.wait_for(started.wait(), 2)

    await bridge._on_transcription("hola que tal", True)

    await sent(bridge)
    assert ws.types().count("turn_end") == 1
    assert bridge.turns.last_ended.end_reason == "interrupted"


async def test_barge_in_ends_the_turn_with_reason_interrupted(make_bridge):
    bridge, ws = make_bridge()
    started = asyncio.Event()
    bridge.orchestrator.stream_response = blocking(started)
    await start_turn(bridge)
    await asyncio.wait_for(started.wait(), 2)

    await bridge._on_transcription("hola que tal", False)
    assert bridge.turns.last_ended.end_reason == "interrupted"

    await sent(bridge)
    # M19: no `interrupted` message any more; the turn_end says why.
    assert ws.of("interrupted") == []
    assert ws.of("turn_end") == [{"type": "turn_end", "turn_id": "t-1", "reason": "interrupted"}]


# ── G-11 ─────────────────────────────────────────────────────────────────────


def _tts_with_one_chunk():
    tts_client = AsyncMock()

    async def audio():
        yield b"\x01\x02"

    tts_client.get_audio_stream = audio
    tts = AsyncMock()
    tts.connect = AsyncMock(return_value=tts_client)
    return tts


async def test_turn_seq_wraps_in_audio_header_instead_of_overflowing(make_bridge):
    bridge, ws = make_bridge(output_mode=("text", "audio"), tts=_tts_with_one_chunk())
    bridge.orchestrator.stream_response = tokens("a")
    bridge.turns._seq = 65535  # no public way to get there short of 65 535 turns

    await run_turn(bridge)

    await sent(bridge)
    frames = [m for m in ws.sent if isinstance(m, bytes)]
    assert frames == [b"\xa1\x00\x00\x01\x02"]
    start = ws.of("turn_start")[0]
    assert start["turn_id"] == "t-65536" and start["turn_seq"] == 0  # same value as the header


async def test_push_turn_seq_wraps_in_audio_header(make_bridge):
    bridge, ws = make_bridge(output_mode=("text", "audio"), tts=_tts_with_one_chunk())
    bridge.turns._seq = 65535

    await push(bridge, ("turn_start", "sk"))
    await wait_until(lambda: any(isinstance(m, bytes) for m in ws.sent))

    await sent(bridge)
    frames = [m for m in ws.sent if isinstance(m, bytes)]
    assert frames == [b"\xa1\x00\x00\x01\x02"]
    assert ws.of("turn_start")[0]["turn_seq"] == 0
    await bridge.close_all()


# ── G-18 ─────────────────────────────────────────────────────────────────────


async def test_disconnected_client_cancels_its_turn_instead_of_draining(make_bridge, monkeypatch):
    monkeypatch.setattr(settings, "SESSION_TEARDOWN_S", 30.0)
    bridge, ws = make_bridge()
    started = asyncio.Event()
    closed = asyncio.Event()

    async def s(*a, **k):
        try:
            yield OrchestratorEvent(type="token", content="hi")
            started.set()
            await asyncio.sleep(3600)
        finally:
            closed.set()

    bridge.orchestrator.stream_response = s
    await start_turn(bridge)
    await asyncio.wait_for(started.wait(), 2)
    ws.inbox.put_nowait({"type": "websocket.disconnect"})

    await bridge._client_input_loop()
    t0 = time.monotonic()
    await asyncio.wait_for(bridge.close_all(), timeout=3.0)

    assert time.monotonic() - t0 < 2.0
    assert closed.is_set(), "the orchestrator stream was left open"


async def test_a_gone_client_does_not_abort_the_channel_turn(make_bridge):
    """Decision 2 of the 5.0 PR 2 plan (G-18 inverted): the turn belongs to the channel, so a
    client that is gone stops receiving it but never cancels its run; its own turn ends
    `closed` when the session closes (here its private hub then ends the run)."""
    failing_ws = AsyncMock()
    sends = []

    async def send_json(payload):
        # 5.0 PR 2b: the `message` echo and the turn_start get through; then the socket breaks.
        sends.append(payload["type"])
        if len(sends) > 2:
            raise RuntimeError("socket closed")

    failing_ws.send_json = AsyncMock(side_effect=send_json)
    failing_ws.send_bytes = AsyncMock(side_effect=RuntimeError("socket closed"))
    sender = QueuedSender(failing_ws)
    bridge, _ = make_bridge(sender=sender)
    closed = asyncio.Event()

    async def s(*a, **k):
        try:
            while True:
                yield OrchestratorEvent(type="token", content="t")
                await asyncio.sleep(0.01)
        finally:
            closed.set()

    bridge.orchestrator.stream_response = s
    turn = await start_turn(bridge)
    try:
        await wait_until(lambda: bridge.turns.client_gone)
        await asyncio.sleep(0.1)
        assert not turn.done() and not closed.is_set(), "a gone client aborted the channel's run"
        await asyncio.wait_for(bridge.close_all(), 3.0)
    finally:
        await sender.aclose(1.0)
    assert closed.is_set()
    assert bridge.turns.last_ended.end_reason == "closed"


async def test_close_all_starting_while_a_send_waits_for_the_cancel_leaves_no_turn(make_bridge, monkeypatch):
    """G-09 (review): `_start_turn` must re-check `_closing` after awaiting the
    cancellation of the previous turn, inside the lock."""
    monkeypatch.setattr(settings, "SESSION_TEARDOWN_S", 5.0)
    bridge, _ = make_bridge()
    gate = asyncio.Event()
    new_turns = []

    async def old():
        try:
            await asyncio.sleep(3600)
        except asyncio.CancelledError:
            await gate.wait()  # slow cancellation
            raise

    async def stream(*a, **k):
        new_turns.append(1)
        await asyncio.sleep(3600)
        yield OrchestratorEvent(type="token", content="x")

    entered = asyncio.Event()

    async def slow(*a, **k):
        entered.set()
        await old()
        yield OrchestratorEvent(type="token", content="x")  # pragma: no cover

    bridge.orchestrator.stream_response = slow
    await start_turn(bridge, "one")
    await asyncio.wait_for(entered.wait(), 2)
    bridge.orchestrator.stream_response = stream

    starter = asyncio.create_task(bridge.turns.start_user_turn("two"))
    await asyncio.sleep(0.02)  # start_user_turn is now awaiting the old turn's cancellation
    closer = asyncio.create_task(bridge.close_all())
    await asyncio.sleep(0.02)  # close_all has begun
    gate.set()
    await asyncio.wait_for(asyncio.gather(starter, closer), timeout=3.0)

    task = bridge.turns.user_task
    live = task is not None and not task.done()
    if live:
        task.cancel()
    assert not live, "a turn is alive after close_all() completed"
    assert new_turns == []


# ── #286 / #271: the push is a turn like any other ──────────────────────────


async def test_a_push_without_progress_is_closed_with_turn_timeout(make_bridge, monkeypatch):
    monkeypatch.setattr(settings, "TURN_TIMEOUT_S", 0.1)
    bridge, ws = make_bridge(client_id="hab_sito")
    sk = bridge._session_key()
    for kind, arg in push_reply("primer aviso", sk=sk, end=False):
        enqueue_push(bridge, kind, arg, run_id="run-p")
    await wait_until(lambda: bool(ws.of("turn_end")))

    msgs = [m for m in await sent(bridge) if isinstance(m, dict)]
    assert [m["type"] for m in msgs] == ["turn_start", "token", "error", "turn_end"]
    assert msgs[2] == {
        "type": "error", "code": "TURN_ERROR", "message": "turn_timeout", "fatal": False, "turn_id": "t-1",
    }  # fmt: skip
    assert bridge.turns.last_ended.end_reason == "timeout"
    await wait_until(lambda: bridge.orchestrator.abort_run.await_count == 1)
    bridge.orchestrator.abort_run.assert_awaited_once_with(sk, "run-p")  # only its run

    # the next push opens its own turn
    await push(bridge, *push_reply("segundo", sk=sk))
    await sent(bridge)
    assert [m["turn_id"] for m in ws.of("turn_start")] == ["t-1", "t-2"]
    assert [m["text"] for m in ws.of("token") if m["turn_id"] == "t-2"] == ["segundo"]
    assert [m["turn_id"] for m in ws.of("turn_end")] == ["t-1", "t-2"]
    await bridge.close_all()


async def test_push_events_reset_the_push_budget(make_bridge, monkeypatch):
    """TURN_TIMEOUT_S is idle-reset for the push too: a slow but live push is not cut."""
    monkeypatch.setattr(settings, "TURN_TIMEOUT_S", 0.4)
    bridge, ws = make_bridge()
    await push(bridge, ("turn_start", "sk"))
    for d in "abcd":
        await asyncio.sleep(0.15)
        await push(bridge, ("chat", {"deltaText": d}))
    await push(bridge, ("turn_end", "sk"))

    await sent(bridge)
    assert ws.of("error") == []
    assert [m["text"] for m in ws.of("token")] == list("abcd")
    assert bridge.turns.last_ended.end_reason == "done"
    await bridge.close_all()


async def test_failed_push_ends_with_reason_error(make_bridge):
    bridge, ws = make_bridge()
    await push(bridge, ("turn_start", "sk"), ("turn_error", {"phase": "error"}))
    await sent(bridge)
    # #397: su TURN_ERROR (sin señal estructural, el genérico) y luego su turn_end.
    assert ws.types() == ["turn_start", "error", "turn_end"]
    assert ws.of("error")[0]["message"] == "orchestrator_error"
    assert bridge.turns.last_ended.origin == "push"
    assert bridge.turns.last_ended.end_reason == "error"
    await bridge.close_all()


async def test_an_interrupting_send_over_a_push_ends_it_with_reason_interrupted(make_bridge):
    """5.0 PR 2b: «send now» cuts the open push (only its run is aborted, by its runId) before
    its chat.send; a plain `send` waits for it (tests/unit/test_channel_queue.py)."""
    bridge, ws = make_bridge(client_id="hab_sito")
    sk = bridge._session_key()
    for kind, arg in push_reply("aviso", sk=sk, end=False):
        enqueue_push(bridge, kind, arg, run_id="run-p")
    await bridge.push_idle()
    bridge.orchestrator.stream_response = tokens("hola")
    await bridge.turns.start_user_turn("hola", interrupt=True)
    replaced = bridge.turns.last_ended
    await asyncio.wait_for(bridge.turns.user_task, 2)

    await sent(bridge)
    order = [(m["type"], m.get("turn_id")) for m in ws.sent if m["type"] in ("turn_start", "turn_end")]
    assert order == [("turn_start", "t-1"), ("turn_end", "t-1"), ("turn_start", "t-2"), ("turn_end", "t-2")]
    assert (replaced.origin, replaced.end_reason) == ("push", "interrupted")
    bridge.orchestrator.abort_run.assert_awaited_once_with(sk, "run-p")
    await bridge.close_all()


async def test_cutting_a_push_without_run_id_aborts_nothing(make_bridge, caplog):
    """5.0 PR 2b: a push without a runId is cut here only: aborting the whole key would take
    the queued messages with it."""
    bridge, ws = make_bridge(client_id="hab_sito")
    sk = bridge._session_key()
    await push(bridge, *push_reply("aviso", sk=sk, end=False))
    assert await bridge.turns.cancel("cancelled") == (True, "t-1")
    await sent(bridge)
    assert ws.of("turn_end")[0]["reason"] == "cancelled"
    bridge.orchestrator.abort_run.assert_not_awaited()
    assert "sin runId" in caplog.text
    await bridge.close_all()


# ── Review of a5ed08f ────────────────────────────────────────────────────────


def _audio_provider(stream):
    provider = AsyncMock()
    provider.connect = AsyncMock(return_value=stream)
    provider.resolve_voice = lambda v: v
    return provider


async def test_a_send_during_the_push_timeout_cut_waits_for_its_abort(make_bridge, monkeypatch):
    """G-08: the timeout cut runs under the turn lock like every other cut, so the user's
    chat.send never goes out before the push's chat.abort (and the abort is not lost)."""
    monkeypatch.setattr(settings, "TURN_TIMEOUT_S", 0.1)
    closing, release = asyncio.Event(), asyncio.Event()
    stream = AsyncMock()

    async def audio():
        await asyncio.sleep(3600)
        yield b""  # pragma: no cover

    async def slow_close():
        closing.set()
        await release.wait()

    stream.get_audio_stream = audio
    stream.close = slow_close
    provider = _audio_provider(stream)
    provider.connect = AsyncMock(side_effect=[stream, None])  # the user turn: TTS down
    bridge, ws = make_bridge(client_id="hab_sito", output_mode=("audio", "text"), tts=provider)
    order = []
    abort_run = bridge.orchestrator.abort_run.side_effect

    async def abort(sk, run_id):
        order.append("abort")
        await abort_run(sk, run_id)

    bridge.orchestrator.abort_run.side_effect = abort

    async def user_stream(*a, **k):
        order.append("send")
        yield OrchestratorEvent(type="token", content="hola")

    bridge.orchestrator.stream_response = user_stream
    enqueue_push(bridge, "turn_start", bridge._session_key(), run_id="run-p")
    await asyncio.wait_for(closing.wait(), 2)  # the timeout cut is releasing the push TTS

    starter = asyncio.create_task(bridge.turns.start_user_turn("hola"))
    await asyncio.sleep(0.05)
    release.set()
    await asyncio.wait_for(starter, 2)
    await asyncio.wait_for(bridge.turns.user_task, 2)

    assert order == ["abort", "send"]
    await sent(bridge)
    frames = [(m["type"], m["turn_id"]) for m in ws.of("turn_start") + ws.of("turn_end")]
    assert sorted(frames) == [("turn_end", "t-1"), ("turn_end", "t-2"), ("turn_start", "t-1"), ("turn_start", "t-2")]
    await bridge.close_all()


async def test_a_second_cancellation_during_tts_release_still_closes_the_stream(make_bridge):
    """#221: a barge-in cuts the turn; close_all cancels the session's worker while that
    turn's TTS is being released. The stream is still closed, and one turn_end goes out.
    (5.0: the release is the session's, not the channel task's: decision 1.)"""
    cancelling = asyncio.Event()
    stream = AsyncMock()

    async def audio():
        try:
            await asyncio.sleep(3600)
            yield b""  # pragma: no cover
        except asyncio.CancelledError:
            cancelling.set()
            await asyncio.shield(asyncio.sleep(0.1))  # slow to stop
            raise

    stream.get_audio_stream = audio
    bridge, ws = make_bridge(output_mode=("audio", "text"), tts=_audio_provider(stream))
    started = asyncio.Event()
    bridge.orchestrator.stream_response = blocking(started)
    task = await start_turn(bridge)
    await asyncio.wait_for(started.wait(), 2)

    assert (await bridge.turns.cancel("interrupted"))[0]  # barge-in
    await asyncio.wait_for(cancelling.wait(), 2)  # the release waits for the audio
    await asyncio.wait_for(bridge.close_all(), 2)  # close_all

    assert task.cancelled()  # the barge-in cut its run
    stream.close.assert_awaited_once()
    assert [m["turn_id"] for m in ws.of("turn_end")] == ["t-1"]


class _YieldingSender:
    """A sender that yields before enqueueing (a real socket write would)."""

    def __init__(self, inner):
        self._inner = inner

    async def send_json(self, payload):
        await asyncio.sleep(0.01)
        await self._inner.send_json(payload)

    async def send_bytes(self, data):
        await asyncio.sleep(0.01)
        await self._inner.send_bytes(data)

    async def flush(self):
        await self._inner.flush()

    async def aclose(self, timeout):
        await self._inner.aclose(timeout)

    def queue_stats(self):
        return self._inner.queue_stats()

    def restrict(self, allowed):
        self._inner.restrict(allowed)


async def test_a_send_while_a_push_opens_never_leaves_two_turns_open(make_bridge):
    """#202: the push opens whole (synchronously, in the channel's worker); an interrupting
    `send` that arrives while its turn_start is being written cuts it, and its own turn opens
    on this socket only after the push's turn_end."""
    ws = RecWS()
    bridge, _ = make_bridge(ws, client_id="hab_sito", sender=_YieldingSender(QueuedSender(ws)))
    bridge.orchestrator.stream_response = tokens("hola")
    enqueue_push(bridge, "turn_start", bridge._session_key())
    await asyncio.sleep(0.003)  # the worker is writing the push's turn_start
    await bridge.turns.start_user_turn("hola", interrupt=True)
    await asyncio.wait_for(bridge.turns.user_task, 2)
    await bridge.push_idle()

    await sent(bridge)
    frames = [(m["type"], m["turn_id"]) for m in ws.sent if m["type"] in ("turn_start", "turn_end")]
    assert frames == [("turn_start", "t-1"), ("turn_end", "t-1"), ("turn_start", "t-2"), ("turn_end", "t-2")]
    await bridge.close_all()


async def test_snapshot_since_is_the_running_turn_s_own(make_bridge):
    bridge, _ = make_bridge()
    bridge.orchestrator.stream_response = tokens("a")
    await run_turn(bridge)
    await asyncio.sleep(0.3)
    bridge.orchestrator.stream_response = blocking()
    assert bridge.turns.snapshot(time.monotonic())["turn"]["kind"] == "idle"
    task = await start_turn(bridge)  # 5.0 PR 2b: in flight from its ack (its turn not open yet)
    assert bridge.turns.snapshot(time.monotonic())["turn"]["kind"] == "normal"
    await wait_until(lambda: bridge.channel.current_turn is not None)
    await bridge.turns.idle()

    turn = bridge.turns.snapshot(time.monotonic())["turn"]
    assert turn["kind"] == "normal" and turn["since_s"] < 0.2
    await bridge.turns.cancel("cancelled")
    assert task.cancelled()



async def test_a_push_cut_by_an_interrupting_send_reports_no_timeout(make_bridge, monkeypatch):
    """Review 1: a push cut by a `send` that interrupts (5.0 PR 2b) never times out after:
    the client never sees a turn_timeout that did not happen. (The cut and the timeout are
    both synchronous in the channel: the first one wins.)"""
    from fogar_gateway.services.stats import gateway_stats

    monkeypatch.setattr(settings, "TURN_TIMEOUT_S", 0.1)
    bridge, ws = make_bridge(client_id="hab_sito")
    bridge.orchestrator.stream_response = tokens("hola")
    await push(bridge, ("turn_start", bridge._session_key()))
    await bridge.turns.start_user_turn("hola", interrupt=True)
    await asyncio.sleep(0.25)  # the push's budget would have run out meanwhile
    await asyncio.wait_for(bridge.turns.user_task, 2)
    await bridge.push_idle()

    await sent(bridge)
    assert ws.of("error") == []
    assert "turn_timeout" not in gateway_stats.snapshot()["turn_errors"]
    frames = [(m["type"], m["turn_id"]) for m in ws.sent if m["type"] in ("turn_start", "turn_end")]
    assert frames == [("turn_start", "t-1"), ("turn_end", "t-1"), ("turn_start", "t-2"), ("turn_end", "t-2")]
    await bridge.close_all()


async def test_cancelling_a_cut_still_releases_the_push_tts(make_bridge):
    """Review 8: the push is cut and the session is closed (its worker cancelled) while the
    push TTS is being dropped; the stream is still closed, its push_tts recorded and its
    turn_end sent. (5.0, decision 1: the cut returns without waiting for the TTS.)"""
    cancelling = asyncio.Event()
    stream = AsyncMock()

    async def audio():
        try:
            await asyncio.sleep(3600)
            yield b""  # pragma: no cover
        except asyncio.CancelledError:
            cancelling.set()
            await asyncio.shield(asyncio.sleep(0.1))  # slow to stop
            raise

    stream.get_audio_stream = audio
    bridge, ws = make_bridge(client_id="hab_sito", output_mode=("audio", "text"), tts=_audio_provider(stream))
    await push(bridge, ("turn_start", bridge._session_key()))
    assert await asyncio.wait_for(bridge.turns.cancel("cancelled"), 2) == (True, "t-1")
    await asyncio.wait_for(cancelling.wait(), 2)
    await asyncio.wait_for(bridge.close_all(), 2)

    stream.close.assert_awaited_once()
    assert [c for c in bridge.tracker.record.await_args_list if c.args[0] == "push_tts"]
    assert [m["turn_id"] for m in ws.of("turn_end")] == ["t-1"]
