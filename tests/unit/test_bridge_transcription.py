"""TurnController.cancel, _on_transcription and close_all, seen from the session.

5.0 (#406): the barge-in left the gateway. A transcription (partial or final) goes to the
client and never cuts anything: cutting is always explicit (`cancel`, `send` with `interrupt`).
"""

import asyncio
import logging
from unittest.mock import AsyncMock, MagicMock

import pytest

from tests.fakes.bridge import GoneSender, blocking, push, sent, start_turn
from tests.fakes.orchestrator import OrchestratorEvent

_AUDIO = dict(input_mode="audio", output_mode=("audio", "text", "status"))


@pytest.fixture
def make_bridge(make_bridge):
    """A voice client whose TTS is down (no stream): these tests are about the turns."""

    def _make(**kw):
        tts = AsyncMock()
        tts.connect = AsyncMock(return_value=None)
        tts.resolve_voice = lambda v: v
        return make_bridge(tts=tts, **kw)

    return _make


def _transcriber():
    t = MagicMock()
    t.close = AsyncMock()
    return t


@pytest.fixture
async def bridge(make_bridge):
    b, _ = make_bridge(**_AUDIO, transcriber=_transcriber())
    return b


def _stream_cancelled_slowly(started: asyncio.Event, *, finally_raises: Exception | None = None):
    """An orchestrator stream that emits one token, then waits; on cancellation it takes
    a moment (or raises `finally_raises`)."""

    async def _s(*a, **k):
        yield OrchestratorEvent(type="token", content="hi")
        started.set()
        try:
            await asyncio.sleep(60)
        finally:
            await asyncio.shield(asyncio.sleep(0.05))
            if finally_raises is not None:
                raise finally_raises

    return _s


async def _running_turn(bridge, stream=None) -> asyncio.Task:
    started = asyncio.Event()
    bridge.orchestrator.stream_response = stream(started) if stream else blocking(started)
    task = await start_turn(bridge)
    await asyncio.wait_for(started.wait(), 2)
    return task


# ── TurnController.cancel ────────────────────────────────────────────────────


async def test_cancel_turn_caller_cancelled_while_waiting_is_not_swallowed(bridge):
    """#221: a caller cancelled while it waits for the turn's chat.abort (5.0 PR 2b: only
    that run's) gets its CancelledError, after the abort finished and the turn ended (its
    turn_end sent)."""
    turn = await _running_turn(bridge, _stream_cancelled_slowly)
    aborting, release = asyncio.Event(), asyncio.Event()
    abort_run = bridge.orchestrator.abort_run.side_effect

    async def slow_abort(sk, run_id):
        aborting.set()
        await release.wait()
        await abort_run(sk, run_id)

    bridge.orchestrator.abort_run.side_effect = slow_abort
    caller = asyncio.create_task(bridge.turns.cancel("cancelled"))
    await asyncio.wait_for(aborting.wait(), 2)
    caller.cancel()
    await asyncio.sleep(0.01)
    assert not caller.done(), "the abort was abandoned"
    release.set()
    await asyncio.gather(caller, return_exceptions=True)

    assert caller.cancelled()
    assert turn.done()
    assert bridge.turns.user_task is None
    assert [m["turn_id"] for m in (await sent(bridge)) if m["type"] == "turn_end"] == ["t-1"]


async def test_inherited_cancellation_does_not_kill_the_cancel_caller(bridge):
    """#221: only a cancellation received *while waiting* is re-raised. A task that
    carries an older, swallowed cancellation (cancelling() > 0) still completes the
    cancel instead of dying in it."""
    turn = await _running_turn(bridge)

    async def caller_with_inherited_cancel():
        me = asyncio.current_task()
        assert me is not None
        me.cancel()
        try:
            await asyncio.sleep(0)
        except asyncio.CancelledError:
            pass  # swallowed upstream: the counter stays at 1
        assert me.cancelling() == 1
        return await bridge.turns.cancel("interrupted")

    assert await asyncio.create_task(caller_with_inherited_cancel()) == (True, "t-1")
    assert turn.done()
    assert bridge.turns.user_task is None


async def test_cancel_turn_logs_the_abort_failure_by_type(bridge, caplog):
    """5.0 PR 2b: what can fail when a turn is cut is its run's chat.abort: logged by type,
    never by its text; the turn still ends once."""
    started = asyncio.Event()
    bridge.orchestrator.stream_response = blocking(started)
    bridge.orchestrator.abort_run.side_effect = RuntimeError("secreto del turno")
    await start_turn(bridge)
    await asyncio.wait_for(started.wait(), 2)
    with caplog.at_level(logging.DEBUG):
        assert await bridge.turns.cancel("cancelled") == (True, "t-1")
    logged = "\n".join(r.getMessage() for r in caplog.records)
    assert "RuntimeError" in logged
    assert "secreto" not in logged
    # the broken turn still got its one turn_end
    assert [m["turn_id"] for m in (await sent(bridge)) if m["type"] == "turn_end"] == ["t-1"]


async def test_cancel_with_no_turn_returns_false(bridge):
    assert await bridge.turns.cancel("cancelled") == (False, None)


async def test_cancel_after_the_turn_ended_returns_false(bridge):
    async def done(*a, **k):
        yield OrchestratorEvent(type="token", content="x")

    bridge.orchestrator.stream_response = done
    task = await start_turn(bridge)
    await asyncio.wait({task})
    await bridge.turns.idle()  # 5.0: the session's turn ends after the channel's (its audio)

    assert await bridge.turns.cancel("cancelled") == (False, None)


async def test_cancel_cancels_the_running_turn(bridge):
    turn = await _running_turn(bridge)

    assert await bridge.turns.cancel("cancelled") == (True, "t-1")

    assert turn.cancelled()
    assert bridge.turns.user_task is None
    assert bridge.turns.snapshot(0.0)["turn"]["kind"] == "idle"


# ── _on_transcription ────────────────────────────────────────────────────────


@pytest.mark.parametrize("text", ["hi", "hello world", "a" * 200])
async def test_partial_never_cancels(bridge, text):
    """#406: a partial, however long, is forwarded to the client and cuts nothing: the turn
    goes on, no turn_end, no chat.abort."""
    turn = await _running_turn(bridge)
    await sent(bridge)
    bridge.client_ws.sent.clear()

    await bridge._on_transcription(text, False)
    await asyncio.sleep(0.01)

    assert await sent(bridge) == [{"type": "transcription_partial", "text": text}]
    assert not turn.done()
    assert bridge.turns.user_task is turn
    bridge.orchestrator.abort_run.assert_not_called()
    turn.cancel()


async def test_partial_with_no_active_turn_forwarded_only(bridge):
    """A partial with no turn in flight is forwarded to the client and starts nothing."""

    await bridge._on_transcription("hello world", False)

    assert await sent(bridge) == [{"type": "transcription_partial", "text": "hello world"}]
    assert bridge.turns.user_task is None


async def test_partial_does_not_call_orchestrator(bridge):
    """Partials never call the orchestrator."""
    calls = []

    async def s(*a, **k):
        calls.append(1)
        yield OrchestratorEvent(type="token", content="x")

    bridge.orchestrator.stream_response = s

    await bridge._on_transcription("hello world", False)
    await asyncio.sleep(0.01)

    assert calls == []
    assert bridge.turns.user_task is None


async def test_final_sends_transcription_to_client(bridge):
    """Final transcription → send {"type":"transcription"} to client."""
    await bridge._on_transcription("hola mundo", True)
    await asyncio.sleep(0)

    assert await sent(bridge) == [{"type": "transcription", "text": "hola mundo"}]


async def test_final_transcription_is_not_logged_at_info(bridge, caplog):
    """#106: the bridge's own final-transcription log moved from INFO to DEBUG.

    Scope is the `fogar_gateway.services.bridge` logger only; the `pipeline_tracker` side
    (#133) is covered in `test_transcript_privacy.py`.
    """
    text = "0123456789" * 4 + "TOP-SECRET-SUFFIX"

    with caplog.at_level(logging.INFO, logger="fogar_gateway.services.bridge"):
        await bridge._on_transcription(text, True)

    bridge_info_logs = [
        r.getMessage()
        for r in caplog.records
        if r.name == "fogar_gateway.services.bridge" and r.levelno >= logging.INFO
    ]
    assert not any("Transcripción final" in m for m in bridge_info_logs)
    assert not any("TOP-SECRET-SUFFIX" in m for m in bridge_info_logs)


async def test_final_transcription_is_debug_only_and_truncated(bridge, caplog):
    text = "0123456789" * 4 + "TOP-SECRET-SUFFIX"

    with caplog.at_level(logging.DEBUG, logger="fogar_gateway.services.bridge"):
        await bridge._on_transcription(text, True)

    records = [record for record in caplog.records if "Transcripción final:" in record.getMessage()]
    assert len(records) == 1
    assert records[0].levelno == logging.DEBUG
    assert text[:40] in records[0].getMessage()
    assert text[40:] not in records[0].getMessage()


async def test_final_does_not_start_active_turn(bridge):
    """Final transcription no longer auto-dispatches to orchestrator: no turn starts —
    the client must send {"type":"send"} explicitly."""
    await bridge._on_transcription("hola", True)
    await asyncio.sleep(0)

    assert bridge.turns.user_task is None
    assert [m["type"] for m in await sent(bridge)] == ["transcription"]


async def test_final_transcription_does_not_cut_the_turn(bridge):
    """#406 (was: the final cancelled the turn in flight): the final goes to the client and
    the turn goes on; what was said joins the queue when the client sends it."""
    old_turn = await _running_turn(bridge)
    await sent(bridge)
    bridge.client_ws.sent.clear()

    await bridge._on_transcription("nueva frase", True)
    await asyncio.sleep(0.01)

    assert await sent(bridge) == [{"type": "transcription", "text": "nueva frase"}]
    assert not old_turn.done()
    assert bridge.turns.user_task is old_turn
    bridge.orchestrator.abort_run.assert_not_called()
    old_turn.cancel()


async def test_final_transcription_does_not_cut_push(make_bridge):
    """#406 (was: a final cut this session's copy of a push): an open push goes on, here and
    in its channel; the session still gets exactly one turn_end for it when it ends."""
    sk = "agent:main:hab_sito"
    bridge, ws = make_bridge(**_AUDIO, transcriber=_transcriber(), client_id="hab_sito")
    await push(bridge, ("turn_start", sk), ("chat", {"sessionKey": sk, "deltaText": "Hola"}))
    assert bridge.turns.snapshot(0.0)["turn"]["kind"] == "push"

    await bridge._on_transcription("para ya", False)
    await bridge._on_transcription("para ya", True)
    await asyncio.sleep(0.01)

    assert ws.of("turn_end") == []
    assert bridge.turns.snapshot(0.0)["turn"]["kind"] == "push"
    bridge.orchestrator.abort_run.assert_not_called()
    await push(bridge, ("turn_end", sk))
    msgs = [m for m in await sent(bridge) if isinstance(m, dict)]
    assert [m["type"] for m in msgs if m["type"] in ("turn_start", "turn_end")] == [
        "turn_start", "turn_end"
    ]
    assert ws.of("turn_end")[0]["reason"] == "done"
    assert [m["type"] for m in msgs if m["type"].startswith("transcription")] == [
        "transcription_partial", "transcription"
    ]


async def test_final_with_disconnected_client_does_not_start_turn(make_bridge):
    """If the client is gone, no new turn is started."""
    bridge, _ = make_bridge(**_AUDIO, transcriber=_transcriber(), sender=GoneSender())

    await bridge._on_transcription("hola", True)

    assert bridge.turns.user_task is None


async def test_partial_send_failure_is_silent(make_bridge):
    """If forwarding a partial fails (the client is gone), no exception propagates."""
    bridge, _ = make_bridge(**_AUDIO, transcriber=_transcriber(), sender=GoneSender())
    started = asyncio.Event()

    async def s(*a, **k):
        started.set()
        await asyncio.sleep(60)
        yield OrchestratorEvent(type="token", content="x")  # pragma: no cover

    bridge.orchestrator.stream_response = s
    task = await start_turn(bridge)  # its turn_start fails: the client is gone
    await asyncio.wait({task}, timeout=1)

    # Must not raise
    await bridge._on_transcription("hello world", False)


# ── close_all ────────────────────────────────────────────────────────────────


async def test_close_all_ends_the_active_turn(bridge):
    """close_all() cancels the turn in flight (7d) and the client still gets its turn_end."""
    turn = await _running_turn(bridge)

    await bridge.close_all()

    assert turn.done()
    assert [m["turn_id"] for m in bridge.client_ws.of("turn_end")] == ["t-1"]
    assert bridge.turns.last_ended.end_reason == "closed"


async def test_partial_before_turn_start_keeps_the_send(make_bridge):
    """#406 (was: a barge-in before turn_start withdrew the send): a delivered message whose
    run has not started stays delivered; nothing is aborted."""
    bridge, ws = make_bridge(**_AUDIO, transcriber=_transcriber())
    bridge.orchestrator.stream_response = blocking()
    await bridge.turns.start_user_turn("x")  # its run has not sent anything yet

    await bridge._on_transcription("hello world", False)
    await bridge._on_transcription("hello world", True)

    msgs = [m for m in await sent(bridge) if isinstance(m, dict)]
    assert [m["state"] for m in ws.of("message")] == ["delivered"]
    assert [m["type"] for m in msgs if m["type"].startswith("transcription")] == [
        "transcription_partial", "transcription"
    ]
    bridge.orchestrator.abort_run.assert_not_called()
    await bridge.close_all()


# ── _on_transcriber_warning ──────────────────────────────────────────────────


async def test_transcriber_warning_forwarded_to_client_without_its_raw_message(bridge):
    # Auditoría 3.0, E-3: the transcriber's free text never reaches the client, only the code.
    await bridge._on_transcriber_warning("buffer_full", "Buffer full (raw upstream text)")
    assert await sent(bridge) == [
        {
            "type": "status",
            "service": "transcriber",
            "state": "degraded",
            "code": "buffer_full",
            "message": "buffer_full",
        }
    ]


async def test_transcriber_warning_uses_code_when_no_message(bridge):
    await bridge._on_transcriber_warning("timeout", None)
    (payload,) = await sent(bridge)
    assert payload["type"] == "status"
    assert payload["state"] == "degraded"
    assert payload["message"] == "timeout"


async def test_transcriber_warning_send_failure_is_silent(make_bridge):
    bridge, _ = make_bridge(**_AUDIO, transcriber=_transcriber(), sender=GoneSender())
    await bridge._on_transcriber_warning("buffer_full", "Buffer full")
    # Must not raise
