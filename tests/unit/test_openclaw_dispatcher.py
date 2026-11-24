import asyncio
import logging
from unittest.mock import AsyncMock, MagicMock

import pytest

from fogar_gateway.core.config import settings
from fogar_gateway.models.schemas import ClientConfig
from fogar_gateway.services.bridge import FogarBridge
from fogar_gateway.services.openclaw.dispatcher import FrameDispatcher
from fogar_gateway.services.openclaw.registry import TurnRegistry
from fogar_gateway.services.reconnection import ConnectionState, ServiceStatus
from tests.fakes.bridge import keyed_conversation, push, sent


def push_open(bridge) -> bool:
    return bridge.turns.snapshot(0.0)["turn"]["kind"] == "push"


class FakeChannels:
    """What the dispatcher asks of `ChannelHub` (5.0, #406): the channel of a push's
    `sessionKey`, only for an indexed conversation. `register` stands for a session attached
    to its conversation's channel: a real bridge maps to its channel, a mock to itself (its
    `enqueue_push` is what a channel's would get)."""

    def __init__(self):
        self._by_key = {}

    def register(self, client_id, target, conversation):
        self._by_key[conversation.session_key] = (
            target.channel if isinstance(target, FogarBridge) else target
        )

    def channel_for(self, session_key):
        return self._by_key.get(session_key)


def make_dispatcher():
    turn_reg = TurnRegistry()
    client_reg = FakeChannels()
    dispatcher = FrameDispatcher(turn_reg, client_reg)
    return dispatcher, turn_reg, client_reg


def _connected_status() -> ServiceStatus:
    return ServiceStatus(
        name="tts",
        state=ConnectionState.CONNECTED,
        connected_at=None,
        reconnect_attempts=0,
        last_error=None,
    )


@pytest.fixture
def make_real_bridge(make_bridge):
    """A real FogarBridge (not AsyncMock) so dispatcher tests can exercise the push
    gating (issue #109)."""

    def _make(push_enabled=True, tool_calls_enabled=False):
        config = ClientConfig(push_enabled=push_enabled, tool_calls_enabled=tool_calls_enabled)
        return make_bridge(client_id="hab_sito", config=config)

    return _make


@pytest.mark.asyncio
async def test_res_unknown_req_id_is_ignored():
    dispatcher, turn_reg, _ = make_dispatcher()
    frame = {"type": "res", "id": "unknown-id", "ok": True, "payload": {}}
    await dispatcher.dispatch(frame)  # must not raise


@pytest.mark.asyncio
async def test_chat_event_no_active_turn_routes_to_client_registry():
    dispatcher, turn_reg, client_reg = make_dispatcher()
    bridge = MagicMock()
    client_reg.register("client-a", bridge, keyed_conversation("client-a"))
    payload = {
        "sessionKey": "agent:main:client-a",
        "runId": "r1",
        "seq": 1,
        "deltaText": "Push!",
    }
    frame = {"type": "event", "event": "chat", "payload": payload}
    await dispatcher.dispatch(frame)
    bridge.enqueue_push.assert_called_once_with("chat", payload, run_id="r1")


@pytest.mark.asyncio
async def test_chat_event_no_session_key_ignored():
    dispatcher, turn_reg, _ = make_dispatcher()
    frame = {"type": "event", "event": "chat", "payload": {"deltaText": "no key"}}
    await dispatcher.dispatch(frame)  # must not raise


@pytest.mark.asyncio
async def test_agent_lifecycle_start_calls_on_push_turn_start():
    dispatcher, _, client_reg = make_dispatcher()
    bridge = MagicMock()
    client_reg.register("client-a", bridge, keyed_conversation("client-a"))
    payload = {
        "sessionKey": "agent:main:client-a",
        "runId": "r1",
        "seq": 1,
        "data": {"phase": "start", "startedAt": 123},
    }
    frame = {"type": "event", "event": "agent", "payload": payload}
    await dispatcher.dispatch(frame)
    bridge.enqueue_push.assert_called_once_with("turn_start", "agent:main:client-a", run_id="r1")


@pytest.mark.asyncio
async def test_agent_lifecycle_end_calls_on_push_turn_end():
    dispatcher, _, client_reg = make_dispatcher()
    bridge = MagicMock()
    client_reg.register("client-a", bridge, keyed_conversation("client-a"))
    payload = {
        "sessionKey": "agent:main:client-a",
        "runId": "r1",
        "seq": 2,
        "data": {"phase": "end", "stopReason": "stop"},
    }
    frame = {"type": "event", "event": "agent", "payload": payload}
    await dispatcher.dispatch(frame)
    bridge.enqueue_push.assert_called_once_with("turn_end", "agent:main:client-a", run_id="r1")


@pytest.mark.asyncio
async def test_health_and_tick_ignored():
    dispatcher, _, _ = make_dispatcher()
    for event_name in ("health", "tick", "presence", "sessions.changed"):
        frame = {"type": "event", "event": event_name, "payload": {}}
        await dispatcher.dispatch(frame)  # must not raise


@pytest.mark.asyncio
async def test_session_tool_update_phase_is_dropped():
    dispatcher, _, client_reg = make_dispatcher()
    channel = MagicMock()
    client_reg.register("client-a", channel, keyed_conversation("client-a"))
    payload = {
        "sessionKey": "agent:main:client-a",
        "data": {"phase": "update", "name": "exec", "toolCallId": "call-1", "partialResult": {}},
    }
    frame = {"type": "event", "event": "session.tool", "payload": payload}
    await dispatcher.dispatch(frame)
    assert not channel.take_frame.called and not channel.enqueue_push.called


@pytest.mark.asyncio
async def test_session_tool_no_active_turn_routes_to_client_registry():
    dispatcher, turn_reg, client_reg = make_dispatcher()
    bridge = MagicMock()
    client_reg.register("client-a", bridge, keyed_conversation("client-a"))
    payload = {
        "sessionKey": "agent:main:client-a",
        "data": {"phase": "start", "name": "exec", "toolCallId": "call-1", "args": {}},
    }
    frame = {"type": "event", "event": "session.tool", "payload": payload}
    await dispatcher.dispatch(frame)
    bridge.enqueue_push.assert_called_once_with("tool", payload["data"], run_id=None)


@pytest.mark.asyncio
async def test_session_tool_no_session_key_ignored():
    dispatcher, _, _ = make_dispatcher()
    frame = {
        "type": "event",
        "event": "session.tool",
        "payload": {"data": {"phase": "start", "name": "exec", "toolCallId": "call-1"}},
    }
    await dispatcher.dispatch(frame)  # must not raise


@pytest.mark.asyncio
async def test_dispatcher_chat_event_push_disabled_client_receives_nothing(make_real_bridge):
    """#109: a chat push event routed through the real dispatcher→bridge
    wiring for a push_enabled=False client, with NO turn_start ever sent,
    must not reach the client — the acceptance criterion verbatim."""
    dispatcher, _, client_reg = make_dispatcher()
    bridge, _ = make_real_bridge(push_enabled=False)
    client_reg.register("hab_sito", bridge, keyed_conversation("hab_sito"))

    payload = {"sessionKey": "agent:main:hab_sito", "runId": "r1", "seq": 1, "deltaText": "Push!"}
    frame = {"type": "event", "event": "chat", "payload": payload}
    await dispatcher.dispatch(frame)
    await bridge.push_idle()

    assert await sent(bridge) == []
    await bridge.close_all()


@pytest.mark.asyncio
async def test_dispatcher_session_tool_push_disabled_client_receives_nothing(make_real_bridge):
    dispatcher, _, client_reg = make_dispatcher()
    bridge, _ = make_real_bridge(push_enabled=False, tool_calls_enabled=True)
    client_reg.register("hab_sito", bridge, keyed_conversation("hab_sito"))

    payload = {
        "sessionKey": "agent:main:hab_sito",
        "data": {
            "phase": "start",
            "name": "exec",
            "toolCallId": "call-1",
            "args": {"command": "ls"},
        },
    }
    frame = {"type": "event", "event": "session.tool", "payload": payload}
    await dispatcher.dispatch(frame)
    await bridge.push_idle()

    assert await sent(bridge) == []
    await bridge.close_all()


@pytest.mark.asyncio
async def test_dispatcher_full_push_lifecycle_disabled_client_receives_nothing(make_real_bridge):
    """Full agent start → chat → tool → agent end sequence through the real
    dispatcher for a push_enabled=False client: zero frames at any point,
    not just the first one — on_push_turn_start's own gate (Task-1-independent,
    predates #109) already no-ops on start; this confirms deliver_push and
    deliver_push_tool_call independently hold the line for whatever arrives
    in between, and that nothing leaks around the (correctly) skipped
    turn_start/turn_end pair."""
    dispatcher, _, client_reg = make_dispatcher()
    bridge, _ = make_real_bridge(push_enabled=False, tool_calls_enabled=True)
    client_reg.register("hab_sito", bridge, keyed_conversation("hab_sito"))

    await dispatcher.dispatch(
        {
            "type": "event",
            "event": "agent",
            "payload": {"sessionKey": "agent:main:hab_sito", "data": {"phase": "start"}},
        }
    )
    await dispatcher.dispatch(
        {
            "type": "event",
            "event": "chat",
            "payload": {"sessionKey": "agent:main:hab_sito", "deltaText": "Push!"},
        }
    )
    await dispatcher.dispatch(
        {
            "type": "event",
            "event": "session.tool",
            "payload": {
                "sessionKey": "agent:main:hab_sito",
                "data": {"phase": "start", "name": "exec", "toolCallId": "call-1", "args": {}},
            },
        }
    )
    await dispatcher.dispatch(
        {
            "type": "event",
            "event": "agent",
            "payload": {"sessionKey": "agent:main:hab_sito", "data": {"phase": "end"}},
        }
    )

    await bridge.push_idle()

    assert await sent(bridge) == []
    assert not push_open(bridge)
    await bridge.close_all()


@pytest.mark.asyncio
async def test_dispatch_agent_end_does_not_wait_for_slow_tts_drain(make_bridge, monkeypatch):
    """#130 S3a — el motivo de todo el cambio: un push cuyo audio TTS tarda en
    drenar no debe retener a dispatch() (y por tanto a _listen y al resto de
    sesiones multiplexadas)."""
    # Pinned so the proof does not depend on the default drain timeout.
    monkeypatch.setattr(settings, "PUSH_TTS_DRAIN_TIMEOUT_S", 30)
    dispatcher, _, client_reg = make_dispatcher()
    audio_cancelled = asyncio.Event()

    async def stuck_audio():
        try:
            await asyncio.sleep(3600)
        except asyncio.CancelledError:
            audio_cancelled.set()
            raise
        yield b""  # pragma: no cover

    stream = AsyncMock()
    stream.get_audio_stream = stuck_audio
    tts = AsyncMock()
    tts.connect = AsyncMock(return_value=stream)
    bridge, _ = make_bridge(client_id="hab_sito", output_mode=("audio", "text"), tts=tts)
    client_reg.register("hab_sito", bridge, keyed_conversation("hab_sito"))
    await push(bridge, ("turn_start", "agent:main:hab_sito"))

    await asyncio.wait_for(
        dispatcher.dispatch(
            {
                "type": "event",
                "event": "agent",
                "payload": {"sessionKey": "agent:main:hab_sito", "data": {"phase": "end"}},
            }
        ),
        timeout=0.5,
    )
    # What is proven: dispatch() returned within the 0.5s wait_for although the
    # drain it enqueued would take PUSH_TTS_DRAIN_TIMEOUT_S (30s) — dispatch did
    # not wait for the handler. (pending == 1 only shows the event is queued.)
    assert bridge.turns.snapshot(0.0)["push_backlog"]["pending"] == 1

    # Cleanup: close_all() must not hang on the stuck audio task.
    await asyncio.wait_for(bridge.close_all(), timeout=2.0)
    assert audio_cancelled.is_set()


# --- Issue #136: unknown frames/events must not vanish silently -------------


async def test_unknown_event_type_is_logged_once_at_info(caplog):
    import logging

    dispatcher, _, _ = make_dispatcher()
    frame = {"type": "event", "event": "brand.new.event", "payload": {"secret": "x"}}

    with caplog.at_level(logging.DEBUG, logger="fogar_gateway.services.openclaw.dispatcher"):
        await dispatcher.dispatch(frame)
        await dispatcher.dispatch(frame)

    infos = [r for r in caplog.records if r.levelno == logging.INFO]
    assert len(infos) == 1
    assert "brand.new.event" in infos[0].getMessage()
    # payload content must never reach the logs
    assert "secret" not in caplog.text


async def test_unknown_frame_type_is_logged_once_at_info(caplog):
    import logging

    dispatcher, _, _ = make_dispatcher()

    with caplog.at_level(logging.DEBUG, logger="fogar_gateway.services.openclaw.dispatcher"):
        await dispatcher.dispatch({"type": "weird"})
        await dispatcher.dispatch({"type": "weird"})

    infos = [r for r in caplog.records if r.levelno == logging.INFO]
    assert len(infos) == 1
    assert "weird" in infos[0].getMessage()


async def test_known_events_are_not_reported_as_unknown(caplog):
    import logging

    dispatcher, _, _ = make_dispatcher()

    with caplog.at_level(logging.DEBUG, logger="fogar_gateway.services.openclaw.dispatcher"):
        await dispatcher.dispatch({"type": "event", "event": "chat", "payload": {}})
        await dispatcher.dispatch({"type": "event", "event": "agent", "payload": {}})
        await dispatcher.dispatch({"type": "event", "event": "session.tool", "payload": {}})
        await dispatcher.dispatch({"type": "res", "id": "x", "payload": {}})

    assert [r for r in caplog.records if r.levelno >= logging.INFO] == []


# --- #285: frames of a run the gateway aborted are dropped by runId ----------

SK = "agent:main:client-a"


def _ack(req_id, run_id):
    return {"type": "res", "id": req_id, "ok": True, "payload": {"runId": run_id, "status": "started"}}


def _chat(run_id, state="delta", text="late"):
    payload = {"sessionKey": SK, "runId": run_id, "state": state, "deltaText": text}
    return {"type": "event", "event": "chat", "payload": payload}


def _agent(run_id, phase):
    payload = {"sessionKey": SK, "runId": run_id, "data": {"phase": phase}}
    return {"type": "event", "event": "agent", "payload": payload}


def _tool(run_id):
    data = {"phase": "start", "name": "exec", "toolCallId": "c1", "args": {}}
    return {"type": "event", "event": "session.tool", "payload": {"sessionKey": SK, "runId": run_id, "data": data}}


async def test_chat_abort_answer_tombstones_the_runs_it_aborted():
    dispatcher, turn_reg, client_reg = make_dispatcher()
    bridge = MagicMock()
    client_reg.register("client-a", bridge, keyed_conversation("client-a"))
    res = {"type": "res", "id": "abort-1", "ok": True, "payload": {"ok": True, "aborted": True, "runIds": ["run-9"]}}
    await dispatcher.dispatch(res)
    await dispatcher.dispatch(_agent("run-9", "start"))
    bridge.enqueue_push.assert_not_called()
    assert turn_reg.is_run_aborted("run-9")
    assert not turn_reg.is_run_aborted("run-8")
    assert not turn_reg.is_run_aborted(None)


async def test_run_tombstone_expires(monkeypatch):
    import fogar_gateway.services.openclaw.registry as registry

    turn_reg = TurnRegistry()
    now = [1000.0]
    monkeypatch.setattr(registry.time, "monotonic", lambda: now[0])
    turn_reg.abort_run("run-1")
    now[0] += registry.RUN_ABORT_TOMBSTONE_S - 1
    assert turn_reg.is_run_aborted("run-1")
    now[0] += 2
    assert not turn_reg.is_run_aborted("run-1")


async def test_agent_error_phase_is_forwarded_as_turn_error():
    """#300: a failed push run ends with `error`; the bridge closes it with reason error."""
    dispatcher, _, client_reg = make_dispatcher()
    bridge = MagicMock()
    client_reg.register("client-a", bridge, keyed_conversation("client-a"))
    await dispatcher.dispatch(_agent("run-1", "error"))
    # #397: with its `data`, which the push classifies into a stable code.
    bridge.enqueue_push.assert_called_once_with("turn_error", {"phase": "error"}, run_id="run-1")


async def test_a_fallback_step_without_a_normal_turn_goes_to_the_push():
    dispatcher, _, client_reg = make_dispatcher()
    bridge = MagicMock()
    client_reg.register("client-a", bridge, keyed_conversation("client-a"))
    await dispatcher.dispatch(_agent("run-1", "fallback_step"))
    bridge.enqueue_push.assert_called_once_with("fallback", {"phase": "fallback_step"}, run_id="run-1")


def _drain(q):
    out = []
    while not q.empty():
        out.append(q.get_nowait())
    return out


# ── `agent` de streams que no son `lifecycle` (2026.10.5; revisión de #254) ──


def _agent_stream(stream, phase, run_id="r1", sk=SK):
    payload = {"sessionKey": sk, "runId": run_id, "stream": stream, "data": {"phase": phase}}
    return {"type": "event", "event": "agent", "payload": payload}


@pytest.mark.parametrize(
    ("stream", "phase"),
    [
        ("item", "start"),
        ("item", "update"),
        ("item", "end"),
        ("tool", "start"),
        ("tool", "result"),
        ("assistant", "end"),
        ("thinking", "error"),
        ("error", "seq gap"),
    ],
)
async def test_only_the_lifecycle_stream_opens_or_ends_a_push(stream, phase):
    """OpenClaw's `item` events carry `data.phase` start/update/end (one per tool activity)
    and are broadcast as `agent` too: an `item` `end` used to close a push and a `tool`
    `start` to open one."""
    dispatcher, turn_reg, client_reg = make_dispatcher()
    bridge = MagicMock()
    client_reg.register("client-a", bridge, keyed_conversation("client-a"))
    await dispatcher.dispatch(_agent_stream(stream, phase))
    bridge.enqueue_push.assert_not_called()


async def test_a_run_status_or_a_model_phase_opens_a_push():
    """5.0 PR 2b.2: a run drained from OpenClaw's queue gets no lifecycle `start`; its first
    event (here a `run_status`, or a lifecycle `model`) opens the push (`open`: unlike `start`,
    it never reopens a push its run already closed)."""
    dispatcher, _, client_reg = make_dispatcher()
    bridge = MagicMock()
    client_reg.register("client-a", bridge, keyed_conversation("client-a"))
    await dispatcher.dispatch(_agent_stream("run_status", "queued"))
    await dispatcher.dispatch(_agent_stream("lifecycle", "model"))
    assert [c.args for c in bridge.enqueue_push.call_args_list] == [("open", SK), ("open", SK)]


@pytest.mark.parametrize(
    ("phase", "kind"), [("start", "turn_start"), ("end", "turn_end"), ("error", "turn_error")]
)
async def test_the_lifecycle_stream_still_opens_and_ends_a_push(phase, kind):
    dispatcher, _, client_reg = make_dispatcher()
    bridge = MagicMock()
    client_reg.register("client-a", bridge, keyed_conversation("client-a"))
    await dispatcher.dispatch(_agent_stream("lifecycle", phase))
    arg = {"phase": "error"} if phase == "error" else SK
    bridge.enqueue_push.assert_called_once_with(kind, arg, run_id="r1")


async def test_an_item_end_does_not_close_the_open_push(make_real_bridge):
    dispatcher, _, client_reg = make_dispatcher()
    bridge, _ = make_real_bridge()
    client_reg.register("hab_sito", bridge, keyed_conversation("hab_sito"))
    sk = "agent:main:hab_sito"
    await dispatcher.dispatch(_agent_stream("lifecycle", "start", sk=sk))
    await bridge.push_idle()
    assert push_open(bridge)
    await dispatcher.dispatch(_agent_stream("item", "end", sk=sk))
    await dispatcher.dispatch(_agent_stream("tool", "start", sk=sk))
    await bridge.push_idle()
    assert push_open(bridge)
    await dispatcher.dispatch(_agent_stream("lifecycle", "end", sk=sk))
    await bridge.push_idle()
    assert not push_open(bridge)
    assert [m["type"] for m in await sent(bridge)] == ["turn_start", "turn_end"]


async def test_session_message_is_known_not_drift_and_reaches_nobody(caplog):
    dispatcher, _, client_reg = make_dispatcher()
    bridge = MagicMock()
    client_reg.register("client-a", bridge, keyed_conversation("client-a"))
    payload = {"sessionKey": "agent:main:client-a", "message": {"role": "assistant"}, "messageSeq": 3}
    with caplog.at_level(logging.DEBUG, logger="fogar_gateway.services.openclaw.dispatcher"):
        await dispatcher.dispatch({"type": "event", "event": "session.message", "payload": payload})
        await dispatcher.dispatch({"type": "event", "event": "brand.new", "payload": {}})
    assert not bridge.enqueue_push.called and not bridge.take_session_message.called
    drift = [r.getMessage() for r in caplog.records if "unhandled" in r.getMessage()]
    assert drift == ["unhandled OpenClaw event 'brand.new' (first occurrence; further ones are "
                     "logged at DEBUG) — possible protocol drift, see docs/openclaw-protocol.md"]


async def test_a_user_session_message_goes_to_its_channel(caplog):
    """5.0 PR 2b.2: a `user` `session.message` goes to its key's channel
    (`take_session_message`), never as a push and never logged; other roles go nowhere."""
    dispatcher, _, client_reg = make_dispatcher()
    channel = MagicMock()
    client_reg.register("client-a", channel, keyed_conversation("client-a"))
    secret = {"role": "user", "content": "SECRETO", "idempotencyKey": "run-b:user"}
    payload = {"sessionKey": "agent:main:client-a", "message": secret, "messageId": "m-1"}
    with caplog.at_level(logging.DEBUG):
        await dispatcher.dispatch({"type": "event", "event": "session.message", "payload": payload})
    channel.take_session_message.assert_called_once_with(payload)
    assert not channel.enqueue_push.called
    assert "SECRETO" not in caplog.text
