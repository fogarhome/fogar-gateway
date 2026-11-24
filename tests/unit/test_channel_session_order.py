"""A session paints its channel's turns in order (5.0 PR 2a.2 review, #406).

What the socket shows decides what a `cancel` cuts; a cut's turn_end never waits behind a
busy TTS; closing the session drains its queue in order (#202: never two turns open).
"""

import asyncio
import time
from unittest.mock import AsyncMock

from fogar_gateway.core.config import settings
from fogar_gateway.services.channel import ChannelHub
from fogar_gateway.services.conversations import SessionConversation
from fogar_gateway.services.openclaw.registry import ClientRegistry, TurnRegistry
from fogar_gateway.services.protocol import OrchestratorEvent
from tests.fakes.bridge import enqueue_push, keyed_conversation, start_turn, tokens, wait_until


def _provider(stream):
    provider = AsyncMock()
    provider.connect = AsyncMock(return_value=stream)
    provider.resolve_voice = lambda v: v
    return provider


def _marks(ws) -> list[tuple]:
    return [
        (m["type"], m["turn_id"], m.get("origin") or m.get("reason"))
        for m in ws.sent
        if isinstance(m, dict) and m["type"] in ("turn_start", "turn_end")
    ]


def _slow_feed_stream(feeding: asyncio.Event):
    stream = AsyncMock()

    async def slow_feed(text):
        feeding.set()
        await asyncio.sleep(3600)

    async def audio():
        await asyncio.sleep(3600)
        yield b""  # pragma: no cover

    stream.send_text_chunk = slow_feed
    stream.get_audio_stream = audio
    return stream


async def test_barge_in_during_the_reply_s_audio_spares_the_push_not_yet_shown(make_bridge):
    """The reply ended in the channel and its audio still plays here; a push opened in the
    channel, its turn_start still queued on this socket. A barge-in cuts the reply only: the
    push is neither cancelled nor aborted, and opens after (as before 5.0)."""
    playing = asyncio.Event()
    stream = AsyncMock()

    async def audio():
        yield b"\x00" * 10
        playing.set()
        await asyncio.sleep(3600)
        yield b""  # pragma: no cover

    stream.get_audio_stream = audio
    bridge, ws = make_bridge(client_id="hab", output_mode=("audio", "text"), tts=_provider(stream))
    bridge.orchestrator.stream_response = tokens("hola")
    await asyncio.wait_for(await start_turn(bridge), 2)  # the channel's turn is done
    await asyncio.wait_for(playing.wait(), 2)  # this socket still plays its audio
    enqueue_push(bridge, "turn_start", bridge._session_key(), "run-p")
    await wait_until(
        lambda: bridge.channel.current_turn is not None
        and bridge.channel.current_turn.origin == "push"
    )

    assert await bridge.turns.cancel("interrupted") == (True, "t-1")  # barge-in
    await wait_until(lambda: len(ws.of("turn_start")) == 2)
    await bridge._sender.flush()

    bridge.orchestrator.abort.assert_not_awaited()
    assert bridge.channel.current_turn is not None  # the push goes on
    assert _marks(ws) == [
        ("turn_start", "t-1", "user"), ("turn_end", "t-1", "interrupted"),
        ("turn_start", "t-2", "push"),
    ]  # fmt: skip
    await bridge.close_all()


async def test_a_cut_s_turn_end_does_not_wait_for_a_stuck_tts(make_bridge, monkeypatch):
    """A-r2-2: a `send` cuts the open push while the worker is stuck feeding its TTS: the
    push's turn_end (`replaced`) goes out at once, the new turn_start behind it."""
    monkeypatch.setattr(settings, "PUSH_TTS_DRAIN_TIMEOUT_S", 5.0)
    feeding = asyncio.Event()
    stream = _slow_feed_stream(feeding)
    bridge, ws = make_bridge(client_id="hab", output_mode=("audio", "text"), tts=_provider(stream))
    sk = bridge._session_key()
    bridge.orchestrator.stream_response = tokens("hola")
    enqueue_push(bridge, "turn_start", sk, "rp")
    enqueue_push(bridge, "chat", {"sessionKey": sk, "runId": "rp", "deltaText": "aviso"}, "rp")
    await asyncio.wait_for(feeding.wait(), 2)  # the worker is stuck in the push's TTS feed

    t0 = time.monotonic()
    await bridge.turns.start_user_turn("hola")
    await wait_until(lambda: bool(ws.of("turn_end")), timeout=1.0)
    assert time.monotonic() - t0 < 1.0
    assert ws.of("turn_end")[0] == {"type": "turn_end", "turn_id": "t-1", "reason": "replaced"}
    assert [t for t, _, _ in _marks(ws)] == ["turn_start", "turn_end"]  # t-2 waits behind it
    await bridge.close_all()


async def test_close_drains_the_queue_in_order_never_two_turns_open(make_bridge):
    """#202 on close: the push cut by a `send` ends `replaced`, and only then the send's own
    turn_start goes out, with its queued end (its reply was already done in the channel)."""
    feeding = asyncio.Event()
    stream = _slow_feed_stream(feeding)
    bridge, ws = make_bridge(client_id="hab", output_mode=("audio", "text"), tts=_provider(stream))
    sk = bridge._session_key()
    bridge.orchestrator.stream_response = tokens("hola")
    enqueue_push(bridge, "turn_start", sk, "rp")
    enqueue_push(bridge, "chat", {"sessionKey": sk, "runId": "rp", "deltaText": "aviso"}, "rp")
    await asyncio.wait_for(feeding.wait(), 2)
    await bridge.turns.start_user_turn("hola")  # cuts the push, opens the user turn
    await asyncio.sleep(0.05)
    await asyncio.wait_for(bridge.close_all(), 5)

    marks = _marks(ws)
    open_ = 0
    for kind, _, _ in marks:
        open_ += 1 if kind == "turn_start" else -1
        assert 0 <= open_ <= 1, f"two turns open at once on the socket: {marks}"
    assert marks[:2] == [("turn_start", "t-1", "push"), ("turn_end", "t-1", "replaced")]
    assert marks[2:] == [("turn_start", "t-2", "user"), ("turn_end", "t-2", "done")]


async def test_a_send_without_a_channel_gets_a_stable_turn_error(make_bridge):
    """A session that could not attach to its conversation's channel answers each `send`
    with its pair and TURN_ERROR `internal_error`, never with silence."""
    bridge, ws = make_bridge(attach=False)
    bridge.turns.attach = lambda: None  # the channel refused it
    await bridge.turns.start_user_turn("hola")
    await bridge._sender.flush()
    assert ws.types() == ["turn_start", "error", "turn_end"]
    assert ws.of("error")[0]["message"] == "internal_error"
    assert ws.of("turn_end")[0]["reason"] == "error"
    bridge.orchestrator.stream_response.assert_not_called()


async def test_a_refused_subscription_leaves_no_channel_behind(make_bridge):
    bridge, _ = make_bridge(attach=False)
    bridge.conversation.current = keyed_conversation("u", id="priv")
    object.__setattr__(bridge.conversation.current, "owner_user_id", "someone-else")
    bridge.turns.attach()
    assert bridge.channel is None
    assert bridge.channel_hub.get(bridge.conversation.session_key) is None


async def test_a_departed_sender_s_tracker_gets_no_llm_events(make_bridge):
    """Its turn goes on in the channel for the others; its own tracker hears nothing more."""
    hub = ChannelHub(orchestrator=AsyncMock(), turn_registry=TurnRegistry())
    conversation = keyed_conversation("shared", id="conv-s")

    def session(client_id):
        return make_bridge(
            client_id=client_id, orchestrator=hub.orchestrator, turn_registry=hub.turn_registry,
            channel_hub=hub, client_registry=ClientRegistry(),
            conversation=SessionConversation(conversation, explicit=True),
        )[0]  # fmt: skip

    tv, mac = session("tv"), session("mac")
    started, go_on = asyncio.Event(), asyncio.Event()

    async def stream(*a, **k):
        yield OrchestratorEvent(type="token", content="a")
        started.set()
        await go_on.wait()
        yield OrchestratorEvent(type="token", content="b")

    hub.orchestrator.stream_response = stream
    await tv.turns.start_user_turn("hola")
    task = tv.turns.user_task
    await asyncio.wait_for(started.wait(), 2)
    await tv.close_all()
    go_on.set()
    await asyncio.wait_for(task, 2)

    stages = [c.args[0] for c in tv.tracker.record.await_args_list]
    assert "llm_start" in stages and "llm_done" not in stages
    await mac.close_all()
    await hub.aclose(1.0)

