"""`services/channel.py` (#406, 5.0 PR 2a.1): the per-conversation channel and its hub.

Pure fan-out, no OpenClaw: every subscriber of a conversation gets its own
start/event/end of each channel turn, in order, and one failing subscriber never stops
the others.
"""

import asyncio
import gc
import logging

import pytest

from fogar_gateway.models.schemas import Client
from fogar_gateway.services.channel import (
    ChannelBusy,
    ChannelError,
    ChannelEvent,
    ChannelHub,
    ChannelProgress,
    ChannelToken,
    ChannelToolCall,
    ChannelTurn,
    ConversationChannel,
)
from fogar_gateway.services.conversations import Conversation, session_key_for
from fogar_gateway.services.openclaw.models import ToolCallEvent
from fogar_gateway.services.turn_progress import Progress
from fogar_gateway.services.turns import EndReason

SECRET = "texto privado del usuario"


def _conversation(cid: str = "c1", agent: str = "main") -> Conversation:
    return Conversation(cid, "dev-1", agent, session_key_for(agent, cid), "default")


class FakeSubscriber:
    """Records what the channel delivers; never blocks (decision 1)."""

    def __init__(self, name: str, conversation_id: str = "c1", *, wants_audio: bool = True):
        self.name = name
        self._client = Client(id=f"client-{name}", key_hash="h", is_active=True)
        self._conversation_id = conversation_id
        self._wants_audio = wants_audio
        self.calls: list[tuple] = []

    @property
    def client(self) -> Client:
        return self._client

    @property
    def conversation_id(self) -> str:
        return self._conversation_id

    @property
    def wants_audio(self) -> bool:
        return self._wants_audio

    async def on_turn_start(self, turn: ChannelTurn) -> None:
        self.calls.append(("start", turn.id))

    async def on_event(self, turn: ChannelTurn, event: ChannelEvent) -> None:
        self.calls.append(("event", turn.id, event))

    async def on_turn_end(self, turn: ChannelTurn, reason: EndReason) -> None:
        self.calls.append(("end", turn.id, reason))

    def kinds(self) -> list[str]:
        return [c[0] for c in self.calls]


class FailingSubscriber(FakeSubscriber):
    async def on_turn_start(self, turn: ChannelTurn) -> None:
        raise RuntimeError(SECRET)

    async def on_event(self, turn: ChannelTurn, event: ChannelEvent) -> None:
        raise RuntimeError(SECRET)

    async def on_turn_end(self, turn: ChannelTurn, reason: EndReason) -> None:
        raise RuntimeError(SECRET)


async def test_turn_fans_out_to_every_subscriber():
    channel = ConversationChannel(_conversation())
    a, b = FakeSubscriber("a"), FakeSubscriber("b")
    await channel.subscribe(a)
    await channel.subscribe(b)

    turn = await channel.open_turn("user", sender=a)
    tool = ToolCallEvent(phase="start", name="search", tool_call_id="t1")
    events: list[ChannelEvent] = [
        ChannelToken("hola"),
        ChannelToolCall(tool),
        ChannelProgress(Progress("thinking")),
        ChannelError("orchestrator_error"),
    ]
    for event in events:
        await channel.emit(turn, event)
    assert await channel.end_turn(turn, "done") is True

    expected = [("start", turn.id), *[("event", turn.id, e) for e in events], ("end", turn.id, "done")]
    assert a.calls == expected
    assert b.calls == expected
    assert turn.origin == "user"
    assert turn.conversation_id == "c1"
    assert turn.by_client_id == "client-a"
    assert turn.by_session is a
    assert turn.ended and turn.end_reason == "done"
    assert channel.current_turn is None


async def test_events_are_tagged():
    assert ChannelToken("x").kind == "token"
    assert ChannelToolCall(ToolCallEvent(phase="start", name="n", tool_call_id="i")).kind == "tool_call"
    assert ChannelProgress(Progress("tool", "n")).kind == "progress"
    assert ChannelError("turn_timeout").kind == "error"


async def test_late_subscriber_gets_open_turn():
    channel = ConversationChannel(_conversation())
    a, late = FakeSubscriber("a"), FakeSubscriber("late")
    await channel.subscribe(a)
    turn = await channel.open_turn("user", sender=a)
    await channel.emit(turn, ChannelToken("hola "))

    await channel.subscribe(late)
    assert late.calls == [("start", turn.id)]
    # The hook for 2a.3: the text the turn has carried so far.
    assert turn.text == "hola "

    await channel.emit(turn, ChannelToken("mundo"))
    await channel.end_turn(turn, "done")
    assert late.kinds() == ["start", "event", "end"]
    assert turn.text == "hola mundo"


async def test_subscribe_twice_is_a_no_op():
    channel = ConversationChannel(_conversation())
    a = FakeSubscriber("a")
    await channel.subscribe(a)
    turn = await channel.open_turn("push")
    await channel.subscribe(a)
    await channel.end_turn(turn, "done")
    assert a.kinds() == ["start", "end"]
    assert channel.subscribers() == [a]


async def test_end_turn_once_per_subscriber():
    channel = ConversationChannel(_conversation())
    a, b, gone = FakeSubscriber("a"), FakeSubscriber("b"), FakeSubscriber("gone")
    for s in (a, b, gone):
        await channel.subscribe(s)
    turn = await channel.open_turn("user", sender=a)
    assert channel.unsubscribe(gone) is True
    assert channel.unsubscribe(gone) is False

    assert await channel.end_turn(turn, "cancelled") is True
    assert await channel.end_turn(turn, "done") is False

    assert a.calls.count(("end", turn.id, "cancelled")) == 1
    assert b.kinds() == ["start", "end"]
    assert gone.kinds() == ["start"]  # left: no on_turn_end (its socket closes its own)
    assert turn.end_reason == "cancelled"

    after = FakeSubscriber("after")
    await channel.subscribe(after)
    await channel.emit(turn, ChannelToken("tarde"))  # an ended turn takes no events
    assert after.calls == []
    assert a.kinds() == ["start", "end"]


async def test_unsubscribed_mid_turn_gets_nothing_more():
    channel = ConversationChannel(_conversation())
    a, b = FakeSubscriber("a"), FakeSubscriber("b")
    await channel.subscribe(a)
    await channel.subscribe(b)
    turn = await channel.open_turn("user", sender=a)
    channel.unsubscribe(a)
    await channel.emit(turn, ChannelToken("x"))
    await channel.end_turn(turn, "done")
    assert a.kinds() == ["start"]
    assert b.kinds() == ["start", "event", "end"]


async def test_open_turn_while_open_is_refused():
    channel = ConversationChannel(_conversation())
    turn = await channel.open_turn("user")
    with pytest.raises(ChannelBusy):
        await channel.open_turn("push")
    await channel.end_turn(turn, "replaced")
    second = await channel.open_turn("push", run_id="run-2")
    assert channel.current_turn is second
    assert second.run_id == "run-2"
    assert second.id != turn.id


async def test_failing_subscriber_does_not_stop_others(caplog):
    caplog.set_level(logging.DEBUG, logger="fogar_gateway.services.channel")
    channel = ConversationChannel(_conversation())
    bad, good = FailingSubscriber("bad"), FakeSubscriber("good")
    await channel.subscribe(bad)
    await channel.subscribe(good)

    turn = await channel.open_turn("user", sender=bad)
    await channel.emit(turn, ChannelToken(SECRET))
    await channel.end_turn(turn, "done")

    assert good.kinds() == ["start", "event", "end"]
    assert channel.subscribers() == [bad, good]  # a failure does not unsubscribe
    failures = [r for r in caplog.records if r.levelno >= logging.WARNING]
    assert len(failures) == 3
    assert all("RuntimeError" in r.getMessage() for r in failures)
    assert all(SECRET not in r.getMessage() for r in caplog.records)


async def test_subscriber_cancellation_propagates_and_nothing_is_lost():
    channel = ConversationChannel(_conversation())

    class Cancelling(FakeSubscriber):
        armed = True

        async def on_event(self, turn, event):
            if self.armed:
                self.armed = False
                raise asyncio.CancelledError
            await super().on_event(turn, event)

    first, second = Cancelling("first"), FakeSubscriber("second")
    await channel.subscribe(first)
    await channel.subscribe(second)
    turn = await channel.open_turn("user")
    with pytest.raises(asyncio.CancelledError):
        await channel.emit(turn, ChannelToken("x"))
    # The pending delivery to `second` goes out with the next call, in order.
    await channel.end_turn(turn, "done")
    assert second.kinds() == ["start", "event", "end"]
    assert first.kinds() == ["start", "end"]


async def test_reentrant_callbacks_do_not_deadlock():
    channel = ConversationChannel(_conversation())

    class Leaving(FakeSubscriber):
        async def on_event(self, turn, event):
            await super().on_event(turn, event)
            channel.unsubscribe(self)
            await channel.end_turn(turn, "error")

    leaving, other = Leaving("leaving"), FakeSubscriber("other")
    await channel.subscribe(leaving)
    await channel.subscribe(other)
    turn = await channel.open_turn("user")
    await asyncio.wait_for(channel.emit(turn, ChannelToken("x")), timeout=1)
    assert leaving.kinds() == ["start", "event"]
    assert other.kinds() == ["start", "event", "end"]
    assert turn.end_reason == "error"


async def test_order_holds_per_subscriber_across_tasks():
    """A subscriber that yields while delivering never sees an event after its end, nor
    one before its start, even when another task ends the turn meanwhile."""
    channel = ConversationChannel(_conversation())

    class Yielding(FakeSubscriber):
        async def on_event(self, turn, event):
            for _ in range(5):
                await asyncio.sleep(0)
            await super().on_event(turn, event)

    slow, fast = Yielding("slow"), FakeSubscriber("fast")
    await channel.subscribe(slow)
    await channel.subscribe(fast)
    turn = await channel.open_turn("user")
    emitting = asyncio.create_task(channel.emit(turn, ChannelToken("x")))
    await asyncio.sleep(0)
    ending = asyncio.create_task(channel.end_turn(turn, "cancelled"))
    await asyncio.gather(emitting, ending)
    assert slow.kinds() == ["start", "event", "end"]
    assert fast.kinds() == ["start", "event", "end"]


async def test_by_session_is_weak_and_by_client_id_durable():
    channel = ConversationChannel(_conversation())
    sender = FakeSubscriber("s")
    turn = await channel.open_turn("user", sender=sender)
    assert turn.by_session is sender
    del sender
    gc.collect()
    assert turn.by_session is None
    assert turn.by_client_id == "client-s"

    await channel.end_turn(turn, "done")
    push = await channel.open_turn("push")
    assert push.by_session is None and push.by_client_id is None


async def test_token_text_is_never_logged(caplog):
    caplog.set_level(logging.DEBUG, logger="fogar_gateway.services.channel")
    channel = ConversationChannel(_conversation())
    await channel.subscribe(FakeSubscriber("a"))
    turn = await channel.open_turn("user")
    await channel.emit(turn, ChannelToken(SECRET))
    await channel.end_turn(turn, "done")
    assert all(SECRET not in r.getMessage() for r in caplog.records)
    assert "texto" not in repr(turn)


async def test_hub_drops_idle_channel():
    hub = ChannelHub()
    conversation = _conversation()
    key = conversation.session_key
    assert hub.get(key) is None
    channel = hub.get_or_create(conversation)
    assert hub.get_or_create(conversation) is channel
    assert hub.get(key) is channel
    assert channel.session_key == key and channel.conversation is conversation

    a = FakeSubscriber("a")
    await channel.subscribe(a)
    assert hub.drop_if_idle(key) is False  # a subscriber

    channel.unsubscribe(a)
    turn = await channel.open_turn("push")
    assert channel.idle is False
    assert hub.drop_if_idle(key) is False  # an open turn

    await channel.end_turn(turn, "done")
    assert channel.idle is True
    assert hub.drop_if_idle(key) is True
    assert hub.get(key) is None
    assert hub.drop_if_idle(key) is False  # already gone
    assert hub.get_or_create(conversation) is not channel


async def test_hub_keeps_channels_apart():
    hub = ChannelHub()
    one, two = _conversation("c1"), _conversation("c2")
    assert hub.get_or_create(one) is not hub.get_or_create(two)
    assert len(hub) == 2
