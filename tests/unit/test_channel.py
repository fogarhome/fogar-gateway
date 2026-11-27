"""`services/channel.py` (#406, 5.0 PR 2a.1): the per-conversation channel and its hub.

Pure fan-out, no OpenClaw: every subscriber of a conversation gets its own
start/event/end of each channel turn, in order, and one failing subscriber never stops
the others. Callbacks are synchronous: delivery happens inside the channel call.
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
    """Records what the channel delivers; only appends (decision 1)."""

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

    def on_turn_start(self, turn: ChannelTurn) -> None:
        self.calls.append(("start", turn.id))

    def on_event(self, turn: ChannelTurn, event: ChannelEvent) -> None:
        self.calls.append(("event", turn.id, event))

    def on_turn_end(self, turn: ChannelTurn, reason: EndReason) -> None:
        self.calls.append(("end", turn.id, reason))

    def kinds(self) -> list[str]:
        return [c[0] for c in self.calls]


class FailingSubscriber(FakeSubscriber):
    def on_turn_start(self, turn: ChannelTurn) -> None:
        raise RuntimeError(SECRET)

    def on_event(self, turn: ChannelTurn, event: ChannelEvent) -> None:
        raise RuntimeError(SECRET)

    def on_turn_end(self, turn: ChannelTurn, reason: EndReason) -> None:
        raise RuntimeError(SECRET)


def test_turn_fans_out_to_every_subscriber():
    channel = ConversationChannel(_conversation())
    a, b = FakeSubscriber("a"), FakeSubscriber("b")
    assert channel.subscribe(a) is True
    assert channel.subscribe(b) is True

    turn = channel.open_turn("user", sender=a)
    tool = ToolCallEvent(phase="start", name="search", tool_call_id="t1")
    events: list[ChannelEvent] = [
        ChannelToken("hola"),
        ChannelToolCall(tool),
        ChannelProgress(Progress("thinking")),
        ChannelError("orchestrator_error"),
    ]
    for event in events:
        assert channel.emit(turn, event) is True
    assert channel.end_turn(turn, "done") is True

    expected = [("start", turn.id), *[("event", turn.id, e) for e in events], ("end", turn.id, "done")]
    assert a.calls == expected
    assert b.calls == expected
    assert turn.origin == "user"
    assert turn.conversation_id == "c1"
    assert turn.by_client_id == "client-a"
    assert turn.by_session is a
    assert turn.ended and turn.end_reason == "done"
    assert channel.current_turn is None


def test_events_are_tagged():
    assert ChannelToken("x").kind == "token"
    assert ChannelToolCall(ToolCallEvent(phase="start", name="n", tool_call_id="i")).kind == "tool_call"
    assert ChannelProgress(Progress("tool", "n")).kind == "progress"
    assert ChannelError("turn_timeout").kind == "error"


def test_late_subscriber_gets_open_turn():
    channel = ConversationChannel(_conversation())
    a, late = FakeSubscriber("a"), FakeSubscriber("late")
    channel.subscribe(a)
    turn = channel.open_turn("user", sender=a)
    channel.emit(turn, ChannelToken("hola "))

    channel.subscribe(late)
    assert late.calls == [("start", turn.id)]
    # The hook for 2a.3: the text the turn has carried so far.
    assert turn.text == "hola "

    channel.emit(turn, ChannelToken("mundo"))
    channel.end_turn(turn, "done")
    assert late.kinds() == ["start", "event", "end"]
    assert turn.text == "hola mundo"


def test_subscribe_twice_is_a_no_op():
    channel = ConversationChannel(_conversation())
    a = FakeSubscriber("a")
    channel.subscribe(a)
    turn = channel.open_turn("push")
    assert channel.subscribe(a) is False
    channel.end_turn(turn, "done")
    assert a.kinds() == ["start", "end"]
    assert channel.subscribers() == [a]


def test_subscriber_of_another_conversation_is_refused():
    channel = ConversationChannel(_conversation("c1"))
    other = FakeSubscriber("other", conversation_id="c2")
    assert channel.subscribe(other) is False
    assert channel.subscribers() == []
    channel.open_turn("push")
    assert other.calls == []


def test_end_turn_once_per_subscriber():
    channel = ConversationChannel(_conversation())
    a, b, gone = FakeSubscriber("a"), FakeSubscriber("b"), FakeSubscriber("gone")
    for s in (a, b, gone):
        channel.subscribe(s)
    turn = channel.open_turn("user", sender=a)
    assert channel.unsubscribe(gone) is True
    assert channel.unsubscribe(gone) is False

    assert channel.end_turn(turn, "cancelled") is True
    assert channel.end_turn(turn, "done") is False

    assert a.calls.count(("end", turn.id, "cancelled")) == 1
    assert b.kinds() == ["start", "end"]
    assert gone.kinds() == ["start"]  # left: no on_turn_end (its socket closes its own)
    assert turn.end_reason == "cancelled"

    after = FakeSubscriber("after")
    channel.subscribe(after)
    assert channel.emit(turn, ChannelToken("tarde")) is False  # an ended turn takes no events
    assert after.calls == []
    assert a.kinds() == ["start", "end"]


def test_foreign_turn_is_left_untouched():
    one, two = ConversationChannel(_conversation("c1")), ConversationChannel(_conversation("c2"))
    a, b = FakeSubscriber("a", "c1"), FakeSubscriber("b", "c2")
    one.subscribe(a)
    two.subscribe(b)
    turn_one = one.open_turn("user")
    turn_two = two.open_turn("user")

    assert two.emit(turn_one, ChannelToken("x")) is False
    assert two.end_turn(turn_one, "done") is False
    assert not turn_one.ended and turn_one.end_reason is None
    assert one.current_turn is turn_one and two.current_turn is turn_two
    assert a.kinds() == ["start"] and b.kinds() == ["start"]
    assert turn_one.text == ""


def test_unsubscribed_mid_turn_gets_nothing_more():
    channel = ConversationChannel(_conversation())
    a, b = FakeSubscriber("a"), FakeSubscriber("b")
    channel.subscribe(a)
    channel.subscribe(b)
    turn = channel.open_turn("user", sender=a)
    channel.unsubscribe(a)
    channel.emit(turn, ChannelToken("x"))
    channel.end_turn(turn, "done")
    assert a.kinds() == ["start"]
    assert b.kinds() == ["start", "event", "end"]


def test_open_turn_while_open_is_refused():
    channel = ConversationChannel(_conversation())
    turn = channel.open_turn("user")
    with pytest.raises(ChannelBusy):
        channel.open_turn("push")
    channel.end_turn(turn, "replaced")
    second = channel.open_turn("push", run_id="run-2")
    assert channel.current_turn is second
    assert second.run_id == "run-2"
    assert second.id != turn.id


def test_failing_subscriber_does_not_stop_others(caplog):
    caplog.set_level(logging.DEBUG, logger="fogar_gateway.services.channel")
    channel = ConversationChannel(_conversation())
    bad, good = FailingSubscriber("bad"), FakeSubscriber("good")
    channel.subscribe(bad)
    channel.subscribe(good)

    turn = channel.open_turn("user", sender=bad)
    channel.emit(turn, ChannelToken(SECRET))
    channel.end_turn(turn, "done")

    assert good.kinds() == ["start", "event", "end"]
    assert channel.subscribers() == [bad, good]  # a failure does not unsubscribe
    failures = [r for r in caplog.records if r.levelno >= logging.WARNING]
    assert len(failures) == 3
    assert all("RuntimeError" in r.getMessage() for r in failures)
    assert all(SECRET not in r.getMessage() for r in caplog.records)


def test_cancelled_error_from_a_callback_propagates():
    channel = ConversationChannel(_conversation())

    class Cancelling(FakeSubscriber):
        def on_event(self, turn, event):
            raise asyncio.CancelledError

    channel.subscribe(Cancelling("c"))
    turn = channel.open_turn("user")
    with pytest.raises(asyncio.CancelledError):
        channel.emit(turn, ChannelToken("x"))


def test_reentrant_callbacks_are_safe():
    channel = ConversationChannel(_conversation())

    class Leaving(FakeSubscriber):
        def on_event(self, turn, event):
            super().on_event(turn, event)
            channel.unsubscribe(self)
            channel.end_turn(turn, "error")

    leaving, other = Leaving("leaving"), FakeSubscriber("other")
    channel.subscribe(leaving)
    channel.subscribe(other)
    turn = channel.open_turn("user")
    channel.emit(turn, ChannelToken("x"))
    # `other` comes after `leaving`: the turn ended before its event, so it gets only the end.
    assert leaving.kinds() == ["start", "event"]
    assert other.kinds() == ["start", "end"]
    assert turn.end_reason == "error"


def test_end_during_start_never_ends_before_start():
    """A callback that ends the turn from inside its `on_turn_start`: those already started
    get one end after their start; those not yet started never hear of the turn."""
    channel = ConversationChannel(_conversation())
    first = FakeSubscriber("first")

    class Ender(FakeSubscriber):
        def on_turn_start(self, turn):
            super().on_turn_start(turn)
            channel.end_turn(turn, "cancelled")

    ender, last = Ender("ender"), FakeSubscriber("last")
    for s in (first, ender, last):
        channel.subscribe(s)
    turn = channel.open_turn("user")
    assert first.kinds() == ["start", "end"]
    assert ender.kinds() == ["start", "end"]
    assert last.calls == []
    assert channel.current_turn is None and turn.ended


def test_by_session_is_weak_and_by_client_id_durable():
    channel = ConversationChannel(_conversation())
    sender = FakeSubscriber("s")
    turn = channel.open_turn("user", sender=sender)
    assert turn.by_session is sender
    del sender
    gc.collect()
    assert turn.by_session is None
    assert turn.by_client_id == "client-s"

    channel.end_turn(turn, "done")
    push = channel.open_turn("push")
    assert push.by_session is None and push.by_client_id is None


def test_token_text_is_never_logged(caplog):
    caplog.set_level(logging.DEBUG, logger="fogar_gateway.services.channel")
    channel = ConversationChannel(_conversation())
    channel.subscribe(FakeSubscriber("a"))
    turn = channel.open_turn("user")
    channel.emit(turn, ChannelToken(SECRET))
    channel.end_turn(turn, "done")
    assert all(SECRET not in r.getMessage() for r in caplog.records)
    assert "texto" not in repr(turn)


def test_hub_drops_idle_channel():
    hub = ChannelHub()
    conversation = _conversation()
    key = conversation.session_key
    assert hub.get(key) is None
    channel = hub.get_or_create(conversation)
    assert hub.get_or_create(conversation) is channel
    assert hub.get(key) is channel
    assert channel.session_key == key and channel.conversation is conversation

    a = FakeSubscriber("a")
    channel.subscribe(a)
    assert hub.drop_if_idle(key) is False  # a subscriber

    channel.unsubscribe(a)
    turn = channel.open_turn("push")
    assert channel.idle is False
    assert hub.drop_if_idle(key) is False  # an open turn

    channel.end_turn(turn, "done")
    assert channel.idle is True
    assert hub.drop_if_idle(key) is True
    assert hub.get(key) is None
    assert hub.drop_if_idle(key) is False  # already gone
    assert hub.get_or_create(conversation) is not channel


def test_hub_keeps_channels_apart():
    hub = ChannelHub()
    one, two = _conversation("c1"), _conversation("c2")
    assert hub.get_or_create(one) is not hub.get_or_create(two)
    assert len(hub) == 2
