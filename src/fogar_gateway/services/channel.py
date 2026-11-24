"""The conversation channel (C3, 5.0; #406): one per conversation, it fans each turn out to
every session attached to it.

A `ConversationChannel` holds the conversation's open turn (one at a time) and its
subscribers: the live sessions that opened the conversation. Each subscriber gets its own
`on_turn_start`, `on_event` and `on_turn_end` of each channel turn and paints its own part
(its `turn_id`/`turn_seq`, its output mode, the TTS only if it is the sender). The channel
never waits for anyone to finish a turn: every socket closes its own `turn_end`.

Callbacks are synchronous (decision 1 of the 5.0 PR 2 plan): a subscriber only enqueues on
its own ordered queue and returns. It must not block, must not await (it cannot), and must
not call into another channel's lock-holding methods. Delivery happens inline, in the
caller, so when a channel method returns every subscriber has been called.

Delivery rules:

- Each subscriber sees a turn's start, its events and its end in order. A callback may call
  back into this channel (unsubscribe, emit, end the turn): the channel changes its state
  before delivering and checks it before every call, so nobody gets `on_turn_end` (or an
  event) before its `on_turn_start`, and nobody gets anything after leaving.
- A subscriber that joins while a turn is open gets `on_turn_start` for it (and what follows);
  `ChannelTurn.text` holds the turn's text so far for whoever needs to catch up (2a.3). An
  event emitted before a subscriber got the turn's start is not replayed: it is in `text`.
- A subscriber that leaves gets nothing more, not even `on_turn_end`: its session ends its own
  turn (`closed`).
- `end_turn` is idempotent: one `on_turn_end` per subscriber that got the turn's
  `on_turn_start` and is still subscribed.
- A failing callback never stops the others (a boundary: logged by type, never by text); a
  `CancelledError` (a `BaseException`) is not caught and propagates.

No user text in logs: token text is counted, never logged, and `ChannelTurn` keeps it out of
its `repr`. This module has no OpenClaw yet (5.0 PR 2a.1); runs arrive in 2a.2.
"""

from __future__ import annotations

import logging
import weakref
from collections.abc import Callable
from dataclasses import dataclass, field
from functools import partial
from typing import TYPE_CHECKING, Literal, Protocol
from uuid import uuid4

from fogar_gateway.services.reconnection import describe_error

if TYPE_CHECKING:
    # Annotations only: `turns.py` will import this module (5.0 PR 2a.2).
    from fogar_gateway.models.schemas import Client
    from fogar_gateway.services.conversations import Conversation
    from fogar_gateway.services.openclaw.models import ToolCallEvent
    from fogar_gateway.services.turn_progress import Progress
    from fogar_gateway.services.turns import EndReason, TurnOrigin

logger = logging.getLogger(__name__)


class ChannelSubscriber(Protocol):
    """A session attached to a conversation's channel.

    The callbacks are synchronous: they only enqueue on the session's own ordered queue and
    return. They must not block, must not await, and must not call into another channel's
    lock-holding methods. Calling back into this channel (e.g. `unsubscribe`) is safe."""

    @property
    def client(self) -> Client: ...

    @property
    def conversation_id(self) -> str: ...

    @property
    def wants_audio(self) -> bool: ...

    def on_turn_start(self, turn: ChannelTurn) -> None: ...

    def on_event(self, turn: ChannelTurn, event: ChannelEvent) -> None: ...

    def on_turn_end(self, turn: ChannelTurn, reason: EndReason) -> None: ...


@dataclass(frozen=True)
class ChannelToken:
    text: str = field(repr=False)  # user-facing text: never in logs
    kind: Literal["token"] = field(default="token", init=False)


@dataclass(frozen=True)
class ChannelToolCall:
    tool_call: ToolCallEvent = field(repr=False)  # its args/result may carry user text
    kind: Literal["tool_call"] = field(default="tool_call", init=False)


@dataclass(frozen=True)
class ChannelProgress:
    progress: Progress
    kind: Literal["progress"] = field(default="progress", init=False)


@dataclass(frozen=True)
class ChannelError:
    code: str  # a stable code (docs/client-protocol.md §9), never exception or OpenClaw text
    kind: Literal["error"] = field(default="error", init=False)


ChannelEvent = ChannelToken | ChannelToolCall | ChannelProgress | ChannelError


@dataclass(eq=False)
class ChannelTurn:
    """A turn of the channel. Every subscriber maps it to its own client-facing turn."""

    id: str  # internal, `uuid4().hex`; never sent to clients
    conversation_id: str
    origin: TurnOrigin
    by_client_id: str | None  # the durable origin: the sender's client (None for a push)
    run_id: str | None = None  # OpenClaw's runId, once known (2a.2)
    ended: bool = False
    end_reason: EndReason | None = None
    # Decision 5: only a weak reference to the sender; the live session is resolved on use
    # (if it left, nobody gets the audio).
    _sender: weakref.ref[ChannelSubscriber] | None = field(default=None, repr=False)
    _text: list[str] = field(default_factory=list, init=False, repr=False)

    @property
    def by_session(self) -> ChannelSubscriber | None:
        """The session that sent this turn, if it is still alive; None for a push."""
        return self._sender() if self._sender is not None else None

    @property
    def text(self) -> str:
        """The text the turn has carried so far (tokens), for a late subscriber (2a.3)."""
        return "".join(self._text)


class ChannelBusy(RuntimeError):
    """`open_turn` with a turn still open: a conversation has one open turn at a time."""


@dataclass(eq=False)
class _Subscription:
    subscriber: ChannelSubscriber
    active: bool = True


class ConversationChannel:
    """One conversation's channel: its subscribers and its open turn."""

    def __init__(self, conversation: Conversation) -> None:
        self.conversation = conversation
        self.session_key = conversation.session_key
        self.current_turn: ChannelTurn | None = None
        self._subscriptions: list[_Subscription] = []
        self._started: list[_Subscription] = []  # those that got the open turn's start

    # -- subscribers -------------------------------------------------------------------

    def subscribers(self) -> list[ChannelSubscriber]:
        return [s.subscriber for s in self._subscriptions]

    def _subscription(self, subscriber: ChannelSubscriber) -> _Subscription | None:
        return next((s for s in self._subscriptions if s.subscriber is subscriber), None)

    def subscribe(self, subscriber: ChannelSubscriber) -> bool:
        """Attach a session. With a turn open, it gets `on_turn_start` for it. False if it was
        already subscribed or belongs to another conversation."""
        # Only the conversation id is checked here; whether the session may see the
        # conversation (`Conversation.visible_to`) is the caller's check (2a.3).
        if subscriber.conversation_id != self.conversation.id:
            logger.debug(
                "Channel subscribe refused: conversation=%s client=%s (other conversation)",
                self.conversation.id, subscriber.client.id,
            )
            return False
        if self._subscription(subscriber) is not None:
            return False
        subscription = _Subscription(subscriber)
        self._subscriptions.append(subscription)
        logger.debug(
            "Channel subscribe: conversation=%s client=%s subscribers=%d",
            self.conversation.id, subscriber.client.id, len(self._subscriptions),
        )
        turn = self.current_turn
        if turn is not None:
            self._start(turn, subscription)
        return True

    def unsubscribe(self, subscriber: ChannelSubscriber) -> bool:
        """Detach a session: it gets nothing more from this channel, not even the open turn's
        `on_turn_end`. False if it was not subscribed."""
        subscription = self._subscription(subscriber)
        if subscription is None:
            return False
        subscription.active = False
        self._subscriptions.remove(subscription)
        if subscription in self._started:
            self._started.remove(subscription)
        logger.debug(
            "Channel unsubscribe: conversation=%s client=%s subscribers=%d",
            self.conversation.id, subscriber.client.id, len(self._subscriptions),
        )
        return True

    # -- turns -------------------------------------------------------------------------

    def open_turn(
        self,
        origin: TurnOrigin,
        *,
        sender: ChannelSubscriber | None = None,
        run_id: str | None = None,
    ) -> ChannelTurn:
        """Open the channel's turn and give every subscriber its `on_turn_start`. Raises
        `ChannelBusy` while another turn is open (end it first)."""
        if self.current_turn is not None:
            raise ChannelBusy(self.conversation.id)
        turn = ChannelTurn(
            id=uuid4().hex,
            conversation_id=self.conversation.id,
            origin=origin,
            by_client_id=sender.client.id if sender is not None else None,
            run_id=run_id,
            _sender=weakref.ref(sender) if sender is not None else None,
        )
        self.current_turn = turn
        self._started = []
        logger.debug(
            "Channel turn open: conversation=%s turn=%s origin=%s subscribers=%d",
            self.conversation.id, turn.id, origin, len(self._subscriptions),
        )
        for subscription in list(self._subscriptions):
            if turn is not self.current_turn:
                break  # a callback ended it: the rest never see it
            if subscription.active and subscription not in self._started:
                self._start(turn, subscription)
        return turn

    def _start(self, turn: ChannelTurn, subscription: _Subscription) -> None:
        # Marked before the call: from here on its `on_turn_end` is owed, even if a callback
        # ends the turn from inside this very `on_turn_start`.
        self._started.append(subscription)
        self._call(subscription, "on_turn_start", partial(subscription.subscriber.on_turn_start, turn))

    def emit(self, turn: ChannelTurn, event: ChannelEvent) -> bool:
        """Give `event` to every subscriber that has the turn. An event for a turn that is not
        this channel's open turn is dropped (False)."""
        if turn is not self.current_turn:
            logger.debug(
                "Channel event dropped: conversation=%s turn=%s kind=%s (not the open turn)",
                self.conversation.id, turn.id, event.kind,
            )
            return False
        if isinstance(event, ChannelToken):
            turn._text.append(event.text)
        for subscription in list(self._started):
            if turn is not self.current_turn:
                break  # a callback ended it
            if subscription.active:
                self._call(
                    subscription, "on_event", partial(subscription.subscriber.on_event, turn, event),
                )
        return True

    def end_turn(self, turn: ChannelTurn, reason: EndReason) -> bool:
        """End the channel's open turn: one `on_turn_end` per subscriber that got its start and
        is still subscribed. Idempotent; False (turn untouched) if `turn` is not this
        channel's open turn."""
        if turn is not self.current_turn:
            logger.debug(
                "Channel turn end ignored: conversation=%s turn=%s (not the open turn)",
                self.conversation.id, turn.id,
            )
            return False
        turn.ended = True
        turn.end_reason = reason
        self.current_turn = None
        started, self._started = self._started, []
        logger.debug(
            "Channel turn end: conversation=%s turn=%s reason=%s text_len=%d subscribers=%d",
            self.conversation.id, turn.id, reason, len(turn.text), len(started),
        )
        for subscription in started:
            if subscription.active:
                self._call(
                    subscription, "on_turn_end",
                    partial(subscription.subscriber.on_turn_end, turn, reason),
                )
        return True

    @property
    def idle(self) -> bool:
        """Nothing keeps the channel alive: no subscribers and no open turn. 2a.2 adds the
        active run."""
        return not self._subscriptions and self.current_turn is None

    # -- delivery ----------------------------------------------------------------------

    def _call(self, subscription: _Subscription, name: str, call: Callable[[], None]) -> None:
        try:
            call()
        except Exception as exc:  # noqa: BLE001 — boundary: one subscriber's failure must not stop the channel's fan-out to the others
            logger.warning(
                "Channel delivery failed: conversation=%s client=%s callback=%s error=%s",
                self.conversation.id, subscription.subscriber.client.id, name,
                describe_error(exc),
            )


class ChannelHub:
    """The live channels, by OpenClaw `sessionKey`."""

    def __init__(self) -> None:
        self._channels: dict[str, ConversationChannel] = {}

    def __len__(self) -> int:
        return len(self._channels)

    def get(self, session_key: str) -> ConversationChannel | None:
        return self._channels.get(session_key)

    def get_or_create(self, conversation: Conversation) -> ConversationChannel:
        """The channel of `conversation.session_key`. The first `Conversation` object given
        for a key is the one the channel keeps (same key → same conversation id)."""
        channel = self._channels.get(conversation.session_key)
        if channel is None:
            channel = ConversationChannel(conversation)
            self._channels[conversation.session_key] = channel
        return channel

    def drop_if_idle(self, session_key: str) -> bool:
        """Forget the channel if nothing keeps it alive (`ConversationChannel.idle`)."""
        channel = self._channels.get(session_key)
        if channel is None or not channel.idle:
            return False
        del self._channels[session_key]
        logger.debug("Channel dropped: conversation=%s", channel.conversation.id)
        return True
