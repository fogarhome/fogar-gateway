"""The conversation channel (C3, 5.0; #406): one per conversation, it fans each turn out to
every session attached to it.

A `ConversationChannel` holds the conversation's open turn (one at a time) and its
subscribers: the live sessions that opened the conversation. Each subscriber gets its own
`on_turn_start`, `on_event` and `on_turn_end` of each channel turn and paints its own part
(its `turn_id`/`turn_seq`, its output mode, the TTS only if it is the sender). The channel
never waits for anyone to finish a turn: callbacks only enqueue on the subscriber
(decision 1 of the 5.0 PR 2 plan), and every socket closes its own `turn_end`.

Delivery rules:

- Every call that changes the channel (`subscribe`, `open_turn`, `emit`, `end_turn`) changes
  its state at once and queues its deliveries in a single channel outbox; the outbox is
  delivered in order, so each subscriber sees start, events and end in the order they
  happened, whichever task made them. A callback may call back into the channel (unsubscribe,
  end the turn): its deliveries join the outbox and go out after the current one, with no
  lock to deadlock on. A call from another task while the outbox is being delivered waits
  until it is empty, so once a call returns its deliveries have been made.
- A subscriber that joins while a turn is open gets `on_turn_start` for it (and what follows);
  `ChannelTurn.text` holds the turn's text so far for whoever needs to catch up (2a.3).
- A subscriber that leaves gets nothing more, not even `on_turn_end`: its session ends its own
  turn (`closed`). Deliveries queued before it left are dropped too.
- `end_turn` is idempotent: one `on_turn_end` per subscriber that got the turn's
  `on_turn_start` and is still subscribed.
- A failing callback never stops the others (a boundary: logged by type, never by text); a
  `CancelledError` propagates, and the deliveries still queued go out with the next call.

No user text in logs: token text is counted, never logged, and `ChannelTurn` keeps it out of
its `repr`. This module has no OpenClaw yet (5.0 PR 2a.1); runs arrive in 2a.2.
"""

from __future__ import annotations

import asyncio
import logging
import weakref
from collections import deque
from collections.abc import Awaitable, Callable
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
    """A session attached to a conversation's channel. Callbacks only enqueue on the
    session (its `QueuedSender`) and return: they never wait for the client."""

    @property
    def client(self) -> Client: ...

    @property
    def conversation_id(self) -> str: ...

    @property
    def wants_audio(self) -> bool: ...

    def on_turn_start(self, turn: ChannelTurn) -> Awaitable[None]: ...

    def on_event(self, turn: ChannelTurn, event: ChannelEvent) -> Awaitable[None]: ...

    def on_turn_end(self, turn: ChannelTurn, reason: EndReason) -> Awaitable[None]: ...


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
    _text: list[str] = field(default_factory=list, repr=False)

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


@dataclass(frozen=True)
class _Delivery:
    subscription: _Subscription
    name: str  # the callback, for the failure log
    call: Callable[[], Awaitable[None]]


class ConversationChannel:
    """One conversation's channel: its subscribers and its open turn."""

    def __init__(self, conversation: Conversation) -> None:
        self.conversation = conversation
        self.session_key = conversation.session_key
        self.current_turn: ChannelTurn | None = None
        self._subscriptions: list[_Subscription] = []
        self._receivers: list[_Subscription] = []  # those that got the open turn's start
        self._outbox: deque[_Delivery] = deque()
        self._drainer: asyncio.Task | None = None
        self._drained = asyncio.Event()
        self._drained.set()

    # -- subscribers -------------------------------------------------------------------

    def subscribers(self) -> list[ChannelSubscriber]:
        return [s.subscriber for s in self._subscriptions]

    def _subscription(self, subscriber: ChannelSubscriber) -> _Subscription | None:
        return next((s for s in self._subscriptions if s.subscriber is subscriber), None)

    async def subscribe(self, subscriber: ChannelSubscriber) -> None:
        """Attach a session. With a turn open, it gets `on_turn_start` for it. Subscribing
        twice is a no-op."""
        if self._subscription(subscriber) is not None:
            return
        subscription = _Subscription(subscriber)
        self._subscriptions.append(subscription)
        logger.debug(
            "Channel subscribe: conversation=%s client=%s subscribers=%d",
            self.conversation.id, subscriber.client.id, len(self._subscriptions),
        )
        turn = self.current_turn
        if turn is not None:
            self._receivers.append(subscription)
            self._queue(subscription, "on_turn_start", partial(subscriber.on_turn_start, turn))
        await self._flush()

    def unsubscribe(self, subscriber: ChannelSubscriber) -> bool:
        """Detach a session: it gets nothing more from this channel, not even the open turn's
        `on_turn_end`. False if it was not subscribed."""
        subscription = self._subscription(subscriber)
        if subscription is None:
            return False
        subscription.active = False
        self._subscriptions.remove(subscription)
        if subscription in self._receivers:
            self._receivers.remove(subscription)
        logger.debug(
            "Channel unsubscribe: conversation=%s client=%s subscribers=%d",
            self.conversation.id, subscriber.client.id, len(self._subscriptions),
        )
        return True

    # -- turns -------------------------------------------------------------------------

    async def open_turn(
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
        self._receivers = list(self._subscriptions)
        logger.debug(
            "Channel turn open: conversation=%s turn=%s origin=%s subscribers=%d",
            self.conversation.id, turn.id, origin, len(self._receivers),
        )
        for subscription in self._receivers:
            self._queue(
                subscription, "on_turn_start", partial(subscription.subscriber.on_turn_start, turn),
            )
        await self._flush()
        return turn

    async def emit(self, turn: ChannelTurn, event: ChannelEvent) -> None:
        """Give `event` to every subscriber that has the turn. An event for a turn that is no
        longer open is dropped."""
        if turn.ended or turn is not self.current_turn:
            logger.debug(
                "Channel event dropped: conversation=%s turn=%s kind=%s (turn not open)",
                self.conversation.id, turn.id, event.kind,
            )
            return
        if isinstance(event, ChannelToken):
            turn._text.append(event.text)
        for subscription in self._receivers:
            self._queue(
                subscription, "on_event", partial(subscription.subscriber.on_event, turn, event),
            )
        await self._flush()

    async def end_turn(self, turn: ChannelTurn, reason: EndReason) -> bool:
        """End the turn: one `on_turn_end` per subscriber that got its start and is still
        subscribed. Idempotent: False if it had already ended."""
        if turn.ended:
            return False
        turn.ended = True
        turn.end_reason = reason
        receivers: list[_Subscription] = []
        if turn is self.current_turn:
            self.current_turn = None
            receivers, self._receivers = self._receivers, []
        logger.debug(
            "Channel turn end: conversation=%s turn=%s reason=%s text_len=%d subscribers=%d",
            self.conversation.id, turn.id, reason, len(turn.text), len(receivers),
        )
        for subscription in receivers:
            self._queue(
                subscription, "on_turn_end",
                partial(subscription.subscriber.on_turn_end, turn, reason),
            )
        await self._flush()
        return True

    @property
    def idle(self) -> bool:
        """Nothing keeps the channel alive: no subscribers, no open turn, nothing to deliver.
        2a.2 adds the active run."""
        return not self._subscriptions and self.current_turn is None and not self._outbox

    # -- delivery ----------------------------------------------------------------------

    def _queue(
        self, subscription: _Subscription, name: str, call: Callable[[], Awaitable[None]],
    ) -> None:
        self._outbox.append(_Delivery(subscription, name, call))

    async def _flush(self) -> None:
        """Deliver the outbox in order. Reentrant calls (from a callback) return at once; a
        call from another task waits until the current delivery empties the outbox."""
        me = asyncio.current_task()
        while self._outbox:
            if self._drainer is me:
                return  # a callback calling back: the running loop below delivers it
            if self._drainer is not None:
                await self._drained.wait()
                continue
            self._drainer = me
            self._drained.clear()
            try:
                while self._outbox:
                    await self._deliver(self._outbox.popleft())
            finally:
                self._drainer = None
                self._drained.set()

    async def _deliver(self, delivery: _Delivery) -> None:
        if not delivery.subscription.active:
            return  # it left after this was queued
        try:
            await delivery.call()
        except Exception as exc:  # noqa: BLE001 — boundary: one subscriber's failure must not stop the channel's fan-out to the others
            logger.warning(
                "Channel delivery failed: conversation=%s client=%s callback=%s error=%s",
                self.conversation.id, delivery.subscription.subscriber.client.id,
                delivery.name, describe_error(exc),
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
