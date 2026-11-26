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
- A guest (5.0 PR 2c.3: a push for the conversation's notify device, whose session is not
  attached) gets that one turn like a subscriber that got its start, and nothing else: no
  messages, no other turn. `drop_guest` removes it; it never keeps the channel alive.
- A failing callback never stops the others (a boundary: logged by type, never by text); a
  `CancelledError` (a `BaseException`) is not caught and propagates.

No user text in logs: token text is counted, never logged, and `ChannelTurn` keeps it out of
its `repr`.

The OpenClaw side: the channel is the only owner of its conversation's runs (5.0 PR 2a.2),
and since 5.0 PR 2b it uses OpenClaw's own queue (`messages.queue`; docs/openclaw-protocol.md,
«C3 queue behaviour»):

- `send_user_message` never aborts before a `chat.send`. The send lock keeps the order in
  which messages reach the channel (two devices sending at once go to OpenClaw in arrival
  order). OpenClaw acks with the run's `runId`, which is also the message's id on the wire.
  The message goes to every subscriber (`on_message`): `pending` if the channel has an open
  turn or another unfinished run of its own, else `delivered`; a pending one becomes
  `delivered` when its own run opens, or `withdrawn` (`withdraw`, any subscriber) while it
  still waits. Only `cancel`, a `send` that interrupts and `withdraw` abort, each by its
  `runId` (`abort_run`): never the whole key.
- The run table (`RunInfo`, decision 5): the channel's own runs, `queued` until their first
  event, `running` with their turn open, then `done`. A run's frames come from the dispatcher
  (`take_frame`); its turn opens on its first event (`chat` status/delta/tool, `agent`
  lifecycle `start`/`model`/`fallback_step` or `run_status`), not only on `start`, which a run
  drained from the queue never gets; it ends on its terminal `chat` (`final`, keeping the
  streamed text when the `final` carries no `message`; `aborted`; `error`, classified #397).
- Steer (2b.2): a `user` `session.message` (`take_session_message`) links OpenClaw's message id
  to its `chat.send` run (`idempotencyKey` `"<runId>:user"`); a copy marked
  `steerTargetRunId` makes that run `absorbed`: its message `delivered`, no turn of its own.
  A run that never opened ends exactly as decision 4 says (`_run_terminal`).
- Reconnect (2b.2): a queued run without a turn survives a drop `unconfirmed` until the
  reconnection re-reads `pendingInputs` (`reconcile_pending`) or `RECONNECT_GRACE_S` passes.
- Origin before the ack (decision 3): while a `chat.send` waits for its ack, frames of a run
  nobody knows yet are held; the ack says whose they are, and what is still nobody's is a push.
  A push opens on its run's first event too (`push_route`), not only on lifecycle `start`.
- Push delivery (2c.3): its text goes to the attached subscribers that may see the
  conversation (`visible_to`, push on, agent allowed); its audio only to the session of the
  conversation's notify device (`ChannelHub.notify_device`, read from the DB before each push
  opener, never cached; that device's attached session, else its most recent one), if it shows
  audio. That session, when not attached, is a guest of the push if nobody is attached (the
  text) or if it plays it. Nobody: the push opens for no one (no `turn_start`, the run goes on).
- One turn open at a time: a run whose first event arrives while another turn is open waits
  (its frames deferred, in order) until that turn ends; so does a push opening while a user
  turn is open.
- TURN_TIMEOUT_S without progress (decision 2): for the open turn's run and, with no turn
  open, for the first run still waiting to open (its clock starts when nothing is ahead of it);
  for the push, its own budget (#286).
- /v1 (2b.3, `send_exclusive`): a Home Assistant message never steers nor is steered into.
  It takes the send lock, waits until the channel is calm (no open turn, no unfinished run of
  its own, no ack in flight, nothing deferred or queued for the worker), sends, and keeps the
  lock until its run ends: WebSocket sends arriving meanwhile wait behind it, in arrival
  order. Its message and turn reach the subscribers like any other (origin: its device; no
  audio); a sink collects its answer; its caller leaving aborts only its run.

Every frame of the key goes through one ordered worker (the push worker: FIFO, #130 S3a), so
a push and the channel's own runs never overtake each other. It also keeps the tombstones of
aborted runs (#285), the startup phases of a push not yet open (#254), the push's
`ChatTextTracker` (#241) and the conversation's turn count (`note_turn`, once per channel
turn). A subscriber leaving never aborts a run (decision 2 of the 5.0 PR 2 plan: G-18
inverted); the channel ends its turn on OpenClaw's terminal (or its timeout) without waiting
for anyone's audio (decision 1). User text (a message, a token) goes to subscribers only,
never to logs (`text_len`).
"""

from __future__ import annotations

import asyncio
import logging
import time
import weakref
from collections import OrderedDict, deque
from collections.abc import Awaitable, Callable, Coroutine, Sequence
from dataclasses import dataclass, field
from functools import partial
from typing import TYPE_CHECKING, Any, Literal, Protocol
from uuid import uuid4

from fogar_gateway.core.config import settings
from fogar_gateway.services import orchestration
from fogar_gateway.services.openclaw.chat_text import ChatTextTracker, extract_message_text
from fogar_gateway.services.openclaw.dispatcher import push_route
from fogar_gateway.services.openclaw.failures import classify_failure, fallback_reason
from fogar_gateway.services.openclaw.models import ToolCallEvent
from fogar_gateway.services.openclaw.registry import TurnRegistry
from fogar_gateway.services.reconnection import describe_error, traceback_no_message
from fogar_gateway.services.stats import gateway_stats
from fogar_gateway.services.teardown import Teardown, retrieve_outcome
from fogar_gateway.services.turn_progress import Progress, ProgressPhase, chat_status_phase

if TYPE_CHECKING:
    # Annotations only: `turns.py` imports this module.
    from fogar_gateway.models.schemas import Client
    from fogar_gateway.services.conversations import Conversation, ConversationIndex
    from fogar_gateway.services.pipeline_tracker import PipelineTracker
    from fogar_gateway.services.protocol import OrchestratorProtocol
    from fogar_gateway.services.turns import EndReason, TurnOrigin

logger = logging.getLogger(__name__)

# Fases de arranque de runs cuyo push aún no se abrió (#254): las de los últimos runs, un rato.
_EARLY_PHASE_RUNS = 8
_EARLY_PHASE_TTL_S = 30.0
# Runs propios ya terminados que se recuerdan: sus frames tardíos (el lifecycle `end` tras el
# `final`) siguen siendo suyos y se descartan, nunca llegan como push.
_DONE_RUNS = 64
_TERMINAL_CHAT = ("final", "aborted", "error")
# What a run fails with when its frames overflow TURN_QUEUE_MAX (final 2b review, m3): an
# internal bound of the gateway, nothing the client can act on; `turn_overflow` stays retired.
OVERFLOW_CODE = "internal_error"
# Pushes cerrados por su lifecycle `end`/`error`, recordados: un evento tardío de su run (un
# `run_status`, una fase) no abre otro push; solo un lifecycle `start` nuevo (#84).
_ENDED_PUSH_RUNS = 64
# `session.message` de usuario vistos: id de OpenClaw (`oc_message_id`) → runId de su
# `chat.send`. La marca de `steer` llega en una copia repetida que puede no traer el enlace.
_OC_MESSAGES = 64
# Tras una caída de OpenClaw, lo que un run en cola (sin turno) espera a que la reconexión
# relea `pendingInputs` y diga si sigue en la cola; si no llega a tiempo, falla
# (`orchestrator_disconnected`), como un run en marcha.
# Medido en vivo (OpenClaw 2026.11.7, `systemctl restart`): 15 s y 22 s sin aceptar conexiones.
RECONNECT_GRACE_S = 45.0
# Tras un reinicio, OpenClaw no lista enseguida lo que tenía en cola: `pendingInputs` sale vacío
# al reconectar y el mensaje reaparece (con su runId) ~10-13 s después, detrás del run con el que
# rehace el turno cortado. Lo que aún no lista se relee cada RECOVERY_POLL_S hasta
# RECOVERY_WINDOW_S tras la reconexión antes de darlo por perdido.
RECOVERY_WINDOW_S = 30.0
RECOVERY_POLL_S = 3.0
# Un push que acaba sin texto espera su respuesta en un `session.message` `assistant`: un cron
# ligado a la conversación no manda deltas y la respuesta llega ~0,1 s después de su `final`
# (OpenClaw 2026.11.7, docs/openclaw-protocol.md, «Live check of 5.0»).
PUSH_REPLY_GRACE_S = 3.0
_DISCONNECTED = "orchestrator_disconnected"
# 2c.3: los eventos de push que pueden abrir uno (`push_route`), y el tope de la lectura de su
# dispositivo de avisos (una lectura por clave primaria en SQLite, en un hilo).
_PUSH_OPENERS = frozenset({"turn_start", "open", "chat", "tool", "fallback"})
NOTIFY_DEVICE_READ_TIMEOUT_S = 2.0
_UNAVAILABLE = "orchestrator_unavailable"
TURN_TIMEOUT = "turn_timeout"
# Solo /v1 (2b.3): su run terminó sin abrir turno propio (`steer` lo plegó en otro run).
TURN_ABSORBED = "turn_absorbed"

MessageState = Literal["pending", "delivered", "withdrawn"]
# `absorbed`: OpenClaw (`steer`) metió su mensaje en el run activo; no tendrá turno propio.
RunState = Literal["queued", "running", "absorbed", "done"]


def _as_dict(value: object) -> dict:
    return value if isinstance(value, dict) else {}


def _str_run_id(payload: dict) -> str | None:
    run_id = payload.get("runId")
    return run_id if isinstance(run_id, str) and run_id else None


def _final_has_text(payload: dict) -> bool:
    delta = payload.get("deltaText")
    return (isinstance(delta, str) and bool(delta)) or bool(
        extract_message_text(payload.get("message"))
    )


def _ends_run(event: str, payload: dict) -> bool:
    """A frame that ends its run: a terminal `chat`, or an `agent` lifecycle `end`/`error`."""
    if event == "chat":
        return payload.get("state") in _TERMINAL_CHAT
    return (
        event == "agent"
        and payload.get("stream") in (None, "lifecycle")
        and _as_dict(payload.get("data")).get("phase") in ("end", "error")
    )


def _ends_push(kind: str, arg: Any) -> bool:
    """A push item (`push_route`) that ends the push: lifecycle `end`/`error` or a terminal
    `chat`."""
    if kind in ("turn_end", "turn_error"):
        return True
    return kind == "chat" and _as_dict(arg).get("state") in _TERMINAL_CHAT


def _lifecycle_error(event: str, payload: dict) -> bool:
    return (
        event == "agent"
        and payload.get("stream") in (None, "lifecycle")
        and _as_dict(payload.get("data")).get("phase") == "error"
    )


def _run_of_idempotency(value: object) -> str | None:
    """`"<runId>:user"` (the `idempotencyKey` of a `chat.send`'s user message) → runId."""
    if isinstance(value, str) and value.endswith(":user") and len(value) > len(":user"):
        return value[: -len(":user")]
    return None


def pending_run_ids(history: object) -> set[str] | None:
    """The runIds `chat.history` says are still waiting or running on the key: every
    `pendingInputs.items[]` in `state:"queued"` (its `runId` is the `chat.send`'s) and the
    `inFlightRun`. None when the answer has no readable `pendingInputs` (the shape was seen
    live once, 2026.11.7; read defensively): then nothing is proven."""
    if not isinstance(history, dict):
        return None
    pending = history.get("pendingInputs")
    items = pending.get("items") if isinstance(pending, dict) else None
    if not isinstance(items, list):
        return None
    ids: set[str] = set()
    for item in items:
        if not isinstance(item, dict):
            continue
        state = item.get("state")
        queued = state == "queued" or (state is None and item.get("queued") is True)
        run_id = item.get("runId")
        if queued and isinstance(run_id, str) and run_id:
            ids.add(run_id)
    in_flight = history.get("inFlightRun")
    run_id = in_flight.get("runId") if isinstance(in_flight, dict) else in_flight
    if isinstance(run_id, str) and run_id:
        ids.add(run_id)
    return ids


def _opens_run(event: str, payload: dict) -> bool:
    """Whether a frame of a run opens its turn (its first event, C3): a `chat` status, delta
    or tool, an `error`, a `final` that carries text, an `agent` lifecycle `start`/`model`/
    `fallback_step` or a `run_status`. A `final` without text (an absorbed or empty run), an
    `aborted` and the other `agent` streams open nothing."""
    if event == "session.tool":
        return True
    if event == "chat":
        state = payload.get("state")
        if state == "aborted":
            return False
        if state == "final":
            return _final_has_text(payload)
        return True
    if event == "agent":
        stream = payload.get("stream")
        if stream == "run_status":
            return True
        if stream in (None, "lifecycle"):
            return _as_dict(payload.get("data")).get("phase") in ("start", "model", "fallback_step")
    return False


def _other_run(open_run: str | None, run_id: object) -> bool:
    """El frame es de otro run que el del push abierto (los dos con runId y distintos)."""
    return isinstance(open_run, str) and isinstance(run_id, str) and run_id != open_run


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

    @property
    def wants_push(self) -> bool:
        """It takes push turns (`push_enabled`); 2a.2."""
        ...

    @property
    def tracker(self) -> PipelineTracker | None:
        """Where the channel records the pipeline of a turn this session sent; 2a.2."""
        ...

    @property
    def wants_progress(self) -> bool:
        """It shows `turn_progress` (#254: `status` in its output mode); 2a.2."""
        ...

    def agent_allowed(self, session_key: str) -> bool:
        """The session may use the agent of `session_key` (`allowed_agents`); 2a.2."""
        ...

    def on_turn_start(self, turn: ChannelTurn) -> None: ...

    def on_event(self, turn: ChannelTurn, event: ChannelEvent) -> None: ...

    def on_turn_end(self, turn: ChannelTurn, reason: EndReason) -> None: ...

    def on_message(self, message: ChannelMessage) -> None:
        """A user message of the conversation and its state (2b): every subscriber, the
        sender too. It carries user text: to the client only, never to logs."""
        ...


@dataclass(frozen=True)
class ChannelMessage:
    """A user message OpenClaw accepted (its `chat.send` acked) and its state, for every
    subscriber. `id` is its run's `runId` (the wire's `message.id`)."""

    id: str
    conversation_id: str
    text: str = field(repr=False)  # user text: clients only, never logs
    by_client_id: str
    by_name: str = field(repr=False)
    state: MessageState


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

    id: str  # `uuid4().hex`: the `turn_id` of /events (stable across reconnections, #412)
    conversation_id: str
    origin: TurnOrigin
    by_client_id: str | None  # the durable origin: the sender's client (None for a push)
    run_id: str | None = None  # OpenClaw's runId, once known (push: its `agent start`)
    by_name: str | None = field(default=None, repr=False)  # the sender's client name
    ended: bool = False
    end_reason: EndReason | None = None
    cancel_reason: EndReason | None = None  # quien la cancela dice por qué (G-08)
    started_at: float = field(default_factory=time.monotonic)
    progress_at: float = field(default_factory=time.monotonic)  # presupuesto del push (#286)
    fallback_reason: str | None = None  # push: la del último `fallback_step` de su run (#397)
    # Push cuyo run acabó sin texto: hasta cuándo espera su respuesta (`session.message`).
    reply_until: float | None = None
    # Its task broke (an unexpected exception, `internal_error`): nobody waits for its audio.
    crashed: bool = False
    # Decision 5: only a weak reference to the sender; the live session is resolved on use
    # (if it left, nobody gets the audio).
    _sender: weakref.ref[ChannelSubscriber] | None = field(default=None, repr=False)
    # The OpenClaw key of its conversation: a session that gets it as a guest (a push for its
    # notify device, 2c.3) checks its agent against this one, not its own conversation's.
    session_key: str | None = None
    # Its conversation (home, owner): a guest re-checks `visible_to` against it (2c.3, the third
    # defence after `notify_client_for` and `_may_get_push`).
    conversation: Conversation | None = field(default=None, repr=False)
    # Who plays the turn's audio: the sender (user turn) or, for a push, the session of the
    # conversation's notify device, if it shows audio (2c.3); nobody else.
    _audio: weakref.ref[ChannelSubscriber] | None = field(default=None, repr=False)
    # 2b.3: who collects the turn's events outside the subscribers (a /v1 request's answer).
    _sink: Callable[[ChannelEvent], None] | None = field(default=None, repr=False)
    _text: list[str] = field(default_factory=list, init=False, repr=False)

    @property
    def by_session(self) -> ChannelSubscriber | None:
        """The session that sent this turn, if it is still alive; None for a push."""
        return self._sender() if self._sender is not None else None

    @property
    def audio_session(self) -> ChannelSubscriber | None:
        """The session that plays this turn's audio, if it is still alive."""
        return self._audio() if self._audio is not None else None

    @property
    def text(self) -> str:
        """The text the turn has carried so far (tokens), for a late subscriber (2a.3)."""
        return "".join(self._text)


class _SenderTracker:
    """The sender's pipeline tracker, only while the sender is still subscribed: a session
    that left gets no `llm_*` events (nothing logged after its session_end, nothing sent to
    its closed socket). Without a sender (a /v1 message, 2b.3) it records while its caller
    waits for the run, and the caller `flush`es it before closing the tracker."""

    def __init__(
        self,
        channel: ConversationChannel,
        sender: ChannelSubscriber | None,
        tracker: PipelineTracker,
    ) -> None:
        self._channel = channel
        self._sender = weakref.ref(sender) if sender is not None else None
        self._tracker = tracker
        self._soon: deque[tuple[str, dict[str, Any]]] = deque()
        self._task: asyncio.Task | None = None

    def _live(self) -> bool:
        if self._sender is None:
            return True
        sender = self._sender()
        return sender is not None and self._channel.is_subscribed(sender)

    async def record(self, stage: str, **meta: Any) -> Any:
        if not self._live():
            return None
        return await self._tracker.record(stage, **meta)

    def start_turn(self) -> None:
        if self._live():
            self._tracker.start_turn()

    def record_soon(self, stage: str, **meta: Any) -> None:
        """Record without waiting, in order (the channel handles its runs' frames
        synchronously): one supervised task drains them."""
        self._soon.append((stage, meta))
        if self._task is None or self._task.done():
            self._task = self._channel._hub.spawn(self._drain(), "llm_events")

    async def _drain(self) -> None:
        while self._soon:
            stage, meta = self._soon.popleft()
            await self.record(stage, **meta)

    async def flush(self) -> None:
        """Wait until every event recorded so far is written (before the tracker closes)."""
        task = self._task
        if task is not None and not task.done():
            await asyncio.wait({task})


@dataclass(eq=False)
class RunInfo:
    """One of the channel's own runs (a `chat.send` it made; decision 5). Ids and states;
    the message text only for the subscribers (never in logs, never in its `repr`)."""

    run_id: str  # OpenClaw's runId from the ack: also the message's id on the wire
    by_client_id: str
    by_name: str = field(repr=False)
    text: str = field(repr=False)
    finished: asyncio.Future[None] = field(repr=False)  # done once the run is `done`
    state: RunState = "queued"
    message: MessageState = "pending"
    turn: ChannelTurn | None = None  # its channel turn, once its first event opened it
    progress_at: float = field(default_factory=time.monotonic)
    fallback_reason: str | None = None  # the last `fallback_step` failure reason (#397)
    tokens: int = 0
    absorbed_into: str | None = None  # `absorbed`: the run `steer` folded it into
    # Tras una caída de OpenClaw, en cola sin confirmar: espera a `pendingInputs` (2b.2).
    unconfirmed: bool = False
    chat_text: ChatTextTracker = field(default_factory=ChatTextTracker, repr=False)
    recorder: _SenderTracker | None = field(default=None, repr=False)
    # 2b.3, a /v1 message: who collects its turn's events (`send_exclusive`), and whether the
    # channel counts its turn when it opens (/v1 counts it on the ack, `on_accepted`).
    sink: Callable[[ChannelEvent], None] | None = field(default=None, repr=False)
    counts_turn: bool = True
    error_code: str | None = None  # the stable code its turn ended with, if it failed
    # Decision 5: only a weak reference to the sender; the live session is resolved on use.
    _sender: weakref.ref[ChannelSubscriber] | None = field(default=None, repr=False)

    @property
    def by_session(self) -> ChannelSubscriber | None:
        return self._sender() if self._sender is not None else None


@dataclass(eq=False)
class SendTicket:
    """One `send` of a session on its way to the channel (2b.3 review N2). `cut`: a client
    `cancel` read after it covers it (it is not sent, or its run is aborted on its ack, never
    announced); `sending`: it is past the send lock, in `chat.send`."""

    cut: bool = False
    sending: bool = False


class ChannelBusy(RuntimeError):
    """`open_turn` with a turn still open: a conversation has one open turn at a time."""


@dataclass(frozen=True)
class ExclusiveOutcome:
    """How a `send_exclusive` message (a /v1 request, 2b.3) ended: `error`, the stable code
    its turn failed with (or why it was never sent); `cut`, someone else stopped it (a client
    `cancel`, the channel closing). Neither: its run ended normally."""

    error: str | None = None
    cut: bool = False


@dataclass(eq=False)
class _Subscription:
    subscriber: ChannelSubscriber
    active: bool = True
    # 2c.3: a guest of one push turn (the notify device's session, not attached to this
    # channel): it gets that turn's start, events and end, and nothing else (no messages, no
    # other turn). Only in `_started`, never in `_subscriptions`.
    guest: bool = False


@dataclass
class _PushBacklog:
    """Eventos de push encolados y aún sin procesar (#130 S3a)."""

    pending: int = 0
    peak: int = 0
    since: float | None = None
    queue: asyncio.Queue[tuple[str, Any, str | None]] = field(default_factory=asyncio.Queue)


class ConversationChannel:
    """One conversation's channel: its subscribers, its open turn and its OpenClaw runs."""

    def __init__(self, conversation: Conversation, hub: ChannelHub | None = None) -> None:
        self.conversation = conversation
        self.session_key = conversation.session_key
        self.current_turn: ChannelTurn | None = None
        self._subscriptions: list[_Subscription] = []
        self._started: list[_Subscription] = []  # those that got the open turn's start
        self._hub = hub if hub is not None else ChannelHub()
        self._tag = f"conv {conversation.id}"  # log prefix: ids only, never text
        # Serializa los envíos (y el corte de un `send` que interrumpe): llegan a OpenClaw en
        # el orden en que llegan al canal (G-08, #201; C3: dos aparatos a la vez, en orden).
        self._lock = asyncio.Lock()
        # Quienes tienen o esperan el lock (un lock recién soltado aún no es de quien espera:
        # `idle` no puede fiarse de `locked()`).
        self._senders = 0
        self._exclusive = 0  # mensajes de /v1 esperando o con su run en marcha (2b.3)
        # Los runs propios sin terminar, en el orden de sus acks (decisión 5).
        self._runs: dict[str, RunInfo] = {}
        self._done_runs: OrderedDict[str, None] = OrderedDict()
        # `chat.send` sin ack todavía, y los frames de runs que nadie conoce mientras tanto
        # (decisión 3): el ack dice de quién son.
        self._acks = 0
        self._held: list[tuple[str, dict]] = []
        # Lo que espera a que se cierre el turno abierto (un turno a la vez), en orden.
        self._deferred: deque[tuple[str, Any, str | None]] = deque()
        self._deferred_runs: set[str] = set()
        # TURN_QUEUE_MAX (final 2b review, m3): runs whose frames did not fit and that fail
        # with `internal_error`; runs unknown yet whose held frames did not fit (see the ack).
        self._overflowed: set[str] = set()
        self._held_dropped: set[str] = set()
        self._defer_push = False
        self._replaying = False
        self._push: ChannelTurn | None = None
        self._ended_push_runs: OrderedDict[str, None] = OrderedDict()
        # `session.message` (2b.2): oc_message_id → runId de su `chat.send`.
        self._oc_messages: OrderedDict[str, str] = OrderedDict()
        # Tras una caída: hasta cuándo esperan los runs sin confirmar, y con qué código fallan.
        self._lost_deadline: float | None = None
        self._reconnected = True  # False desde una caída hasta que OpenClaw vuelve
        self._recovery_until: float | None = None  # fin de RECOVERY_WINDOW_S tras reconectar
        self._lost_code = _DISCONNECTED
        # Runs dados por fallidos sin conexión: se abortan (por runId) al reconectar.
        self._abort_on_reconnect: set[str] = set()
        self._push_text = ChatTextTracker()  # #241: texto ya emitido en la respuesta del push
        # 2c.3: el dispositivo de avisos de la conversación (un client id), leído de la BD justo
        # antes de cada evento que abriría un push (`_read_notify_device`); None = ninguno.
        self._notify_client_id: str | None = None
        # #254: runId → (última fase de arranque, cuándo) de runs sin push abierto todavía.
        self._early_phases: OrderedDict[str, tuple[ProgressPhase, float]] = OrderedDict()
        # Worker (#130 S3a): el dispatcher encola sin esperar desde el bucle de OpenClaw; un
        # solo worker por canal conserva el orden de todos los frames de la clave.
        self._push_worker: asyncio.Task | None = None
        self._backlog = _PushBacklog()
        # 2b.3: los mensajes de /v1 que esperan a que el canal quede en calma
        # (`send_exclusive`); se despiertan cada vez que algo termina.
        self._quiet_waiters: list[asyncio.Future[None]] = []
        # Una vez puesto no vuelve atrás (el hub lo cierra o lo olvida): ni turnos ni avisos.
        self._closed = False

    @property
    def orchestrator(self) -> OrchestratorProtocol:
        orchestrator = self._hub.orchestrator
        if orchestrator is None:
            raise RuntimeError("channel hub without an orchestrator")
        return orchestrator

    @property
    def _turn_registry(self) -> TurnRegistry:
        return self._hub.turn_registry

    # -- subscribers -------------------------------------------------------------------

    def subscribers(self) -> list[ChannelSubscriber]:
        return [s.subscriber for s in self._subscriptions]

    def _subscription(self, subscriber: ChannelSubscriber) -> _Subscription | None:
        return next((s for s in self._subscriptions if s.subscriber is subscriber), None)

    def subscribe(self, subscriber: ChannelSubscriber) -> bool:
        """Attach a session. With a turn open, it gets `on_turn_start` for it; then the
        messages still pending (2b). False if it was already subscribed or belongs to another
        conversation."""
        # Whether the device may see the conversation (`Conversation.visible_to`) is checked by
        # the caller (`TurnController.attach`, 2a.3); here, the conversation id and the
        # private-owner safety net.
        if subscriber.conversation_id != self.conversation.id:
            logger.debug(
                "Channel subscribe refused: conversation=%s client=%s (other conversation)",
                self.conversation.id, subscriber.client.id,
            )
            return False
        owner = self.conversation.owner_user_id
        if owner is not None and subscriber.client.user_id != owner:
            # M20: lo privado nunca a un dispositivo del hogar ni a uno de otra persona. Quien
            # abre una conversación ya pasó `visible_to`: esto es la red de seguridad que antes
            # tenía `ClientRegistry.route`.
            logger.warning(
                "Channel subscribe refused: conversation=%s client=%s (private, not its owner)",
                self.conversation.id, subscriber.client.id,
            )
            return False
        if self._subscription(subscriber) is not None:
            return False
        guest = self._guest(subscriber)
        if guest is not None:
            # 2c.3: it already has the open push as a guest (it plays it): it stays in that
            # turn and is attached from now on, without a second `on_turn_start`.
            guest.guest = False
            self._subscriptions.append(guest)
            for info in list(self._runs.values()):
                if info.message == "pending" and guest.active:
                    self._tell(guest, self._message(info))
            return True
        subscription = _Subscription(subscriber)
        self._subscriptions.append(subscription)
        logger.debug(
            "Channel subscribe: conversation=%s client=%s subscribers=%d",
            self.conversation.id, subscriber.client.id, len(self._subscriptions),
        )
        turn = self.current_turn
        if turn is not None and turn.cancel_reason is None:
            # Uno que se está cortando no se le enseña a quien llega.
            self._start(turn, subscription)
        for info in list(self._runs.values()):
            if info.message == "pending" and subscription.active:
                self._tell(subscription, self._message(info))
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

    def _guest(self, subscriber: ChannelSubscriber) -> _Subscription | None:
        return next(
            (s for s in self._started if s.guest and s.subscriber is subscriber), None
        )

    def drop_guest(self, subscriber: ChannelSubscriber) -> bool:
        """A guest of the open push (2c.3) leaves it: it gets nothing more of it, not even its
        `on_turn_end` (its session ends its own copy). False if it was not a guest."""
        guest = self._guest(subscriber)
        if guest is None:
            return False
        guest.active = False
        self._started.remove(guest)
        logger.debug(
            "Channel guest left: conversation=%s client=%s",
            self.conversation.id, subscriber.client.id,
        )
        return True

    # -- turns -------------------------------------------------------------------------

    def open_turn(
        self,
        origin: TurnOrigin,
        *,
        sender: ChannelSubscriber | None = None,
        run_id: str | None = None,
        audio_to: ChannelSubscriber | None = None,
        by_client_id: str | None = None,
        by_name: str | None = None,
        sink: Callable[[ChannelEvent], None] | None = None,
        guests: Sequence[ChannelSubscriber] = (),
    ) -> ChannelTurn:
        """Open the channel's turn and give every subscriber its `on_turn_start`. Raises
        `ChannelBusy` while another turn is open (end it first). `audio_to` (default: the
        sender) is who plays its audio; `by_client_id`/`by_name` (default: the sender's) say
        who sent it, also once that session is gone; `sink` also gets every event of the turn
        (a /v1 request's answer, 2b.3); `guests` (2c.3, a push for the notify device's session
        when it is not attached) get this turn, and only this one, as if attached."""
        if self.current_turn is not None:
            raise ChannelBusy(self.conversation.id)
        audio = audio_to if audio_to is not None else sender
        if by_client_id is None and sender is not None:
            by_client_id, by_name = sender.client.id, sender.client.name
        turn = ChannelTurn(
            id=uuid4().hex,
            conversation_id=self.conversation.id,
            origin=origin,
            by_client_id=by_client_id,
            run_id=run_id,
            by_name=by_name,
            session_key=self.session_key,
            conversation=self.conversation,
            _sender=weakref.ref(sender) if sender is not None else None,
            _audio=weakref.ref(audio) if audio is not None else None,
            _sink=sink,
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
        for guest in guests:
            if turn is not self.current_turn:
                break
            if self._subscription(guest) is None:
                self._start(turn, _Subscription(guest, guest=True))
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
        sink = turn._sink
        if sink is not None:
            try:
                sink(event)
            except Exception as exc:  # noqa: BLE001 — boundary: a broken /v1 collector must not stop the fan-out
                logger.warning(
                    "Channel sink failed: conversation=%s turn=%s error=%s",
                    self.conversation.id, turn.id, describe_error(exc),
                )
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
        channel's open turn. What waited for the turn to close goes on right after."""
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
        self._settle_absorbed(turn.run_id)
        self._touch_head()
        self._replay()
        self._wake_quiet()
        return True

    @property
    def idle(self) -> bool:
        """Nothing keeps the channel alive: no subscribers, no open turn, no run of its own
        unfinished (queued, running or waiting for its ack), nobody sending or waiting to send
        (the send lock, a /v1 message waiting for calm), nothing waiting for the worker and no
        `chat.abort` promised for the next reconnection (`_abort_on_reconnect`)."""
        return (
            not self._subscriptions
            and self.current_turn is None
            and not self._runs
            and self._acks == 0
            and self._senders == 0
            and not self._quiet_waiters
            and not self._held
            and self._backlog.pending == 0
            and not self._deferred
            and not self._abort_on_reconnect
        )

    # -- state for the sessions (watchdogs, /system/state) ------------------------------

    def user_turn_running(self) -> bool:
        """A message of this channel is still in flight: waiting for its ack, queued in
        OpenClaw or with its reply running."""
        return bool(self._runs) or self._acks > 0

    def push_backlog(self) -> tuple[int, int, float | None]:
        """(pending, peak, since) of the worker's queue (#219: numbers only)."""
        backlog = self._backlog
        return backlog.pending, backlog.peak, backlog.since

    def is_subscribed(self, subscriber: ChannelSubscriber) -> bool:
        return self._subscription(subscriber) is not None

    def pending_messages(self) -> list[ChannelMessage]:
        """Its messages still waiting for their turn, in order (`GET /conversations/{id}`)."""
        return [self._message(info) for info in self._runs.values() if info.message == "pending"]

    def _note_turn(self) -> None:
        """Cuenta un turno de la conversación (una vez por turno del canal)."""
        note = self._hub.note_turn
        if note is not None:
            note(self.conversation.id)

    # -- messages (2b) -----------------------------------------------------------------

    def _message(self, info: RunInfo) -> ChannelMessage:
        return ChannelMessage(
            id=info.run_id,
            conversation_id=self.conversation.id,
            text=info.text,
            by_client_id=info.by_client_id,
            by_name=info.by_name,
            state=info.message,
        )

    def _announce(self, info: RunInfo, state: MessageState) -> None:
        """The message's new state, to every subscriber (the sender too)."""
        info.message = state
        message = self._message(info)
        logger.debug(
            "Channel message: conversation=%s id=%s state=%s text_len=%d subscribers=%d",
            self.conversation.id, info.run_id, state, len(info.text), len(self._subscriptions),
        )
        for subscription in list(self._subscriptions):
            if subscription.active:
                self._tell(subscription, message)

    def _tell(self, subscription: _Subscription, message: ChannelMessage) -> None:
        on_message = getattr(subscription.subscriber, "on_message", None)
        if on_message is not None:
            self._call(subscription, "on_message", partial(on_message, message))

    # -- sending (2b: OpenClaw's queue) ------------------------------------------------

    async def send_user_message(
        self,
        sender: ChannelSubscriber,
        text: str,
        *,
        interrupt: bool = False,
        ticket: SendTicket | None = None,
    ) -> RunInfo | str | None:
        """Send `text` from `sender` to OpenClaw (`chat.send`), never aborting anything first:
        during a turn it waits in OpenClaw's queue. With `interrupt` («send now»), the active
        run alone is aborted first (its turn ends `interrupted` for everyone) and the queue is
        left as it is. Behind a /v1 message (`send_exclusive`) it waits, in arrival order, until
        that message's run has ended: OpenClaw's `steer` would fold it into /v1's run.

        Returns the accepted message's `RunInfo` (its turn opens with its run's first event);
        a stable error code if OpenClaw did not take it (the session answers its `send` alone,
        `TurnController.start_user_turn`); or None if nothing was sent: the channel is closing,
        `sender` left while waiting (G-09), `sender` may not use this agent (#397: the
        channel is left untouched), or a `cancel` cut its `ticket` (before the `chat.send`,
        or before its ack: then its run is aborted by its runId and never announced)."""
        if self._closed:
            return None
        if not sender.agent_allowed(self.session_key):
            return None
        self._senders += 1
        try:
            async with self._lock:
                if interrupt and not self._closed and self.is_subscribed(sender):
                    await self._interrupt()
                if self._closed or not self.is_subscribed(sender):
                    return None  # close_all() pudo empezar durante la espera
                if ticket is not None:
                    if ticket.cut:
                        return None  # un `cancel` lo cubrió antes de salir
                    ticket.sending = True
                return await self._chat_send(
                    text,
                    sender=sender,
                    by_client_id=sender.client.id,
                    by_name=sender.client.name,
                    tracker=sender.tracker,
                    ticket=ticket,
                )
        finally:
            self._senders -= 1

    async def send_exclusive(
        self,
        text: str,
        *,
        by_client_id: str,
        by_name: str,
        sink: Callable[[ChannelEvent], None],
        tracker: PipelineTracker | None = None,
        on_accepted: Callable[[], Coroutine[Any, Any, None]] | None = None,
    ) -> ExclusiveOutcome:
        """A message whose answer must be its own run's (a /v1 request, 5.0 PR 2b.3): it never
        steers into another run, nor lets another message steer into its own.

        `queueMode` is untried live, so the channel guarantees it instead: holding the send
        lock (WebSocket sends and other /v1 messages queue behind it, in arrival order), it
        waits until the channel is calm (no open turn, no unfinished run of its own, no ack in
        flight, nothing deferred or waiting for the worker), sends, and keeps the lock until its
        run has ended. Its message and its turn reach the subscribers like any other (origin
        `by_client_id`/`by_name`; nobody plays its audio); `sink` gets its turn's events (the
        answer). `on_accepted` runs once OpenClaw acked it (/v1 counts its turn there).

        Cancelling the caller (the HTTP client left) cuts only its own run: its turn ends
        `cancelled` for every subscriber and `chat.abort` goes by its `runId`, never the key;
        so does a failing `on_accepted`. If it cannot be sent within `TURN_TIMEOUT_S` (the lock
        and the calm), it gives up with `turn_timeout`. A run that ends without ever opening
        its own turn (`steer` folded it into a run that started after its `chat.send`, e.g. a
        push) is `turn_absorbed`: its answer went to someone else's turn."""
        self._senders += 1
        self._exclusive += 1
        loop = asyncio.get_running_loop()
        deadline = loop.time() + settings.TURN_TIMEOUT_S
        try:
            try:
                async with asyncio.timeout_at(deadline):
                    await self._lock.acquire()
            except TimeoutError:
                logger.warning(f"[{self._tag}] /v1 sin turno en TURN_TIMEOUT_S: no se envía")
                return ExclusiveOutcome(error=TURN_TIMEOUT)
            try:
                try:
                    async with asyncio.timeout_at(deadline):
                        await self._until_quiet()
                except TimeoutError:
                    logger.warning(f"[{self._tag}] /v1 sin calma en TURN_TIMEOUT_S: no se envía")
                    return ExclusiveOutcome(error=TURN_TIMEOUT)
                if self._closed:
                    return ExclusiveOutcome(error=_UNAVAILABLE)
                result = await self._chat_send(
                    text,
                    sender=None,
                    by_client_id=by_client_id,
                    by_name=by_name,
                    tracker=tracker,
                    sink=sink,
                    counts_turn=False,
                )
                if isinstance(result, str):
                    return ExclusiveOutcome(error=result)
                if result is None:
                    return ExclusiveOutcome(error=_UNAVAILABLE)
                info = result
                try:
                    if on_accepted is not None:
                        await on_accepted()
                    await asyncio.wait({info.finished})
                except BaseException:
                    # Quien espera se va (o su `on_accepted` falló): su run no sigue solo.
                    await self._cut_own(info)
                    raise
                if info.recorder is not None:
                    await info.recorder.flush()
                if info.finished.cancelled():
                    return ExclusiveOutcome(error=info.error_code, cut=info.error_code is None)
                if info.error_code is None and info.turn is None:
                    logger.warning(f"[{self._tag}] run {info.run_id} de /v1 absorbido: sin respuesta propia")
                    return ExclusiveOutcome(error=TURN_ABSORBED)
                return ExclusiveOutcome(error=info.error_code)
            finally:
                self._lock.release()
        finally:
            self._senders -= 1
            self._exclusive -= 1
            # Nadie más lo va a mirar si no tiene suscriptores (el de un origen de confianza).
            self._hub.drop_if_idle(self.session_key, self)

    @property
    def nothing_ahead(self) -> bool:
        """No open turn and no unfinished run of the channel's own: a message sent now is the
        next thing to start (what a client `cancel` stops, like `cancel_head`)."""
        return self.current_turn is None and not self._runs

    @property
    def exclusive_pending(self) -> bool:
        """A /v1 message waits for its turn or has its run going: a session's sends wait
        behind it (2b.3)."""
        return self._exclusive > 0

    async def interrupt_now(self) -> None:
        """«Enviar ahora» de una sesión cuyo envío aún espera su turno (detrás de /v1, p. ej.):
        el turno abierto del canal (o el mensaje entregado que aún no empezó) se corta ya,
        sin esperar al lock de envío (2b.3); el envío sale después, en su orden."""
        if not self._closed:
            await self._interrupt()

    def _orphan_ack(self, sending: asyncio.Future[str]) -> None:
        """The ack of a `chat.send` whose sender left (I3): its run is tombstoned and
        aborted by its runId, and what was held for it is dropped; the rest is released."""
        self._acks -= 1
        run_id: str | None = None
        if not sending.cancelled() and sending.exception() is None:
            run_id = sending.result()
        if run_id is not None:
            logger.info(f"[{self._tag}] ack de un envío abandonado: chat.abort de su run {run_id}")
            self._abort_run(run_id)
            self._held = [(e, p) for e, p in self._held if _str_run_id(p) != run_id]
            self._hub.spawn(self._abort(run_id), "chat_abort")
        self._release_held()
        self._wake_quiet()
        self._hub.drop_if_idle(self.session_key, self)

    def _quiet(self) -> bool:
        """Nothing in the channel could steer a new message into another run, or be steered
        into it: no open turn, no unfinished run of its own, no ack in flight, nothing held,
        deferred or waiting for the worker."""
        return (
            self.current_turn is None
            and not self._runs
            and self._acks == 0
            and not self._held
            and not self._deferred
            and self._backlog.pending == 0
        )

    async def _until_quiet(self) -> None:
        loop = asyncio.get_running_loop()
        while not self._closed and not self._quiet():
            waiter: asyncio.Future[None] = loop.create_future()
            self._quiet_waiters.append(waiter)
            try:
                await waiter
            finally:
                if waiter in self._quiet_waiters:
                    self._quiet_waiters.remove(waiter)

    def _wake_quiet(self) -> None:
        """Something ended: whoever waits for calm looks again (`_until_quiet`)."""
        waiters, self._quiet_waiters = self._quiet_waiters, []
        for waiter in waiters:
            if not waiter.done():
                waiter.set_result(None)

    async def _cut_own(self, info: RunInfo) -> None:
        """The caller of `send_exclusive` left: its run (only it) is aborted by its `runId`.
        With its turn open, the turn ends `cancelled` for everyone; before it opened, its
        message is `withdrawn`."""
        if info.state == "done":
            return
        if info.state == "absorbed":
            # Su mensaje ya se entregó dentro de otro turno: ni `withdrawn` ni abort (ese run
            # es de otro); solo deja de esperarse.
            self._finish(info, cut=True)
            return
        turn = info.turn
        if turn is not None and turn is self.current_turn:
            await self.cancel("cancelled", expected=turn)
            return
        logger.info(f"[{self._tag}] run {info.run_id} de /v1 cortado: chat.abort")
        self._abort_run(info.run_id)
        if turn is None and info.message != "withdrawn":
            self._announce(info, "withdrawn")
        self._finish(info, cut=True)
        await self._abort_step(info.run_id, "chat.abort de un run de /v1")

    async def _chat_send(
        self,
        text: str,
        *,
        sender: ChannelSubscriber | None,
        by_client_id: str,
        by_name: str,
        tracker: PipelineTracker | None,
        sink: Callable[[ChannelEvent], None] | None = None,
        counts_turn: bool = True,
        ticket: SendTicket | None = None,
    ) -> RunInfo | str | None:
        """The `chat.send` itself, with the send lock held: its ack registers the run and
        announces its message (`pending` behind an open turn or another run, else
        `delivered`). A stable code if OpenClaw did not take it; None if the channel closed
        while waiting for the ack (the run is aborted)."""
        recorder = _SenderTracker(self, sender, tracker) if tracker is not None else None
        code: str | None = None
        run_id = ""
        accepted = False
        self._acks += 1
        orphaned = False
        sending = asyncio.ensure_future(self.orchestrator.send(text, self.session_key))
        try:
            run_id = await asyncio.shield(sending)
            accepted = True
        except asyncio.CancelledError:
            # Quien envía se va antes del ack (/v1 que se desconecta, un close_all): el
            # `chat.send` ya salió. Su ack, cuando llegue, entierra y aborta ese run; hasta
            # entonces lo retenido sigue retenido (sus frames no son un push).
            orphaned = True
            sending.add_done_callback(self._orphan_ack)
            raise
        except orchestration.OrchestratorError as e:
            code = str(e)  # siempre un código estable (#228)
        except Exception as e:  # noqa: BLE001 — frontera: un envío roto no cierra el canal
            # Solo el tipo: el mensaje puede llevar texto del usuario.
            logger.error(f"[{self._tag}] chat.send falló: {type(e).__name__}")
            logger.debug(f"[{self._tag}] chat.send: {traceback_no_message(e)}")
            code = "internal_error"
        finally:
            if not orphaned:
                self._acks -= 1
                if not accepted:
                    # Sin run propio (un fallo): lo retenido no espera más; lo que no es de
                    # nadie va al push.
                    self._release_held()
        if code is not None:
            logger.error(f"[{self._tag}] Orchestrator error: {code}")
            if tracker is not None:
                tracker.start_turn()
            gateway_stats.turn_started("normal")
            gateway_stats.turn_error(code)
            return code
        if self._closed:
            # El canal se cerró mientras se esperaba el ack: el run no se registra (nadie
            # resolvería su `finished`); se aborta, solo él, y lo retenido se suelta.
            logger.info(f"[{self._tag}] ack de un canal ya cerrado: chat.abort de su run")
            self._abort_run(run_id)
            self._held.clear()
            await self._abort_step(run_id, "chat.abort de un run tras el cierre")
            return None
        if ticket is not None and ticket.cut:
            # Un `cancel` del cliente llegó mientras se esperaba el ack (N2): el run no se
            # registra ni se anuncia; se entierra y se aborta (solo él), y lo suyo retenido se
            # descarta.
            logger.info(f"[{self._tag}] envío cancelado antes de su ack: chat.abort de {run_id}")
            self._abort_run(run_id)
            self._held = [(e, p) for e, p in self._held if _str_run_id(p) != run_id]
            self._release_held()
            await self._abort_step(run_id, "chat.abort de un envío cancelado")
            return None
        ahead = self.current_turn is not None or bool(self._runs)
        info = RunInfo(
            run_id=run_id,
            by_client_id=by_client_id,
            by_name=by_name,
            text=text,
            finished=asyncio.get_running_loop().create_future(),
            recorder=recorder,
            sink=sink,
            counts_turn=counts_turn,
            _sender=weakref.ref(sender) if sender is not None else None,
        )
        self._runs[run_id] = info
        logger.debug(
            f"[{self._tag}] mensaje aceptado: run={run_id} "
            f"{'pendiente' if ahead else 'entregado'} text_len={len(text)}"
        )
        overflowed = run_id in self._held_dropped
        if overflowed:
            # Parte de lo suyo retenido no cupo (TURN_QUEUE_MAX): no se pinta a medias.
            self._held_dropped.discard(run_id)
            self._held = [(e, p) for e, p in self._held if _str_run_id(p) != run_id]
        self._announce(info, "pending" if ahead else "delivered")
        self._release_held()
        if overflowed:
            self._overflow_run(run_id)
        self._wake()
        return info

    async def _interrupt(self) -> None:
        """«Enviar ahora»: aborta el run activo (solo ese, por su runId) y su turno termina
        `interrupted` para todos; la cola de OpenClaw queda como está."""
        turn = self.current_turn
        if turn is not None:
            await self.cancel("interrupted", expected=turn)
        else:
            await self.cancel_head("interrupted")

    async def cancel(
        self, reason: EndReason, *, expected: ChannelTurn | None = None
    ) -> ChannelTurn | None:
        """Para el turno abierto (o `expected`, si sigue abierto), lo haya iniciado quien lo
        haya iniciado: su run (solo ese) se aborta en OpenClaw y su turno termina con `reason`
        para todos (también un push). Dos llamadas a la vez lo cancelan una vez
        (G-08: el cambio de estado es síncrono). Devuelve el turno cancelado."""
        turn = expected if expected is not None else self.current_turn
        if turn is None or turn.ended or turn is not self.current_turn:
            return None
        if turn.origin == "push":
            return await self._cut_push(turn, reason)
        run_id = turn.run_id
        info = self._runs.get(run_id) if run_id is not None else None
        # Antes del abort: nada más de su run llega (#285), tampoco su `aborted`.
        self._abort_run(run_id)
        self._mark_cancel(turn, reason)
        if info is not None:
            self._finish(info, cut=True)
        self.end_turn(turn, reason)
        await self._abort_step(run_id, "chat.abort del turno")
        return turn

    async def cancel_head(self, reason: EndReason) -> bool:
        """Sin turno abierto: el siguiente run propio, cuyo turno aún no se abrió (nada va
        delante), se aborta y su mensaje queda `withdrawn` para todos (no llegó a tener turno).
        Vale entregado o aún `pending`: entre dos turnos OpenClaw ya lo está corriendo aunque
        su primer evento no haya llegado (revisión final de la 2b, m1). `withdraw` sigue
        siendo solo para los pendientes."""
        head = self._head()
        if head is None or head.message == "withdrawn":
            return False
        logger.info(f"[{self._tag}] run {head.run_id} sin empezar: chat.abort ({reason})")
        self._abort_run(head.run_id)
        self._announce(head, "withdrawn")
        self._finish(head, cut=True)
        await self._abort_step(head.run_id, "chat.abort del run sin empezar")
        return True

    async def withdraw(self, message_id: str) -> bool:
        """Quita un mensaje que aún espera su turno (`pending`), lo envíe quien lo envíe:
        `chat.abort` de su runId (solo ese) y `withdrawn` para todos. False (nada cambia) si
        no está pendiente: ya entregado, ya quitado, de otra conversación o desconocido."""
        info = self._runs.get(message_id)
        if info is None or info.turn is not None or info.message != "pending":
            return False
        logger.info(f"[{self._tag}] mensaje {message_id} quitado de la cola")
        self._abort_run(message_id)
        self._announce(info, "withdrawn")
        self._finish(info, cut=True)
        await self._abort_step(message_id, "chat.abort de un pendiente")
        return True

    def _mark_cancel(self, turn: ChannelTurn, reason: EndReason) -> None:
        # Desde aquí el turno ya no cuenta progreso ni suena (#254, A-2): los suscriptores lo
        # ven en `cancel_reason` mientras su sesión lo cierra.
        if turn.cancel_reason is None:
            turn.cancel_reason = reason

    async def _abort_step(self, run_id: str | None, what: str) -> None:
        # Termina aunque cancelen a quien llama (#221); esa cancelación se relanza después.
        td = Teardown(f"[{self._tag}] {what}")
        await td.step(self._abort(run_id), what)
        td.raise_if_cancelled()

    async def _abort(self, run_id: str | None) -> None:
        """`chat.abort` de un run (solo ese). Sin runId (un push sin él) no se aborta nada:
        abortar la clave entera se llevaría también los mensajes en la cola (2b); el turno ya
        se cerró aquí."""
        if run_id is None:
            logger.warning(f"[{self._tag}] turno cortado sin runId: no se aborta nada en OpenClaw")
            return
        await self.orchestrator.abort_run(self.session_key, run_id)

    def _spawn_abort(self, run_id: str | None) -> None:
        if run_id is None:
            logger.warning(f"[{self._tag}] turno cortado sin runId: no se aborta nada en OpenClaw")
            return
        self._hub.spawn(self._abort(run_id), "chat_abort")

    # -- frames from the dispatcher ----------------------------------------------------

    def take_frame(self, event: str, payload: dict, *, hold: bool = False) -> bool:
        """A frame (`chat`, `agent`, `session.tool`) of this channel's key. True if the channel
        took it: a frame of one of its own runs (queued for the worker) or, with `hold`, of a
        run nobody knows while a `chat.send` of this channel waits for its ack (held until the
        ack names its run; decision 3). False: not the channel's (a push)."""
        run_id = _str_run_id(payload)
        if run_id is None or self._closed:
            return False
        if run_id in self._runs or run_id in self._done_runs:
            self._enqueue(("run", (event, payload), run_id))
            return True
        if hold and self._acks > 0:
            push = self._push
            if push is not None and not push.ended:
                # El push abierto no es de nadie que espere su ack: sus frames no se retienen
                # (retenidos, su tope #286 vencería sin que le faltara progreso; 2b.2).
                if push.run_id == run_id and not any(
                    _str_run_id(held) == run_id for _, held in self._held
                ):
                    return False
                if push.run_id is None or push.run_id == run_id:
                    # Retenido (detrás de uno suyo que ya espera, para no adelantarlo): su
                    # llegada cuenta como progreso del push (#286).
                    push.progress_at = time.monotonic()
            if len(self._held) >= settings.TURN_QUEUE_MAX and not _ends_run(event, payload):
                # m3: si resulta ser de un run propio, el ack lo hace fallar (`internal_error`);
                # el fin de un run nunca se descarta (sin él su turno no se cerraría).
                logger.warning(f"[{self._tag}] demasiados frames retenidos: se descarta uno")
                self._held_dropped.add(run_id)
            else:
                self._held.append((event, payload))
            return True
        return False

    def _release_held(self) -> None:
        """Tras un ack (o un `chat.send` fallido): lo retenido de un run ya conocido va a su
        run; lo que sigue sin dueño y sin más acks pendientes, al push."""
        held, self._held = self._held, []
        for event, payload in held:
            run_id = _str_run_id(payload)
            if event == "session.message":
                if self._acks > 0:
                    self._held.append((event, payload))
                else:
                    self._enqueue(("smsg", payload, None))
            elif run_id is not None and (run_id in self._runs or run_id in self._done_runs):
                self._enqueue(("run", (event, payload), run_id))
            elif self._acks > 0:
                self._held.append((event, payload))
            else:
                self._forward_push(event, payload)
        if self._acks == 0:
            self._held_dropped.clear()  # lo que no fue de nadie era de un push

    def _forward_push(self, event: str, payload: dict) -> None:
        """Un frame que resultó no ser de un run propio: lo que el dispatcher habría hecho con
        él como push (`push_route`). Un push abierto así cuenta su tope (#286) desde ahora, no
        desde que llegó retenido."""
        route = push_route(event, payload)
        if route is None:
            return
        kind, arg = route
        self.enqueue_push(kind, arg, _str_run_id(payload))

    def take_session_message(self, payload: dict) -> None:
        """A `user` (2b.2) or `assistant` `session.message` of this key, from the dispatcher:
        to the worker, in order with the runs' frames (a steered run's empty `final` comes
        after it; a cron push's reply comes only here, `_push_reply`). It carries user or agent
        text: never logged."""
        if self._closed:
            return
        if self._acks > 0 and len(self._held) < settings.TURN_QUEUE_MAX:
            # Puede ser del `chat.send` cuyo ack aún no llegó: espera con lo retenido, en orden.
            self._held.append(("session.message", payload))
            return
        self._enqueue(("smsg", payload, None))

    def fail_run(self, run_id: str, code: str, *, crashed: bool = False) -> None:
        """One of the channel's runs failed outside OpenClaw's frames (a stable `code`): its
        turn (opened if it had none) carries TURN_ERROR `code` and ends. `crashed`: an
        unexpected failure, nobody waits for its audio. In order with its frames."""
        self._enqueue(("fail", (code, crashed), run_id))

    def connection_lost(self, code: str) -> None:
        """The OpenClaw connection dropped: every run of this channel with its turn open (or
        frames already waiting) ends with `code` (`orchestrator_disconnected`); one still in
        OpenClaw's queue waits, unconfirmed, for the reconnection to say whether it is still
        there (`reconcile_pending`), at most `RECONNECT_GRACE_S`."""
        if self._runs or self._held:
            self._enqueue(("lost", code, None))

    @property
    def needs_reconcile(self) -> bool:
        """After a reconnection: whether to re-read `pendingInputs` (someone is attached, or
        runs wait unconfirmed)."""
        return not self._closed and (
            bool(self._subscriptions) or any(i.unconfirmed for i in self._runs.values())
        )

    def reconnected(self) -> None:
        """OpenClaw is back (`ChannelHub.reconnected`): the worker decides, in order behind the
        drop (`lost`) it may not have handled yet (`_on_reconnected`)."""
        if not self._closed:
            self._enqueue(("reconnected", None, None))

    def _on_reconnected(self) -> None:
        self._reconnected = True
        if any(i.unconfirmed for i in self._runs.values()):
            # Desde ahora decide la relectura de `pendingInputs` (`_reconcile`), dentro de la
            # ventana de recuperación de OpenClaw; el tope de la caída ya no corre.
            self._lost_deadline = None
            self._recovery_until = time.monotonic() + RECOVERY_WINDOW_S
        for run_id in sorted(self._abort_on_reconnect):
            self._spawn_abort(run_id)
        self._abort_on_reconnect.clear()
        if self.needs_reconcile:
            self._hub.spawn(self.reconcile_pending(), "pending_inputs")

    async def reconcile_pending(self) -> None:
        """OpenClaw is back (`ReconnectingOpenClawClient.on_connected`): re-read the key's
        `chat.history` `pendingInputs` (a read method) and keep pending the unconfirmed runs it
        still lists; the rest end with `orchestrator_disconnected`. Unreadable: nothing is
        proven, they all end. Only ids and counts are logged."""
        if self._closed:
            return
        alive: set[str] | None
        try:
            history = await self.orchestrator.request(
                "chat.history", {"sessionKey": self.session_key, "limit": 1}
            )
            alive = pending_run_ids(history)
        except Exception as e:  # noqa: BLE001 — frontera: sin lectura, nada demuestra que siga en cola
            logger.warning(f"[{self._tag}] pendingInputs tras reconectar: {describe_error(e)}")
            alive = None
        self._enqueue(("reconcile", alive, None))

    # -- the worker --------------------------------------------------------------------

    def enqueue_push(self, kind: str, arg: Any, run_id: str | None = None) -> None:
        """Encola un evento de push para el worker (#130 S3a). Síncrono, nunca bloquea: lo
        llama el bucle de OpenClaw vía FrameDispatcher. `kind`: "turn_start" | "open" |
        "chat" | "tool" | "fallback" | "turn_end" | "turn_error" (`push_route`); `arg`: la
        session_key (turn_start, open, turn_end), el payload (chat/tool) o el `data` del `agent` (fallback/turn_error, #397);
        `run_id`: el runId del evento, si lo trae. No hace nada una vez cerrado el canal."""
        self._enqueue((kind, arg, run_id))

    def _enqueue(self, item: tuple[str, Any, str | None]) -> None:
        if self._closed:
            return
        if self._push_worker is None or self._push_worker.done():
            self._push_worker = self._hub.spawn(self._push_worker_loop(), "push_worker")
        backlog = self._backlog
        if backlog.pending == 0:
            backlog.since = time.monotonic()
        backlog.pending += 1
        backlog.peak = max(backlog.peak, backlog.pending)
        backlog.queue.put_nowait(item)

    def _wake(self) -> None:
        """El worker recalcula sus topes (un run nuevo que vigilar), salvo si es él quien llama."""
        worker = self._push_worker
        if worker is not None and worker is asyncio.current_task():
            return
        if self._active_run() is not None:
            self._enqueue(("tick", None, None))

    async def push_idle(self) -> None:
        """Espera a que el worker termine todo lo encolado (para tests)."""
        await self._backlog.queue.join()

    async def _push_worker_loop(self) -> None:
        queue = self._backlog.queue
        while not self._closed:
            budget = self._budget_left()
            # Con eventos en cola el turno no está parado: solo se espera con tope si no hay.
            if budget is not None and queue.empty():
                try:
                    item = await asyncio.wait_for(queue.get(), timeout=max(0.0, budget))
                except TimeoutError:
                    self._safely("timeout", self._on_timeouts)
                    if self._backlog.pending == 0:
                        self._hub.drop_if_idle(self.session_key, self)
                    continue
            else:
                item = await queue.get()
            try:
                if self._would_open_push(item):
                    await self._read_notify_device()
                self._handle(item)
            except Exception as e:  # noqa: BLE001 — frontera: un evento roto no para el worker
                self._log_push_failure(item[0], e)
            finally:
                backlog = self._backlog
                backlog.pending = max(0, backlog.pending - 1)
                if backlog.pending == 0:
                    backlog.since = None
                    self._wake_quiet()
                queue.task_done()
            if self._backlog.pending == 0:
                self._hub.drop_if_idle(self.session_key, self)

    def _would_open_push(self, item: tuple[str, Any, str | None]) -> bool:
        """The item is a push event that would open a push now (none open): its notify device
        is read first (2c.3). Also when it will wait for the open turn (`_must_defer`): the
        value read now is the one it opens with."""
        kind, arg, run_id = item
        if kind not in _PUSH_OPENERS or self._run_aborted(run_id) or self._defer_push:
            # Ya espera un push (leído al llegar el primero): los que van detrás no releen.
            return False
        push = self._push
        return (push is None or push.ended) and self._push_opens(kind, arg, run_id)

    async def _read_notify_device(self) -> None:
        """2c.3: who the conversation's notify device is, read from the DB (in a thread, by the
        hub's `notify_device`) once per push opener, never cached: every change (the choice,
        the device disabled or retired, its person disabled) counts from the next push, and a
        push is rare. The worker awaits it in line, so the key's frames keep their order (the
        ones behind it wait in the queue); the event loop never blocks. A failed or slow read
        is no notify device: the push still reaches the attached sessions, with no audio."""
        lookup = self._hub.notify_device
        index = self._hub.index
        indexed = index.get(self.session_key) if index is not None else None
        if lookup is None or indexed is None:
            self._notify_client_id = None
            return
        # Its home and owner from the index entry (what the DB says), not from whichever object
        # the channel was created with; `self.conversation` only if they disagree on the id.
        conversation = indexed if indexed.id == self.conversation.id else self.conversation
        try:
            # Al vencer el tope, el hilo de la lectura sigue hasta acabar (no se puede cortar);
            # su resultado se descarta.
            self._notify_client_id = await asyncio.wait_for(
                lookup(conversation), timeout=NOTIFY_DEVICE_READ_TIMEOUT_S
            )
        except Exception as e:  # noqa: BLE001 — boundary: a DB read that fails must not stop the push worker
            self._notify_client_id = None
            logger.warning(
                f"[{self._tag}] dispositivo de avisos ilegible ({describe_error(e)}): el aviso "
                "no suena en ningún sitio"
            )

    def _safely(self, what: str, step: Callable[[], None]) -> None:
        try:
            step()
        except Exception as e:  # noqa: BLE001 — frontera: un paso roto no para el worker
            self._log_push_failure(what, e)

    def _log_push_failure(self, what: str, e: Exception) -> None:
        # Solo el tipo: el payload puede llevar texto del usuario.
        logger.warning(f"[{self._tag}] push {what} falló: {type(e).__name__}")

    def _handle(self, item: tuple[str, Any, str | None]) -> None:
        kind, arg, run_id = item
        if kind == "tick":
            return
        if self._must_defer(kind, arg, run_id):
            self._defer(item)
            return
        if kind == "run" and run_id is not None:
            self._run_frame(run_id, arg[0], arg[1])
        elif kind == "fail" and run_id is not None:
            self._run_fail(run_id, arg[0], arg[1])
        elif kind == "lost":
            self._lost(arg)
        elif kind == "smsg":
            self._session_message(arg)
        elif kind == "reconcile":
            self._reconcile(arg)
        elif kind == "reconnected":
            self._on_reconnected()
        else:
            self._push_item(kind, arg, run_id)

    # -- one turn at a time: what waits for the open turn -------------------------------

    def _must_defer(self, kind: str, arg: Any, run_id: str | None) -> bool:
        if kind in ("run", "fail"):
            info = self._runs.get(run_id) if run_id is not None else None
            if info is None:
                return False  # terminado o desconocido: su manejador lo descarta
            if run_id in self._deferred_runs:
                return True  # detrás de lo suyo que ya espera
            if info.turn is not None or self.current_turn is None:
                return False
            if info.state == "absorbed":
                return False  # no abrirá turno: su `final` vacío solo lo da por terminado
            if kind == "fail":
                return True
            event, payload = arg
            return _opens_run(event, payload) or _lifecycle_error(event, payload)
        if kind in ("lost", "smsg", "reconcile", "reconnected"):
            return False
        if self._defer_push:
            return True
        current = self.current_turn
        return current is not None and current.origin == "user" and self._push_opens(kind, arg, run_id)

    def _push_opens(self, kind: str, arg: Any, run_id: str | None) -> bool:
        """Este evento de push abriría un push (no hay ninguno abierto): su primer evento."""
        if kind == "turn_start":
            return True
        if run_id is not None and run_id in self._ended_push_runs:
            return False
        if kind in ("open", "tool", "fallback"):
            return True
        return kind == "chat" and _as_dict(arg).get("state") not in _TERMINAL_CHAT

    def _defer(self, item: tuple[str, Any, str | None]) -> None:
        kind, arg, run_id = item
        if run_id is not None and run_id in self._overflowed and kind == "run":
            return  # ya falla por TURN_QUEUE_MAX: lo suyo no se pinta
        if len(self._deferred) >= settings.TURN_QUEUE_MAX:
            if kind in ("run", "fail") and run_id is not None and run_id in self._runs:
                self._overflow_deferred_run(run_id)
                return
            if not _ends_push(kind, arg):
                logger.warning(f"[{self._tag}] demasiados eventos en espera de turno: se descarta uno")
                return
            # El fin de un push nunca se descarta: sin él el push no se cerraría.
        self._deferred.append(item)
        self._mark_deferred(item)

    def _overflow_deferred_run(self, run_id: str) -> None:
        """m3: a run whose frames do not fit while it waits for the open turn fails, once: its
        waiting frames are dropped and replaced by its failure (`internal_error`, kept past
        the cap: one per run), and it is buried and aborted by its runId."""
        if run_id in self._overflowed:
            return
        self._bury_overflowed(run_id)
        self._deferred = deque(
            i for i in self._deferred if not (i[0] in ("run", "fail") and i[2] == run_id)
        )
        item: tuple[str, Any, str | None] = ("fail", (OVERFLOW_CODE, False), run_id)
        self._deferred.append(item)
        self._mark_deferred(item)

    def _overflow_run(self, run_id: str) -> None:
        """m3: a run whose held frames did not fit fails with `internal_error` (in order behind
        what was released of it) and is buried and aborted by its runId."""
        if run_id in self._overflowed:
            return
        self._bury_overflowed(run_id)
        self.fail_run(run_id, OVERFLOW_CODE)

    def _bury_overflowed(self, run_id: str) -> None:
        logger.warning(
            f"[{self._tag}] run {run_id}: más de TURN_QUEUE_MAX eventos en espera; falla "
            f"({OVERFLOW_CODE}) y chat.abort de ese run"
        )
        self._overflowed.add(run_id)
        self._abort_run(run_id)
        self._spawn_abort(run_id)

    def _mark_deferred(self, item: tuple[str, Any, str | None]) -> None:
        kind, _, run_id = item
        if kind in ("run", "fail"):
            if run_id is not None:
                self._deferred_runs.add(run_id)
        elif kind != "lost":
            self._defer_push = True

    def _replay(self) -> None:
        """Con el turno cerrado, lo que esperaba sigue, en orden; lo que vuelve a encontrar un
        turno abierto (lo abrió algo de lo anterior) espera otra vez, con lo que va detrás."""
        if self._replaying or not self._deferred:
            return
        if self._closed:
            self._deferred.clear()
            return
        self._replaying = True
        try:
            while self._deferred and self.current_turn is None:
                items, self._deferred = self._deferred, deque()
                self._deferred_runs.clear()
                self._defer_push = False
                while items:
                    item = items.popleft()
                    try:
                        self._handle(item)
                    except Exception as e:  # noqa: BLE001 — frontera: un evento roto no para el resto
                        self._log_push_failure(item[0], e)
                    if self._deferred:
                        for rest in items:
                            self._mark_deferred(rest)
                        self._deferred.extend(items)
                        break
        finally:
            self._replaying = False

    # -- the channel's own runs ---------------------------------------------------------

    def _head(self) -> RunInfo | None:
        """Sin turno abierto, el primer run propio que aún no abrió el suyo (el siguiente)."""
        if self.current_turn is not None:
            return None
        # Uno absorbido (`steer`) nunca tendrá turno propio: no es el siguiente.
        return next((i for i in self._runs.values() if i.turn is None and i.state == "queued"), None)

    def _active_run(self) -> RunInfo | None:
        """El run cuyo tope de TURN_TIMEOUT_S corre: el del turno abierto, o el siguiente."""
        turn = self.current_turn
        if turn is not None:
            if turn.origin != "user" or turn.run_id is None:
                return None
            return self._runs.get(turn.run_id)
        head = self._head()
        # Uno sin confirmar tras una caída espera a la reconexión (RECONNECT_GRACE_S), no a su tope.
        return None if head is None or head.unconfirmed else head

    def _touch_head(self) -> None:
        """Nada va delante del siguiente run: su tope empieza ahora."""
        head = self._head()
        if head is not None:
            head.progress_at = time.monotonic()
            self._wake()

    def _run_frame(self, run_id: str, event: str, payload: dict) -> None:
        info = self._runs.get(run_id)
        if info is None:
            logger.debug(f"[{self._tag}] frame de un run terminado descartado")
            return
        info.progress_at = time.monotonic()
        info.unconfirmed = False  # un frame suyo demuestra que sigue vivo
        if event == "chat":
            state = payload.get("state")
            if state in _TERMINAL_CHAT:
                self._run_terminal(info, state, payload)
                return
            phase = chat_status_phase(payload)
            turn = self._open_run(info)
            if phase is not None:  # #254: una fase de arranque, sin texto
                self.emit(turn, ChannelProgress(Progress(phase)))
                return
            delta = info.chat_text.feed(payload)
            if delta:
                self._run_token(info, turn, delta)
        elif event == "session.tool":
            tool_call = ToolCallEvent.from_session_tool_payload(_as_dict(payload.get("data")))
            if tool_call is not None:
                self.emit(self._open_run(info), ChannelToolCall(tool_call))
        elif event == "agent":
            data = _as_dict(payload.get("data"))
            if payload.get("stream") == "error" and data.get("reason") == "seq gap":
                # 2026.11.7: el aviso de OpenClaw en lugar del `start` de un run sacado de la
                # cola. No es un fallo: no abre nada.
                logger.debug(f"[{self._tag}] run {run_id}: aviso seq gap")
                return
            if payload.get("stream") in (None, "lifecycle") and data.get("phase") == "fallback_step":
                # #397: la razón del fallo de su run, para clasificar su error después.
                info.fallback_reason = fallback_reason(data, info.fallback_reason)
            if _lifecycle_error(event, payload):
                self._run_lifecycle_error(info, data)
            elif _opens_run(event, payload):
                self._open_run(info)

    def _open_run(self, info: RunInfo) -> ChannelTurn:
        """El turno del run, abierto con su primer evento (C3): su mensaje pasa a `delivered`
        (si esperaba) antes del turn_start."""
        if info.turn is not None:
            return info.turn
        if info.message == "pending":
            self._announce(info, "delivered")
        turn = self.open_turn(
            "user",
            sender=info.by_session,
            run_id=info.run_id,
            by_client_id=info.by_client_id,
            by_name=info.by_name,
            sink=info.sink,
        )
        info.turn = turn
        info.state = "running"
        if info.counts_turn:
            self._note_turn()
        gateway_stats.turn_started("normal")
        if info.recorder is not None:
            # El pipeline del turno en la sesión que lo envió: desde que se abre (lo que
            # esperó en la cola de OpenClaw no cuenta como latencia del modelo).
            info.recorder.start_turn()
            info.recorder.record_soon("llm_start")
        return turn

    def _run_token(self, info: RunInfo, turn: ChannelTurn, text: str) -> None:
        info.tokens += 1
        if info.tokens == 1 and info.recorder is not None:
            info.recorder.record_soon("llm_first_token")  # antes que el token, como antes
        self.emit(turn, ChannelToken(text))

    def _run_terminal(self, info: RunInfo, state: str, payload: dict) -> None:
        """El `chat` terminal del run: `final` (con la cola de texto que falte; sin `message`,
        el texto ya llegó en los deltas), `aborted` o `error` (#300, #397).

        Uno que nunca abrió su turno (decisión 4 de la 2b): un `final` sin texto (absorbido por
        `steer`, o vacío) no abre nada y su mensaje queda `delivered`; un `final` con texto
        (p. ej. la respuesta de un comando `/queue …`) abre, emite su texto y cierra en el
        acto; `aborted` no abre nada (si era un pendiente, `withdrawn`); `error` abre y cierra
        con su TURN_ERROR, para que un pendiente que falla no desaparezca en silencio."""
        if info.turn is None:
            if state == "final":
                text = info.chat_text.feed(payload)
                if not text:
                    # Sin evento propio ni texto: absorbido por el run activo, o vacío.
                    if info.message == "pending":
                        self._announce(info, "delivered")
                    self._finish(info)
                    return
                turn = self._open_run(info)
                self._run_token(info, turn, text)
                self._end_run(info, "done")
                return
            if state == "aborted":
                if payload.get("stopReason") == "restart" and info.state == "queued":
                    # OpenClaw se apaga: lo vuelve a poner en cola al arrancar (con su runId).
                    # No es un `withdrawn`: queda sin confirmar, como tras una caída.
                    info.unconfirmed = True
                    self._lost_code = _DISCONNECTED
                    if self._lost_deadline is None:
                        self._lost_deadline = time.monotonic() + RECONNECT_GRACE_S
                    logger.info(f"[{self._tag}] mensaje en cola parado por el reinicio de OpenClaw: sin confirmar")
                    return
                if info.message == "pending":
                    self._announce(info, "withdrawn")
                self._finish(info)
                return
            if info.state == "absorbed":
                # Su mensaje ya llegó dentro de otro turno: nada que abrir.
                self._finish(info)
                return
            turn = self._open_run(info)
            self._run_error(info, turn, classify_failure(payload, fallback_reason=info.fallback_reason))
            return
        turn = info.turn
        if state == "final":
            delta = info.chat_text.feed(payload)  # #241: lo que los deltas no trajeron
            if delta:
                self._run_token(info, turn, delta)
            self._end_run(info, "done")
        elif state == "aborted":
            self._end_run(info, "cancelled")  # lo abortó otro (no el gateway: lápida)
        else:
            self._run_error(info, turn, classify_failure(payload, fallback_reason=info.fallback_reason))

    def _run_lifecycle_error(self, info: RunInfo, data: dict) -> None:
        """`agent` lifecycle `error` de un run propio (#300). Con su turno abierto, su `chat`
        `error` llega detrás y lo cierra (con `errorKind`). Uno que nunca abrió su turno
        (decisión 4) lo abre y lo cierra ya con su TURN_ERROR; su `chat` `error` tardío se
        descarta (run terminado)."""
        if info.turn is not None:
            return
        if info.state == "absorbed":
            self._finish(info)
            return
        turn = self._open_run(info)
        self._run_error(info, turn, classify_failure(data, fallback_reason=info.fallback_reason))

    def _run_error(
        self, info: RunInfo, turn: ChannelTurn, code: str, *, crashed: bool = False
    ) -> None:
        # Solo el código estable: nunca el texto de OpenClaw (#228, #397).
        logger.error(f"[{self._tag}] Orchestrator error: {code}")
        gateway_stats.turn_error(code)
        info.error_code = code
        if crashed:
            turn.crashed = True
        self.emit(turn, ChannelError(code))
        self._end_run(info, "timeout" if code == "turn_timeout" else "error")

    def _end_run(self, info: RunInfo, reason: EndReason) -> None:
        turn = info.turn
        if reason == "done" and info.recorder is not None:
            info.recorder.record_soon("llm_done", token_count=info.tokens)
        self._finish(info)
        if turn is not None and not turn.ended:
            self.end_turn(turn, reason)

    def _finish(self, info: RunInfo, *, cut: bool = False) -> None:
        """El run terminó. `cut`: lo cortó el gateway (cancel, interrupt, withdraw, cierre):
        su `finished` queda cancelado, como antes la tarea del turno."""
        if info.state == "done":
            return
        info.state = "done"
        self._runs.pop(info.run_id, None)
        self._deferred_runs.discard(info.run_id)
        self._overflowed.discard(info.run_id)
        # m5: a late frame of it is never a push, even once this channel is dropped.
        self._turn_registry.finish_run(info.run_id)
        self._done_runs[info.run_id] = None
        while len(self._done_runs) > _DONE_RUNS:
            self._done_runs.popitem(last=False)
        if not info.finished.done():
            if cut:
                info.finished.cancel()
            else:
                info.finished.set_result(None)
        self._touch_head()
        self._wake_quiet()

    def _run_fail(self, run_id: str, code: str, crashed: bool) -> None:
        info = self._runs.get(run_id)
        if info is None:
            return
        self._run_error(info, self._open_run(info), code, crashed=crashed)

    def _lost(self, code: str) -> None:
        """La conexión con OpenClaw cayó. Un run con su turno abierto (o con frames ya en
        espera de turno) falla ya con `code`: lo que traía se perdió. Uno absorbido ya entregó
        su mensaje: termina sin más. Uno aún en la cola de OpenClaw (sin turno) queda sin
        confirmar hasta que la reconexión relea `pendingInputs` (`_reconcile`), como mucho
        RECONNECT_GRACE_S; entonces falla si no sigue en la cola."""
        self._held.clear()
        runs = list(self._runs.values())
        failing = [i for i in runs if i.turn is not None or i.run_id in self._deferred_runs]
        waiting = [i for i in runs if i not in failing and i.state == "queued"]
        for info in runs:
            if info.state == "absorbed" and info not in failing:
                self._finish(info)
        for info in waiting:
            info.unconfirmed = True
        self._reconnected = False
        self._recovery_until = None
        if waiting or any(i.unconfirmed for i in runs):
            self._lost_code = code
            self._lost_deadline = time.monotonic() + RECONNECT_GRACE_S
            logger.info(
                f"[{self._tag}] OpenClaw caído: {len(waiting)} mensaje(s) en cola sin confirmar"
            )
        for info in failing:
            # Su cliente sabrá que falló: que no siga corriendo en OpenClaw. Se entierra ya y se
            # aborta por su runId al reconectar (`_on_reconnected`).
            self._abort_run(info.run_id)
            self._abort_on_reconnect.add(info.run_id)
            self._handle(("fail", (code, False), info.run_id))

    def _reconcile(self, alive: set[str] | None) -> None:
        """Tras reconectar (`reconcile_pending`): los runs sin confirmar que `pendingInputs`
        (o `inFlightRun`) lista siguen pendientes. Los demás no fallan aún: tras un reinicio
        OpenClaw restaura su cola unos segundos después de aceptar conexiones, así que se
        relee cada RECOVERY_POLL_S (también si la lectura falló, `alive` None) hasta que
        vence RECOVERY_WINDOW_S (`_on_timeouts`); un frame o un `session.message` suyo
        también lo confirma."""
        unconfirmed = [i for i in self._runs.values() if i.unconfirmed]
        if not unconfirmed:
            logger.debug(f"[{self._tag}] pendingInputs releído: nada sin confirmar")
            return
        now = time.monotonic()
        kept = 0
        for info in unconfirmed:
            if alive is not None and info.run_id in alive:
                info.unconfirmed = False
                info.progress_at = now
                kept += 1
        missing = len(unconfirmed) - kept
        logger.info(
            f"[{self._tag}] pendingInputs tras reconectar: {kept} sigue(n) en cola, "
            f"{missing} sin confirmar"
        )
        until = self._recovery_until
        if missing and (until is None or until <= now):
            self._fail_unconfirmed(connected=True)
        elif missing:
            self._lost_deadline = until  # si las relecturas no llegan, falla al cerrarse la ventana
            self._hub.spawn(self._reread_later(), "pending_inputs_again")
        self._touch_head()

    async def _reread_later(self) -> None:
        await asyncio.sleep(RECOVERY_POLL_S)
        if not self._closed and any(i.unconfirmed for i in self._runs.values()):
            await self.reconcile_pending()

    def _fail_unconfirmed(self, *, connected: bool) -> None:
        """Los runs aún sin confirmar fallan, cada uno con su par y su TURN_ERROR. A quien se
        le dijo que falló no le sigue corriendo en OpenClaw: cada uno se entierra (#285) y se
        aborta por su runId; sin conexión (vence RECONNECT_GRACE_S), en la próxima
        reconexión (`_on_reconnected`)."""
        self._lost_deadline = None
        self._recovery_until = None
        for info in [i for i in self._runs.values() if i.unconfirmed]:
            info.unconfirmed = False
            self._abort_run(info.run_id)
            if connected:
                self._spawn_abort(info.run_id)
            else:
                self._abort_on_reconnect.add(info.run_id)
            self._handle(("fail", (self._lost_code, False), info.run_id))

    def _on_timeouts(self) -> None:
        left = self._push_budget_left()
        push = self._push
        if left is not None and left <= 0 and push is not None:
            if push.reply_until is not None:
                self._close_push(push)  # ninguna respuesta: acaba vacío, como antes
            else:
                self._push_timed_out()
        deadline = self._lost_deadline
        if deadline is not None and not any(i.unconfirmed for i in self._runs.values()):
            # Ya confirmados (o terminados) por otra vía: no queda nada que vigilar.
            self._lost_deadline = self._recovery_until = None
        elif deadline is not None and deadline <= time.monotonic():
            logger.warning(
                f"[{self._tag}] OpenClaw no confirmó a tiempo los mensajes en cola "
                f"(RECONNECT_GRACE_S={RECONNECT_GRACE_S}s, RECOVERY_WINDOW_S={RECOVERY_WINDOW_S}s): fallan"
            )
            self._fail_unconfirmed(connected=self._reconnected)
        info = self._active_run()
        if info is not None and info.progress_at + settings.TURN_TIMEOUT_S <= time.monotonic():
            self._run_timed_out(info)

    def _budget_left(self) -> float | None:
        """Segundos hasta el primer tope que vence (el del push, el del run activo)."""
        budgets = []
        push = self._push_budget_left()
        if push is not None:
            budgets.append(push)
        info = self._active_run()
        if info is not None:
            budgets.append(info.progress_at + settings.TURN_TIMEOUT_S - time.monotonic())
        if self._lost_deadline is not None:
            budgets.append(self._lost_deadline - time.monotonic())
        return min(budgets) if budgets else None

    def _run_timed_out(self, info: RunInfo) -> None:
        """Decisión 2: un run sin progreso en TURN_TIMEOUT_S se cierra con `turn_timeout` (con
        su par también si nunca abrió) y se aborta en OpenClaw (solo ese run)."""
        logger.warning(
            f"[{self._tag}] run sin progreso en TURN_TIMEOUT_S={settings.TURN_TIMEOUT_S}s — "
            "se cierra."
        )
        self._abort_run(info.run_id)
        if info.turn is None and self.current_turn is not None:
            self._finish(info)
        else:
            self._run_error(info, self._open_run(info), "turn_timeout")
        self._spawn_abort(info.run_id)

    # -- session.message: steered messages (2b.2) --------------------------------------

    def _session_message(self, payload: dict) -> None:
        """A `user` `session.message`. Copies of one message share OpenClaw's `messageId`
        (`oc_message_id`) and are not discarded as duplicates: each updates what is known of
        it. The link to its `chat.send` (`idempotencyKey` `"<runId>:user"`) may come on one
        copy and `steer`'s mark (`__openclaw.steerTargetRunId`, the run that absorbed it) on
        another, so the link is remembered by `oc_message_id`. A mark naming another run
        makes the message's run `absorbed` (`_absorb`). Text never read, never logged."""
        message = _as_dict(payload.get("message"))
        meta = _as_dict(message.get("__openclaw"))
        if message.get("role") == "assistant":
            self._push_reply(message, meta.get("runId"))
            return
        oc_message_id = payload.get("messageId", meta.get("id"))
        run_id = (
            _run_of_idempotency(message.get("idempotencyKey"))
            or _run_of_idempotency(meta.get("idempotencyKey"))
            or _run_of_idempotency(payload.get("idempotencyKey"))
        )
        if isinstance(oc_message_id, str) and oc_message_id:
            known = self._oc_messages
            if run_id is not None:
                known.pop(oc_message_id, None)
                known[oc_message_id] = run_id
                while len(known) > _OC_MESSAGES:
                    known.popitem(last=False)
            else:
                run_id = known.get(oc_message_id)
        confirmed = self._runs.get(run_id) if run_id is not None else None
        if confirmed is not None and confirmed.unconfirmed:
            # OpenClaw lo restauró tras reiniciarse: sigue vivo.
            confirmed.unconfirmed = False
            confirmed.progress_at = time.monotonic()
            self._touch_head()
        target = meta.get("steerTargetRunId")
        if run_id is None or not isinstance(target, str) or not target or target == run_id:
            return
        info = self._runs.get(run_id)
        if info is not None:
            self._absorb(info, target)

    def _absorb(self, info: RunInfo, target: str) -> None:
        """`steer` folded this message into run `target`: it is `delivered` for everyone (once)
        and will never open a turn of its own; its answer goes on inside `target`'s turn,
        whose sender (and audio) does not change. Its empty `final` only ends it."""
        if info.turn is not None or info.state != "queued":
            return  # ya con turno propio, o ya absorbido/terminado
        info.state = "absorbed"
        info.absorbed_into = target
        info.unconfirmed = False
        logger.debug(f"[{self._tag}] run {info.run_id} absorbido (steer) por {target}")
        if info.message == "pending":
            self._announce(info, "delivered")
        if self.current_turn is None or self.current_turn.run_id != target:
            # El run que lo absorbió ya no está abierto aquí: nada más que esperar de él.
            self._settle_absorbed(None)
        self._touch_head()

    def _settle_absorbed(self, ended_run: str | None) -> None:
        """Al terminar un turno, los runs absorbidos por él (o por uno que ya no es de nadie
        aquí) terminan: su `final` vacío ya no hace falta."""
        for info in list(self._runs.values()):
            if info.state != "absorbed":
                continue
            target = info.absorbed_into
            open_target = self.current_turn is not None and self.current_turn.run_id == target
            if target == ended_run or (target not in self._runs and not open_target):
                self._finish(info)

    # -- push --------------------------------------------------------------------------

    def _push_item(self, kind: str, arg: Any, run_id: str | None) -> None:
        if self._waiting_reply(kind, run_id):
            # Su run ya acabó: lo que llegue tarde de él (su `final`, una fase) no cuenta.
            logger.debug(f"[{self._tag}] push {kind} tras el fin de su run: descartado")
            return
        if self._run_aborted(run_id):
            # #285: lo que quedaba en cola de un run abortado no llega (ni abre push).
            logger.debug(f"[{self._tag}] push {kind} de un run abortado descartado")
        elif kind == "turn_start":
            self._push_start(run_id)
        elif kind == "open":
            self._push_open(run_id)
        elif kind == "chat":
            self._push_chat(arg, run_id)
        elif kind == "tool":
            self._push_tool(arg, run_id)
        elif kind == "fallback":
            self._push_fallback(arg, run_id)
        elif kind == "turn_end":
            self._push_end()
        elif kind == "turn_error":
            self._push_end(failure=arg)
        else:
            logger.warning(f"[{self._tag}] push event desconocido: {kind!r}")

    def _push_budget_left(self) -> float | None:
        """Segundos que le quedan al push abierto sin progreso (#286); None si no aplica."""
        push = self._push
        if push is None or push.ended:
            return None
        if push.reply_until is not None:
            return push.reply_until - time.monotonic()
        return push.progress_at + settings.TURN_TIMEOUT_S - time.monotonic()

    def _run_aborted(self, run_id: str | None) -> bool:
        return run_id is not None and self._turn_registry.is_run_aborted(run_id)

    def _abort_run(self, run_id: str | None) -> None:
        """Lápida (#285): desde ya, nada de ese run llega (tampoco su `aborted`)."""
        if run_id is not None:
            self._turn_registry.abort_run(run_id)

    def _may_get_push(self, subscriber: ChannelSubscriber) -> bool:
        """Takes this conversation's push: it accepts pushes, may use its agent and may see the
        conversation (M20, `visible_to`: checked again on every delivery path, 2c.3)."""
        client = subscriber.client
        return (
            subscriber.wants_push
            and subscriber.agent_allowed(self.session_key)
            and self.conversation.visible_to(client.home_id, client.user_id)
        )

    def _indexed(self) -> bool:
        """Its key is a Fogar conversation's (`ConversationIndex`): only then does a push have a
        notify device. A live channel of an unindexed key (a cron session,
        an owner-less stand-in) reaches its attached sessions only."""
        index = self._hub.index
        return index is not None and index.get(self.session_key) is not None

    def _push_audience(self) -> list[ChannelSubscriber]:
        """Los suscriptores que reciben un push (`_may_get_push`)."""
        return [s.subscriber for s in self._subscriptions if self._may_get_push(s.subscriber)]

    def _notify_session(self) -> ChannelSubscriber | None:
        """2c.3: the live session of the conversation's notify device that gets its push: the
        one attached to this channel or, if none, the device's most recent one. None without a
        notify device, if it is not connected, or if that session may not get it (push off,
        agent not allowed, conversation not visible to it, closing)."""
        client_id = self._notify_client_id
        sessions_of = self._hub.sessions_of
        if client_id is None or sessions_of is None or not self._indexed():
            return None
        live = [
            s for s in sessions_of(client_id)
            if s.client.id == client_id
            # Solo una sesión que ya abrió su conversación (claim_conversation, antes de
            # `ready`): nada de un push antes de su `ready`.
            and getattr(s, "claimed", False)
            and not getattr(s, "closing", False)
            and self._may_get_push(s)
        ]
        attached = [s for s in live if self.is_subscribed(s)]
        if attached:
            return attached[-1]
        return live[-1] if live else None

    def _open_push(self, run_id: str | None) -> ChannelTurn | None:
        """El push abierto, o uno nuevo abierto con este primer evento de su run (2b.2: no
        solo con lifecycle `start`, que un run sacado de la cola no trae). None si no se abre:
        su run ya cerró su push (solo un `start` nuevo lo reabre, #84) o el agente no está
        permitido. (#112, «un push no abre encima de un turno de /v1»: desde la 2b.3 el turno de
        /v1 es un turno del canal, y un push espera a que se cierre como detrás de cualquier
        otro, `_must_defer`.)"""
        push = self._push
        if push is not None and not push.ended:
            return push
        if run_id is not None and run_id in self._ended_push_runs:
            logger.debug(f"[{self._tag}] evento tardío de un push ya cerrado: descartado")
            return None
        self._push_start(run_id)
        push = self._push
        return push if push is not None and not push.ended else None

    def _push_open(self, run_id: str | None) -> None:
        """`run_status` o lifecycle `model`: abre el push de su run si no hay uno abierto; si
        lo hay, solo cuenta como progreso (#84, #286)."""
        push = self._push
        if push is not None and not push.ended:
            push.progress_at = time.monotonic()
            return
        self._open_push(run_id)

    def _push_start(self, run_id: str | None = None) -> None:
        if run_id is not None:
            self._ended_push_runs.pop(run_id, None)  # un `start` nuevo de su run lo reabre (#84)
        key = self.session_key
        # A passive subscriber (a device's /events, #412) never makes a push abort here.
        subscribers = [
            s.subscriber for s in self._subscriptions if not getattr(s.subscriber, "passive", False)
        ]
        if subscribers and not any(s.agent_allowed(key) for s in subscribers):
            # C-r7-1: push de un agente retirado de allowed_agents de todos los adjuntos: no
            # llega a nadie y se para en OpenClaw.
            logger.info(f"[{self._tag}] push de un agente no permitido: descartado")
            aborts = {run_id}
            push = self._push
            if push is not None:
                # Su push ya abierto (retirado a medias) se corta ya, sin esperar a turn_timeout.
                aborts.add(push.run_id)
                self._end_push(push, "cancelled")
            self._abort_run(run_id)
            for aborted in aborts:
                self._spawn_abort(aborted)
            return
        if self._push is not None:
            # #84: los `agent start` intermedios (tool use, varios pasos) no abren otro turno.
            self._push.progress_at = time.monotonic()
            logger.debug(f"[{self._tag}] push_turn_start ignorado: ya hay un push abierto")
            return
        # 2c.3: el texto, a los adjuntos que pueden verlo; el audio, solo a la sesión del
        # dispositivo de avisos (si su `output_mode` lo lleva), adjunta o no. Si no está adjunta,
        # recibe este turno como invitada: si no hay nadie adjunto (el texto) o si suena.
        audience = self._push_audience()
        notify = self._notify_session()
        audio = notify if notify is not None and notify.wants_audio else None
        guests: list[ChannelSubscriber] = []
        # A passive subscriber (a device's /events, #412) gets the text but does not count as
        # "someone sees it": the notify device's delivery stays as without it.
        watching = [s for s in audience if not getattr(s, "passive", False)]
        if notify is not None and not self.is_subscribed(notify) and (not watching or audio):
            guests.append(notify)
        self._push_text.reset()
        turn = self.open_turn("push", run_id=run_id, audio_to=audio, guests=guests)
        self._push = turn
        logger.debug(
            "Channel push open: conversation=%s attached=%d notify_device=%s guest=%s audio=%s",
            self.conversation.id, len(audience), notify is not None, bool(guests),
            audio is not None,
        )
        if audience or guests:  # un push que alguien ve cuenta en la conversación
            self._note_turn()
            gateway_stats.turn_started("push")
        early = self._take_early_phase(run_id)
        if early is not None:  # #254: la fase que su run contó antes de abrirse
            self.emit(turn, ChannelProgress(Progress(early)))

    def _push_chat(self, payload: dict, run_id: str | None = None) -> None:
        push = self._push
        phase = chat_status_phase(payload)
        run_id = run_id if run_id is not None else _str_run_id(payload)
        if phase is not None and push is not None and not push.ended and _other_run(push.run_id, run_id):
            # #254: fase de arranque de otro run con un push abierto: se guarda para cuando
            # se abra el suyo.
            self._note_early_phase(run_id, phase)
            return
        if push is None or push.ended:
            if payload.get("state") in _TERMINAL_CHAT:
                return  # #109: nada fuera de un turn_start/turn_end
            # 2b.2: el primer evento de su run abre el push (sin esperar a un `start`).
            push = self._open_push(run_id)
            if push is None:
                if phase is not None:
                    self._note_early_phase(run_id, phase)
                return
        push.progress_at = time.monotonic()
        if phase is not None:  # #254: una fase de arranque, sin texto
            self.emit(push, ChannelProgress(Progress(phase)))
            return
        # #241: `final` lleva el texto completo sin deltaText (OpenClaw 2026.10.5).
        delta = self._push_text.feed(payload)
        if delta:
            self.emit(push, ChannelToken(delta))

    def _push_tool(self, data: dict, run_id: str | None = None) -> None:
        push = self._open_push(run_id)
        if push is None:
            return
        push.progress_at = time.monotonic()
        tool_call = ToolCallEvent.from_session_tool_payload(data)
        if tool_call is not None:
            self.emit(push, ChannelToolCall(tool_call))

    def _push_fallback(self, data: dict, run_id: str | None) -> None:
        """#397: un `fallback_step` del run del push (abierto por él si no lo estaba): su
        razón, para clasificar su error."""
        push = self._open_push(run_id)
        if push is not None and not _other_run(push.run_id, run_id):
            push.progress_at = time.monotonic()
            push.fallback_reason = fallback_reason(data, push.fallback_reason)

    def _push_end(self, *, failure: dict | None = None) -> None:
        """Fin del agente (`end`, o `error` #300 con su `data`): el push del canal se cierra ya
        (cada suscriptor termina de sonar su audio antes de su turn_end, decisión 1). Si falló,
        antes su TURN_ERROR con el código clasificado (#397)."""
        push = self._push
        if push is None or push.ended:
            # #84: un fin sin push abierto (huérfano o ya cerrado) no emite nada.
            logger.debug(f"[{self._tag}] push_turn_end ignorado: no hay push abierto")
            return
        if failure is None and not self._push_text.all_out and PUSH_REPLY_GRACE_S > 0:
            # Sin texto: su respuesta puede llegar después como `session.message` (cron).
            if push.reply_until is None:
                push.reply_until = time.monotonic() + PUSH_REPLY_GRACE_S
            return
        self._close_push(push, failure)

    def _close_push(self, push: ChannelTurn, failure: dict | None = None) -> None:
        if failure is not None:
            code = classify_failure(failure, fallback_reason=push.fallback_reason)
            logger.warning(f"[{self._tag}] push falló en OpenClaw: {code}")
            gateway_stats.turn_error(code)
            self.emit(push, ChannelError(code))
        self._push = None
        if push.run_id is not None:
            # Lo que llegue tarde de su run (un `run_status`, una fase) no abre otro push.
            self._ended_push_runs.pop(push.run_id, None)
            self._ended_push_runs[push.run_id] = None
            while len(self._ended_push_runs) > _ENDED_PUSH_RUNS:
                self._ended_push_runs.popitem(last=False)
        self.end_turn(push, "done" if failure is None else "error")

    def _push_reply(self, message: dict, run_id: object) -> None:
        """An `assistant` `session.message`: the reply of the push that ended without text and
        waits for it (`reply_until`), unless it names another run (a cron's has no `runId`).
        Its text goes out as the push's text (and voice), then the push ends `done`. Any other
        `assistant` message is a copy of text already sent: ignored. Text never logged."""
        push = self._push
        if push is None or push.ended or push.reply_until is None or _other_run(push.run_id, run_id):
            return
        text = extract_message_text(message)
        if text:
            self.emit(push, ChannelToken(text))
        self._close_push(push)

    def _waiting_reply(self, kind: str, run_id: str | None) -> bool:
        """The open push waits for its reply: a frame of its own run (or without a runId) is
        late and dropped; an opener of another run ends it first, as it was (one push at a time)."""
        push = self._push
        if push is None or push.ended or push.reply_until is None:
            return False
        if run_id is not None and run_id != push.run_id and kind in _PUSH_OPENERS:
            self._close_push(push)
            return False
        return kind != "turn_error"  # un error de su run (raro tras el fin) aún lo cierra

    def _push_timed_out(self) -> None:
        """#286: un push sin progreso en TURN_TIMEOUT_S se cierra como el turno normal."""
        push = self._push
        if push is None or push.ended:
            return
        logger.warning(
            f"[{self._tag}] push sin progreso en "
            f"TURN_TIMEOUT_S={settings.TURN_TIMEOUT_S}s — se cierra."
        )
        gateway_stats.turn_error("turn_timeout")
        self.emit(push, ChannelError("turn_timeout"))
        self._end_push(push, "timeout")
        self._spawn_abort(push.run_id)

    def _end_push(self, push: ChannelTurn, reason: EndReason) -> None:
        # Antes del abort: nada más de su run llega, ni lo que el worker tiene en cola ni lo
        # que venga (#285).
        self._abort_run(push.run_id)
        self._mark_cancel(push, reason)
        self._push = None
        self.end_turn(push, reason)

    async def _cut_push(self, expected: ChannelTurn | None, reason: EndReason) -> ChannelTurn | None:
        """Corta el push abierto (7e): lo cierra para todos y lo aborta en OpenClaw (solo su
        run). Con `expected`, solo ese push. Devuelve el push cortado, o None si no había."""
        push = self._push
        if push is None or push.ended:
            return None
        if expected is not None and expected is not push:
            return None
        self._end_push(push, reason)
        await self._abort_step(push.run_id, "chat.abort del push")
        return push

    # -- progress (#254) ---------------------------------------------------------------

    def _note_early_phase(self, run_id: object, phase: ProgressPhase) -> None:
        """Guarda la última fase de arranque de un run sin push abierto (acotado: los últimos
        `_EARLY_PHASE_RUNS` runs, `_EARLY_PHASE_TTL_S`)."""
        # Siempre (acotado): quién verá el push (los adjuntos o la sesión del dispositivo de
        # avisos, 2c.3) se sabe al abrirse, y lo recibe quien reciba su turno.
        if not isinstance(run_id, str):
            return
        early = self._early_phases
        early.pop(run_id, None)
        early[run_id] = (phase, time.monotonic())
        while len(early) > _EARLY_PHASE_RUNS:
            early.popitem(last=False)

    def _take_early_phase(self, run_id: str | None) -> ProgressPhase | None:
        if run_id is None:
            return None
        entry = self._early_phases.pop(run_id, None)
        if entry is None or time.monotonic() - entry[1] > _EARLY_PHASE_TTL_S:
            return None
        return entry[0]

    # -- closing -----------------------------------------------------------------------

    def _retire(self) -> None:
        """El hub lo olvida (inactivo): su worker, que solo espera eventos, se para."""
        self._closed = True
        self._wake_quiet()
        worker = self._push_worker
        if worker is not None and not worker.done() and worker is not asyncio.current_task():
            worker.cancel()

    async def aclose(self, limit: float | None = None) -> None:
        """Cierre del canal (apagado del gateway, o el hub privado de una sesión de tests): su
        worker se cancela, el run de su turno abierto se aborta (solo ese) y el turno se cierra
        (`closed`); sus demás runs se dan por terminados (siguen en la cola de OpenClaw). Cada
        paso termina aunque cancelen a quien cierra (#221)."""
        self._closed = True
        self._wake_quiet()
        async with Teardown(f"[{self._tag}] cierre del canal") as td:
            await td.cancel(self._push_worker, "push worker", limit)
            current = self.current_turn
            if current is not None and current.origin == "user" and current.run_id is not None:
                self._abort_run(current.run_id)
                if self._hub.orchestrator is not None:
                    await td.step(self._abort(current.run_id), "chat.abort del turno", limit)
            backlog = self._backlog
            while not backlog.queue.empty():
                backlog.queue.get_nowait()
                backlog.queue.task_done()
            backlog.pending = 0
            backlog.since = None
            self._held.clear()
            self._deferred.clear()
            self._deferred_runs.clear()
            for info in list(self._runs.values()):
                if info.turn is not None:
                    self._mark_cancel(info.turn, "closed")
                self._finish(info, cut=True)
            current = self.current_turn
            if current is not None:
                self._push = None
                self.end_turn(current, "closed")

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
    """The live channels, by OpenClaw `sessionKey`, and what they share: the orchestrator, the
    `TurnRegistry` (tombstones and active runs, #285), the conversation index (a push's
    `sessionKey` → its conversation), the turn counter and, for pushes (2c.3), the notify
    device lookup and the live sessions of a device."""

    def __init__(
        self,
        *,
        index: ConversationIndex | None = None,
        orchestrator: OrchestratorProtocol | None = None,
        turn_registry: TurnRegistry | None = None,
        note_turn: Callable[[str], None] | None = None,
        notify_device: Callable[[Conversation], Awaitable[str | None]] | None = None,
        sessions_of: Callable[[str], list[ChannelSubscriber]] | None = None,
    ) -> None:
        self._channels: dict[str, ConversationChannel] = {}
        self.index = index
        # Set after construction in main.py: the OpenClaw client needs the dispatcher, and the
        # dispatcher this hub.
        self.orchestrator = orchestrator
        self.turn_registry = turn_registry if turn_registry is not None else TurnRegistry()
        self.note_turn = note_turn
        # 2c.3: a conversation's notify device (`db_client.notify_device`: its client id, or
        # None) and a device's live sessions, the most recent last (`ClientRegistry`). Without
        # them a push reaches only the attached sessions, and sounds nowhere.
        self.notify_device = notify_device
        self.sessions_of = sessions_of
        # 5.0 #412: who hears of a new channel (`EventHub.on_channel`, the devices with /events).
        # Synchronous, called before the channel is handed out: no turn can open before it.
        self.on_new_channel: Callable[[ConversationChannel], None] | None = None
        self._tasks: set[asyncio.Task] = set()

    def __len__(self) -> int:
        return len(self._channels)

    def get(self, session_key: str) -> ConversationChannel | None:
        return self._channels.get(session_key)

    def get_or_create(self, conversation: Conversation) -> ConversationChannel:
        """The channel of `conversation.session_key`. The first `Conversation` object given
        for a key is the one the channel keeps (same key → same conversation id)."""
        channel = self._channels.get(conversation.session_key)
        if channel is None:
            channel = ConversationChannel(conversation, self)
            self._channels[conversation.session_key] = channel
            if self.on_new_channel is not None:
                self.on_new_channel(channel)
        return channel

    def channels(self) -> list[ConversationChannel]:
        """The live channels (a device opening /events attaches to those it may see)."""
        return list(self._channels.values())

    def channel_for(self, session_key: str) -> ConversationChannel | None:
        """The channel of a push's `sessionKey` (decision 5: the conversation from the index,
        synchronously). None for a key no conversation has: that push goes nowhere."""
        channel = self._channels.get(session_key)
        if channel is not None:
            return channel
        conversation = self.index.get(session_key) if self.index is not None else None
        return self.get_or_create(conversation) if conversation is not None else None

    def connection_lost(self, code: str) -> None:
        """The OpenClaw connection dropped: every live channel ends its unfinished runs."""
        for channel in list(self._channels.values()):
            channel.connection_lost(code)

    def reconnected(self) -> None:
        """OpenClaw is back (`ReconnectingOpenClawClient.on_connected`, 2b.2): every live
        channel's worker aborts the runs it failed while disconnected and, with subscribers or
        runs that wait unconfirmed since the drop, re-reads its `pendingInputs`
        (`ConversationChannel.reconcile_pending`). Never blocks."""
        for channel in list(self._channels.values()):
            channel.reconnected()

    def drop_if_idle(self, session_key: str, channel: ConversationChannel | None = None) -> bool:
        """Forget the channel if nothing keeps it alive (`ConversationChannel.idle`). With
        `channel`, only if it is still that key's channel."""
        current = self._channels.get(session_key)
        if current is None or (channel is not None and current is not channel) or not current.idle:
            return False
        del self._channels[session_key]
        current._retire()
        logger.debug("Channel dropped: conversation=%s", current.conversation.id)
        return True

    # -- tasks -------------------------------------------------------------------------

    def spawn(self, coro: Coroutine[Any, Any, None], name: str) -> asyncio.Task:
        """A supervised task of the channels (push workers, the turn counter)."""
        task = asyncio.create_task(coro, name=name)
        self.supervise(task)
        return task

    def supervise(self, task: asyncio.Task) -> None:
        """Kept until done and its failure logged by type (#131, #221)."""
        self._tasks.add(task)

        def _on_done(t: asyncio.Task) -> None:
            self._tasks.discard(t)
            if t.cancelled():
                return
            exc = t.exception()  # always retrieved: no "never retrieved" warning
            if exc is not None:
                logger.error("Channel task '%s' failed: %s", t.get_name(), describe_error(exc))
                logger.debug("'%s': %s", t.get_name(), traceback_no_message(exc))

        task.add_done_callback(_on_done)

    async def aclose(self, limit: float | None = None) -> None:
        """Close every channel (gateway shutdown): their turns are cancelled, never drained."""
        # Todos a la vez, bajo un mismo tope: el apagado cabe en el de uvicorn.
        loop = asyncio.get_running_loop()
        deadline = None if limit is None else loop.time() + limit

        def left() -> float | None:
            return None if deadline is None else max(0.0, deadline - loop.time())

        async with Teardown("cierre de los canales") as td:
            closing = [
                asyncio.ensure_future(channel.aclose(limit)) for channel in self._channels.values()
            ]
            self._channels.clear()
            for task in closing:
                await td.wait(task, "cierre de un canal", left())
                if not task.done():
                    # Abandonado por el tope: su excepción se recoge igual (como Teardown.step).
                    task.cancel()
                    task.add_done_callback(retrieve_outcome)
            for task in list(self._tasks):
                task.cancel()
            for task in list(self._tasks):
                await td.wait(task, f"tarea {task.get_name()}", left())
