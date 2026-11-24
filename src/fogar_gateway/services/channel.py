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
its `repr`.

The OpenClaw side (5.0 PR 2a.2): the channel is the only owner of its conversation's runs.
It moved here from `TurnController` (services/turns.py) as it was: the send lock and the
order under it (G-08, #201), the user turn's task (`stream_response` with its
TURN_TIMEOUT_S), the push worker (FIFO, TURN_TIMEOUT_S idle budget per push, #286), the
tombstones of aborted runs (#285), the startup phases of a push not yet open (#254), the
push's `ChatTextTracker` (#241) and the conversation's turn count (`note_turn`, once per
channel turn). A subscriber leaving never cancels the channel's task nor aborts its run
(decision 2 of the 5.0 PR 2 plan: G-18 inverted); the channel ends its turn on OpenClaw's
terminal (or its timeout) without waiting for anyone's audio (decision 1).
"""

from __future__ import annotations

import asyncio
import logging
import time
import weakref
from collections import OrderedDict
from collections.abc import Callable, Coroutine
from dataclasses import dataclass, field
from functools import partial
from typing import TYPE_CHECKING, Any, Literal, Protocol
from uuid import uuid4

from fogar_gateway.core.config import settings
from fogar_gateway.services import orchestration
from fogar_gateway.services.openclaw.chat_text import ChatTextTracker
from fogar_gateway.services.openclaw.failures import (
    ORCHESTRATOR_AGENT_UNAVAILABLE_ERROR,
    classify_failure,
    fallback_reason,
)
from fogar_gateway.services.openclaw.models import ToolCallEvent
from fogar_gateway.services.openclaw.registry import TurnRegistry
from fogar_gateway.services.reconnection import describe_error, traceback_no_message
from fogar_gateway.services.stats import gateway_stats
from fogar_gateway.services.teardown import Teardown
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
    run_id: str | None = None  # OpenClaw's runId, once known (push: its `agent start`)
    ended: bool = False
    end_reason: EndReason | None = None
    cancel_reason: EndReason | None = None  # quien la cancela dice por qué (G-08)
    started_at: float = field(default_factory=time.monotonic)
    progress_at: float = field(default_factory=time.monotonic)  # presupuesto del push (#286)
    fallback_reason: str | None = None  # push: la del último `fallback_step` de su run (#397)
    # Its task broke (an unexpected exception, `internal_error`): nobody waits for its audio.
    crashed: bool = False
    # Decision 5: only a weak reference to the sender; the live session is resolved on use
    # (if it left, nobody gets the audio).
    _sender: weakref.ref[ChannelSubscriber] | None = field(default=None, repr=False)
    # Who plays the turn's audio: the sender (user turn) or, in 2a, the most recently attached
    # subscriber that wants audio and push (push; 2c replaces it by the notification device).
    _audio: weakref.ref[ChannelSubscriber] | None = field(default=None, repr=False)
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


class ChannelBusy(RuntimeError):
    """`open_turn` with a turn still open: a conversation has one open turn at a time."""


@dataclass(eq=False)
class _Subscription:
    subscriber: ChannelSubscriber
    active: bool = True


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
        # Serializa cancelar/arrancar (G-08, #201): un turno nuevo no arranca hasta que el
        # anterior terminó, y el abort de un push cortado sale antes del chat.send del usuario.
        self._lock = asyncio.Lock()
        self._user_task: asyncio.Task | None = None
        self._user_turn: ChannelTurn | None = None
        self._push: ChannelTurn | None = None
        self._push_text = ChatTextTracker()  # #241: texto ya emitido en la respuesta del push
        # #254: runId → (última fase de arranque, cuándo) de runs sin push abierto todavía.
        self._early_phases: OrderedDict[str, tuple[ProgressPhase, float]] = OrderedDict()
        # Worker de push (#130 S3a): el dispatcher encola sin esperar desde el bucle de
        # OpenClaw; un solo worker por canal conserva el orden start → deltas → tool → end.
        self._push_worker: asyncio.Task | None = None
        self._backlog = _PushBacklog()
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
        audio_to: ChannelSubscriber | None = None,
    ) -> ChannelTurn:
        """Open the channel's turn and give every subscriber its `on_turn_start`. Raises
        `ChannelBusy` while another turn is open (end it first). `audio_to` (default: the
        sender) is who plays its audio."""
        if self.current_turn is not None:
            raise ChannelBusy(self.conversation.id)
        audio = audio_to if audio_to is not None else sender
        turn = ChannelTurn(
            id=uuid4().hex,
            conversation_id=self.conversation.id,
            origin=origin,
            by_client_id=sender.client.id if sender is not None else None,
            run_id=run_id,
            _sender=weakref.ref(sender) if sender is not None else None,
            _audio=weakref.ref(audio) if audio is not None else None,
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
        """Nothing keeps the channel alive: no subscribers, no open turn, no user turn's task
        still running and no push event waiting for the worker."""
        task = self._user_task
        return (
            not self._subscriptions
            and self.current_turn is None
            and (task is None or task.done())
            and self._backlog.pending == 0
        )

    # -- state for the sessions (watchdogs, /admin/state) ------------------------------

    @property
    def user_task(self) -> asyncio.Task | None:
        """The task running the channel's user turn, if one was started (done or not)."""
        return self._user_task

    def user_turn_running(self) -> bool:
        task = self._user_task
        return task is not None and not task.done()

    def push_backlog(self) -> tuple[int, int, float | None]:
        """(pending, peak, since) of the push worker's queue (#219: numbers only)."""
        backlog = self._backlog
        return backlog.pending, backlog.peak, backlog.since

    def is_subscribed(self, subscriber: ChannelSubscriber) -> bool:
        return self._subscription(subscriber) is not None

    def _note_turn(self) -> None:
        """Cuenta un turno de la conversación (una vez por turno del canal)."""
        note = self._hub.note_turn
        if note is not None:
            note(self.conversation.id)

    # -- user turn ---------------------------------------------------------------------

    async def send_user_message(
        self, sender: ChannelSubscriber, text: str, *, interrupt: bool = False
    ) -> ChannelTurn | None:
        """Sustituye el turno en curso (normal o push) por uno nuevo para `text`, de `sender`.

        Today's order under the lock (G-08, #201): the user turn in flight is cancelled
        (`replaced`), the open push is cut, any other run active on the key is aborted, and
        only then the new turn opens (every subscriber gets its `on_turn_start`) and its task
        starts. `interrupt` is not used yet (5.0 PR 2b). None if nothing started: the channel
        is closing, or `sender` left while waiting (G-09)."""
        del interrupt  # 2b
        if self._closed:
            return None
        allowed = sender.agent_allowed(self.session_key)
        tracker = sender.tracker
        async with self._lock:
            if self._closed:
                return None
            await self._replace_user_turn("replaced")
            # Dentro del lock: el abort del push termina antes de que salga el chat.send.
            await self._cut_push(None, "replaced")
            await self._abort_other_runs()
            if self._closed or not self.is_subscribed(sender):
                return None  # close_all() pudo empezar durante la espera
            turn = self.open_turn("user", sender=sender)
            self._user_turn = turn
            task = asyncio.create_task(self._run_user_turn(turn, text, tracker, allowed), name="turn")
            self._hub.supervise(task)
            task.add_done_callback(self._after_user_task)
            self._user_task = task
            return turn

    def _after_user_task(self, _task: asyncio.Task) -> None:
        self._hub.drop_if_idle(self.session_key, self)

    async def _replace_user_turn(self, reason: EndReason) -> None:
        """Cancela el turno del usuario en curso con `reason`, bajo el lock de quien llama.
        G-r2-1: también uno cuya tarea aún no corrió recibe su fin (cada suscriptor, su par)."""
        turn, task = self._user_turn, self._user_task
        if turn is None or turn.ended:
            return
        self._mark_cancel(turn, reason)
        if task is not None and not task.done():
            await self._cancel_and_wait(task)
        self.end_turn(turn, reason)  # no-op si su tarea ya lo cerró

    async def cancel(
        self, reason: EndReason, *, push: bool = True, expected: ChannelTurn | None = None
    ) -> ChannelTurn | None:
        """Cancela el turno abierto (o `expected`, si sigue abierto): el del usuario o, con
        `push`, el push. G-08 (#201): el objetivo se captura al entrar y las llamadas se
        serializan; dos llamadas a la vez lo cancelan una vez. Devuelve el turno cancelado."""
        turn = expected if expected is not None else self.current_turn
        if turn is None or turn.ended or (turn.origin == "push" and not push):
            return None
        async with self._lock:
            if turn is not self.current_turn:  # otro lo canceló mientras esperábamos el lock
                return None
            if turn.origin == "user":
                await self._replace_user_turn(reason)
                return turn
            return await self._cut_push(turn, reason)

    def _mark_cancel(self, turn: ChannelTurn, reason: EndReason) -> None:
        # Desde aquí el turno ya no cuenta progreso ni suena (#254, A-2): los suscriptores lo
        # ven en `cancel_reason` mientras su tarea se limpia.
        if turn.cancel_reason is None:
            turn.cancel_reason = reason

    async def _cancel_and_wait(self, task: asyncio.Task) -> None:
        # Sin tope: el turno nuevo no arranca hasta que el viejo terminó. Si cancelan a quien
        # llama, se termina de esperar y se relanza (#221); solo esa cancelación: una heredada
        # (cancelling() > 0) no debe tumbar el STT en un barge-in.
        td = Teardown(f"[{self._tag}] cancelar turno")
        await td.cancel(task, "turno anterior")
        if self._user_task is task:
            self._user_task = None
        td.raise_if_cancelled()

    async def _abort_other_runs(self) -> None:
        """Ningún otro run activo en la session_key cuando sale el chat.send del usuario:
        OpenClaw lo plegaría en ese run (docs/openclaw-protocol.md, #219) y la respuesta
        llegaría con otro runId. Pasa con un push que no llegó a abrirse (aún en la cola del
        worker) o que nadie veía. Lápida y chat.abort, bajo el lock de quien llama."""
        runs = self._turn_registry.active_runs(self.session_key)
        if not runs:
            return
        for run_id in runs:
            self._turn_registry.abort_run(run_id)
        logger.info(
            f"[{self._tag}] {len(runs)} run(s) de OpenClaw activos en la sesión: "
            f"chat.abort antes del send"
        )
        await self.orchestrator.abort(self.session_key)

    async def _run_user_turn(
        self, turn: ChannelTurn, text: str, tracker: PipelineTracker | None, allowed: bool
    ) -> None:
        if tracker is not None:
            tracker.start_turn()
        self._note_turn()
        gateway_stats.turn_started("normal")
        # Desde aquí sus suscriptores vieron (o verán) su turn_start: le sigue su fin (G-10).
        reason: EndReason = "error"
        try:
            if not allowed:
                # #397 (decisión a): la política del gateway, antes del chat.send. Solo el
                # gateway sabe qué agentes puede usar cada dispositivo; OpenClaw solo diría
                # INVALID_REQUEST.
                code = ORCHESTRATOR_AGENT_UNAVAILABLE_ERROR
                logger.warning(f"[{self._tag}] Turno sin agente disponible: {code}")
                gateway_stats.turn_error(code)
                self.emit(turn, ChannelError(code))
                return
            reason = await self._stream(turn, text, tracker)
        except asyncio.CancelledError:
            reason = turn.cancel_reason or "cancelled"
            raise
        except Exception as e:  # noqa: BLE001 — frontera: un turno roto no cierra el canal
            # Solo el tipo: el mensaje puede llevar texto del usuario.
            logger.error(f"[{self._tag}] Turno falló: {type(e).__name__}")
            turn.crashed = True
            gateway_stats.turn_error("internal_error")
            self.emit(turn, ChannelError("internal_error"))
        finally:
            self.end_turn(turn, reason)

    async def _stream(
        self, turn: ChannelTurn, text: str, tracker: PipelineTracker | None
    ) -> EndReason:
        # Los suscriptores solo encolan (decisión 1): nadie frena el stream de los demás, y uno
        # que se va no lo aborta (decisión 2: G-18 se invierte).
        async def on_token(token_text: str) -> None:
            self.emit(turn, ChannelToken(token_text))

        async def on_tool_call(tool_call: ToolCallEvent) -> None:
            self.emit(turn, ChannelToolCall(tool_call))

        async def on_progress(phase: ProgressPhase) -> None:
            self.emit(turn, ChannelProgress(Progress(phase)))

        try:
            await orchestration.call_orchestrator(
                self.orchestrator,
                text,
                self.session_key,
                tracker=tracker,
                on_token=on_token,
                on_tool_call=on_tool_call,
                on_progress=on_progress,
            )
            return "done"
        except RuntimeError as e:
            # Solo OrchestratorError lleva un código estable; cualquier otro texto de
            # excepción puede llevar detalle interno y no sale del proceso (#238).
            stable = isinstance(e, orchestration.OrchestratorError)
            code = str(e) if stable else "internal_error"
            logger.error(f"[{self._tag}] Orchestrator error: {code} ({type(e).__name__})")
            gateway_stats.turn_error(code)
            if not stable:
                logger.debug(f"[{self._tag}] turno: {traceback_no_message(e)}")
            self.emit(turn, ChannelError(code))
            return "timeout" if code == "turn_timeout" else "error"

    # -- push --------------------------------------------------------------------------

    def enqueue_push(self, kind: str, arg: Any, run_id: str | None = None) -> None:
        """Encola un evento de push para el worker (#130 S3a). Síncrono, nunca bloquea: lo
        llama el bucle de OpenClaw vía FrameDispatcher. `kind`: "turn_start" | "chat" |
        "tool" | "fallback" | "turn_end" | "turn_error"; `arg`: la session_key (turn_start,
        turn_end), el payload (chat/tool) o el `data` del `agent` (fallback/turn_error, #397);
        `run_id`: el runId del evento, si lo trae. No hace nada una vez cerrado el canal."""
        if self._closed:
            return
        if self._push_worker is None or self._push_worker.done():
            self._push_worker = self._hub.spawn(self._push_worker_loop(), "push_worker")
        backlog = self._backlog
        if backlog.pending == 0:
            backlog.since = time.monotonic()
        backlog.pending += 1
        backlog.peak = max(backlog.peak, backlog.pending)
        backlog.queue.put_nowait((kind, arg, run_id))

    async def push_idle(self) -> None:
        """Espera a que el worker termine todo lo encolado (para tests)."""
        await self._backlog.queue.join()

    def _push_budget_left(self) -> float | None:
        """Segundos que le quedan al push abierto sin progreso (#286); None si no aplica."""
        push = self._push
        if push is None or push.ended:
            return None
        return push.progress_at + settings.TURN_TIMEOUT_S - time.monotonic()

    async def _push_worker_loop(self) -> None:
        queue = self._backlog.queue
        while not self._closed:
            budget = self._push_budget_left()
            # Con eventos en cola el push no está parado: solo se espera con tope si no hay.
            if budget is not None and queue.empty():
                try:
                    kind, arg, run_id = await asyncio.wait_for(queue.get(), timeout=budget)
                except TimeoutError:
                    await self._run_push_step("timeout", self._push_timed_out())
                    continue
            else:
                kind, arg, run_id = await queue.get()
            try:
                if self._run_aborted(run_id):
                    # #285: lo que quedaba en cola de un run abortado no llega (ni abre push).
                    logger.debug(f"[{self._tag}] push {kind} de un run abortado descartado")
                elif kind == "turn_start":
                    await self._push_start(run_id)
                elif kind == "chat":
                    self._push_chat(arg)
                elif kind == "tool":
                    self._push_tool(arg)
                elif kind == "fallback":
                    self._push_fallback(arg, run_id)
                elif kind == "turn_end":
                    self._push_end()
                elif kind == "turn_error":
                    self._push_end(failure=arg)
                else:
                    logger.warning(f"[{self._tag}] push event desconocido: {kind!r}")
            except Exception as e:  # noqa: BLE001 — frontera: un evento roto no para el worker
                self._log_push_failure(kind, e)
            finally:
                backlog = self._backlog
                backlog.pending = max(0, backlog.pending - 1)
                if backlog.pending == 0:
                    backlog.since = None
                queue.task_done()
            if self._backlog.pending == 0:
                self._hub.drop_if_idle(self.session_key, self)

    async def _run_push_step(self, what: str, step: Coroutine[Any, Any, None]) -> None:
        try:
            await step
        except Exception as e:  # noqa: BLE001 — frontera: un paso roto no para el worker
            self._log_push_failure(what, e)

    def _log_push_failure(self, what: str, e: Exception) -> None:
        # Solo el tipo: el payload puede llevar texto del usuario.
        logger.warning(f"[{self._tag}] push {what} falló: {type(e).__name__}")

    def _run_aborted(self, run_id: str | None) -> bool:
        return run_id is not None and self._turn_registry.is_run_aborted(run_id)

    def _abort_run(self, run_id: str | None) -> None:
        if run_id is not None:
            self._turn_registry.abort_run(run_id)

    def _push_audience(self) -> list[ChannelSubscriber]:
        """Los suscriptores que reciben un push: lo aceptan y pueden usar su agente."""
        key = self.session_key
        return [
            s.subscriber
            for s in self._subscriptions
            if s.subscriber.wants_push and s.subscriber.agent_allowed(key)
        ]

    async def _push_start(self, run_id: str | None = None) -> None:
        key = self.session_key
        subscribers = [s.subscriber for s in self._subscriptions]
        if subscribers and not any(s.agent_allowed(key) for s in subscribers):
            # C-r7-1: push de un agente retirado de allowed_agents de todos los adjuntos: no
            # llega a nadie y se para en OpenClaw.
            logger.info(f"[{self._tag}] push de un agente no permitido: descartado")
            push = self._push
            if push is not None:
                # Su push ya abierto (retirado a medias) se corta ya, sin esperar a turn_timeout.
                async with self._lock:
                    await self._cut_push(push, "cancelled")
            self._abort_run(run_id)
            await self.orchestrator.abort(key)
            return
        while True:
            # Un turno del usuario vive hasta que OpenClaw lo termina: no se abre un push
            # encima (su audio lo espera cada suscriptor en su cola, decisión 1).
            # asyncio.wait (no await): nuestra cancelación se propaga, la del turno no.
            while self._user_task is not None and not self._user_task.done():
                await asyncio.wait({self._user_task})
            # Abrirlo, bajo el lock: un `send` o un corte lo ven entero, registrado y con su
            # on_turn_start ya entregado, nunca a medias (#202).
            async with self._lock:
                if self._closed:
                    return
                if self._user_task is not None and not self._user_task.done():
                    continue  # un turno que se estaba sustituyendo: el usuario gana
                if self._push is not None:
                    # #84: los `agent start` intermedios (tool use, varios pasos) no abren
                    # otro turno; tampoco el de un push que se está cortando.
                    self._push.progress_at = time.monotonic()
                    logger.debug(
                        f"[{self._tag}] push_turn_start ignorado: ya hay un push abierto"
                    )
                    return
                audience = self._push_audience()
                audio = next((s for s in reversed(audience) if s.wants_audio), None)
                self._push_text.reset()
                turn = self.open_turn("push", run_id=run_id, audio_to=audio)
                self._push = turn
                break
        if audience:  # un push que alguien ve cuenta en la conversación
            self._note_turn()
            gateway_stats.turn_started("push")
        early = self._take_early_phase(run_id)
        if early is not None:  # #254: la fase que su run contó antes de abrirse
            self.emit(turn, ChannelProgress(Progress(early)))

    def _push_chat(self, payload: dict) -> None:
        push = self._push
        phase = chat_status_phase(payload)
        run_id = payload.get("runId")
        if phase is not None and (
            push is None or push.ended or _other_run(push.run_id, run_id)
        ):
            # #254: fase de arranque de un run cuyo push aún no se abrió (llega antes de su
            # `agent start`): se guarda para cuando se abra.
            self._note_early_phase(run_id, phase)
            return
        if push is None or push.ended:
            return  # #109: nada fuera de un turn_start/turn_end
        push.progress_at = time.monotonic()
        if phase is not None:  # #254: una fase de arranque, sin texto
            self.emit(push, ChannelProgress(Progress(phase)))
            return
        # #241: `final` lleva el texto completo sin deltaText (OpenClaw 2026.10.5).
        delta = self._push_text.feed(payload)
        if delta:
            self.emit(push, ChannelToken(delta))

    def _push_tool(self, data: dict) -> None:
        push = self._push
        if push is None or push.ended:
            return
        push.progress_at = time.monotonic()
        tool_call = ToolCallEvent.from_session_tool_payload(data)
        if tool_call is not None:
            self.emit(push, ChannelToolCall(tool_call))

    def _push_fallback(self, data: dict, run_id: str | None) -> None:
        """#397: un `fallback_step` del run del push abierto: su razón, para clasificar su error."""
        push = self._push
        if push is not None and not push.ended and not _other_run(push.run_id, run_id):
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
        if failure is not None:
            code = classify_failure(failure, fallback_reason=push.fallback_reason)
            logger.warning(f"[{self._tag}] push falló en OpenClaw: {code}")
            gateway_stats.turn_error(code)
            self.emit(push, ChannelError(code))
        self._push = None
        self.end_turn(push, "done" if failure is None else "error")

    async def _push_timed_out(self) -> None:
        """#286: un push sin progreso en TURN_TIMEOUT_S se cierra como el turno normal."""
        # Bajo el lock, como todo corte (G-08): un `send` que llegue mientras tanto espera, y
        # el chat.abort del push sale antes que su chat.send. Se decide dentro: si un `send`
        # o un barge-in lo cortaron mientras se esperaba el lock, no hubo timeout.
        async with self._lock:
            push = self._push
            if push is None or push.ended:
                return
            logger.warning(
                f"[{self._tag}] push sin progreso en "
                f"TURN_TIMEOUT_S={settings.TURN_TIMEOUT_S}s — se cierra."
            )
            gateway_stats.turn_error("turn_timeout")
            self.emit(push, ChannelError("turn_timeout"))
            await self._cut_push(push, "timeout")

    async def _cut_push(self, expected: ChannelTurn | None, reason: EndReason) -> ChannelTurn | None:
        """Corta el push abierto (7e): lo cierra para todos y lo aborta en OpenClaw. Con
        `expected`, solo ese push. Devuelve el push cortado, o None si no había."""
        push = self._push
        if push is None or push.ended:
            return None
        if expected is not None and expected is not push:
            return None
        # Antes del abort: nada más de su run llega, ni lo que el worker tiene en cola ni lo
        # que venga. abort() ve el run ya con lápida y no espera su `aborted`.
        self._abort_run(push.run_id)
        self._mark_cancel(push, reason)
        self._push = None
        self.end_turn(push, reason)
        td = Teardown(f"[{self._tag}] corte del push")
        await td.step(self.orchestrator.abort(self.session_key), "chat.abort del push")
        td.raise_if_cancelled()
        return push

    # -- progress (#254) ---------------------------------------------------------------

    def _note_early_phase(self, run_id: object, phase: ProgressPhase) -> None:
        """Guarda la última fase de arranque de un run sin push abierto (acotado: los últimos
        `_EARLY_PHASE_RUNS` runs, `_EARLY_PHASE_TTL_S`). Solo si alguien la vería."""
        if not isinstance(run_id, str) or not any(
            s.subscriber.wants_progress for s in self._subscriptions if s.subscriber.wants_push
        ):
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
        worker = self._push_worker
        if worker is not None and not worker.done() and worker is not asyncio.current_task():
            worker.cancel()

    async def aclose(self, limit: float | None = None) -> None:
        """Cierre del canal (apagado del gateway, o el hub privado de una sesión de tests):
        su worker y su turno se cancelan y su turno abierto se cierra (`closed`). Cada paso
        termina aunque cancelen a quien cierra (#221)."""
        self._closed = True
        async with Teardown(f"[{self._tag}] cierre del canal") as td:
            await td.cancel(self._push_worker, "push worker", limit)
            turn = self._user_turn
            if turn is not None and not turn.ended:
                self._mark_cancel(turn, "closed")
            await td.cancel(self._user_task, "turno activo", limit)
            backlog = self._backlog
            while not backlog.queue.empty():
                backlog.queue.get_nowait()
                backlog.queue.task_done()
            backlog.pending = 0
            backlog.since = None
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
    `sessionKey` → its conversation) and the turn counter."""

    def __init__(
        self,
        *,
        index: ConversationIndex | None = None,
        orchestrator: OrchestratorProtocol | None = None,
        turn_registry: TurnRegistry | None = None,
        note_turn: Callable[[str], None] | None = None,
    ) -> None:
        self._channels: dict[str, ConversationChannel] = {}
        self.index = index
        # Set after construction in main.py: the OpenClaw client needs the dispatcher, and the
        # dispatcher this hub.
        self.orchestrator = orchestrator
        self.turn_registry = turn_registry if turn_registry is not None else TurnRegistry()
        self.note_turn = note_turn
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
        return channel

    def channel_for(self, session_key: str) -> ConversationChannel | None:
        """The channel of a push's `sessionKey` (decision 5: the conversation from the index,
        synchronously). None for a key no conversation has: that push goes nowhere."""
        channel = self._channels.get(session_key)
        if channel is not None:
            return channel
        conversation = self.index.get(session_key) if self.index is not None else None
        return self.get_or_create(conversation) if conversation is not None else None

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
        async with Teardown("cierre de los canales") as td:
            for channel in list(self._channels.values()):
                await td.step(channel.aclose(limit), f"canal {channel.conversation.id}")
            self._channels.clear()
            for task in list(self._tasks):
                await td.cancel(task, f"tarea {task.get_name()}", limit)
