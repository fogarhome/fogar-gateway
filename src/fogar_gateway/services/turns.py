"""Turnos de cara al cliente: `TurnController` y el TTS de cada turno (M19; #271, #285, #286).

Un turno es lo que el cliente ve entre un `turn_start` y su `turn_end`. Hay dos orígenes:
`user` (un `send`) y `push` (OpenClaw abre una ejecución por su cuenta). Desde la 5.0 (PR 2a,
#406) cada conversación tiene un canal (`services/channel.py`), el único dueño de sus runs en
OpenClaw: el lock de envío, la tabla de runs, el worker de sus frames, las lápidas, la cuenta de
turnos. `TurnController` es la parte de una sesión: un suscriptor del canal que pinta cada
turno del canal a su manera, con las mismas reglas de siempre por socket:

- Un solo turno abierto por sesión (#202): las callbacks del canal solo encolan en la cola
  ordenada de la sesión; su worker abre el siguiente turno detrás del `turn_end` del anterior
  (decisión 1 del plan de la PR 2: el canal no espera a nadie, cada socket cierra lo suyo).
- Exactamente un `turn_end` por `turn_start`, sea cual sea el camino (`_end`, idempotente).
- Su propia numeración (`t-{n}`, `turn_seq` & 0xFFFF), su `output_mode`, sus `tool_calls`, su
  `push_enabled` (sin él no ve los push) y su `turn_progress` (#254) con su ritmo.
- Un TTS por turno (`TurnTts`), solo en la sesión que suena: la que envió un turno del usuario
  o, para un push, la que el canal elige (2a: la última adjunta con audio; 2c lo sustituye).
  Abrir, alimentar, terminar esperando el audio por inactividad, y liberar; las métricas
  (`tts_start`, `tts_first_chunk`, `tts_done`) son las mismas para los dos orígenes.

Cada turno lleva su conversación y su origen en `turn_start`, y el motivo de su fin
(`EndReason`) en `turn_end` (M19). La conversación de la sesión se evalúa al empezar cada turno
del usuario (cambio de día, services/conversations.py): la sesión deja el canal de ayer y se
adjunta al de hoy.

Varias sesiones en una conversación (5.0 PR 2a.3): cada una es un suscriptor más del canal y
recibe su propio par de cada turno. Una sesión que se adjunta con un turno abierto recibe su
`turn_start` y, si muestra texto, un `token` con lo que el turno ya llevaba (`ChannelTurn.text`),
sin audio; después, los mensajes que siguen pendientes. Una sesión que se va (desconexión,
revocación, inactividad) cierra lo suyo con `closed` y se desuscribe: nunca cancela el turno
del canal ni aborta su run.

La cola de OpenClaw (5.0 PR 2b): un `send` ya no sustituye el turno en curso; su mensaje entra
en la cola (`pending`) y su turno se abre cuando le toca. Cada mensaje aceptado llega a cada
sesión como `message` (por su cola ordenada, con su estado: `pending`, `delivered`,
`withdrawn`), también a quien lo envió; `turn_start` dice quién lo inició (`by`). Solo cortan
`cancel`, un `send` con `interrupt` y `withdraw` (un pendiente).
"""

import asyncio
import logging
import time
from collections import deque
from collections.abc import Callable, Coroutine
from dataclasses import dataclass, field
from typing import Any, Literal

from fogar_gateway.core.config import settings
from fogar_gateway.models.schemas import Client, ClientConfig, Handshake
from fogar_gateway.services.channel import (
    ChannelError,
    ChannelEvent,
    ChannelHub,
    ChannelMessage,
    ChannelProgress,
    ChannelToken,
    ChannelToolCall,
    ChannelTurn,
    ConversationChannel,
    SendTicket,
)
from fogar_gateway.services.conversations import SessionConversation
from fogar_gateway.services.openclaw.failures import ORCHESTRATOR_AGENT_UNAVAILABLE_ERROR
from fogar_gateway.services.openclaw.models import ToolCallEvent
from fogar_gateway.services.outbound import ClientGone, OutboundSender, log_send_failed
from fogar_gateway.services.pacing import AudioPacer
from fogar_gateway.services.pipeline_tracker import PipelineTracker
from fogar_gateway.services.reconnection import describe_error, traceback_no_message
from fogar_gateway.services.speech.ports import TextToSpeechProvider, TtsStream
from fogar_gateway.services.stats import gateway_stats
from fogar_gateway.services.teardown import Teardown
from fogar_gateway.services.turn_progress import Progress, TurnProgress

logger = logging.getLogger(__name__)

TurnOrigin = Literal["user", "push"]
# done: terminó solo · error: falló (TURN_ERROR o fallo del agente) · cancelled: `cancel` de
# un cliente · interrupted: un `send` que interrumpe, o el usuario habló encima (barge-in o
# transcripción final) · replaced: la sesión pasó a otra conversación (cambio de día), o un
# `send` que no llegó al canal dejó atrás el turno de este socket · timeout: TURN_TIMEOUT_S
# sin progreso · closed: la sesión se cierra o el cliente se fue.
EndReason = Literal["done", "error", "cancelled", "interrupted", "replaced", "timeout", "closed"]


def _tool_call_message(turn_id: str, tool_call: ToolCallEvent) -> dict:
    return {
        "type": "tool_call",
        "turn_id": turn_id,
        "phase": tool_call.phase,
        "name": tool_call.name,
        "tool_call_id": tool_call.tool_call_id,
        "args": tool_call.args,
        "result": tool_call.result,
        "is_error": tool_call.is_error,
    }


@dataclass(eq=False)
class Turn:
    """Un turno de cara al cliente. Solo IDs, tiempos y estados: nunca texto."""

    turn_id: str  # `t-<contador completo>`
    seq: int  # el contador & 0xFFFF: el uint16 de `turn_start` y del header de audio (G-11)
    origin: TurnOrigin
    started_at: float
    progress_at: float  # último evento del canal (el push cuenta en in_flight, #286)
    conversation_id: str | None = None  # su conversación
    channel: ChannelTurn | None = None  # el turno del canal que pinta
    opened: bool = False  # su turn_start ya está en la cola del cliente
    ended: bool = False  # su turn_end ya está en la cola (o se abandonó al cerrar)
    end_reason: EndReason | None = None
    cancel_reason: EndReason | None = None  # quien lo corta en esta sesión dice por qué
    silent: bool = False  # sin audio en esta sesión: dejó su canal, o un `send` lo dejó atrás
    tts: "TurnTts | None" = None  # push: su TTS
    pending: "_PendingTts | None" = None  # user: su TTS mientras conecta (E-2)
    progress: TurnProgress = field(default_factory=TurnProgress)  # su `turn_progress` (#254)
    progress_flush: asyncio.Task | None = None  # el `turn_progress` aplazado por el ritmo

    @property
    def halted(self) -> bool:
        """Terminado o cancelado (aquí o en su canal): ni audio ni progreso desde ya (A-2)."""
        return (
            self.ended
            or self.cancel_reason is not None
            or (self.channel is not None and self.channel.cancel_reason is not None)
        )


@dataclass
class _TtsStats:
    """Tiempos (ms) del TTS de un turno; solo números (el evento `push_tts`)."""

    send_ms: float = 0.0
    end_ms: float = 0.0
    drain_ms: float = 0.0
    close_ms: float = 0.0
    dropped: bool = False
    chars: int = 0

    def add(self, key: str, t0: float) -> None:
        setattr(self, key, getattr(self, key) + (time.monotonic() - t0) * 1000)

    def as_meta(self) -> dict[str, Any]:
        return {
            k: round(v, 1) if isinstance(v, float) else v for k, v in self.__dict__.items()
        }


class TurnTts:
    """El TTS de un turno (#271): `start`, `feed`, `end` + `drain`, `release`.

    Toda espera está acotada por PUSH_TTS_DRAIN_TIMEOUT_S (#199, #214); la del audio, por
    inactividad (cada trozo reinicia la ventana), así que una respuesta larga que suena a su
    ritmo nunca se corta. Un TTS caído se abandona una vez (un WARNING) y el turno sigue solo
    con texto. Su audio sale con el `turn_seq` del turno y nunca después de su `turn_end`.
    """

    def __init__(
        self,
        stream: TtsStream,
        turn: Turn,
        *,
        sender: OutboundSender,
        tracker: PipelineTracker,
        client_id: str,
        on_client_gone: Callable[[], None],
        supervise: Callable[[asyncio.Task], None],
    ) -> None:
        self._stream = stream
        self._turn = turn
        self._sender = sender
        self._tracker = tracker
        self._client_id = client_id
        self._on_client_gone = on_client_gone
        self._supervise = supervise
        self._audio: asyncio.Task | None = None
        self._progress_at: float | None = None
        self._started = False
        self._shut = False
        self._released = False
        self.dropped = False
        self.stats = _TtsStats()

    @property
    def live(self) -> bool:
        return not self.dropped and not self._released

    def _meta(self) -> dict[str, str]:
        # Las métricas del push llevan su origen: turn_e2e_ms no las mezcla con las del turno
        # del usuario (las del usuario quedan igual que antes).
        return {"origin": "push"} if self._turn.origin == "push" else {}

    def progressing(self, now: float) -> bool:
        """Su audio sigue saliendo (avanzó en la ventana de PUSH_TTS_DRAIN_TIMEOUT_S)."""
        audio = self._audio
        return (
            audio is not None
            and not audio.done()
            and self._progress_at is not None
            and now - self._progress_at < settings.PUSH_TTS_DRAIN_TIMEOUT_S
        )

    async def start(self, voice: str) -> None:
        await self._tracker.record("tts_start", voice=voice, **self._meta())
        if not self.live:  # liberado mientras se registraba (corte o cancelación)
            return
        self._started = True
        self._progress_at = time.monotonic()
        name = "push_audio" if self._turn.origin == "push" else "turn_audio"
        self._audio = asyncio.create_task(self._pipe(), name=name)
        # Supervisada: se puede abandonar sin recoger su resultado (#221).
        self._supervise(self._audio)

    async def _pipe(self) -> None:
        header = bytes([0xA1]) + self._turn.seq.to_bytes(2, "big")
        pacer = AudioPacer(settings.AUDIO_LEAD_S, settings.AUDIO_PACING_ENABLED)
        first = True
        async for chunk in self._stream.get_audio_stream():
            self._progress_at = time.monotonic()
            if not chunk:
                continue
            if first:
                first = False
                await self._tracker.record("tts_first_chunk", **self._meta())
            async for frame in pacer.frames(chunk):
                if self._turn.halted:
                    return  # nada después de su turn_end, ni desde que se cancela
                try:
                    await self._sender.send_bytes(header + frame)
                except Exception as e:  # noqa: BLE001 — frontera: envío a un cliente que puede haberse ido
                    if isinstance(e, ClientGone):
                        self._on_client_gone()
                    logger.debug(
                        f"[{self._client_id}] envío de audio al cliente falló: {describe_error(e)}"
                    )
                    return
                self._progress_at = time.monotonic()  # cada frame con ritmo es progreso
        await self._tracker.record("tts_done", **self._meta())

    async def feed(self, text: str) -> None:
        if not self.live:
            return
        self.stats.chars += len(text)
        t0 = time.monotonic()
        try:
            await asyncio.wait_for(
                self._stream.send_text_chunk(text), timeout=settings.PUSH_TTS_DRAIN_TIMEOUT_S
            )
        except Exception as e:  # noqa: BLE001 — frontera: el TTS caído no rompe el turno
            await self.drop(f"send_text_chunk {type(e).__name__}")
        finally:
            self.stats.add("send_ms", t0)

    async def end(self) -> None:
        """Avisa al TTS de que no hay más texto."""
        if not self.live:
            return
        t0 = time.monotonic()
        try:
            await asyncio.wait_for(self._stream.end(), timeout=settings.PUSH_TTS_DRAIN_TIMEOUT_S)
        except Exception as e:  # noqa: BLE001 — frontera: el TTS caído no rompe el turno
            await self.drop(f"end {type(e).__name__}")
        finally:
            self.stats.add("end_ms", t0)

    async def drain(self) -> None:
        """Espera a que salga todo su audio, acotada por INACTIVIDAD.

        La ventana empieza al empezar a esperar (7e: tras una pausa larga del agente el sello
        de progreso es viejo). asyncio.wait, no wait_for: una cancelación ajena (barge-in,
        close_all) se propaga en vez de confundirse con la del audio.
        """
        audio = self._audio
        if not self.live or audio is None:
            return
        t0 = time.monotonic()
        self._progress_at = t0
        try:
            while not audio.done():
                idle_left = settings.PUSH_TTS_DRAIN_TIMEOUT_S - (
                    time.monotonic() - (self._progress_at or t0)
                )
                if idle_left <= 0:
                    await self.drop("el audio no progresa")
                    break
                await asyncio.wait({audio}, timeout=idle_left)
        finally:
            self.stats.add("drain_ms", t0)

    async def finish(self) -> None:
        await self.end()
        await self.drain()

    async def drop(self, reason: str, level: int = logging.WARNING) -> None:
        """Abandona el TTS (una vez por turno: las siguientes llamadas no hacen nada)."""
        if not self.live:
            return
        self.dropped = True
        self.stats.dropped = True
        rest = "; el turno sigue solo con texto." if level >= logging.WARNING else "."
        logger.log(
            level,
            f"[{self._client_id}] TTS del turno {self._turn.turn_id} abandonado ({reason}){rest}",
        )
        await self._shutdown()

    async def release(self) -> None:
        """Para el audio y cierra el stream; el push registra su evento `push_tts`."""
        if self._released:
            return
        self._released = True
        await self._shutdown()
        if self._started and self._turn.origin == "push":
            await self._tracker.record("push_tts", **self.stats.as_meta())

    async def _shutdown(self) -> None:
        """Para el audio y cierra el stream. Los dos pasos terminan aunque cancelen a quien
        libera (#221): una segunda cancelación (un barge-in y después close_all) no deja el
        stream abierto; esa cancelación se relanza al final. Una heredada no (como en el
        barge-in, `raise_if_cancelled`). Cada paso, acotado por PUSH_TTS_DRAIN_TIMEOUT_S
        (#204: un audio que ignora la cancelación no cuelga a nadie)."""
        if self._shut:
            return
        self._shut = True
        limit = settings.PUSH_TTS_DRAIN_TIMEOUT_S
        td = Teardown(f"[{self._client_id}] TTS del turno {self._turn.turn_id}")
        await td.cancel(self._audio, "audio del turno", limit)
        t0 = time.monotonic()
        await td.step(self._stream.close(), "cierre del TTS", limit)
        self.stats.add("close_ms", t0)
        td.raise_if_cancelled()


class _PendingTts:
    """El TTS de un turno del usuario mientras conecta (auditoría 3.0, E-2): el texto que llega
    antes de conectar se guarda y se le da en orden en cuanto conecta."""

    def __init__(
        self, connect: "Callable[[], Coroutine[Any, Any, TurnTts | None]]", voice: str, client_id: str
    ):
        self._voice = voice
        self._client_id = client_id
        self._buffer: list[str] = []
        self._lock = asyncio.Lock()  # el vaciado del buffer y los tokens nuevos, en orden
        self._ready = False
        self.tts: TurnTts | None = None
        self._task = asyncio.create_task(self._open(connect), name="tts_connect")

    async def _open(self, connect: "Callable[[], Coroutine[Any, Any, TurnTts | None]]") -> None:
        # La corrutina se crea aquí: una tarea cancelada antes de empezar no deja una sin esperar.
        self.tts = await connect()  # sin await hasta aquí: release() lo ve aunque cancelen
        async with self._lock:
            self._ready = True
            if self.tts is not None:
                await self.tts.start(self._voice)
                for text in self._buffer:
                    await self.tts.feed(text)
            self._buffer.clear()

    async def feed(self, text: str) -> None:
        async with self._lock:
            if not self._ready:
                self._buffer.append(text)
            elif self.tts is not None:
                await self.tts.feed(text)

    async def connected(self) -> "TurnTts | None":
        """Espera a la conexión (acotada por el proveedor) y a que reciba el texto pendiente.
        None si se soltó (`release`) mientras tanto: un turno cortado."""
        await asyncio.wait({self._task})
        if self._task.cancelled():
            return None
        self._task.result()  # un fallo inesperado sigue rompiendo el turno, como antes
        return self.tts

    async def release(self) -> None:
        """Cancela la conexión si sigue en marcha y libera el TTS que haya abierto (#221)."""
        td = Teardown(f"[{self._client_id}] TTS del turno")
        await td.cancel(self._task, "conexión del TTS", settings.PUSH_TTS_DRAIN_TIMEOUT_S)
        if self.tts is not None:
            await td.step(self.tts.release(), "TTS del turno")
        td.raise_if_cancelled()


# Fines que terminan de sonar su audio antes del turn_end (lo que no es un corte). El timeout
# del turno del usuario también (como antes: `stream_response` devolvía y se drenaba); el del
# push es un corte.
_DRAINED: frozenset[str] = frozenset({"done", "error"})


def _drains(turn: "Turn", reason: EndReason) -> bool:
    if turn.channel is not None and turn.channel.crashed:
        return False  # una tarea rota no espera a la voz (como antes: sin drenar)
    return reason in _DRAINED or (turn.origin == "user" and reason == "timeout")


@dataclass
class _Inbox:
    """La cola ordenada de la sesión (decisión 1): lo que el canal le entrega, en orden."""

    pending: int = 0
    peak: int = 0
    since: float | None = None
    queue: asyncio.Queue[tuple] = field(default_factory=asyncio.Queue)


class TurnController:
    """Los turnos de una sesión: suscriptor del canal de su conversación. Ver el docstring del
    módulo.

    Lo que es de la sesión (socket de entrada, STT, watchdogs, revalidación, cierre) queda en
    `FogarBridge`, que llama a `attach`, `start_user_turn`, `cancel`, `in_flight` y `close`.
    Lo que es de la conversación (runs, abortos, cola de push, lock de envío) es del canal
    (`services/channel.py`).
    """

    def __init__(
        self,
        *,
        client: Client,
        config: ClientConfig,
        handshake: Handshake,
        tts: TextToSpeechProvider,
        tracker: PipelineTracker,
        sender: OutboundSender,
        conversation: SessionConversation,
        channels: ChannelHub,
        spawn: Callable[[Coroutine[Any, Any, None], str], asyncio.Task],
        supervise: Callable[[asyncio.Task], None],
    ) -> None:
        self._client = client
        self._client_id = client.id
        self._config = config
        # C-r7-1: el `allowed_agents` vigente; la revalidación del bridge lo refresca.
        self.allowed_agents = config.allowed_agents
        self._wants_text = "text" in handshake.output_mode
        self._wants_audio = "audio" in handshake.output_mode
        # `turn_progress` (#254) va con los mensajes de diagnóstico del turno: `status`.
        self._wants_progress = "status" in handshake.output_mode
        self._tts = tts
        self._tracker = tracker
        self._sender = sender
        self._conversation = conversation
        self._channels = channels
        self._spawn = spawn
        self._supervise = supervise

        self._seq = 0
        self._channel: ConversationChannel | None = None  # el canal al que está adjunta
        self._last_sent: asyncio.Future[None] | None = None  # el run de su último `send`
        self._by_channel: dict[str, Turn] = {}  # ChannelTurn.id → su turno en esta sesión
        self._live: list[Turn] = []  # sus turnos sin turn_end, en orden
        # Serializa los `send` de la sesión (su cambio de día y su envío al canal).
        self._send_lock = asyncio.Lock()
        # 2b.3: los `send` del cliente esperan aquí, en su orden, a que su tarea los envíe
        # (`submit_send`): el bucle de entrada nunca se para detrás de un envío (uno que
        # espera a que termine el turno de /v1 puede tardar).
        self._sends: deque[tuple[str, SendTicket]] = deque()
        self._send_task: asyncio.Task | None = None
        self._sending: SendTicket | None = None  # el que su tarea tiene entre manos
        # Worker de la sesión: un solo worker conserva el orden start → eventos → fin de
        # cada turno, y el turn_start del siguiente detrás del turn_end del anterior.
        self._worker: asyncio.Task | None = None
        self._inbox = _Inbox()
        self._backlog_peak = 0  # máximo de la cola de push de los canales que ya dejó
        self._cuts: set[asyncio.Task] = set()  # turn_end de cortes en curso (on_turn_end)
        # Una vez puesto no vuelve atrás: ni turnos nuevos (G-09) ni turnos del canal.
        self._closing = False
        # El cliente se fue (desconexión o ClientGone): deja de enviar y de sonar (G-18).
        self.client_gone = False
        self.last_ended: Turn | None = None

    # ── Suscriptor del canal (ChannelSubscriber) ──────────────────────────

    @property
    def client(self) -> Client:
        return self._client

    @property
    def conversation_id(self) -> str:
        return self._conversation.id

    @property
    def wants_audio(self) -> bool:
        return self._wants_audio

    @property
    def wants_push(self) -> bool:
        return self._config.push_enabled

    @property
    def wants_progress(self) -> bool:
        return self._wants_progress

    @property
    def tracker(self) -> PipelineTracker:
        return self._tracker

    def agent_allowed(self, session_key: str) -> bool:
        allowed = self.allowed_agents
        parts = session_key.split(":")
        if allowed is None or len(parts) < 3 or parts[0] != "agent":
            return True
        return parts[1].lower() in {a.lower() for a in allowed}

    def on_turn_start(self, turn: ChannelTurn) -> None:
        if self._closing:
            return
        if turn.origin == "push":
            if not self._config.push_enabled:
                return  # el push entero se descarta para esta sesión
            channel = self._channel
            if channel is not None and not self.agent_allowed(channel.session_key):
                # C-r7-1: push de un agente retirado de allowed_agents: no llega al cliente.
                logger.info(f"[{self._client_id}] push de un agente no permitido: descartado")
                return
        local = self._new_turn(turn.origin)
        local.conversation_id = turn.conversation_id
        local.channel = turn
        self._by_channel[turn.id] = local
        self._live.append(local)
        self._put(("turn_start", local))
        caught_up = turn.text
        if caught_up:
            # 2a.3: se adjunta con el turno ya en marcha. Lo dicho hasta ahora le llega en un
            # solo `token` (solo si muestra texto), nunca como voz: este turno no suena aquí.
            local.silent = True
            if self._wants_text:
                self._put(("event", local, ChannelToken(caught_up)))

    def on_event(self, turn: ChannelTurn, event: ChannelEvent) -> None:
        local = self._by_channel.get(turn.id)
        if local is not None and not local.ended:
            self._put(("event", local, event))

    def on_turn_end(self, turn: ChannelTurn, reason: EndReason) -> None:
        local = self._by_channel.get(turn.id)
        if local is None or local.ended:
            return
        self._put(("turn_end", local, reason))
        if local.opened and not _drains(local, reason):
            # Un corte (un `send` que lo sustituye, su timeout, un agente retirado): su turn_end
            # sale ya (A-r2-2), sin esperar detrás de un worker ocupado con su TTS; lo que venga
            # después sigue en la cola, detrás de él. Su fin encolado ya no hará nada.
            local.cancel_reason = local.cancel_reason or reason
            task = asyncio.create_task(self._cut(local, reason), name="turn_cut")
            self._cuts.add(task)
            task.add_done_callback(self._cuts.discard)
            self._supervise(task)

    def on_message(self, message: ChannelMessage) -> None:
        """Un mensaje de usuario de la conversación (2b), por la cola ordenada de la sesión:
        detrás de lo que ya esperaba, delante del turno que lo responda."""
        if self._closing:
            return
        self._put(("message", message))

    # ── Canal ─────────────────────────────────────────────────────────────

    @property
    def channel(self) -> ConversationChannel | None:
        """El canal al que está adjunta la sesión, si lo está."""
        return self._channel

    def attach(self) -> None:
        """Se adjunta al canal de la conversación de la sesión (tras el health check, antes de
        `ready`: claim_conversation). Con un turno abierto, recibe su turn_start ya.

        Síncrono a propósito: claim_conversation lo llama justo antes de encolar `ready`, y un
        await aquí dejaría que un turn_start del canal saliera antes que `ready`."""
        if self._closing or self._channel is not None:
            return
        conversation = self._conversation.current
        client = self._client
        if not conversation.visible_to(client.home_id, client.user_id):
            # M20: el handshake ya lo miró (4100); un dispositivo nunca se adjunta a una
            # conversación que no puede ver (privada de otra persona, u otro hogar). Sin canal,
            # cada `send` recibe un TURN_ERROR estable.
            logger.warning(
                f"[{self._client_id}] La conversación {conversation.id} no es visible para este "
                "dispositivo: no se adjunta a su canal."
            )
            return
        channel = self._channels.get_or_create(conversation)
        self._channel = channel  # antes de subscribe: su on_turn_start lo mira
        if not channel.subscribe(self):
            # No debería pasar (el handshake ya miró `visible_to`): sin canal, cada `send`
            # recibe un TURN_ERROR estable en vez de silencio.
            self._channel = None
            self._channels.drop_if_idle(channel.session_key, channel)
            logger.warning(
                f"[{self._client_id}] La sesión no pudo adjuntarse al canal de su conversación "
                f"{self._conversation.id}."
            )

    async def _follow_conversation(self) -> None:
        """Decisión 6 (cambio de día): la sesión cierra su turno del canal de ayer (`replaced`,
        sin esperar a su TTS), lo deja (el turno sigue para los demás) y se adjunta al de hoy."""
        channel = self._channel
        if channel is not None and channel.conversation.id != self._conversation.id:
            self._channel = None
            await self._leave(channel, "replaced")
        self.attach()

    async def _leave(self, channel: ConversationChannel, reason: EndReason) -> None:
        """Deja `channel` (cambio de día): nada más llega de él. Sus turnos en esta sesión se
        cierran con `reason`: los abiertos ya (sin esperar a su audio), los que aún esperan su
        turn_start en la cola, detrás de él (G-r2-1: todo turno visto recibe su par). Nunca
        cancela el turno del canal ni aborta su run (decisión 2)."""
        channel.unsubscribe(self)
        self._backlog_peak = max(self._backlog_peak, channel.push_backlog()[1])
        self._channels.drop_if_idle(channel.session_key, channel)
        conversation_id = channel.conversation.id
        for turn in self._live:
            if turn.conversation_id == conversation_id:
                turn.silent = True
        for turn in list(self._live):
            if turn.conversation_id == conversation_id and turn.opened:
                turn.cancel_reason = turn.cancel_reason or reason
                await self._end(turn, reason)
        self._enqueue(("leave", conversation_id, reason))

    # ── Estado ────────────────────────────────────────────────────────────

    def in_flight(self) -> bool:
        """Hay un turno en curso: el único cálculo para los dos watchdogs.

        El turno del usuario está acotado por dentro (TURN_TIMEOUT_S sin progreso en el canal, y su
        audio por inactividad). El push y la cola, por TURN_TIMEOUT_S sin progreso, así que
        un worker atascado no para el idle watchdog para siempre; un push cuyo audio sigue
        sonando (con ritmo, puede durar más) cuenta mientras ese audio avanza.
        """
        now = time.monotonic()
        channel = self._channel
        if channel is not None and channel.user_turn_running():
            return True
        if self.sends_waiting:
            return True  # acotado: el turno que espera (/v1, TURN_TIMEOUT_S) o el ack
        for turn in self._live:
            if turn.origin == "user":
                return True
            if now - turn.progress_at < settings.TURN_TIMEOUT_S:
                return True
            if turn.tts is not None and turn.tts.progressing(now):
                return True
        if channel is not None and self._config.push_enabled:
            pending, _, since = channel.push_backlog()
            if pending > 0 and (since is None or now - since < settings.TURN_TIMEOUT_S):
                return True
        inbox = self._inbox
        return inbox.pending > 0 and (
            inbox.since is None or now - inbox.since < settings.TURN_TIMEOUT_S
        )

    def snapshot(self, now: float) -> dict[str, Any]:
        """El turno en curso y la cola de push para /admin/state (#219): solo números."""
        kind, since = "idle", None
        user = next((t for t in self._live if t.origin == "user"), None)
        push = next((t for t in self._live if t.origin == "push"), None)
        channel = self._channel
        if user is not None:
            kind, since = "normal", user.started_at
        elif channel is not None and channel.user_turn_running():
            kind = "normal"
        elif push is not None:
            kind, since = "push", push.started_at
        pending = self._inbox.pending
        if channel is not None:
            pending += channel.push_backlog()[0]
        return {
            "turn": {"kind": kind, "since_s": round(now - since, 3) if since else None},
            "push_backlog": {"pending": pending, "peak": max(pending, self.push_backlog_peak)},
        }

    @property
    def push_backlog_peak(self) -> int:
        """El máximo de lo que esperó: la cola de push de su canal y la de la sesión."""
        channel = self._channel
        peak = channel.push_backlog()[1] if channel is not None else 0
        return max(peak, self._backlog_peak, self._inbox.peak)

    @property
    def user_task(self) -> asyncio.Future[None] | None:
        """Done once the run of the last message this session sent ended in its channel (its
        reply or an error; cancelled if the gateway cut it: a cancel, an interrupt, a withdraw,
        the close); None before any, and once cut."""
        last = self._last_sent
        return None if last is None or last.cancelled() else last

    async def sends_idle(self) -> None:
        """Espera a que salgan los `send` que esperan en la cola de la sesión (para tests)."""
        while self.sends_waiting:
            task = self._send_task
            if task is not None and not task.done():
                await asyncio.wait({task})
            else:
                await asyncio.sleep(0)

    async def idle(self) -> None:
        """Espera a que la cola de la sesión esté vacía (para tests)."""
        await self._inbox.queue.join()

    async def push_idle(self) -> None:
        """Espera a que el canal y la sesión terminen todo lo encolado (para tests)."""
        channel = self._channel
        if channel is not None:
            await channel.push_idle()
        await self.idle()

    # ── Turno del usuario ─────────────────────────────────────────────────

    def submit_send(self, text: str) -> None:
        """Un `send` del cliente (el bucle de entrada, 2b.3): a la cola de envíos de la sesión,
        sin esperar. Una sola tarea los envía en su orden de llegada (`start_user_turn`); uno
        que espera (detrás del turno de /v1) no tiene id ni sale nada al cable hasta que se
        envía, y el bucle de entrada sigue leyendo (audio, `cancel`, `withdraw`). «Enviar
        ahora» es `interrupt_now` y después esto: el envío no vuelve a cortar nada (N1)."""
        if self._closing:
            logger.info(f"[{self._client_id}] send ignorado: la sesión se está cerrando.")
            return
        self._sends.append((text, SendTicket()))
        if self._send_task is None or self._send_task.done():
            self._send_task = self._spawn(self._send_loop(), "turn_sends")

    def cancel_sends(self) -> None:
        """Un `cancel` del cliente (N2) para «lo que iba a empezar»: sus `send` aún sin
        respuesta de OpenClaw, solo si nada va delante de ellos (sin turno abierto ni run sin
        terminar en el canal, `nothing_ahead`): el que está en `chat.send` se aborta con su ack
        sin anunciarse; los que esperan aquí no salen. Con algo delante, el `cancel` para eso y
        sus mensajes siguen y quedan pendientes como cualquier otro (da igual si su ack ya
        llegó). Detrás de /v1 (aproximado: `exclusive_pending`) los que esperan siguen."""
        channel = self._channel
        if channel is None or not channel.nothing_ahead:
            return
        behind_v1 = channel.exclusive_pending
        current = self._sending
        if current is not None and (current.sending or not behind_v1):
            current.cut = True
        if not behind_v1:
            for _, ticket in self._sends:
                ticket.cut = True

    async def _send_loop(self) -> None:
        while self._sends and not self._closing:
            text, ticket = self._sends.popleft()
            if ticket.cut:
                logger.info(f"[{self._client_id}] send cancelado antes de salir")
                continue
            self._sending = ticket
            try:
                await self.start_user_turn(text, ticket=ticket)
            except Exception as e:  # noqa: BLE001 — frontera: un envío roto no para los siguientes
                # Solo el tipo: el mensaje puede llevar texto del usuario.
                logger.error(f"[{self._client_id}] send falló: {describe_error(e)}")
                logger.debug(f"[{self._client_id}] send: {traceback_no_message(e)}")
            finally:
                self._sending = None

    @property
    def sends_waiting(self) -> bool:
        """Hay `send` de la sesión aún sin enviar (esperan en su cola o en el canal)."""
        task = self._send_task
        return bool(self._sends) or (task is not None and not task.done())

    async def interrupt_now(self) -> None:
        """`send` con `interrupt` (2b.3): el corte, ya, en el bucle de entrada; el envío va
        después por la cola, en su orden. Corta lo que suena aquí y el turno abierto del canal
        (lo iniciara quien lo iniciara, /v1 incluido); los envíos de la sesión que aún esperan
        no se tocan."""
        channel = self._channel
        if self._closing or channel is None or not self.agent_allowed(channel.session_key):
            return
        for turn in self._live:
            turn.silent = True
        await self._cut_finishing("interrupted")
        await channel.interrupt_now()

    async def start_user_turn(
        self, text: str, *, interrupt: bool = False, ticket: SendTicket | None = None
    ) -> None:
        """Envía `text` al canal de la conversación (C3, 2b): entra en la cola de OpenClaw;
        el turno en curso sigue. Con `interrupt` («enviar ahora»), el canal aborta antes el run
        activo (su turno termina `interrupted` para todos) y esta sesión deja de sonar lo que
        aún sonaba.

        Rechazado una vez que la sesión se cierra (G-09): el bucle de entrada sigue leyendo
        mientras corre close_all(), y un turno creado entonces sobreviviría al cierre.
        """
        if self._closing:
            logger.info(f"[{self._client_id}] send ignorado: la sesión se está cerrando.")
            return
        async with self._send_lock:
            if self._closing:
                return
            if interrupt:
                # 7e: cortar deja atrás el audio de todo turno anterior en esta sesión: lo que
                # aún suena de uno que ya terminó en su canal se corta, y los que aún esperan
                # su turn_start en la cola reciben su par y su texto, pero sin voz.
                for turn in self._live:
                    turn.silent = True
                await self._cut_finishing("interrupted")
            # El cambio de día se evalúa al empezar cada turno (services/conversations.py).
            await self._conversation.refresh()
            await self._follow_conversation()
            channel = self._channel
            if self._closing:
                return
            if channel is None:
                await self._refuse_send()
                return
            if not self.agent_allowed(channel.session_key):
                # #397 (decisión a): la política del gateway, antes del chat.send (OpenClaw solo
                # diría INVALID_REQUEST). Solo esta sesión recibe su par con el error: el turno
                # del canal, sus push y sus runs no se tocan (las demás sesiones no lo ven).
                code = ORCHESTRATOR_AGENT_UNAVAILABLE_ERROR
                logger.warning(f"[{self._client_id}] Turno sin agente disponible: {code}")
                gateway_stats.turn_started("normal")
                gateway_stats.turn_error(code)
                await self._refuse_send(code)
                return
            result = await channel.send_user_message(self, text, interrupt=interrupt, ticket=ticket)
            if isinstance(result, str):
                # OpenClaw no lo tomó (sin conexión, rechazo): solo esta sesión recibe su par
                # con el código estable; el canal y sus turnos siguen igual.
                await self._refuse_send(result)
            elif result is not None:
                self._last_sent = result.finished

    async def withdraw(self, message_id: str) -> bool:
        """Quita un mensaje pendiente de la conversación (2b), lo enviara quien lo enviara:
        todas las sesiones reciben su `message` `withdrawn`. False si ya no estaba pendiente
        (se ignora: entregado, ya quitado o desconocido)."""
        channel = self._channel
        if self._closing or channel is None:
            return False
        return await channel.withdraw(message_id)

    async def _refuse_send(self, code: str = "internal_error") -> None:
        """Un `send` que no llega al canal (sin canal, o un agente no permitido) recibe su par
        con un TURN_ERROR estable (`code`). Por la cola de la sesión, como cualquier turno
        (#202): su turn_start sale detrás del turn_end de los anteriores."""
        turn = self._new_turn("user")
        turn.conversation_id = self._conversation.id
        self._live.append(turn)
        self._enqueue(("refused", turn, code))

    async def _paint_refused(self, turn: Turn, code: str) -> None:
        if turn.ended:
            return
        for earlier in list(self._live):
            # Red de seguridad: la cola ya cerró los anteriores; nunca dos turnos abiertos.
            if earlier is not turn and earlier.opened:
                earlier.cancel_reason = earlier.cancel_reason or "replaced"
                await self._end(earlier, "replaced")
        if await self._open(turn):
            await self._send_turn_error(turn, code)
        await self._end(turn, "error")

    async def cancel(
        self, reason: EndReason, *, push: bool = True, own_only: bool = False
    ) -> tuple[bool, str | None]:
        """Cancela lo que la sesión muestra ahora: el turno abierto en este socket (el del
        usuario y, con `push`, también un push). Si es una copia de un turno que ya terminó en
        el canal (su audio aún suena), se corta solo aquí y el turno abierto del canal sigue
        (se abrirá después, como antes); si es la copia del turno abierto del canal (o el
        socket aún no muestra nada), se cancela en el canal: el `cancel` del cliente para el
        turno para todos (C3). Con `own_only` (la voz: barge-in y transcripción final, hasta
        que la 2b quite el barge-in) solo corta un turno que esta sesión envió; de un push,
        solo su copia aquí (el canal sigue), y el turno de otra sesión, nada: un cliente de
        voz que oye la tele no le corta su turno. Devuelve (cancelado, su turn_id en esta
        sesión)."""
        channel = self._channel
        current = channel.current_turn if channel is not None else None
        mine = self._by_channel.get(current.id) if current is not None else None
        shown = next((t for t in self._live if t.opened and not t.ended), None)
        if shown is not None and shown is not mine:
            if shown.cancel_reason is not None:
                return False, None  # ya se está cortando aquí (su turn_end va en camino)
            if shown.origin == "push" and not push:
                return False, None
            source = shown.channel
            if own_only and shown.origin == "user" and source is not None and source.by_session is not self:
                return False, None  # el turno de otra sesión: la voz de aquí no lo corta
            shown.cancel_reason = shown.cancel_reason or reason
            await self._end(shown, reason)
            return True, shown.turn_id
        if channel is None or current is None or mine is None:
            if channel is not None and current is None:
                # Un mensaje entregado cuyo run aún no abrió su turno: «Parar» lo aborta (la
                # voz, solo si es de esta sesión).
                await channel.cancel_head(reason, sender=self if own_only else None)
            return False, None
        if current.origin == "push" and not push:
            return False, None
        if own_only and current.by_session is not self:
            if current.origin != "push" or not mine.opened:
                return False, None
            # Solo su copia: el push sigue en el canal para las demás sesiones.
            mine.cancel_reason = mine.cancel_reason or reason
            await self._end(mine, reason)
            return True, mine.turn_id
        if await channel.cancel(reason, push=push, expected=current) is None:
            return False, None
        if mine.opened:
            # Su fin ya está en la cola; aquí sale ya (A-r2-2), sin esperar a que el worker
            # termine lo que esté haciendo con su TTS.
            mine.cancel_reason = mine.cancel_reason or reason
            await self._end(mine, reason)
        return True, mine.turn_id

    async def _cut_finishing(self, reason: EndReason, *, push: bool = True) -> Turn | None:
        """Corta los turnos de la sesión cuyo turno del canal ya terminó solo y que siguen
        abiertos aquí porque su audio suena (7e, A-r2-2): su turn_end sale ya, sin esperar a
        cerrar su TTS. Devuelve el primero cortado."""
        first: Turn | None = None
        for turn in list(self._live):
            source = turn.channel
            if not turn.opened or turn.ended or source is None or not source.ended:
                continue
            if source.end_reason is None or not _drains(turn, source.end_reason):
                continue  # su fin (un corte) ya está en la cola y no espera a nada
            if turn.tts is None and turn.pending is None:
                continue  # sin audio que esperar: su fin ya sale tal cual (`done`)
            if turn.origin == "push" and not push:
                continue
            turn.cancel_reason = reason
            await self._end(turn, reason)
            first = first or turn
        return first

    # ── Worker de la sesión ───────────────────────────────────────────────

    def _put(self, item: tuple) -> None:
        """Lo que entrega el canal (síncrono, nunca bloquea). Nada una vez cerrándose, salvo
        lo de turnos que la sesión ya vio (su fin)."""
        if self._closing and item[0] in ("turn_start", "message"):
            return
        self._enqueue(item)

    def _enqueue(self, item: tuple) -> None:
        if self._worker is None or self._worker.done():
            self._worker = self._spawn(self._worker_loop(), "turn_worker")
        inbox = self._inbox
        if inbox.pending == 0:
            inbox.since = time.monotonic()
        inbox.pending += 1
        inbox.peak = max(inbox.peak, inbox.pending)
        inbox.queue.put_nowait(item)

    async def _worker_loop(self) -> None:
        queue = self._inbox.queue
        while True:
            item = await queue.get()
            kind = item[0]
            try:
                if kind == "turn_start":
                    await self._paint_start(item[1])
                elif kind == "event":
                    await self._paint_event(item[1], item[2])
                elif kind == "turn_end":
                    await self._paint_end(item[1], item[2])
                elif kind == "refused":
                    await self._paint_refused(item[1], item[2])
                elif kind == "message":
                    await self._paint_message(item[1])
                elif kind == "leave":
                    _, conversation_id, reason = item
                    for turn in list(self._live):
                        if turn.conversation_id == conversation_id:
                            await self._end(turn, reason)
            except Exception as e:  # noqa: BLE001 — frontera: un evento roto no para el worker
                # Solo el tipo: el evento puede llevar texto del usuario.
                origin = item[1].origin if kind not in ("leave", "message") else kind
                logger.warning(f"[{self._client_id}] {origin} {kind} falló: {type(e).__name__}")
            finally:
                inbox = self._inbox
                inbox.pending = max(0, inbox.pending - 1)
                if inbox.pending == 0:
                    inbox.since = None
                queue.task_done()

    async def _paint_message(self, message: ChannelMessage) -> None:
        """El `message` de un mensaje de usuario (2b). Lleva su texto: solo al cliente, nunca
        a los logs ni al tracker."""
        if self.client_gone:
            return
        payload = {
            "type": "message",
            "id": message.id,
            "conversation": message.conversation_id,
            "text": message.text,
            "from": {
                "source": "device",
                "device_id": message.by_client_id,
                "name": message.by_name,
                "self": message.by_client_id == self._client.id,
            },
            "state": message.state,
        }
        try:
            await self._sender.send_json(payload)
        except Exception as e:  # noqa: BLE001 — frontera: envío a un cliente que puede haberse ido
            self._send_failed("message", e)

    async def _paint_start(self, turn: Turn) -> None:
        if turn.ended:
            return
        if not await self._open(turn):
            # Sin turn_start no hay turno en esta sesión: lo que llegue de él se descarta.
            self._forget(turn)
            return
        source = turn.channel
        if (
            not self._wants_audio
            or turn.halted
            or self._closing
            or turn.silent
            or source is None
            or source.audio_session is not self
        ):
            return
        if turn.origin == "user":
            # El TTS se conecta a la vez (auditoría 3.0, E-2): un fogar-speaker colgado no
            # retrasa el texto; solo el `turn_end`, que espera a la voz como antes.
            turn.pending = _PendingTts(
                lambda: self._connect_tts(turn), self._config.tts_voice or "", self._client_id
            )
            return
        tts = await self._connect_tts(turn)
        if tts is None:
            return
        if turn.ended:  # cortado mientras conectaba: este TTS es de un push cerrado
            await tts.release()
            return
        turn.tts = tts
        await tts.start(self._config.tts_voice or "")

    async def _paint_event(self, turn: Turn, event: ChannelEvent) -> None:
        if turn.ended or not turn.opened:
            return
        turn.progress_at = time.monotonic()
        if isinstance(event, ChannelToken):
            await self._paint_token(turn, event.text)
        elif isinstance(event, ChannelToolCall):
            # El progreso (solo el nombre) no depende de tool_calls_enabled (#254).
            await self._offer_tool_progress(turn, event.tool_call)
            if self._config.tool_calls_enabled:
                try:
                    await self._sender.send_json(_tool_call_message(turn.turn_id, event.tool_call))
                except Exception as e:  # noqa: BLE001 — frontera: envío a un cliente que puede haberse ido
                    self._send_failed("tool_call", e)
        elif isinstance(event, ChannelProgress):
            await self._offer_progress(turn, event.progress)
        elif isinstance(event, ChannelError):
            await self._send_turn_error(turn, event.code)

    async def _paint_token(self, turn: Turn, text: str) -> None:
        if self._wants_text and not self.client_gone:
            try:
                await self._sender.send_json({"type": "token", "turn_id": turn.turn_id, "text": text})
                turn.progress.text_arrived()  # solo si este cliente ve el token
            except Exception as e:  # noqa: BLE001 — frontera: envío a un cliente que puede haberse ido
                self._send_failed("token", e)
        if self.client_gone:
            return  # G-18: nadie a quien sonárselo
        if turn.pending is not None:
            await turn.pending.feed(text)
        elif turn.tts is not None:
            await turn.tts.feed(text)

    async def _paint_end(self, turn: Turn, reason: EndReason) -> None:
        """El fin del turno del canal en esta sesión: si terminó solo, su audio termina de
        sonar antes del turn_end (decisión 1: solo espera esta sesión); si lo cortaron, el
        turn_end sale ya (A-r2-2) y su TTS se suelta."""
        if turn.ended:
            return
        try:
            if _drains(turn, reason):
                await self._finish_tts(turn)
            elif turn.pending is not None and turn.pending.tts is not None:
                await turn.pending.tts.end()  # el servidor TTS sabe que no hay más
        except Exception:
            await self._end(turn, reason)
            raise
        # Cancelado aquí (close_all cancela el worker), su turn_end lo manda el cierre.
        await self._end(turn, reason)

    async def _finish_tts(self, turn: Turn) -> None:
        if turn.pending is not None:
            pending = turn.pending
            tts = pending.tts if self.client_gone else await pending.connected()
            if tts is not None and not turn.ended:
                await tts.end()  # el servidor TTS sabe que no hay más
                if not self.client_gone:  # G-18: nadie a quien sonárselo
                    await tts.drain()
        elif turn.tts is not None:
            await turn.tts.finish()

    # ── Progreso (#254) ───────────────────────────────────────────────────

    def _progress_open(self, turn: Turn) -> bool:
        """Este turno cuenta progreso ahora: el cliente lo pidió, su turn_start salió, y ni
        terminó ni se está cancelando (#254)."""
        return self._wants_progress and turn.opened and not turn.halted

    async def _offer_tool_progress(self, turn: Turn, tool_call: ToolCallEvent) -> None:
        """El progreso de un evento de herramienta. Un evento raro (deriva de OpenClaw) solo
        pierde su progreso: nunca rompe el turno."""
        if not self._progress_open(turn):
            return
        try:
            progress = turn.progress.tool(tool_call)
        except Exception as e:  # noqa: BLE001 — frontera: un evento de herramienta raro no rompe el turno
            logger.debug(f"[{self._client_id}] progreso de herramienta descartado: {type(e).__name__}")
            return
        await self._offer_progress(turn, progress)

    async def _offer_progress(self, turn: Turn, progress: Progress) -> None:
        """Un estado nuevo del turno: sale ya, se aplaza (el ritmo) o no sale (repetido).

        Solo con el turno abierto: nunca antes de su turn_start ni después de su turn_end, ni
        con el turno ya cancelado.
        """
        if not self._progress_open(turn):
            return
        decision = turn.progress.offer(progress)
        if isinstance(decision, Progress):
            await self._send_progress(turn, decision)
        elif decision is not None and turn.progress_flush is None:
            task = asyncio.create_task(self._flush_progress(turn, decision), name="turn_progress")
            turn.progress_flush = task
            self._supervise(task)

    async def _flush_progress(self, turn: Turn, delay: float) -> None:
        """Envía el estado aplazado en cuanto el ritmo lo deja (el más reciente, no todos)."""
        try:
            while True:
                await asyncio.sleep(delay)
                if turn.halted:
                    return
                decision = turn.progress.flush()
                if isinstance(decision, Progress):
                    await self._send_progress(turn, decision)
                    return
                if decision is None:
                    return
                delay = decision
        finally:
            if turn.progress_flush is asyncio.current_task():
                turn.progress_flush = None

    async def _send_progress(self, turn: Turn, progress: Progress) -> None:
        # Sin await entre mirar `halted` y encolar: `_end` lo marca antes de encolar turn_end,
        # así que este turn_progress nunca sale detrás de él. Un turno ya cancelado (barge-in,
        # `cancel`, un `send` que lo sustituye) tampoco cuenta nada mientras se limpia.
        if turn.halted:
            return
        try:
            await self._sender.send_json(progress.message(turn.turn_id))
        except Exception as e:  # noqa: BLE001 — frontera: envío a un cliente que puede haberse ido
            self._send_failed("turn_progress", e)

    # ── Comunes ───────────────────────────────────────────────────────────

    def _new_turn(self, origin: TurnOrigin) -> Turn:
        self._seq += 1
        now = time.monotonic()
        return Turn(
            turn_id=f"t-{self._seq}",
            seq=self._seq & 0xFFFF,
            origin=origin,
            started_at=now,
            progress_at=now,
        )

    def _by(self, turn: Turn) -> dict | None:
        """Quién inició el turno (2b): su aparato; None en un push."""
        if turn.origin == "push":
            return None
        source = turn.channel
        if source is None:  # un `send` de esta sesión que no llegó al canal
            device_id, name = self._client.id, self._client.name
        elif source.by_client_id is None:
            return None
        else:
            device_id, name = source.by_client_id, source.by_name or ""
        return {"device_id": device_id, "name": name, "self": device_id == self._client.id}

    async def _open(self, turn: Turn) -> bool:
        try:
            await self._sender.send_json(
                {
                    "type": "turn_start",
                    "turn_id": turn.turn_id,
                    "turn_seq": turn.seq,
                    "conversation": turn.conversation_id,
                    "origin": turn.origin,
                    "by": self._by(turn),
                }
            )
        except Exception as e:  # noqa: BLE001 — frontera: envío a un cliente que puede haberse ido
            self._send_failed("turn_start", e)
            return False
        turn.opened = True
        return True

    async def _connect_tts(self, turn: Turn) -> TurnTts | None:
        stream = await self._tts.connect(
            voice=self._tts.resolve_voice(self._config.tts_voice),
            speed=self._config.tts_speed,
            client_id=self._client_id,
        )
        if stream is None:
            return None
        tts = TurnTts(
            stream,
            turn,
            sender=self._sender,
            tracker=self._tracker,
            client_id=self._client_id,
            on_client_gone=self._note_client_gone,
            supervise=self._supervise,
        )
        return tts

    def _note_client_gone(self) -> None:
        self.client_gone = True

    def _forget(self, turn: Turn) -> None:
        """Un turno que esta sesión nunca abrió: sin par, sin nada más."""
        turn.ended = True
        if turn in self._live:
            self._live.remove(turn)
        if turn.channel is not None and self._by_channel.get(turn.channel.id) is turn:
            del self._by_channel[turn.channel.id]

    async def _end(self, turn: Turn, reason: EndReason, *, release: bool = True) -> bool:
        """El único que envía un turn_end: exactamente uno por turno, quien llegue primero.

        Lo marca terminado y encola su turn_end sin ningún await entre medias (su audio deja
        de salir, y nada abre otro turno antes: A-1); después libera su TTS (con su evento
        `push_tts`). Cada paso termina aunque cancelen a quien cierra (#221, `Teardown`); esa
        cancelación se relanza al final.
        """
        if turn.ended:
            return False
        opened = turn.opened
        self._forget(turn)
        turn.end_reason = reason
        self.last_ended = turn
        if turn.progress_flush is not None:
            turn.progress_flush.cancel()  # no hace falta esperarla: ya ve `ended`
        logger.debug(f"[{self._client_id}] turno {turn.turn_id} ({turn.origin}) terminado: {reason}")
        if opened:
            try:
                await self._sender.send_json(
                    {"type": "turn_end", "turn_id": turn.turn_id, "reason": reason}
                )
            except Exception as e:  # noqa: BLE001 — frontera: envío a un cliente que puede haberse ido
                self._send_failed("turn_end", e)
        if release:
            td = Teardown(f"[{self._client_id}] fin del turno {turn.turn_id}")
            await td.step(self._release_turn_tts(turn, reason), "TTS del turno")
            td.raise_if_cancelled()
        return True

    async def _cut(self, turn: Turn, reason: EndReason) -> None:
        """El turn_end de un corte, en su propia tarea (A-r2-2), y después su TTS. Si el cierre
        de la sesión la cancela (`_close_turns`), termina ya: la liberación del TTS sigue sola,
        supervisada y acotada por PUSH_TTS_DRAIN_TIMEOUT_S, y nunca la retiene."""
        if not await self._end(turn, reason, release=False):
            return
        release = asyncio.create_task(self._release_turn_tts(turn, reason), name="turn_cut_tts")
        self._supervise(release)  # su fallo, al log por tipo (#221)
        try:
            await asyncio.wait({release})
        except asyncio.CancelledError:
            release.cancel()
            raise

    async def _release_turn_tts(self, turn: Turn, reason: EndReason) -> None:
        if turn.tts is not None:
            if reason not in _DRAINED:
                await turn.tts.drop(f"turno {reason}", logging.INFO)
            await turn.tts.release()
        if turn.pending is not None:
            await turn.pending.release()

    async def _send_turn_error(self, turn: Turn, code: str) -> None:
        try:
            await self._sender.send_json(
                {
                    "type": "error",
                    "code": "TURN_ERROR",
                    "message": code,
                    "fatal": False,
                    "turn_id": turn.turn_id,
                }
            )
        except Exception as e:  # noqa: BLE001 — frontera: envío a un cliente que puede haberse ido
            self._send_failed("TURN_ERROR", e)

    def _send_failed(self, what: str, e: BaseException) -> None:
        if isinstance(e, ClientGone):
            self.client_gone = True
        log_send_failed(self._client_id, what, e)

    # ── Cierre ────────────────────────────────────────────────────────────

    def stop_intake(self) -> None:
        """Desde ya, ni turnos nuevos (G-09) ni turnos del canal."""
        self._closing = True

    async def close(self, td: Teardown, limit: float) -> None:
        """Pasos de close_all() para los turnos; cada uno acotado por `limit`.

        7d / G-23 / decisión 2: la sesión deja su canal; el turno en curso no se cancela ni se
        aborta (sigue en el canal para las demás sesiones adjuntas, o se acaba solo), y cada
        turno abierto aquí recibe su turn_end `closed` con el socket aún abierto (G-10). Lo
        mismo en un cierre por política (4200, 4020) y en la desconexión.
        """
        await self._close_turns(td, limit)

    async def _close_turns(self, td: Teardown, limit: float) -> None:
        self._closing = True
        # Los envíos que aún esperan se descartan: sin id, nada salió de ellos al cable (ni su
        # par); uno ya en `chat.send` deja su run al canal, que lo aborta cuando llegue su ack.
        self._sends.clear()
        if self._send_task is not asyncio.current_task():
            await td.cancel(self._send_task, "envíos en espera", limit)
        channel, self._channel = self._channel, None
        if channel is not None:
            channel.unsubscribe(self)
            self._backlog_peak = max(self._backlog_peak, channel.push_backlog()[1])
            self._channels.drop_if_idle(channel.session_key, channel)
        # El worker antes que el TTS o el audio que pueda estar moviendo.
        await td.cancel(self._worker, "worker de turnos", limit)
        # Lo que quedaba en su cola, en orden (#202: nunca dos turnos abiertos a la vez): un fin
        # encolado sale con su motivo; el turn_start de un `send` aceptado sale (G-r2-1, A-r3-1)
        # solo después de cerrar los anteriores; lo demás (eventos, pushes sin abrir) se descarta.
        inbox = self._inbox
        items = []
        while not inbox.queue.empty():
            items.append(inbox.queue.get_nowait())
            inbox.queue.task_done()
        inbox.pending = 0
        inbox.since = None
        for item in items:
            kind = item[0]
            if kind == "turn_end" and item[1].opened:
                await self._close_one(td, item[1], item[2], limit)
            elif kind == "leave":
                for turn in list(self._live):
                    if turn.conversation_id == item[1] and turn.opened:
                        await self._close_one(td, turn, item[2], limit)
            elif kind in ("turn_start", "refused") and not item[1].ended:
                turn = item[1]
                source = turn.channel
                if turn.origin != "user" or (source is not None and source.by_session is not self):
                    # Un push, o el turno de otra sesión, que este socket nunca mostró: nada.
                    # Un `send` aceptado de esta sesión sí recibe su par (G-r2-1, A-r3-1).
                    self._forget(turn)
                    continue
                for earlier in list(self._live):
                    if earlier is not turn and earlier.opened:
                        await self._close_one(td, earlier, "closed", limit)
                await self._open(turn)
        for turn in list(self._live):
            # Su turn_end, tras soltar su TTS (su audio ya no sale); uno que nunca abrió no
            # recibe nada (nunca un turn_end solo).
            await self._close_one(td, turn, "closed", limit)
        # Los turn_end de cortes en curso, bajo un mismo tope; lo que no acabe se cancela (no
        # sobrevive a la sesión: están supervisadas, su resultado se recoge igual).
        loop = asyncio.get_running_loop()
        deadline = loop.time() + limit
        for task in list(self._cuts):
            await td.wait(task, "turn_end de un corte", max(0.0, deadline - loop.time()))
        for task in list(self._cuts):
            if not task.done():
                task.cancel()

    async def _close_one(self, td: Teardown, turn: Turn, reason: EndReason, limit: float) -> None:
        if turn.ended:
            return
        if turn.tts is not None:
            await td.step(turn.tts.release(), "cierre del TTS del push", limit)
        if turn.pending is not None:
            await td.step(turn.pending.release(), "cierre del TTS del turno", limit)
        await self._end(turn, reason, release=False)
