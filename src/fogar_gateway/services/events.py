"""
events.py
~~~~~~~~~
`GET /events` (5.0, #412): un stream SSE por aparato con todo lo que pasa en las conversaciones
que puede ver y los avisos que le tocan.

- **Un suscriptor por canal.** Cada canal solo reparte a sus suscriptores (y con nadie
  adjunto un aviso se descarta), así que un aparato con `/events` tiene un adaptador
  `ChannelSubscriber` en cada canal vivo de una conversación que puede ver (M20,
  `visible_to`). Los canales que nacen después le llegan por `ChannelHub.on_new_channel`.
  Las reglas de entrega del canal (avisos, agentes permitidos) valen igual que para el
  WebSocket.
- **Sobrevive a la conexión.** Un stream HTTP dura como mucho `STREAM_MAX_S` (el apagado ordenado
  no puede esperarlo); el aparato sigue suscrito `RESUME_S` tras su último corte y guarda sus
  eventos en un búfer acotado (`BUFFER_MAX`). Reconectar con `Last-Event-ID` dentro de ese margen
  reenvía lo perdido; fuera, el cliente recibe `resync` y vuelve a pedir el estado.
- **Ids que siempre suben**, también entre reinicios (base: µs al arrancar): un `Last-Event-ID`
  de otro proceso cae fuera del búfer.
- **Mismo JSON que el WebSocket** (`turn_start`, `token`, `tool_call`, `turn_progress`, `error`,
  `turn_end`, `message`) y además `conversation` en cada evento. `turn_id` es el del turno del
  canal, estable entre reconexiones. Lleva texto de usuario: solo al cliente, nunca a logs.
"""

import asyncio
import json
import logging
import time
from collections import deque
from dataclasses import dataclass, field

from fogar_gateway.models.schemas import Client, ClientConfig
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
)
from fogar_gateway.services.pipeline_tracker import PipelineTracker
from fogar_gateway.services.turns import EndReason, _tool_call_message

logger = logging.getLogger(__name__)

STREAM_MAX_S = 25.0  # por debajo del --timeout-graceful-shutdown (35 s), como /system/state/stream
RESUME_S = 120.0  # cuánto sigue suscrito un aparato tras su último corte
BUFFER_MAX = 2000  # eventos que guarda un aparato para reanudar
QUEUE_MAX = 1000  # eventos pendientes de un stream; un lector más lento se corta y reanuda
KEEPALIVE_S = 15.0

_ids = time.time_ns() // 1000  # base temporal: los ids suben también entre reinicios


def current_id() -> int:
    """El último id repartido (`as_of`): un evento posterior tendrá uno mayor."""
    return _ids


def _next_id() -> int:
    global _ids
    _ids += 1
    return _ids


def sse(data: dict, event_id: int | None = None) -> str:
    head = f"id: {event_id}\n" if event_id is not None else ""
    return f"{head}data: {json.dumps(data, ensure_ascii=False)}\n\n"


def _by(turn: ChannelTurn, client_id: str) -> dict | None:
    if turn.by_client_id is None:
        return None  # un aviso
    return {
        "device_id": turn.by_client_id,
        "name": turn.by_name or "",
        "self": turn.by_client_id == client_id,
    }


def message_payload(message: ChannelMessage, client_id: str) -> dict:
    """El `message` del WebSocket (2b), con `from.self` relativo a `client_id`."""
    return {
        "type": "message",
        "id": message.id,
        "conversation": message.conversation_id,
        "text": message.text,
        "from": {
            "source": "device",
            "device_id": message.by_client_id,
            "name": message.by_name,
            "self": message.by_client_id == client_id,
        },
        "state": message.state,
    }


def turn_payload(turn: ChannelTurn, client_id: str) -> dict:
    """El `turn_start` (sin `turn_seq`: por SSE no hay audio)."""
    return {
        "type": "turn_start",
        "turn_id": turn.id,
        "conversation": turn.conversation_id,
        "origin": turn.origin,
        "by": _by(turn, client_id),
    }


def _agent_allowed(config: ClientConfig, session_key: str) -> bool:
    """Como `TurnController.agent_allowed`: el agente de la clave está en `allowed_agents`."""
    allowed = config.allowed_agents
    parts = session_key.split(":")
    if allowed is None or len(parts) < 3 or parts[0] != "agent":
        return True
    return parts[1].lower() in {a.lower() for a in allowed}


class _Adapter:
    """El suscriptor de un aparato en un canal: traduce sus callbacks a eventos del aparato.

    `passive`: recibe los avisos como cualquier adjunto, pero no cuenta como «alguien lo ve»
    cuando el canal decide si el dispositivo de avisos lo recibe como invitado (la entrega de la
    2c no cambia por tener una web abierta)."""

    passive = True
    wants_audio = False  # la voz por /events llega en la 5c
    wants_progress = True
    tracker: PipelineTracker | None = None

    def __init__(self, device: "DeviceEvents", channel: ConversationChannel) -> None:
        self._device = device
        self.channel = channel
        self._started: set[str] = set()
        self._phase: dict[str, tuple[str, str | None]] = {}

    @property
    def client(self) -> Client:
        return self._device.client

    @property
    def conversation_id(self) -> str:
        return self.channel.conversation.id

    @property
    def wants_push(self) -> bool:
        return self._device.config.push_enabled

    def agent_allowed(self, session_key: str) -> bool:
        return _agent_allowed(self._device.config, session_key)

    def on_turn_start(self, turn: ChannelTurn) -> None:
        if turn.origin == "push":
            # Como TurnController: sin avisos (`push_enabled`) o de un agente no permitido, el
            # aviso entero se descarta para este aparato (ni su par).
            key = turn.session_key or self.channel.session_key
            if not self.wants_push or not self.agent_allowed(key):
                return
        self._started.add(turn.id)
        self._device.publish(turn_payload(turn, self.client.id))
        if turn.text:  # llega a mitad de turno: lo que lleva, en un solo token (como el WS)
            self._token(turn, turn.text)

    def on_event(self, turn: ChannelTurn, event: ChannelEvent) -> None:
        if turn.id not in self._started:
            return
        base = {"conversation": turn.conversation_id}
        if isinstance(event, ChannelToken):
            self._token(turn, event.text)
        elif isinstance(event, ChannelToolCall):
            if self._device.config.tool_calls_enabled:
                self._device.publish({**_tool_call_message(turn.id, event.tool_call), **base})
        elif isinstance(event, ChannelProgress):
            # shortcut: sin el ritmo de TurnProgress (solo sin repetir fase); si un cliente
            # SSE lo necesita, reutilizar TurnProgress con su tarea de flush.
            phase = (event.progress.phase, event.progress.tool)
            if turn.cancel_reason is None and self._phase.get(turn.id) != phase:
                self._phase[turn.id] = phase
                self._device.publish({**event.progress.message(turn.id), **base})
        elif isinstance(event, ChannelError):
            self._device.publish({
                "type": "error", "code": "TURN_ERROR", "message": event.code, "fatal": False,
                "turn_id": turn.id, **base,
            })

    def on_turn_end(self, turn: ChannelTurn, reason: EndReason) -> None:
        if turn.id not in self._started:
            return
        self._started.discard(turn.id)
        self._phase.pop(turn.id, None)
        self._device.publish({
            "type": "turn_end", "turn_id": turn.id, "conversation": turn.conversation_id,
            "reason": reason,
        })

    def on_message(self, message: ChannelMessage) -> None:
        self._device.publish(message_payload(message, self.client.id))

    def _token(self, turn: ChannelTurn, text: str) -> None:
        self._device.publish({
            "type": "token", "turn_id": turn.id, "conversation": turn.conversation_id, "text": text,
        })

@dataclass(eq=False)
class _Listener:
    queue: asyncio.Queue[tuple[int, str] | None] = field(
        default_factory=lambda: asyncio.Queue(QUEUE_MAX)
    )
    overflowed: bool = False


class DeviceEvents:
    """Los eventos de un aparato: sus adaptadores, su búfer y sus streams abiertos."""

    def __init__(self, hub: "EventHub", client: Client, config: ClientConfig) -> None:
        self._hub = hub
        self.client = client
        self.config = config
        self._adapters: dict[str, _Adapter] = {}  # session_key → adaptador
        self._buffer: deque[tuple[int, str]] = deque(maxlen=BUFFER_MAX)
        self._floor = current_id()  # el último id que este aparato ya no puede reenviar
        self._listeners: set[_Listener] = set()
        self._expiry: asyncio.TimerHandle | None = None
        self.closed = False

    # -- canales ----------------------------------------------------------------------

    def attach(self, channel: ConversationChannel) -> _Adapter | None:
        """Se suscribe al canal si puede ver su conversación (M20). Idempotente."""
        if self.closed:
            return None
        adapter = self._adapters.get(channel.session_key)
        if adapter is not None and adapter.channel is channel:
            return adapter
        conversation = channel.conversation
        if not conversation.visible_to(self.client.home_id, self.client.user_id):
            return None
        adapter = _Adapter(self, channel)
        self._adapters[channel.session_key] = adapter
        if not channel.subscribe(adapter):
            del self._adapters[channel.session_key]
            return None
        return adapter

    def adapter_for(self, channel: ConversationChannel) -> _Adapter | None:
        adapter = self._adapters.get(channel.session_key)
        return adapter if adapter is not None and adapter.channel is channel else None

    def forget(self, channel: ConversationChannel) -> None:
        """El hub soltó el canal (ya no está vivo): fuera su adaptador."""
        adapter = self._adapters.get(channel.session_key)
        if adapter is not None and adapter.channel is channel:
            del self._adapters[channel.session_key]

    # -- eventos ----------------------------------------------------------------------

    def publish(self, data: dict) -> None:
        """Síncrono (lo llaman los callbacks del canal): le da id, lo guarda y lo encola."""
        if self.closed:
            return
        event_id = _next_id()
        line = sse(data, event_id)
        if len(self._buffer) == self._buffer.maxlen:
            self._floor = self._buffer[0][0]
        self._buffer.append((event_id, line))
        for listener in list(self._listeners):
            if listener.overflowed:
                continue
            try:
                listener.queue.put_nowait((event_id, line))
            except asyncio.QueueFull:
                # Un lector lento no hace crecer la memoria: se le corta y reanuda del búfer.
                listener.overflowed = True
                self._wake(listener)

    def replay(self, last_id: int) -> list[str] | None:
        """Lo posterior a `last_id`, o None si ya no está en el búfer (→ `resync`)."""
        if last_id < self._floor or last_id > current_id():
            return None
        return [line for event_id, line in self._buffer if event_id > last_id]

    # -- streams ----------------------------------------------------------------------

    def listen(self) -> _Listener:
        if self._expiry is not None:
            self._expiry.cancel()
            self._expiry = None
        listener = _Listener()
        self._listeners.add(listener)
        return listener

    def unlisten(self, listener: _Listener) -> None:
        self._listeners.discard(listener)
        if not self._listeners and not self.closed:
            loop = asyncio.get_running_loop()
            self._expiry = loop.call_later(RESUME_S, self._expire)

    def _expire(self) -> None:
        self._expiry = None
        if not self._listeners:
            self._hub.close_device(self.client.id)

    def _wake(self, listener: _Listener) -> None:
        # Un hueco en la cola para el centinela: el stream termina y el cliente reanuda.
        while True:
            try:
                listener.queue.put_nowait(None)
                return
            except asyncio.QueueFull:
                listener.queue.get_nowait()

    def close(self) -> None:
        """Se desuscribe de todo y corta sus streams (revocado, caducado o apagado)."""
        if self.closed:
            return
        self.closed = True
        if self._expiry is not None:
            self._expiry.cancel()
            self._expiry = None
        for adapter in list(self._adapters.values()):
            adapter.channel.unsubscribe(adapter)
            self._hub.channel_hub.drop_if_idle(adapter.channel.session_key, adapter.channel)
        self._adapters.clear()
        self._buffer.clear()
        for listener in list(self._listeners):
            self._wake(listener)
        self._listeners.clear()


class EventHub:
    """Los aparatos con `/events` (`app.state.events`)."""

    def __init__(self, channel_hub: ChannelHub) -> None:
        self.channel_hub = channel_hub
        self._devices: dict[str, DeviceEvents] = {}
        channel_hub.on_new_channel = self.on_channel

    def device(self, client_id: str) -> DeviceEvents | None:
        return self._devices.get(client_id)

    def open(self, client: Client, config: ClientConfig) -> DeviceEvents:
        """El de este aparato (el que siga vivo, con su búfer, o uno nuevo adjunto a los
        canales vivos que puede ver). Su configuración, la de esta petición."""
        device = self._devices.get(client.id)
        if device is None or device.closed:
            device = DeviceEvents(self, client, config)
            self._devices[client.id] = device
            for channel in self.channel_hub.channels():
                device.attach(channel)
        else:
            device.client, device.config = client, config
        return device

    def on_channel(self, channel: ConversationChannel) -> None:
        """Un canal nuevo: se adjuntan los aparatos con `/events` que pueden verlo. Síncrono."""
        for device in list(self._devices.values()):
            device.attach(channel)

    def close_device(self, client_id: str) -> None:
        device = self._devices.pop(client_id, None)
        if device is not None:
            device.close()

    def close_all(self) -> None:
        for client_id in list(self._devices):
            self.close_device(client_id)

class TransientSender:
    """El emisor de un POST de un aparato sin `/events` vivo: suscrito solo hasta el ack de su
    `chat.send` (el run sigue aunque se vaya). No recibe nada."""

    passive = True
    wants_audio = False
    wants_progress = False
    wants_push = False
    tracker: PipelineTracker | None = None

    def __init__(self, client: Client, config: ClientConfig, conversation_id: str) -> None:
        self.client = client
        self._config = config
        self.conversation_id = conversation_id

    def agent_allowed(self, session_key: str) -> bool:
        return _agent_allowed(self._config, session_key)

    def on_turn_start(self, turn: ChannelTurn) -> None:
        pass

    def on_event(self, turn: ChannelTurn, event: ChannelEvent) -> None:
        pass

    def on_turn_end(self, turn: ChannelTurn, reason: EndReason) -> None:
        pass

    def on_message(self, message: ChannelMessage) -> None:
        pass
