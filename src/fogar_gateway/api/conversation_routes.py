"""
conversation_routes.py
~~~~~~~~~~~~~~~~~~~~~~
/conversations — conversaciones (M19). Auth (M20): la máquina (`X-Admin-Token`) las ve
todas; un usuario (Bearer, cualquier rol) ve las del hogar y las suyas, nunca las privadas de otro,
y las que crea son suyas (de su dispositivo).

  GET  /conversations                — lista (filtro `client_id`), la última en hablarse
                                             primero, sin las del día vacías; el título, de OpenClaw
                                             (caché de TITLES_TTL_S; si no responde, null)
  GET  /conversations/{id}/messages  — lo dicho, de `chat.history` (`limit`, 200 por defecto);
                                             503 `orchestrator_unavailable` sin OpenClaw
  POST /conversations                — conversación nueva del usuario, de su dispositivo
                                             (agente validado como en el handshake); la máquina,
                                             403 `user_required`
  GET  /conversations/agents         — los agentes con los que quien llama puede empezar
                                             una y el que tocaría sin elegir (para su selector)

Chat por HTTP (5.0, #412; lo que pasa después llega por `GET /events`):

  GET    /conversations/{id}                     — la conversación con su turno en curso, sus
                                                   pendientes y `as_of`
  POST   /conversations/{id}/messages            — escribe (`interrupt`: enviar ahora) → 202
                                                   `{message_id}` en cuanto OpenClaw lo acepta
  DELETE /conversations/{id}/messages/{msg_id}   — quita un pendiente (204; 404 si ya no lo es)
  POST   /conversations/{id}/cancel              — para el turno en curso (o el siguiente sin
                                                   empezar), para todos; con `turn_id`, solo
                                                   ese turno si sigue abierto

El gateway no guarda texto: títulos y mensajes se leen de OpenClaw en cada petición y nunca se
registran en el log.
"""

import asyncio
import logging
from datetime import UTC, datetime

from fastapi import APIRouter, Depends, HTTPException, Query, Request, Response, status
from fastapi.responses import JSONResponse, StreamingResponse
from starlette.background import BackgroundTask

from fogar_gateway.api.deps import Caller, Principal, require_scope
from fogar_gateway.core.agent_policy import AgentPolicyError, resolve_agent
from fogar_gateway.core.config import settings
from fogar_gateway.core.exceptions import ClientInactive, ClientNotFound
from fogar_gateway.models.admin_schemas import (
    CancelTurn,
    ClientIdFilter,
    ConversationAgents,
    ConversationCreate,
    ConversationIdPath,
    ConversationLive,
    ConversationMessage,
    ConversationMessages,
    ConversationResponse,
    HistoryLimit,
    MessageCreate,
)
from fogar_gateway.models.schemas import ClientConfig
from fogar_gateway.services import events as ev
from fogar_gateway.services.channel import ChannelSubscriber, RunInfo
from fogar_gateway.services.conversations import (
    Conversation,
    ConversationClientMissing,
    ConversationNotFound,
    ConversationRow,
    history_messages,
)
from fogar_gateway.services.db_client import db_client
from fogar_gateway.services.openclaw.client import (
    ORCHESTRATOR_UNAVAILABLE_ERROR,
    OrchestratorRequestError,
)
from fogar_gateway.services.reconnection import describe_error
from fogar_gateway.services.scopes import CONVERSATIONS_READ, CONVERSATIONS_WRITE

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/conversations")


def _utc(value: datetime) -> datetime:
    """SQLite devuelve las fechas sin zona; se guardan en UTC y así salen (con su `+00:00`)."""
    return value if value.tzinfo is not None else value.replace(tzinfo=UTC)


def _response(row: ConversationRow, title: str | None = None) -> ConversationResponse:
    r = row.record
    return ConversationResponse(
        id=r.id,
        client_id=r.client_id,
        client_name=row.client_name,
        agent=r.agent.lower(),
        kind=r.kind,
        day=r.day,
        created_at=_utc(r.created_at),
        last_turn_at=_utc(r.last_turn_at) if r.last_turn_at is not None else None,
        turn_count=r.turn_count,
        title=title,
        owner_user_id=r.owner_user_id,
    )


def _visible_to(principal: Principal) -> tuple[str, str | None] | None:
    if isinstance(principal, Caller):
        return principal.device.home_id, principal.device.user_id
    return None  # la máquina


async def _caller_config(caller: Caller) -> ClientConfig:
    try:
        _, config = await db_client.revalidate(caller.device.key_hash)
    except (ClientNotFound, ClientInactive):  # retirado entre medias
        raise HTTPException(status_code=401, detail="not_authenticated") from None
    return config


@router.get("", response_model=list[ConversationResponse])
async def list_conversations(
    request: Request,
    client_id: ClientIdFilter = None,
    principal: Principal = Depends(require_scope(CONVERSATIONS_READ)),
) -> list[ConversationResponse]:
    rows = await request.app.state.conversations.list_rows(
        client_id, visible_to=_visible_to(principal)
    )
    cache = request.app.state.conversation_titles
    titles = await cache.get(request.app.state.openclaw) if rows else {}
    return [_response(row, titles.get(row.record.session_key.lower())) for row in rows]


@router.get("/agents", response_model=ConversationAgents)
async def get_conversation_agents(
    request: Request, principal: Principal = Depends(require_scope(CONVERSATIONS_READ))
) -> ConversationAgents:
    """Los del roster de OpenClaw que quien llama puede usar (`allowed_agents` de su dispositivo;
    la máquina, todos) y el de la cascada. Sin roster (OpenClaw sin conectar), su
    `allowed_agents` (o ninguno). Mismas reglas que el POST."""
    # La máquina no tiene dispositivo: la cascada sin la política de ningún cliente.
    config = await _caller_config(principal) if isinstance(principal, Caller) else None
    info = request.app.state.openclaw.gateway_info
    allowed = config.allowed_agents if config is not None else None
    if info is not None:
        agents = [a for a in info.agents if allowed is None or a in allowed]
    else:
        agents = list(allowed or [])
    try:
        default: str | None = resolve_agent(requested=None, client_config=config, gateway_info=info)
    except AgentPolicyError:
        default = None  # el de la cascada no está permitido: hay que elegir
    return ConversationAgents(agents=agents, default=default)


@router.get("/{conversation_id}/messages", response_model=ConversationMessages)
async def get_conversation_messages(
    conversation_id: ConversationIdPath,
    request: Request,
    limit: HistoryLimit = 200,
    before: str | None = Query(default=None, min_length=1, max_length=200),
    principal: Principal = Depends(require_scope(CONVERSATIONS_READ)),
) -> ConversationMessages:
    conversation = await request.app.state.conversations.get(conversation_id)
    visible = _visible_to(principal)
    if conversation is None or (visible is not None and not conversation.visible_to(*visible)):
        raise HTTPException(status_code=404, detail="Conversation not found")
    try:
        payload = await request.app.state.openclaw.request(
            "chat.history",
            {"sessionKey": conversation.session_key, "limit": _HISTORY_WINDOW if before else limit},
        )
    except OrchestratorRequestError as e:
        code = str(e)  # estable: orchestrator_unavailable | orchestrator_error
        unavailable = code == ORCHESTRATOR_UNAVAILABLE_ERROR
        raise HTTPException(status_code=503 if unavailable else 502, detail=code) from None
    messages = history_messages(payload.get("messages"))
    has_more = bool(payload.get("hasMore"))
    if before is not None:
        # shortcut: chat.history no tiene `before`; se recorta una ventana de las últimas
        # _HISTORY_WINDOW. Más atrás, 404: si hace falta, paginar con su `cursor`.
        index = next((i for i, m in enumerate(messages) if m["id"] == before), None)
        if index is None:
            raise HTTPException(status_code=404, detail="message_not_found")
        has_more = has_more or index > limit
        messages = messages[max(0, index - limit) : index]
    return ConversationMessages(
        conversation_id=conversation.id,
        messages=[ConversationMessage(**m) for m in messages],
        has_more=has_more,
    )


_HISTORY_WINDOW = 1000  # el tope de `chat.history`


@router.post("", response_model=ConversationResponse, status_code=status.HTTP_201_CREATED)
async def create_conversation(
    body: ConversationCreate,
    request: Request,
    principal: Principal = Depends(require_scope(CONVERSATIONS_WRITE)),
) -> ConversationResponse:
    if not isinstance(principal, Caller):
        # Una conversación es de un dispositivo (M20): la máquina no tiene ninguno.
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="user_required")
    client_id, config = principal.device.id, await _caller_config(principal)  # de su dispositivo
    try:
        agent = resolve_agent(
            requested=body.agent,
            client_config=config,
            gateway_info=request.app.state.openclaw.gateway_info,
        )
    except AgentPolicyError as e:
        raise HTTPException(status_code=422, detail=f"agent_{e.kind}") from None
    service = request.app.state.conversations
    try:
        conversation = await service.create(client_id, agent)
    except ConversationClientMissing:  # su dispositivo, retirado entre medias
        raise HTTPException(status_code=401, detail="not_authenticated") from None
    row = await service.row(conversation.id)
    if row is None:  # borrada entre medias (su usuario, con sus conversaciones)
        raise HTTPException(status_code=401, detail="not_authenticated")
    response = _response(row)
    if body.message is not None:
        # Si el envío falla, su error (la conversación ya existe y sale en la lista).
        response.message_id = await _send(request, principal, config, conversation, body.message)
    return response


# ---------------------------------------------------------------------------
# Chat por HTTP (5.0, #412)
# ---------------------------------------------------------------------------


def _device(principal: Principal) -> Caller:
    if not isinstance(principal, Caller):
        # Escribir, quitar y parar son de un aparato (M20): la máquina no tiene ninguno.
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="user_required")
    return principal


async def _conversation(request: Request, principal: Principal, conversation_id: str) -> Conversation:
    """La conversación si quien llama puede verla (la máquina, todas); si no, 404."""
    service = request.app.state.conversations
    if isinstance(principal, Caller):
        try:
            return await service.get_visible(
                conversation_id, principal.device.home_id, principal.device.user_id
            )
        except ConversationNotFound:
            raise HTTPException(status_code=404, detail="Conversation not found") from None
    conversation = await service.get(conversation_id)
    if conversation is None:
        raise HTTPException(status_code=404, detail="Conversation not found")
    return conversation


async def _send(
    request: Request,
    caller: Caller,
    config: ClientConfig,
    conversation: Conversation,
    text: str,
    *,
    interrupt: bool = False,
    speak: bool = False,
) -> str:
    """Manda `text` al canal de la conversación como un `send` del WebSocket y devuelve su
    `message_id` (el `runId` del ack). El emisor es el adaptador del `/events` del aparato, o
    uno transitorio suscrito hasta el ack. Sin `await` entre crear el canal y suscribirse."""
    if not text.strip():
        raise HTTPException(status_code=422, detail="empty_message")
    hub = request.app.state.channel_hub
    channel = hub.get_or_create(conversation)
    device = request.app.state.events.device(caller.device.id)
    sender: ChannelSubscriber | None = device.attach(channel) if device is not None else None
    transient: ev.TransientSender | None = None
    if sender is None:
        transient = ev.TransientSender(caller.device, config, conversation.id)
        if not channel.subscribe(transient):
            raise HTTPException(status_code=503, detail="not_sent")
        sender = transient
    try:
        if not sender.agent_allowed(channel.session_key):
            raise HTTPException(status_code=403, detail="agent_not_permitted")
        try:
            # Detrás de un /v1 en marcha (`send_exclusive`) espera su turno: con tope.
            async with asyncio.timeout(settings.TURN_TIMEOUT_S):
                result = await channel.send_user_message(sender, text, interrupt=interrupt)
        except TimeoutError:
            raise HTTPException(status_code=503, detail="turn_timeout") from None
    finally:
        if transient is not None:
            channel.unsubscribe(transient)
            hub.drop_if_idle(channel.session_key, channel)
    if isinstance(result, RunInfo):
        if speak and device is not None:
            device.mark_speak(result.run_id, channel)  # 5c: su respuesta suena por su /events
        return result.run_id
    if isinstance(result, str):  # OpenClaw no lo aceptó: su código estable
        raise HTTPException(status_code=502, detail=result)
    raise HTTPException(status_code=503, detail="not_sent")  # el canal se cerraba


@router.get("/{conversation_id}", response_model=ConversationLive)
async def get_conversation(
    conversation_id: ConversationIdPath,
    request: Request,
    principal: Principal = Depends(require_scope(CONVERSATIONS_READ)),
) -> ConversationLive:
    conversation = await _conversation(request, principal, conversation_id)
    row = await request.app.state.conversations.row(conversation.id)
    if row is None:
        raise HTTPException(status_code=404, detail="Conversation not found")
    cache = request.app.state.conversation_titles
    titles = await cache.get(request.app.state.openclaw)
    base = _response(row, titles.get(row.record.session_key.lower()))
    # Su configuración (avisos, agentes): la máquina lo ve todo.
    config = await _caller_config(principal) if isinstance(principal, Caller) else None
    # Sin await desde aquí: el estado y `as_of` son del mismo instante.
    me = principal.device.id if isinstance(principal, Caller) else ""
    channel = request.app.state.channel_hub.get(conversation.session_key)
    turn = channel.current_turn if channel is not None else None
    if turn is not None and turn.origin == "push" and config is not None:
        if not ev.push_shown(config, turn.session_key or conversation.session_key):
            turn = None  # como por /events: un aviso que este aparato no recibe
    live_turn = None
    if turn is not None and turn.cancel_reason is None:
        live_turn = {**ev.turn_payload(turn, me), "text": turn.text}
    pending = [ev.message_payload(m, me) for m in channel.pending_messages()] if channel else []
    return ConversationLive(
        **base.model_dump(), turn=live_turn, pending=pending, as_of=ev.current_id()
    )


@router.post("/{conversation_id}/messages", status_code=status.HTTP_202_ACCEPTED)
async def post_message(
    conversation_id: ConversationIdPath,
    body: MessageCreate,
    request: Request,
    principal: Principal = Depends(require_scope(CONVERSATIONS_WRITE)),
) -> JSONResponse:
    caller = _device(principal)
    conversation = await _conversation(request, caller, conversation_id)
    config = await _caller_config(caller)
    message_id = await _send(
        request, caller, config, conversation, body.text, interrupt=body.interrupt,
        speak=body.speak,
    )
    return JSONResponse({"message_id": message_id}, status_code=status.HTTP_202_ACCEPTED)


@router.delete(
    "/{conversation_id}/messages/{message_id}", status_code=status.HTTP_204_NO_CONTENT
)
async def withdraw_message(
    conversation_id: ConversationIdPath,
    message_id: ConversationIdPath,
    request: Request,
    principal: Principal = Depends(require_scope(CONVERSATIONS_WRITE)),
) -> Response:
    caller = _device(principal)
    conversation = await _conversation(request, caller, conversation_id)
    channel = request.app.state.channel_hub.get(conversation.session_key)
    if channel is None or not await channel.withdraw(message_id):
        raise HTTPException(status_code=404, detail="not_pending")
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.post("/{conversation_id}/cancel")
async def cancel_turn(
    conversation_id: ConversationIdPath,
    request: Request,
    body: CancelTurn | None = None,
    principal: Principal = Depends(require_scope(CONVERSATIONS_WRITE)),
) -> dict:
    """Como el `cancel` del WebSocket: el turno abierto para todos o, sin turno abierto, el
    siguiente run cuyo turno aún no empezó. `{"cancelled": false}` si no había nada. Con
    `turn_id`, solo ese turno: si ya no es el abierto, nada («Enviar ahora» no para el
    mensaje que quería enviar si el turno acabó justo antes)."""
    caller = _device(principal)
    conversation = await _conversation(request, caller, conversation_id)
    channel = request.app.state.channel_hub.get(conversation.session_key)
    if channel is None:
        return {"cancelled": False}
    current = channel.current_turn
    if body is not None and body.turn_id is not None:
        if current is None or current.id != body.turn_id:
            return {"cancelled": False}
        return {"cancelled": await channel.cancel("cancelled", expected=current) is not None}
    if current is not None:
        return {"cancelled": await channel.cancel("cancelled", expected=current) is not None}
    return {"cancelled": await channel.cancel_head("cancelled")}


@router.post("/{conversation_id}/messages/{message_id}/speech")
async def message_speech(
    conversation_id: ConversationIdPath,
    message_id: ConversationIdPath,
    request: Request,
    principal: Principal = Depends(require_scope(CONVERSATIONS_WRITE)),
) -> StreamingResponse:
    """La voz de una respuesta del agente ya terminada (5c), también del historial: el TTS se
    paga solo cuando alguien la pide. PCM16 mono a 24 kHz en streaming, como el audio de un
    turno. 404 si no es una respuesta que se pueda ver; 503 `tts_unavailable` sin TTS."""
    caller = _device(principal)  # gasta TTS: de un aparato con `conversations:write`
    conversation = await _conversation(request, caller, conversation_id)
    try:
        payload = await request.app.state.openclaw.request(
            "chat.history", {"sessionKey": conversation.session_key, "limit": _HISTORY_WINDOW}
        )
    except OrchestratorRequestError as e:
        code = str(e)
        unavailable = code == ORCHESTRATOR_UNAVAILABLE_ERROR
        raise HTTPException(status_code=503 if unavailable else 502, detail=code) from None
    message = next(
        (m for m in history_messages(payload.get("messages")) if m["id"] == message_id), None
    )
    if message is None or message["role"] != "assistant":
        raise HTTPException(status_code=404, detail="message_not_found")
    config = await _caller_config(caller)
    provider = request.app.state.speech.tts
    # Todo el TTS (de pago) acotado por PUSH_TTS_DRAIN_TIMEOUT_S sin progreso, como el de un aviso.
    limit = settings.PUSH_TTS_DRAIN_TIMEOUT_S
    stream = None
    try:
        stream = await asyncio.wait_for(
            provider.connect(
                provider.resolve_voice(config.tts_voice), config.tts_speed, caller.device.id
            ),
            timeout=limit,
        )
        if stream is not None:
            await asyncio.wait_for(stream.send_text_chunk(message["text"]), timeout=limit)
            await asyncio.wait_for(stream.end(), timeout=limit)
    except Exception as e:  # noqa: BLE001 — frontera: un TTS caído es 503 con código estable
        logger.warning(f"Voz bajo demanda: TTS no disponible ({describe_error(e)})")
        if stream is not None:  # conectado y caído a medias: no queda abierto
            await _close_tts(stream)
        stream = None
    if stream is None:
        raise HTTPException(status_code=503, detail="tts_unavailable")
    tts = stream
    closed = False

    async def close() -> None:
        # Idempotente y también como tarea de fondo: si el cliente se va antes de que empiece
        # el cuerpo, el `finally` del generador no corre y el TTS (de pago) quedaría abierto.
        nonlocal closed
        if not closed:
            closed = True
            await _close_tts(tts)

    async def audio():
        try:
            chunks = tts.get_audio_stream()
            while True:
                try:  # el tope, solo sobre la espera del TTS (nunca alrededor del `yield`)
                    chunk = await asyncio.wait_for(anext(chunks), timeout=limit)
                except StopAsyncIteration:
                    break
                if chunk:
                    yield chunk
        except Exception as e:  # noqa: BLE001 — frontera: el audio se corta, el cliente lo ve
            logger.warning(f"Voz bajo demanda cortada ({describe_error(e)})")
        finally:
            await close()

    return StreamingResponse(
        audio(),
        media_type="audio/L16;rate=24000;channels=1",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
        background=BackgroundTask(close),
    )


async def _close_tts(stream) -> None:
    try:
        await stream.close()
    except Exception as e:  # noqa: BLE001 — limpieza: va a DEBUG
        logger.debug(f"cierre del TTS bajo demanda: {describe_error(e)}")
