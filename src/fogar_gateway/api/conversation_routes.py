"""
conversation_routes.py
~~~~~~~~~~~~~~~~~~~~~~
/admin/conversations — conversaciones (M19). Auth (M20): la máquina (`X-Admin-Token`) las ve
todas; un usuario (Bearer, cualquier rol) ve las del hogar y las suyas, nunca las privadas de otro,
y las que crea son suyas (de su dispositivo).

  GET  /admin/conversations                — lista (filtro `client_id`), la última en hablarse
                                             primero, sin las del día vacías; el título, de OpenClaw
                                             (caché de TITLES_TTL_S; si no responde, null)
  GET  /admin/conversations/{id}/messages  — lo dicho, de `chat.history` (`limit`, 200 por defecto);
                                             503 `orchestrator_unavailable` sin OpenClaw
  POST /admin/conversations                — conversación nueva del usuario, de su dispositivo
                                             (agente validado como en el handshake); la máquina,
                                             403 `user_required`
  GET  /admin/conversations/agents         — los agentes con los que quien llama puede empezar
                                             una y el que tocaría sin elegir (para su selector)

El gateway no guarda texto: títulos y mensajes se leen de OpenClaw en cada petición y nunca se
registran en el log.
"""

import logging
from datetime import UTC, datetime

from fastapi import APIRouter, Depends, HTTPException, Request, status

from fogar_gateway.api.deps import Caller, Principal, current_principal
from fogar_gateway.core.agent_policy import AgentPolicyError, resolve_agent
from fogar_gateway.core.exceptions import ClientInactive, ClientNotFound
from fogar_gateway.models.admin_schemas import (
    ClientIdFilter,
    ConversationAgents,
    ConversationCreate,
    ConversationIdPath,
    ConversationMessage,
    ConversationMessages,
    ConversationResponse,
    HistoryLimit,
)
from fogar_gateway.models.schemas import ClientConfig
from fogar_gateway.services.conversations import (
    ConversationClientMissing,
    ConversationRow,
    history_messages,
)
from fogar_gateway.services.db_client import db_client
from fogar_gateway.services.openclaw.client import (
    ORCHESTRATOR_UNAVAILABLE_ERROR,
    OrchestratorRequestError,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/admin/conversations")


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
        return principal.device.home_id, principal.user.id
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
    principal: Principal = Depends(current_principal),
) -> list[ConversationResponse]:
    rows = await request.app.state.conversations.list_rows(
        client_id, visible_to=_visible_to(principal)
    )
    cache = request.app.state.conversation_titles
    titles = await cache.get(request.app.state.openclaw) if rows else {}
    return [_response(row, titles.get(row.record.session_key.lower())) for row in rows]


@router.get("/agents", response_model=ConversationAgents)
async def get_conversation_agents(
    request: Request, principal: Principal = Depends(current_principal)
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
    principal: Principal = Depends(current_principal),
) -> ConversationMessages:
    conversation = await request.app.state.conversations.get(conversation_id)
    visible = _visible_to(principal)
    if conversation is None or (visible is not None and not conversation.visible_to(*visible)):
        raise HTTPException(status_code=404, detail="Conversation not found")
    try:
        payload = await request.app.state.openclaw.request(
            "chat.history", {"sessionKey": conversation.session_key, "limit": limit}
        )
    except OrchestratorRequestError as e:
        code = str(e)  # estable: orchestrator_unavailable | orchestrator_error
        unavailable = code == ORCHESTRATOR_UNAVAILABLE_ERROR
        raise HTTPException(status_code=503 if unavailable else 502, detail=code) from None
    return ConversationMessages(
        conversation_id=conversation.id,
        messages=[ConversationMessage(**m) for m in history_messages(payload.get("messages"))],
        has_more=bool(payload.get("hasMore")),
    )


@router.post("", response_model=ConversationResponse, status_code=status.HTTP_201_CREATED)
async def create_conversation(
    body: ConversationCreate, request: Request, principal: Principal = Depends(current_principal)
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
    return _response(row)
