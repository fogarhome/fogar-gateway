import asyncio
import json
import logging
import uuid
from collections.abc import Callable
from dataclasses import dataclass

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import BaseModel

from fogar_gateway.api.deps import missing_scope
from fogar_gateway.core.agent_policy import (
    MODEL_ALIAS,
    AgentPolicyError,
    agent_from_model,
    resolve_agent,
)
from fogar_gateway.core.exceptions import ClientInactive, ClientNotFound
from fogar_gateway.models.schemas import Client, ClientConfig
from fogar_gateway.services.channel import ChannelEvent, ChannelToken, ConversationChannel
from fogar_gateway.services.conversations import (
    Conversation,
    ConversationClientMissing,
    ConversationNotFound,
    note_turn_safely,
)
from fogar_gateway.services.db_client import db_client
from fogar_gateway.services.openclaw.client import _traceback_no_message
from fogar_gateway.services.orchestration import OrchestratorError
from fogar_gateway.services.pipeline_tracker import PipelineTracker
from fogar_gateway.services.reconnection import describe_error
from fogar_gateway.services.scopes import CONVERSATIONS_WRITE
from fogar_gateway.services.session_registry import SessionStatus
from fogar_gateway.services.text import without_lone_surrogates

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/v1")

# Cada cuánto mira /v1 sin stream si el cliente sigue ahí (D-r6-1).
_DISCONNECT_POLL_S = 0.25

# La conversación elegida (5.0, #413), como `x-openclaw-session-key` en OpenClaw.
CONVERSATION_HEADER = "x-fogar-conversation"
_CONVERSATION_ID_MAX = 64  # como `conversation` en el handshake

# Solo /v1 (5.0 PR 2b.3): otro aparato paró su turno (`cancel`), o el canal se cerró, antes
# de que terminara su respuesta.
TURN_CANCELLED_ERROR = "turn_cancelled"


def _log_unexpected_error(exc: BaseException) -> None:
    """Solo el tipo a ERROR; traceback sin mensaje ni líneas de código a DEBUG (patrón de
    #226): el mensaje de la excepción puede llevar texto de usuario o secretos."""
    logger.error("HTTP orchestrator unexpected error (%s)", type(exc).__name__)
    logger.debug("HTTP orchestrator error traceback (no message): %s", _traceback_no_message(exc))


class _ChatMessage(BaseModel):
    role: str
    content: str = ""


class _ChatCompletionRequest(BaseModel):
    messages: list[_ChatMessage] = []
    stream: bool = False
    model: str = ""


@dataclass
class V1Caller:
    """El aparato que llama a /v1/* (5.0, #413: siempre con Bearer)."""

    client: Client
    config: ClientConfig


async def resolve_v1_caller(request: Request) -> V1Caller:
    """Dependency de auth para /v1/*: `Authorization: Bearer <token>` de un aparato (el mismo
    token que en el handshake WS) con `conversations:write`. Sin él, 401; sin el permiso, 403
    `missing_scope`. Ninguna IP exime (5.0, #413): ya no hay origen de confianza."""
    auth = request.headers.get("authorization")
    if not auth or not auth.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="authentication required")
    token = auth[len("Bearer ") :]
    try:
        client, config = await db_client.get_session(token)
    except (ClientNotFound, ClientInactive):
        raise HTTPException(status_code=401, detail="authentication required") from None
    except ValueError as e:
        # D-r8-1: `allowed_agents` corrupto (editado a mano), como en el handshake: solo el tipo.
        logger.error(f"/v1: configuración del cliente ilegible: {describe_error(e)}")
        raise HTTPException(status_code=500, detail="internal_error") from None
    if CONVERSATIONS_WRITE not in client.scopes:  # #410: hablar por /v1 es escribir
        raise missing_scope(CONVERSATIONS_WRITE)
    return V1Caller(client=client, config=config)


@router.get("/models")
async def list_models(request: Request, caller: V1Caller = Depends(resolve_v1_caller)):
    # Alias first, then the roster agents (#234). The roster is whatever OpenClaw announced
    # (`agents.list`); without it only the alias is advertised. An authenticated client with
    # an explicit `allowed_agents` list only sees the agents it may actually use.
    models = [{"id": MODEL_ALIAS, "object": "model", "created": 0, "owned_by": "fogar-gateway"}]
    orchestrator = getattr(request.app.state, "openclaw", None)
    gateway_info = getattr(orchestrator, "gateway_info", None)
    allowed = caller.config.allowed_agents
    for agent_id in gateway_info.agents if gateway_info is not None else ():
        if agent_id == MODEL_ALIAS or (allowed is not None and agent_id not in allowed):
            continue
        models.append({"id": agent_id, "object": "model", "created": 0, "owned_by": "openclaw"})
    return JSONResponse({"object": "list", "data": models})


def _extract_last_user_message(messages: list[_ChatMessage]) -> str:
    for msg in reversed(messages):
        if msg.role == "user":
            return without_lone_surrogates(msg.content)  # G-r3-1: OpenClaw lo guardaría tal cual
    return ""


def _make_http_tracker(session_id: str, registry, client_id: str) -> PipelineTracker:
    return PipelineTracker(
        session_id=session_id,
        client_id=client_id,
        input_mode="text",
        output_mode=[],
        registry=registry,
    )


@router.post("/chat/completions")
async def chat_completions(
    request: Request,
    body: _ChatCompletionRequest,
    caller: V1Caller = Depends(resolve_v1_caller),
):
    text = _extract_last_user_message(body.messages)
    if not text.strip():
        # D-r6-3: un turno vacío no va a OpenClaw ni cuenta en la conversación.
        return JSONResponse({"error": "empty_message"}, status_code=400)
    stream = body.stream
    orchestrator = request.app.state.openclaw
    session_registry = request.app.state.session_registry
    completion_id = f"chatcmpl-{uuid.uuid4().hex[:12]}"

    # The conversation (5.0, #413): the one in `X-Fogar-Conversation` if this device may see it
    # (M20), else its default one (one per client, agent and day, M19). A chosen conversation
    # fixes the agent, like `conversation` in the handshake (4101 there, 409 here).
    conversations = request.app.state.conversations
    chosen = request.headers.get(CONVERSATION_HEADER)
    existing: Conversation | None = None
    requested = agent_from_model(body.model)
    if chosen is not None:
        try:
            if not 0 < len(chosen) <= _CONVERSATION_ID_MAX:
                raise ConversationNotFound()
            existing = await conversations.get_visible(
                chosen, caller.client.home_id, caller.client.user_id
            )
        except ConversationNotFound:
            return JSONResponse({"error": "conversation_not_found"}, status_code=404)
        if requested and requested.strip().lower() != existing.agent.lower():
            return JSONResponse({"error": "conversation_agent_mismatch"}, status_code=409)
        requested = existing.agent

    # body.model is the requested agent (analogous to handshake.agent in WS), except the
    # advertised model name `fogar-gateway`, which means "no explicit agent" (#217, G-36);
    # then the cascade (#105). The device's policy applies to a chosen conversation's agent too.
    try:
        resolved_agent = resolve_agent(
            requested=requested,
            client_config=caller.config,
            gateway_info=orchestrator.gateway_info,
        )
    except AgentPolicyError as e:
        return JSONResponse(
            {
                "error": "forbidden",
                "reason": (
                    "agent_not_permitted" if e.kind == "not_permitted" else "agent_not_available"
                ),
                "message": str(e),
            },
            status_code=403,
        )

    client_id = caller.client.id
    by_name = caller.client.name
    if existing is not None:
        conversation = existing
    else:
        try:
            conversation = await conversations.default_for(client_id, resolved_agent)
        except ConversationClientMissing:
            # Borrado entre la autenticación y el alta: como una clave que ya no vale.
            raise HTTPException(status_code=401, detail="authentication required") from None
        except Exception as e:  # noqa: BLE001 — frontera: sin conversación no hay turno (502 estable)
            _log_unexpected_error(e)
            return JSONResponse({"error": "internal_error"}, status_code=502)
    conversation_id = conversation.id
    # 5.0 PR 2b.3: the turn goes through the conversation's channel, like a WebSocket message,
    # but alone: it waits until the channel is calm and its answer is its own run's.
    hub = request.app.state.channel_hub

    async def _count_turn() -> None:
        """El turno cuenta en su conversación en cuanto OpenClaw acepta su chat.send (aunque
        el cliente se vaya después); un rechazo previo no cuenta."""
        await note_turn_safely(conversations, conversation_id)

    tracker = _make_http_tracker(f"http:{completion_id}", session_registry, client_id)
    session_registry.register(tracker)

    async def _turn(on_token: Callable[[str], None]) -> None:
        """The /v1 turn through the channel. Raises OrchestratorError with a stable code."""

        def sink(event: ChannelEvent) -> None:
            if isinstance(event, ChannelToken):
                on_token(event.text)

        # Resolved right before the call, with no await in between: a channel fetched earlier
        # could have been dropped as idle by another request meanwhile (2b.3 review M2);
        # `send_exclusive` pins it before its first await.
        channel: ConversationChannel = hub.get_or_create(conversation)
        outcome = await channel.send_exclusive(
            text,
            by_client_id=client_id,
            by_name=by_name,
            sink=sink,
            tracker=tracker,
            on_accepted=_count_turn,
        )
        if outcome.error is not None:
            raise OrchestratorError(outcome.error)
        if outcome.cut:
            raise OrchestratorError(TURN_CANCELLED_ERROR)

    if stream:

        async def generate():
            queue: asyncio.Queue[str | None] = asyncio.Queue()
            orchestrator_error: Exception | None = None
            error_message = ""

            def _on_token(t: str) -> None:
                chunk = {
                    "id": completion_id,
                    "object": "chat.completion.chunk",
                    "choices": [{"delta": {"content": t}, "index": 0, "finish_reason": None}],
                }
                queue.put_nowait(f"data: {json.dumps(chunk)}\n\n")

            async def _run():
                nonlocal orchestrator_error, error_message
                # G-26 (#221): solo un turno terminado es `completed`; cancelado, `error`.
                status: SessionStatus = "error"
                try:
                    await _turn(_on_token)
                except OrchestratorError as e:
                    # Stable code (orchestrator_error, turn_timeout...): safe to relay (#228).
                    logger.error("HTTP orchestrator error (%s)", e)
                    orchestrator_error = e
                    error_message = str(e)
                except Exception as e:  # noqa: BLE001 — frontera: el SSE cierra con internal_error
                    # Cualquier otro fallo no es un éxito truncado (#217, G-37): se cierra como
                    # error sin filtrar el detalle interno al cliente.
                    _log_unexpected_error(e)
                    orchestrator_error = e
                    error_message = "internal_error"
                else:
                    status = "completed"
                finally:
                    # El tracker, antes del centinela: al verlo, el generador cancela esta tarea
                    # si aún no terminó, y el cierre se quedaría sin hacer (la sesión, activa).
                    try:
                        await tracker.close(status=status)
                    except Exception as e:  # noqa: BLE001 — limpieza: va a DEBUG
                        logger.debug("HTTP tracker close failed (%s)", describe_error(e))
                    finally:
                        await queue.put(None)  # siempre, también si el cierre falló

            task = asyncio.create_task(_run())
            try:
                while True:
                    item = await queue.get()
                    if item is None:
                        break
                    yield item
            finally:
                if not task.done():
                    # El cliente se fue: el turno se corta (solo su run, por su runId).
                    task.cancel()
                    # asyncio.wait no relanza la cancelación esperada de `task`; una ajena
                    # (cancelan a quien cierra el stream) sí se propaga (#221).
                    await asyncio.wait({task})
                    if not task.cancelled() and (e := task.exception()) is not None:
                        logger.debug("HTTP stream turn failed (%s)", describe_error(e))

            if orchestrator_error is not None:
                error = {
                    "error": {
                        "message": error_message,
                        "type": "server_error",
                    },
                }
                yield f"data: {json.dumps(error)}\n\n"
                final = {
                    "id": completion_id,
                    "object": "chat.completion.chunk",
                    "choices": [{"delta": {}, "index": 0, "finish_reason": "error"}],
                }
                yield f"data: {json.dumps(final)}\n\n"
                yield "data: [DONE]\n\n"
                return

            final = {
                "id": completion_id,
                "object": "chat.completion.chunk",
                "choices": [{"delta": {}, "index": 0, "finish_reason": "stop"}],
            }
            yield f"data: {json.dumps(final)}\n\n"
            yield "data: [DONE]\n\n"

        return StreamingResponse(
            generate(),
            media_type="text/event-stream",
            # D-r6-5: que un proxy (nginx) no retenga los fragmentos.
            headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
        )

    tokens: list[str] = []
    orchestrator_error: Exception | None = None
    status: SessionStatus = "error"  # G-26 (#221): una cancelación no es un éxito
    disconnected = False
    turn = asyncio.create_task(_turn(tokens.append))
    try:
        # D-r6-1: como en la rama de stream, si el cliente se va, el turno se cancela (y con
        # él, solo su run en OpenClaw); si no, la conversación seguiría ocupada hasta
        # TURN_TIMEOUT_S.
        while not turn.done():
            await asyncio.wait({turn}, timeout=_DISCONNECT_POLL_S)
            if not turn.done() and await request.is_disconnected():
                disconnected = True
                break
        if not disconnected:
            turn.result()
    except OrchestratorError as e:
        logger.error("HTTP orchestrator error (%s)", e)
        orchestrator_error = e
    except Exception as e:  # noqa: BLE001 — frontera: 502 con internal_error
        # Any other exception (RuntimeError included) may carry raw text: never relayed.
        _log_unexpected_error(e)
        orchestrator_error = RuntimeError("internal_error")
    else:
        if not disconnected:
            status = "completed"
    finally:
        if not turn.done():
            # El cliente se fue, o cancelan este handler (#221: esa cancelación sigue su curso).
            turn.cancel()
            await asyncio.wait({turn})
        # Siempre recogida (también si terminó mientras nos cancelaban): sin «Task exception
        # was never retrieved», que saldría a ERROR con el texto de la excepción.
        if turn.done() and not turn.cancelled() and (exc := turn.exception()) is not None:
            logger.debug("HTTP turn ended with %s", describe_error(exc))
        await tracker.close(status=status)

    if disconnected:
        logger.info("HTTP client disconnected; turn aborted")
        return JSONResponse({"error": "client_disconnected"}, status_code=499)
    if orchestrator_error:
        return JSONResponse({"error": str(orchestrator_error)}, status_code=502)

    content = "".join(tokens)
    return JSONResponse(
        {
            "id": completion_id,
            "object": "chat.completion",
            "choices": [
                {
                    "message": {"role": "assistant", "content": content},
                    "index": 0,
                    "finish_reason": "stop",
                }
            ],
            "usage": {
                "prompt_tokens": 0,
                "completion_tokens": len(tokens),
                "total_tokens": len(tokens),
            },
        }
    )
