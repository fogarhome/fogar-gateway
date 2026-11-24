import asyncio
import json
import logging
import time

from fastapi import APIRouter, WebSocket, WebSocketDisconnect
from pydantic import ValidationError

from fogar_gateway.core.agent_policy import AgentPolicyError, AgentPolicyKind, resolve_agent
from fogar_gateway.core.config import settings
from fogar_gateway.core.exceptions import ClientInactive, ClientNotFound
from fogar_gateway.core.logging import fingerprint_key
from fogar_gateway.core.network import client_ip_for_log
from fogar_gateway.core.transport import close_or_abort, drop_slow_client
from fogar_gateway.models.close_codes import Close, CloseFrame, close_sent, send_close
from fogar_gateway.models.schemas import PROTOCOL_VERSION, Client, ClientConfig, Handshake
from fogar_gateway.services.bridge import FogarBridge
from fogar_gateway.services.conversations import (
    NEW,
    ConversationClientMissing,
    ConversationNotFound,
    ConversationService,
    SessionConversation,
    new_conversation,
)
from fogar_gateway.services.db_client import db_client
from fogar_gateway.services.openclaw.models import GatewayInfo
from fogar_gateway.services.outbound import ClientGone, QueuedSender
from fogar_gateway.services.pipeline_tracker import PipelineTracker
from fogar_gateway.services.reconnection import describe_error, traceback_no_message
from fogar_gateway.services.stats import gateway_stats
from fogar_gateway.services.teardown import Teardown

logger = logging.getLogger(__name__)

router = APIRouter()

# Una clave por cada `AgentPolicyKind`: un tipo nuevo sin su cierre lo detectan mypy y los tests.
AGENT_CLOSE: dict[AgentPolicyKind, CloseFrame] = {
    "not_permitted": Close.AGENT_NOT_PERMITTED,
    "not_available": Close.AGENT_NOT_AVAILABLE,
}


class _Rejected(Exception):
    """Un handshake rechazado: `close` va al cliente; `kind`, al log."""

    def __init__(self, kind: str, close: CloseFrame, stat: str) -> None:
        super().__init__(kind)
        self.kind, self.close, self.stat = kind, close, stat


async def _authenticate(handshake: Handshake) -> tuple[Client, ClientConfig, str]:
    """`(Client, ClientConfig, quién para el log)` de la `client_key` del handshake: un
    dispositivo emparejado o uno con sesión (M20). Lanza ClientNotFound/ClientInactive."""
    key = handshake.client_key
    client, config = await db_client.get_session(key)
    await db_client.touch(client.id, handshake.capabilities)
    return client, config, f"fp={fingerprint_key(key)}"


async def _open_conversation(
    service: ConversationService,
    client: Client,
    config: ClientConfig,
    handshake: Handshake,
    gateway_info: GatewayInfo | None,
) -> SessionConversation:
    """La conversación de la sesión: la pedida (`conversation`, una que pueda ver: del hogar o,
    desde un dispositivo personal, de su usuario; M20), una nueva (`"new"`) o, sin pedir, la del
    día. Lanza `_Rejected` (41xx, 402x)."""
    requested = handshake.conversation
    existing = None
    if requested is not None and requested != NEW:
        try:
            existing = await service.get_visible(requested, client.home_id, client.user_id)
        except ConversationNotFound:
            raise _Rejected(
                "conversation_not_found", Close.CONVERSATION_NOT_FOUND, "conversation"
            ) from None
        asked = (handshake.agent or "").strip()
        if asked and asked != existing.agent.lower():
            raise _Rejected("conversation_agent", Close.CONVERSATION_AGENT_MISMATCH, "conversation")
    try:
        # Una conversación existente fija su agente: se le aplican la lista y el roster.
        agent = resolve_agent(
            requested=existing.agent if existing is not None else handshake.agent,
            client_config=config,
            gateway_info=gateway_info,
        )
    except AgentPolicyError as e:
        raise _Rejected(f"agent {e.kind}", AGENT_CLOSE[e.kind], "agent") from None
    if existing is not None:
        conversation = existing
    elif requested == NEW:
        # Se guarda tras el health check, junto a claim_conversation (C-3).
        conversation = new_conversation(client.id, agent)
    else:
        conversation = await service.default_for(client.id, agent)
    return SessionConversation(conversation, explicit=requested is not None, service=service)


@router.websocket("/ws/stream")
async def gateway_websocket(websocket: WebSocket):
    await websocket.accept()

    # 1. FASE DE HANDSHAKE INITIAL
    try:
        raw_msg = await asyncio.wait_for(
            websocket.receive_text(), timeout=settings.HANDSHAKE_TIMEOUT_S
        )
        if len(raw_msg) > settings.HANDSHAKE_MAX_BYTES:
            # Antes de parsear/autenticar: json.loads + pydantic cuestan ~8x el tamaño.
            logger.warning(
                f"Handshake rechazado: {len(raw_msg)} caracteres > HANDSHAKE_MAX_BYTES."
            )
            gateway_stats.handshake_rejected("too_large")
            await send_close(websocket, Close.HANDSHAKE_TOO_LARGE)
            return
        msg_data = json.loads(raw_msg)
        # Antes de validar: un cliente de otra versión debe ver 4003, no 4000 por un campo que
        # esa versión no tenía. `type(...) is int`: `true` no es la versión 5.
        if isinstance(msg_data, dict):
            protocol = msg_data.get("protocol")
            if type(protocol) is not int or protocol != PROTOCOL_VERSION:
                # Solo un entero se registra: otro tipo puede traer la credencial o texto del usuario.
                shown = protocol if type(protocol) is int else type(protocol).__name__
                logger.warning(f"Handshake rechazado: protocol={shown} != {PROTOCOL_VERSION}.")
                gateway_stats.handshake_rejected("protocol")
                await send_close(websocket, Close.PROTOCOL_UNSUPPORTED)
                return
        handshake = Handshake.model_validate(msg_data)  # JSON que no es objeto → ValidationError (D-r4-1)
    except (ValueError, RecursionError) as e:
        # ValueError cubre JSONDecodeError, ValidationError y el límite de 4300 dígitos de un
        # entero; RecursionError, un anidamiento profundo (D-r5-2).
        # G-27b (#216): str(ValidationError) incluye `input_value=` con el dict entero
        # (la client_key). Solo campo y tipo de error; de JSONDecodeError, su mensaje
        # (posición, sin el documento); de lo demás, solo el tipo.
        if isinstance(e, ValidationError):
            detail = ", ".join(
                f"{'.'.join(str(p) for p in err['loc'])}:{err['type']}"
                for err in e.errors(include_input=False, include_url=False, include_context=False)
            )
        elif isinstance(e, json.JSONDecodeError):
            detail = f"JSON inválido ({e.msg})"
        else:
            detail = f"JSON inválido ({type(e).__name__})"
        logger.error(f"Payload inicial inválido de handshake. {detail}")
        gateway_stats.handshake_rejected("invalid")
        await send_close(websocket, Close.HANDSHAKE_INVALID)
        return
    except TimeoutError:
        logger.warning("Handshake no recibido dentro de HANDSHAKE_TIMEOUT_S.")
        gateway_stats.handshake_rejected("timeout")
        await send_close(websocket, Close.HANDSHAKE_TIMEOUT)
        return
    except KeyError:
        # #232: starlette's receive_text() lee message["text"]; un primer frame binario
        # (o cualquier mensaje sin "text") lanza KeyError. Sin contenido en el log.
        logger.warning("Handshake rechazado: el primer mensaje no es texto.")
        gateway_stats.handshake_rejected("not_text")
        await send_close(websocket, Close.HANDSHAKE_NOT_TEXT)
        return
    except WebSocketDisconnect:
        logger.info("Cliente desconectado antes o durante el handshake.")
        return

    # 2. RESOLVER IDENTIDAD EN LA BD LOCAL
    request_id = websocket.state.request_id
    try:
        client, config, who = await _authenticate(handshake)
    except (ClientNotFound, ClientInactive):
        # La client_key solo como huella.
        who = f"fp={fingerprint_key(handshake.client_key)}"
        logger.warning(
            f"request_id={request_id} source_ip={client_ip_for_log(websocket)} "
            f"{who} Handshake rechazado: credencial inválida o cliente inactivo."
        )
        gateway_stats.handshake_rejected("invalid_key")
        await send_close(websocket, Close.INVALID_CREDENTIAL)
        return
    except ValueError as e:
        # C-r7-2: fila con `allowed_agents` corrupto (editada a mano): solo el tipo, nunca el valor.
        logger.error(f"request_id={request_id} Configuración del cliente ilegible: {describe_error(e)}")
        await send_close(websocket, Close.SETUP_FAILED)
        return
    logger.info(f"request_id={request_id} client_id={client.id} {who} Handshake verificado.")

    # 3. INSTANCIAR EL PUENTE DE MICROSERVICIOS
    app_state = websocket.scope["app"].state
    openclaw = app_state.openclaw

    # Agente (#105, #245) y conversación (M19): la cascada y la política de resolve_agent, y la
    # conversación pedida (del propio cliente) o la del día.
    try:
        conversation = await _open_conversation(
            app_state.conversations, client, config, handshake, openclaw.gateway_info
        )
    except _Rejected as e:
        logger.warning(f"[{client.id}] Handshake rechazado: {e.kind}")
        gateway_stats.handshake_rejected(e.stat)
        await send_close(websocket, e.close)
        return
    except ConversationClientMissing:
        # Borrado entre la autenticación y el alta de la conversación: su clave ya no vale.
        logger.warning(f"[{client.id}] Handshake rechazado: el cliente ya no existe.")
        gateway_stats.handshake_rejected("invalid_key")
        await send_close(websocket, Close.INVALID_CREDENTIAL)
        return
    except Exception as e:  # noqa: BLE001 — frontera: sin conversación no hay sesión (1011)
        logger.error(f"[{client.id}] Fallo al abrir la conversación: {describe_error(e)}")
        logger.debug(f"[{client.id}] conversación: {traceback_no_message(e)}")
        await send_close(websocket, Close.SETUP_FAILED)
        return
    resolved_agent = conversation.current.agent

    session_id = f"{client.id}:{int(time.time() * 1000)}"
    session_registry = app_state.session_registry
    # One outbound path per session: sender -> tracker -> bridge (issue #130 S1); a client
    # that stops reading is dropped (issue #130 S2).
    # Created only after every handshake rejection above, so no writer task leaks.
    async def _on_slow_client() -> None:
        # `tracker` is created right below; this only runs when the writer's send times
        # out, long after the session is fully constructed.
        bridge.note_close_reason("slow_client")
        await drop_slow_client(
            websocket,
            record=lambda: tracker.record("client_slow", timeout_s=settings.CLIENT_SEND_TIMEOUT_S),
        )

    async def _on_send_error() -> None:
        bridge.note_close_reason("error")
        await close_or_abort(websocket, Close.INTERNAL_ERROR)

    sender = QueuedSender(websocket, on_slow=_on_slow_client, on_error=_on_send_error)
    # G-24 (#221): the writer task is already running; if building the rest fails, close
    # it and the session instead of leaving them orphaned. Construction is synchronous,
    # so only an Exception can land here.
    try:
        tracker = PipelineTracker(
            session_id=session_id,
            client_id=client.id,
            input_mode=handshake.input_mode,
            output_mode=handshake.output_mode,
            registry=session_registry,
            sender=sender,
        )
        session_registry.register(tracker)
        bridge = FogarBridge(
            client=client,
            config=config,
            client_ws=websocket,
            orchestrator=openclaw,
            speech=app_state.speech,
            tracker=tracker,
            handshake=handshake,
            client_registry=app_state.client_registry,
            conversation=conversation,
            sender=sender,
            turn_registry=app_state.turn_registry,
            channel_hub=app_state.channel_hub,
        )
    except Exception as e:  # noqa: BLE001 — frontera: sesión a medio construir
        logger.error(f"[{client.id}] Fallo al construir la sesión: {describe_error(e)}")
        logger.debug(f"[{client.id}] construcción de la sesión: {traceback_no_message(e)}")
        async with Teardown(f"[{client.id}] sesión a medio construir") as td:
            session_registry.close(session_id, "error")  # no-op si no llegó a registrarse
            await td.step(sender.aclose(settings.SESSION_TEARDOWN_S), "cierre de la salida")
            await td.step(
                send_close(websocket, Close.SETUP_FAILED),
                "cierre del WebSocket",
                settings.SESSION_TEARDOWN_S,
            )
        return

    # One single try/finally covers setup (connect_internal_services, health
    # check, ready send) and bridge.run(): any early return/exception below
    # still reaches bridge.close_all() — a failed handshake during an
    # OpenClaw outage used to leak the transcriber socket, the bridge
    # registration, and leave the session "active" forever (issue #101).
    # bridge.run() already calls close_all() itself in its own finally on
    # the normal-completion path, so this always runs it a second time —
    # close_all() is idempotent, the first call's status wins.
    # `clean`: bridge.run() volvió sin excepción. Cualquier otra salida (una excepción en el
    # arranque, `ready` sin enviar) que no haya mandado ya su cierre de la tabla cierra con 1011.
    clean = False
    try:
        try:
            await bridge.connect_internal_services()
        except Exception as e:  # noqa: BLE001 — frontera: el arranque fallido cierra con 1011
            logger.error(
                f"[{client.id}] Fallo al inicializar puentes internos: {describe_error(e)}"
            )
            logger.debug(f"[{client.id}] puentes internos: {traceback_no_message(e)}")
            await send_close(websocket, Close.SETUP_FAILED)
            return

        # 3.5 HEALTH CHECK — verifica disponibilidad de servicios antes de abrir la sesión
        try:
            live_capabilities = await bridge.health_check()
        except ClientGone:
            # #205: el cliente se fue durante el arranque; es una desconexión normal.
            # El `finally` de abajo hace close_all / sender.aclose.
            logger.info(f"[{client.id}] Cliente desconectado durante el health check.")
            bridge.note_close_reason("client_disconnect")
            return
        if live_capabilities is None:
            logger.warning(f"[{client.id}] Health check falló. Cerrando sesión.")
            bridge.note_close_reason("error")
            try:
                await sender.flush()  # deliver the queued `status` before the 1011 close
            except ClientGone:
                pass
            await send_close(websocket, Close.ORCHESTRATOR_UNAVAILABLE)
            return

        # 3.55 CONVERSACIÓN (M19): solo una sesión que pasó el health check la abre; la que la
        # tuviera se cierra (4102) y se espera a que suelte sus turnos antes de `ready`.
        if handshake.conversation == NEW:
            # Solo ahora: un handshake que acaba en 1011 no deja una conversación vacía (C-3).
            try:
                await app_state.conversations.save(conversation.current)
            except ConversationClientMissing:
                logger.warning(f"[{client.id}] Handshake rechazado: el cliente ya no existe.")
                gateway_stats.handshake_rejected("invalid_key")
                await send_close(websocket, Close.INVALID_CREDENTIAL)
                return
            except Exception as e:  # noqa: BLE001 — frontera: sin conversación no hay sesión (1011)
                logger.error(f"[{client.id}] Fallo al abrir la conversación: {describe_error(e)}")
                logger.debug(f"[{client.id}] conversación: {traceback_no_message(e)}")
                await send_close(websocket, Close.SETUP_FAILED)
                return
        await bridge.claim_conversation()

        # 3.6 SEND READY — confirms session is established and announces capabilities
        # resolved_agent comes from the policy helper above.
        try:
            await sender.send_json(
                {
                    "type": "ready",
                    "protocol": PROTOCOL_VERSION,
                    "session_id": session_id,
                    "agent": resolved_agent,
                    "conversation": {"id": conversation.id, "agent": resolved_agent},
                    "input_mode": handshake.input_mode,
                    "output_mode": handshake.output_mode,
                    "requested_capabilities": {
                        "barge_in": config.barge_in_enabled,
                        "tts": "audio" in handshake.output_mode,
                        "transcriber": handshake.input_mode == "audio",
                    },
                    "live_capabilities": live_capabilities,
                }
            )
            await sender.flush()
        except Exception as e:  # noqa: BLE001 — frontera: sin `ready` no hay sesión
            logger.warning(f"[{client.id}] Failed to send ready: {describe_error(e)}")
            bridge.note_close_reason("client_disconnect" if isinstance(e, ClientGone) else "error")
            return

        # 4. LANZAR LOOPS CONCURRENTES
        try:
            await bridge.run()
        except Exception as e:  # noqa: BLE001 — frontera: el finally cierra la sesión
            logger.error(
                f"[{client.id}] Error crítico de Runtime en el Puente Principal: "
                f"{describe_error(e)}"
            )
            logger.debug(f"[{client.id}] puente principal: {traceback_no_message(e)}")
            bridge.note_close_reason("error")
        else:
            clean = True
    finally:
        # Each step runs even if this handler is cancelled; the cancellation is
        # re-raised at the end (#221). close_all() bounds each of its own steps.
        async with Teardown(f"[{client.id}] fin de sesión") as td:
            await td.step(bridge.close_all(status="error"), "close_all")
            # close_all() queued session_end; drain it before the socket goes away.
            await td.step(sender.aclose(settings.SESSION_TEARDOWN_S), "drenado de la salida")
            # Un solo cierre por sesión: si ya salió uno de la tabla (4200, 4102, 1011 del
            # arranque, 1013…), ese es el que vale.
            if close_sent(websocket) is None and "DISCONNECTED" not in websocket.client_state.name:
                # 1000 si la sesión acabó bien (inactividad, silencio); 1011 si fue un fallo.
                ok = clean and not bridge.ended_in_error
                frame = Close.NORMAL if ok else Close.INTERNAL_ERROR
                await td.step(
                    send_close(websocket, frame), "cierre del WebSocket", settings.SESSION_TEARDOWN_S
                )
            logger.info(f"[{client.id}] --- Sesión de entrada WebSocket Concluida ---")
