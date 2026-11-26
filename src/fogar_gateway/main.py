import asyncio
import logging
import math
from collections.abc import Callable
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from fogar_gateway import paths
from fogar_gateway.api.auth_routes import router as auth_router
from fogar_gateway.api.conversation_routes import router as conversation_router
from fogar_gateway.api.dictation_routes import router as dictation_router
from fogar_gateway.api.events_routes import router as events_router
from fogar_gateway.api.health_routes import router as health_router
from fogar_gateway.api.home_routes import router as home_router
from fogar_gateway.api.openai_routes import router as openai_router
from fogar_gateway.api.pairing_routes import admin_router as pairing_admin_router
from fogar_gateway.api.pairing_routes import public_router as pairing_public_router
from fogar_gateway.api.routes import router as stream_router
from fogar_gateway.api.system_routes import router as system_router
from fogar_gateway.api.web_routes import WEB_DIR, web_available
from fogar_gateway.api.web_routes import router as web_router
from fogar_gateway.core.body_limit import BodySizeLimitMiddleware
from fogar_gateway.core.config import settings
from fogar_gateway.core.logging import configure_logging, pin_websockets_logging
from fogar_gateway.core.network import warn_if_open_proxy_config
from fogar_gateway.core.request_id import RequestIdMiddleware
from fogar_gateway.core.startup_checks import (
    acquire_single_process_lock,
    check_speech_settings,
    release_single_process_lock,
    warn_insecure_tokens,
)
from fogar_gateway.core.transport import TransportCaptureMiddleware
from fogar_gateway.db.database import dispose_engine, require_database, run_migrations
from fogar_gateway.services.auth import LoginLimiter
from fogar_gateway.services.channel import ChannelHub
from fogar_gateway.services.conversations import ConversationService, TitleCache, note_turn_safely
from fogar_gateway.services.db_client import db_client
from fogar_gateway.services.events import EventHub
from fogar_gateway.services.openclaw.client import OpenClawClient
from fogar_gateway.services.openclaw.dispatcher import FrameDispatcher
from fogar_gateway.services.openclaw.models import OrchestratorAccess
from fogar_gateway.services.openclaw.reconnecting import ReconnectingOpenClawClient
from fogar_gateway.services.openclaw.registry import ClientRegistry, TurnRegistry
from fogar_gateway.services.pairing import PairingStore
from fogar_gateway.services.reconnection import (
    ConnectionState,
    describe_error,
    to_wire_state,
    traceback_no_message,
)
from fogar_gateway.services.session_registry import SessionRegistry
from fogar_gateway.services.speech.factory import build_stt_provider, build_tts_provider
from fogar_gateway.services.speech.registry import SpeechProviders
from fogar_gateway.services.teardown import Teardown

configure_logging(settings.LOG_FORMAT)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # M6f (#218): un solo proceso por base de datos. El estado vivo (sesiones, cachés,
    # ClientRegistry) es por proceso, así que varios workers sobre la misma BD no están
    # soportados. El lock se toma antes que nada y se suelta al final del apagado; si el
    # arranque falla a medias, también (finally). El SO lo libera si el proceso muere.
    # Regla 5 de las rutas (M14c): dónde está cada cosa, antes de tocar nada. De la URL de una BD
    # que no es un fichero solo sale el esquema (podría llevar contraseña).
    config = paths.config_file()
    logger.info(
        "Rutas: base %s · configuración %s%s · BD %s", paths.home(), config,
        "" if config.is_file() else " (no existe)",
        paths.sqlite_file(settings.DATABASE_URL) or settings.DATABASE_URL.split(":", 1)[0],
    )
    if not web_available():
        logger.warning("Sin la web compilada en %s: / no sirve el panel (la API va igual)", WEB_DIR)
    # Decisión 5 de M14c: sin BD se para aquí, antes de que el candado cree data/ y gateway.lock.
    require_database()
    lock_handle = None
    if settings.SINGLE_PROCESS_LOCK:
        lock_handle = acquire_single_process_lock(settings.DATABASE_URL)
    try:
        async with _lifespan_body(app):
            yield
    finally:
        release_single_process_lock(lock_handle)


@asynccontextmanager
async def _lifespan_body(app: FastAPI):
    run_migrations()
    logger.info("BD inicializada")
    warn_if_open_proxy_config()
    warn_insecure_tokens(settings)
    # B-r4-2: `websockets` a DEBUG volcaría las tramas de OpenClaw (el connect lleva el token),
    # no solo las de ElevenLabs.
    pin_websockets_logging()
    # Fatal en arranque: un proveedor de pago mal configurado no debe esperar al primer uso.
    check_speech_settings(
        settings, stt_provider=settings.STT_PROVIDER, tts_provider=settings.TTS_PROVIDER
    )

    # Conversaciones (M19): el índice sessionKey → conversación, cargado de la BD, reparte el push.
    conversations = ConversationService()
    await conversations.load()
    turn_registry = TurnRegistry()
    client_registry = ClientRegistry(conversations.index)
    # Un canal por conversación (5.0, #406): el único dueño de sus runs; reparte cada turno a
    # las sesiones adjuntas. Su orquestador se pone en cuanto existe (el cliente necesita el
    # dispatcher, y el dispatcher los canales).
    channel_hub = ChannelHub(
        index=conversations.index,
        turn_registry=turn_registry,
        note_turn=lambda conversation_id: _count_turn(conversation_id),
        # 2c.3: dónde suena un aviso. Su dispositivo de avisos, leído de la BD en cada aviso
        # (en un hilo), y las sesiones vivas de ese aparato (la más reciente al final).
        notify_device=lambda conversation: db_client.notify_device(
            conversation.home_id, conversation.owner_user_id
        ),
        sessions_of=lambda client_id: [b.turns for b in client_registry.for_client(client_id)],
    )

    def _count_turn(conversation_id: str) -> None:
        channel_hub.spawn(note_turn_safely(conversations, conversation_id), "conversation_turn")

    dispatcher = FrameDispatcher(turn_registry, channel_hub)
    # Acceso a OpenClaw (permisos, emparejamiento): lo actualiza el cliente, lo lee /system/state.
    openclaw_access = OrchestratorAccess()
    inner = OpenClawClient(
        host=settings.OPENCLAW_HOST,
        port=settings.OPENCLAW_PORT,
        tls=settings.OPENCLAW_TLS,
        token=settings.OPENCLAW_TOKEN,
        turn_registry=turn_registry,
        dispatcher=dispatcher,
        access=openclaw_access,
    )
    openclaw = ReconnectingOpenClawClient(
        inner,
        name="openclaw",
        initial_backoff=settings.ORCHESTRATOR_RECONNECT_INITIAL_BACKOFF,
        max_backoff=settings.ORCHESTRATOR_RECONNECT_MAX_BACKOFF,
    )
    channel_hub.orchestrator = openclaw
    # Never raises: a failed first connect is logged and retried in the background.
    await openclaw.connect()

    # Wired *after* the initial connect on purpose: on_state_change firing
    # CONNECTED on a normal first-time success would send a spurious
    # "restored" notice to every connected-but-idle client, even though
    # nothing was ever broken.
    # `_notification_tasks` holds a strong reference to each fire-and-forget
    # broadcast task so the event loop's weak reference doesn't let it get
    # GC'd mid-flight; discarded (and its failure logged) once it completes.
    _notification_tasks: set[asyncio.Task] = set()

    def _notification_done(task: asyncio.Task) -> None:
        _notification_tasks.discard(task)
        if not task.cancelled() and (e := task.exception()) is not None:
            logger.warning("Aviso de estado a los clientes falló (%s)", describe_error(e))
            logger.debug("aviso de estado: %s", traceback_no_message(e))

    def _broadcaster(service: str) -> Callable[[ConnectionState], None]:
        def _on_state_change(state: ConnectionState) -> None:
            task = asyncio.create_task(
                client_registry.broadcast_status(service, to_wire_state(state))
            )
            _notification_tasks.add(task)
            task.add_done_callback(_notification_done)

        return _on_state_change

    openclaw.on_state_change = _broadcaster("orchestrator")
    # 5.0 PR 2b.2: tras una reconexión, cada canal relee los mensajes en cola de OpenClaw
    # (`chat.history` `pendingInputs`) y confirma o da por perdidos los suyos.
    openclaw.on_connected = channel_hub.reconnected

    # Proveedores de voz singleton (STT_PROVIDER / TTS_PROVIDER), inyectados al bridge.
    stt = build_stt_provider(settings)
    tts = build_tts_provider(settings)

    # Only the orchestrator is fatal: a provider whose start() fails degrades, it must
    # not abort startup with OpenClaw already connected and never closed (#110).
    for provider in (stt, tts):
        try:
            await provider.start()
        except Exception as e:  # noqa: BLE001 — frontera: solo OpenClaw es fatal (#110)
            logger.error("Speech provider %s failed to start (%s)", provider.name, describe_error(e))
    # Wired after start() on purpose (same reason as the orchestrator): a first-time
    # success must not broadcast a spurious "restored".
    stt.on_state_change = _broadcaster("transcriber")
    tts.on_state_change = _broadcaster("tts")
    speech = SpeechProviders(stt, tts)

    app.state.openclaw = openclaw
    app.state.openclaw_access = openclaw_access
    app.state.speech = speech
    app.state.turn_registry = turn_registry  # read by routes.py for queue depths (#219)
    app.state.client_registry = client_registry
    app.state.channel_hub = channel_hub
    events = EventHub(channel_hub, tts=lambda: speech.tts)  # /events de cada aparato (5.0, #412)
    app.state.events = events
    # El dispositivo de avisos puede escuchar por /events?audio=1 (5c): sus adaptadores cuentan.
    channel_hub.sessions_of = lambda client_id: [
        *(b.turns for b in client_registry.for_client(client_id)),
        *events.sessions_of(client_id),
    ]
    app.state.conversations = conversations
    app.state.conversation_titles = TitleCache()  # títulos de /conversations
    app.state.session_registry = SessionRegistry()
    app.state.pairing = PairingStore()  # códigos de emparejamiento, en memoria (#317)
    app.state.login_limiter = LoginLimiter()  # intentos de inicio de sesión, en memoria (M20)

    try:
        yield
    finally:
        # uvicorn closes every WebSocket (1012) and waits for its handler before this
        # runs, so each session has already closed itself (close_all) by now.
        # 1. Unhook the state-change broadcasts first (G-68): nothing closing below
        #    may schedule a new notification task.
        openclaw.on_state_change = None
        openclaw.on_connected = None
        stt.on_state_change = None
        tts.on_state_change = None
        # The devices' /events let go of their channels before those close.
        app.state.events.close_all()
        # Every step runs even if the lifespan is cancelled; failures go to DEBUG (#221).
        async with Teardown("apagado") as td:
            # 2. Speech providers, then OpenClaw.
            for provider in (speech.stt, speech.tts):
                await td.step(provider.aclose(), f"cierre del proveedor {provider.name}")
            await td.step(app.state.conversation_titles.aclose(), "títulos de conversaciones")
            # Los turnos que siguen en los canales (sin nadie adjunto) antes que OpenClaw.
            await td.step(
                channel_hub.aclose(settings.SESSION_TEARDOWN_S), "canales de conversación"
            )
            await td.step(openclaw.close(), "cierre de OpenClaw")
            # 3. Notification tasks still in flight.
            for task in list(_notification_tasks):
                await td.cancel(task, "aviso de estado")
            # 4. Dispose the DB engine last — nothing above touches the DB.
            dispose_engine()


app = FastAPI(
    title="Fogar Gateway",
    description="Puerta de entrada de Fogar en casa: aparatos, OpenClaw, voz y administración.",
    lifespan=lifespan,
    # Sin /docs, /redoc ni /openapi.json: el mapa de la API no se publica y Swagger no carga
    # scripts de un CDN en el origen del panel (auditoría 3.0, D-4).
    docs_url=None,
    redoc_url=None,
    openapi_url=None,
)



def _json_safe(value):
    """Sustituye NaN/Infinity por su texto: no son JSON válido y romperían el 422."""
    if isinstance(value, float) and not math.isfinite(value):
        return str(value)
    if isinstance(value, dict):
        return {k: _json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(v) for v in value]
    return value


@app.exception_handler(RequestValidationError)
async def _validation_error_handler(request: Request, exc: RequestValidationError):
    # Mismo cuerpo que el handler por defecto de FastAPI ({"detail": [...]}), pero un
    # `NaN`/`Infinity` en la entrada ya no convierte el 422 en 500 (#217). Sin `input`: el 422
    # nunca refleja el cuerpo (claves, códigos o megas de basura; auditoría 3.0, D-3).
    errors = [{k: v for k, v in e.items() if k != "input"} for e in exc.errors()]
    return JSONResponse(
        status_code=422,
        content=jsonable_encoder({"detail": _json_safe(errors)}),
    )


app.add_middleware(RequestIdMiddleware)
# Captures uvicorn's transport per WebSocket so a slow client can be dropped (issue #130 S2).
app.add_middleware(TransportCaptureMiddleware)
# La última que se añade es la más externa: corta el cuerpo antes de cualquier parseo (D-3).
app.add_middleware(BodySizeLimitMiddleware)

app.include_router(stream_router)  # WS /ws/stream
app.include_router(openai_router)  # HTTP /v1/*
app.include_router(health_router)  # HTTP /healthz, /ready
app.include_router(auth_router)  # HTTP /auth/* (M20)
app.include_router(system_router)  # HTTP /system/* (5.0)
app.include_router(home_router)  # HTTP /home, /home/users, /home/devices (5.0)
app.include_router(conversation_router)  # HTTP /conversations
app.include_router(events_router)  # HTTP /events (5.0, SSE por aparato)
app.include_router(dictation_router)  # WS /speech/dictation (5.0, dictado)
app.include_router(pairing_public_router)  # HTTP /.well-known/fogar-gateway, /pair
app.include_router(pairing_admin_router)  # HTTP /home/pairing
# La web va la última: su GET /{path} recoge lo que no es de la API (rutas de Vue).
app.include_router(web_router)  # HTTP / (panel)
