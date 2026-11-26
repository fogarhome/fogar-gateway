"""
admin_routes.py
~~~~~~~~~~~~~~~
/admin/* — gestión y observabilidad del gateway.

Auth (M20, api/deps.require_admin): X-Admin-Token = ADMIN_TOKEN (la CLI) o Bearer de un dueño o administrador.

Clientes:
  GET    /admin/clients              — lista clientes (sin client_key; huella key_fp)
  POST   /admin/clients              — crear cliente (devuelve client_key generada)
  GET    /admin/clients/{id}         — detalle de un cliente
  PATCH  /admin/clients/{id}         — actualizar campos (nombre, config, etc.)
  DELETE /admin/clients/{id}         — borrar cliente
  POST   /admin/clients/{id}/rotate-key — regenerar client_key

Observabilidad:
  GET    /admin/sessions             — sesiones en memoria
  GET    /admin/sessions/{id}        — detalle de sesión
  GET    /admin/orchestrator/status
  POST   /admin/orchestrator/reconnect  (202)
  GET    /admin/transcriber/status
  GET    /admin/tts/status
  GET    /admin/state                — instantánea viva (gateway con `dashboards`, servicios con el acceso
                                        a OpenClaw: permisos, token y emparejamiento; totales, sesiones)
  GET    /admin/state/stream         — la misma instantánea por SSE cada STATE_STREAM_INTERVAL_S
  GET    /admin/access               — vías para llegar al gateway, verificadas (caché 15 s)
"""

import asyncio
import json
import logging
import time
from collections import defaultdict
from datetime import UTC, datetime
from typing import Any

from fastapi import APIRouter, Depends, FastAPI, HTTPException, Request, status
from fastapi.responses import StreamingResponse
from sqlalchemy.exc import IntegrityError
from sqlmodel import Session, select

from fogar_gateway._version import __version__
from fogar_gateway.api.deps import require_admin
from fogar_gateway.db.database import get_db_session, get_engine
from fogar_gateway.db.models import ClientRecord
from fogar_gateway.models.admin_schemas import (
    ClientCreate,
    ClientResponse,
    ClientUpdate,
    ClientWithKey,
)
from fogar_gateway.services.auth import hash_key, new_key
from fogar_gateway.services.notify_device import clear_notify_device
from fogar_gateway.services.openclaw.models import OrchestratorAccess, access_texts
from fogar_gateway.services.reconnection import ServiceStatus
from fogar_gateway.services.scopes import (
    DEFAULT_SCOPES,
    HOUSEHOLD_CAP,
    SCOPE_NOT_ALLOWED,
    normalize_scopes,
    to_list,
)
from fogar_gateway.services.stats import gateway_stats

_ROTATE_KEY_ATTEMPTS = 3
STATE_STREAM_INTERVAL_S = 1.0
# Vida máxima de cada stream: por debajo del --timeout-graceful-shutdown (35 s) de la
# imagen, para que un panel abierto no bloquee el apagado; el panel reconecta.
STATE_STREAM_MAX_S = 25.0
# Flujos /admin/state/stream abiertos ahora: el dashboard abre uno al entrar en Estado, y
# `fogar-gw onboard` lo usa para confirmar que te has conectado. La CLI nunca abre ninguno.
_open_streams = 0

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/admin", dependencies=[Depends(require_admin)])


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _get_or_404(session: Session, client_id: str) -> ClientRecord:
    rec = session.get(ClientRecord, client_id)
    if rec is None or rec.retired_at is not None:
        raise HTTPException(status_code=404, detail="Client not found")
    return rec


def _check_agent_consistency(default_agent: str | None, allowed_agents: list[str] | None) -> None:
    """422 si el estado resultante deja `default_agent` fuera de `allowed_agents`.

    `allowed_agents is None` = sin restricción; `[]` = todo denegado (#105), así que
    un `default_agent` con `[]` es inconsistente.
    """
    if default_agent is None or allowed_agents is None:
        return
    if default_agent.lower() not in [a.lower() for a in allowed_agents]:  # filas de la 2.x (C-1)
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="default_agent must be one of allowed_agents",
        )


def check_household_scopes(scopes: frozenset[str]) -> None:
    """Un aparato del hogar llega como mucho a `home:read` (#410): 422 si piden más."""
    if not scopes <= HOUSEHOLD_CAP:
        raise HTTPException(status_code=422, detail=SCOPE_NOT_ALLOWED)


def _lists_to_json(data: dict[str, Any]) -> dict[str, Any]:
    """Serializa a JSON las listas que la BD guarda como texto (si vienen y no son null)."""
    for field in ("allowed_agents", "output_mode"):
        if data.get(field) is not None:
            data[field] = json.dumps(data[field])
    return data


# ---------------------------------------------------------------------------
# Client CRUD
# ---------------------------------------------------------------------------


@router.get("/clients", response_model=list[ClientResponse])
def list_clients(session: Session = Depends(get_db_session)) -> list[ClientResponse]:
    records = session.exec(select(ClientRecord).where(ClientRecord.retired_at.is_(None))).all()  # type: ignore[union-attr]
    return [ClientResponse.from_record(r) for r in records]


def create_client_record(
    session: Session,
    body: ClientCreate,
    *,
    client_id: str | None = None,
    user_id: str | None = None,
    capabilities: list[str] | None = None,
) -> ClientRecord:
    """Crea el cliente: la única vía, la usan `POST /admin/clients`, el emparejamiento (#317) y
    el inicio de sesión (M20, un dispositivo personal con `user_id`). `client_id` fija el id; si
    no, uno nuevo. Guarda solo el hash de `body.client_key` (M20): quien necesite enseñar la
    clave la pone en `body` antes."""
    _check_agent_consistency(body.default_agent, body.allowed_agents)
    scopes = normalize_scopes(body.scopes) if body.scopes is not None else DEFAULT_SCOPES
    if user_id is None:
        check_household_scopes(scopes)
    data = _lists_to_json(body.model_dump())
    data["scopes"] = json.dumps(to_list(scopes))
    data["key_hash"] = hash_key(data.pop("client_key") or new_key())
    data["user_id"] = user_id  # M20: con usuario, un dispositivo personal (un inicio de sesión)
    data["capabilities"] = json.dumps(capabilities) if capabilities is not None else None
    if client_id is not None:
        data["id"] = client_id
    record = ClientRecord(**data)
    session.add(record)
    try:
        session.commit()
    except IntegrityError:
        # client_key duplicada (única restricción alcanzable tras validar la entrada).
        # `from None` y sin loguear la excepción: su mensaje/parámetros contienen la
        # client_key completa (credencial bearer, G-27a).
        session.rollback()
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT, detail="client_key already exists"
        ) from None
    session.refresh(record)
    return record


@router.post("/clients", response_model=ClientWithKey, status_code=status.HTTP_201_CREATED)
def create_client(
    body: ClientCreate,
    session: Session = Depends(get_db_session),
) -> ClientWithKey:
    key = body.client_key or new_key()
    record = create_client_record(session, body.model_copy(update={"client_key": key}))
    return ClientWithKey.from_record(record, key)


@router.get("/clients/{client_id}", response_model=ClientResponse)
def get_client(
    client_id: str,
    session: Session = Depends(get_db_session),
) -> ClientResponse:
    return ClientResponse.from_record(_get_or_404(session, client_id))


@router.patch("/clients/{client_id}", response_model=ClientResponse)
def update_client(
    client_id: str,
    body: ClientUpdate,
    session: Session = Depends(get_db_session),
) -> ClientResponse:
    rec = _get_or_404(session, client_id)
    patch = body.model_dump(exclude_unset=True)
    # Solo si el PATCH toca alguno de los dos campos: una fila antigua inconsistente
    # que se edita por otros campos no debe fallar (solo se valida la entrada, #122).
    if "default_agent" in patch or "allowed_agents" in patch:
        _check_agent_consistency(
            patch.get("default_agent", rec.default_agent),
            patch["allowed_agents"]
            if "allowed_agents" in patch
            else (json.loads(rec.allowed_agents) if rec.allowed_agents else None),
        )
    if "scopes" in patch:
        scopes = normalize_scopes(patch["scopes"])
        # Uno del hogar, hasta su tope; uno personal solo puede perder lo que pidió (para pedir
        # más, su persona inicia sesión otra vez).
        limit = HOUSEHOLD_CAP if rec.user_id is None else normalize_scopes(
            ClientResponse.from_record(rec).scopes
        )
        if not scopes <= limit:
            raise HTTPException(status_code=422, detail=SCOPE_NOT_ALLOWED)
        patch["scopes"] = json.dumps(to_list(scopes))
    for field, value in _lists_to_json(patch).items():
        setattr(rec, field, value)
    session.add(rec)
    session.commit()
    session.refresh(rec)
    return ClientResponse.from_record(rec)


def retire_client(session: Session, rec: ClientRecord) -> None:
    """Retirar, no borrar (M20): sus conversaciones conservan hogar y dispositivo. La clave pasa
    a un valor que nadie conoce y deja de ser dispositivo de avisos donde estuviera elegido
    (#407). Sus sesiones vivas las corta `close_client_sessions`."""
    clear_notify_device(session, rec.id)
    rec.retired_at = datetime.now(UTC)
    rec.is_active = False
    rec.key_hash = hash_key(new_key())  # nadie conoce la clave: ya no vale
    session.add(rec)
    session.commit()


def close_client_sessions(app: FastAPI, client_id: str) -> None:
    """Cierra ya (4200) las sesiones vivas de un cliente, sin esperar a la revalidación."""
    for bridge in app.state.client_registry.for_client(client_id):
        bridge.revoke()


def _retire_client_record(client_id: str) -> None:
    with Session(get_engine()) as session:
        retire_client(session, _get_or_404(session, client_id))


@router.delete("/clients/{client_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_client(client_id: str, request: Request) -> None:
    await asyncio.to_thread(_retire_client_record, client_id)
    close_client_sessions(request.app, client_id)


@router.post("/clients/{client_id}/rotate-key", response_model=ClientWithKey)
def rotate_client_key(
    client_id: str,
    session: Session = Depends(get_db_session),
) -> ClientWithKey:
    rec = _get_or_404(session, client_id)
    for _ in range(_ROTATE_KEY_ATTEMPTS):
        key = new_key()
        rec.key_hash = hash_key(key)
        session.add(rec)
        try:
            session.commit()
            break
        except IntegrityError:
            # Colisión de client_key: reintento con otra. Sin loguear ni encadenar la
            # excepción (su mensaje lleva la clave, G-27a).
            session.rollback()
    else:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT, detail="could not generate a unique client_key"
        ) from None
    session.refresh(rec)
    return ClientWithKey.from_record(rec, key)


# ---------------------------------------------------------------------------
# Sessions — observabilidad en memoria
# ---------------------------------------------------------------------------


def _serialize_events(events) -> list[dict]:
    return [
        {
            "stage": e.stage,
            "ts_ms": round(e.ts_ms),
            "elapsed_ms": round(e.elapsed_ms),
            "turn": e.turn,  # la web agrupa las etapas por turno (M18)
            "meta": e.meta,
        }
        for e in events
    ]


def _session_summary(record) -> dict:
    t = record.tracker
    return {
        "session_id": record.session_id,
        "client_id": record.client_id,
        "status": record.status,
        "input_mode": record.input_mode,
        "output_mode": record.output_mode,
        "started_at": record.started_at.isoformat(),
        "ended_at": record.ended_at.isoformat() if record.ended_at else None,
        "turn_count": t.turn_count,
        "last_latencies": {
            "llm_first_token_ms": t.llm_first_token_ms(),
            "tts_first_chunk_ms": t.tts_first_chunk_ms(),
            "turn_e2e_ms": t.turn_e2e_ms(),
        },
    }


def _session_detail(record) -> dict:
    t = record.tracker
    events = t.events

    end_event = next((e for e in reversed(events) if e.stage == "session_end"), None)
    duration_s = end_event.meta.get("duration_s") if end_event else None

    by_turn: dict[int, dict] = defaultdict(dict)
    for e in events:
        by_turn[e.turn][e.stage] = e

    llm_latencies = [
        round(turn_events["llm_first_token"].ts_ms - turn_events["llm_start"].ts_ms, 1)
        for turn_events in by_turn.values()
        if "llm_start" in turn_events and "llm_first_token" in turn_events
    ]
    avg_llm_first_token_ms = (
        round(sum(llm_latencies) / len(llm_latencies), 1) if llm_latencies else None
    )

    e2e_latencies = list(t.turn_e2e_latencies().values())
    avg_turn_e2e_ms = round(sum(e2e_latencies) / len(e2e_latencies), 1) if e2e_latencies else None

    return {
        **_session_summary(record),
        "summary": {
            "turn_count": t.turn_count,
            "duration_s": duration_s,
            "avg_llm_first_token_ms": avg_llm_first_token_ms,
            "avg_turn_e2e_ms": avg_turn_e2e_ms,
        },
        "events": _serialize_events(events),
    }


@router.get("/sessions")
async def list_sessions(request: Request) -> dict:
    registry = request.app.state.session_registry
    sessions = registry.get_all()
    active = sum(1 for s in sessions if s.status == "active")
    return {
        "active": active,
        "total": len(sessions),
        "sessions": [_session_summary(s) for s in sessions],
    }


@router.get("/sessions/{session_id}")
async def get_session(session_id: str, request: Request) -> dict:
    registry = request.app.state.session_registry
    record = registry.get(session_id)
    if record is None:
        raise HTTPException(status_code=404, detail="Session not found")
    return _session_detail(record)


# ---------------------------------------------------------------------------
# Orchestrators — estado y control
# ---------------------------------------------------------------------------


@router.get("/orchestrator/status")
async def get_orchestrator_status(request: Request) -> dict:
    s = request.app.state.openclaw.status()
    return {
        "name": s.name,
        "state": s.state.value,
        "connected_at": s.connected_at.isoformat() if s.connected_at else None,
        "reconnect_attempts": s.reconnect_attempts,
        "last_error": s.last_error,
    }


@router.post("/orchestrator/reconnect", status_code=202)
async def post_orchestrator_reconnect(request: Request) -> dict:
    """Retry now: cuts the reconnect loop's wait short (#272). Does not block on the handshake.

    `{"accepted": false}` when nothing was done: OpenClaw is already CONNECTED (reconnecting
    would tear down a healthy socket and fail in-flight turns) or the gateway is shutting down."""
    return {"accepted": request.app.state.openclaw.trigger_reconnect()}


# ---------------------------------------------------------------------------
# Transcriber / TTS — estado (solo lectura; ver docstring del módulo)
# ---------------------------------------------------------------------------


@router.get("/transcriber/status")
async def get_transcriber_status(request: Request) -> dict:
    """Transcriber has no process-level connection (one instance per audio
    session) — this reports live readiness via the STT provider's probe, wrapped
    in the same shape as the other two status endpoints for consistency."""
    stt = request.app.state.speech.stt
    await stt.probe()
    s = stt.status()
    body = {
        "name": s.name,
        "provider": stt.name,
        "state": s.state.value,
        "connected_at": s.connected_at.isoformat() if s.connected_at else None,
        "reconnect_attempts": s.reconnect_attempts,
        "last_error": s.last_error,
    }
    counters = getattr(stt, "counters", None)  # diagnóstico opcional (ElevenLabs)
    if callable(counters):
        body["counters"] = counters()
    return body


@router.get("/tts/status")
async def get_tts_status(request: Request) -> dict:
    tts = request.app.state.speech.tts
    s = tts.status()
    body = {
        "name": s.name,
        "provider": tts.name,
        "state": s.state.value,
        "connected_at": s.connected_at.isoformat() if s.connected_at else None,
        "reconnect_attempts": s.reconnect_attempts,
        "last_error": s.last_error,
    }
    counters = getattr(tts, "counters", None)  # diagnóstico opcional (ElevenLabs)
    if callable(counters):
        body["counters"] = counters()
    return body


# ---------------------------------------------------------------------------
# Proveedores de voz — estado (se eligen al arrancar con STT_PROVIDER/TTS_PROVIDER)
# ---------------------------------------------------------------------------


@router.get("/speech")
async def get_speech(request: Request) -> dict:
    speech = request.app.state.speech
    return {
        "stt": {"provider": speech.stt.name, "state": speech.stt.state.value},
        "tts": {"provider": speech.tts.name, "state": speech.tts.state.value},
        # La puerta solo existe en el STT de ElevenLabs: con fogar es siempre "off".
        "audio_gate": getattr(speech.stt, "audio_gate", "off"),
    }


# ---------------------------------------------------------------------------
# Estado vivo (#219) — barato cada segundo: estado en memoria, sin probe()
# ---------------------------------------------------------------------------


def _service_state(s: ServiceStatus) -> dict[str, Any]:
    return {"state": s.state.value, "last_error": s.last_error}


def _build_state(app: FastAPI) -> dict[str, Any]:
    speech = app.state.speech
    access = getattr(app.state, "openclaw_access", OrchestratorAccess()).as_dict()
    sessions = []
    for bridge in app.state.client_registry.bridges():
        try:
            row = bridge.snapshot()
            record = app.state.session_registry.get(row["session_id"])
            if record is not None:
                summary = _session_summary(record)
                row["started_at"] = summary["started_at"]
                row["last_latencies"] = summary["last_latencies"]
        except Exception as e:  # noqa: BLE001 — una sesión rota no tumba la instantánea entera
            logger.debug("snapshot de sesión omitido: %s", type(e).__name__)
            continue
        sessions.append(row)
    return {
        "gateway": {
            "started_at": gateway_stats.started_at.isoformat(),
            "uptime_s": round(time.monotonic() - gateway_stats.started_monotonic, 3),
            "version": __version__,
            "dashboards": _open_streams,
        },
        "services": {
            "orchestrator": {
                **_service_state(app.state.openclaw.status()),
                # Permisos concedidos, admin, token en uso y emparejamiento (M19); sin secretos.
                **access,
                **access_texts(access),
            },
            "stt": {"provider": speech.stt.name, **_service_state(speech.stt.status())},
            "tts": {"provider": speech.tts.name, **_service_state(speech.tts.status())},
        },
        "totals": {**gateway_stats.snapshot(), "sessions_active": len(sessions)},
        "sessions": sessions,
    }


@router.get("/state")
async def get_state(request: Request) -> dict:
    return _build_state(request.app)


@router.get("/state/stream")
async def get_state_stream(request: Request) -> StreamingResponse:
    async def state_stream():
        global _open_streams
        _open_streams += 1
        try:
            deadline = time.monotonic() + STATE_STREAM_MAX_S
            while time.monotonic() < deadline and not await request.is_disconnected():
                yield f"data: {json.dumps(_build_state(request.app))}\n\n"
                await asyncio.sleep(STATE_STREAM_INTERVAL_S)
        finally:
            _open_streams -= 1

    return StreamingResponse(
        state_stream(),
        media_type="text/event-stream",
        # D-r6-5: que un proxy (nginx) no retenga los eventos.
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


_ACCESS_TTL_S = 15.0
_access_cache: tuple[float, dict] | None = None  # (monotonic, respuesta)


@router.get("/access")
async def get_access() -> dict:
    """Vías de acceso verificadas; `collect` abre sockets y subprocesos, va a un hilo."""
    global _access_cache
    from starlette.concurrency import run_in_threadpool

    from fogar_gateway import access
    from fogar_gateway.api.pairing_routes import (
        INSTANCE_ID,  # import tardío: ese módulo importa este
    )
    from fogar_gateway.cli.serve import PORT
    from fogar_gateway.core.config import settings

    now = time.monotonic()
    if _access_cache and now - _access_cache[0] < _ACCESS_TTL_S:
        return _access_cache[1]
    routes = await run_in_threadpool(
        access.collect, settings.GATEWAY_HOST, PORT, access.configured_public_urls(), INSTANCE_ID
    )
    body = {"docker": access.in_docker(), "routes": [r.as_dict() for r in routes]}
    _access_cache = (now, body)
    return body
