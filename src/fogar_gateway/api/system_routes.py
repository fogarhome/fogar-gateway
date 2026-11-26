"""
system_routes.py
~~~~~~~~~~~~~~~~
El sistema (5.0, #411; antes en `/admin`): estado en vivo, sesiones, servicios, voz y vías de
acceso. Leer exige `home:read`; reconectar OpenClaw, `home:admin`.

  GET    /system/state               — instantánea viva (gateway con `dashboards`, servicios con el
                                       acceso a OpenClaw: permisos, token y emparejamiento; totales,
                                       sesiones)
  GET    /system/state/stream        — la misma instantánea por SSE cada STATE_STREAM_INTERVAL_S
  GET    /system/sessions[/{id}]     — sesiones en memoria
  GET    /system/services/orchestrator | transcriber | tts — estado de cada servicio
  POST   /system/services/orchestrator/reconnect (202)
  GET    /system/speech              — proveedores de voz
  GET    /system/access              — vías para llegar al gateway, verificadas (caché 15 s)
"""

import asyncio
import json
import logging
import time
from collections import defaultdict
from typing import Any

from fastapi import APIRouter, Depends, FastAPI, HTTPException, Request
from fastapi.responses import StreamingResponse

from fogar_gateway._version import __version__
from fogar_gateway.api.deps import require_admin, require_scope
from fogar_gateway.services.openclaw.models import OrchestratorAccess, access_texts
from fogar_gateway.services.reconnection import ServiceStatus
from fogar_gateway.services.scopes import (
    HOME_READ,
)
from fogar_gateway.services.stats import gateway_stats

STATE_STREAM_INTERVAL_S = 1.0
# Vida máxima de cada stream: por debajo del --timeout-graceful-shutdown (35 s) de la
# imagen, para que un panel abierto no bloquee el apagado; el panel reconecta.
STATE_STREAM_MAX_S = 25.0
# Flujos /system/state/stream abiertos ahora: el dashboard abre uno al entrar en Estado, y
# `fogar-gw onboard` lo usa para confirmar que te has conectado. La CLI nunca abre ninguno.
_open_streams = 0

logger = logging.getLogger(__name__)

# Leer es `home:read`; reconectar OpenClaw, `home:admin` (5.0, #411).
router = APIRouter(prefix="/system", dependencies=[Depends(require_scope(HOME_READ))])


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


@router.get("/services/orchestrator")
async def get_orchestrator_status(request: Request) -> dict:
    s = request.app.state.openclaw.status()
    return {
        "name": s.name,
        "state": s.state.value,
        "connected_at": s.connected_at.isoformat() if s.connected_at else None,
        "reconnect_attempts": s.reconnect_attempts,
        "last_error": s.last_error,
    }


@router.post(
    "/services/orchestrator/reconnect", status_code=202, dependencies=[Depends(require_admin)]
)
async def post_orchestrator_reconnect(request: Request) -> dict:
    """Retry now: cuts the reconnect loop's wait short (#272). Does not block on the handshake.

    `{"accepted": false}` when nothing was done: OpenClaw is already CONNECTED (reconnecting
    would tear down a healthy socket and fail in-flight turns) or the gateway is shutting down."""
    return {"accepted": request.app.state.openclaw.trigger_reconnect()}


# ---------------------------------------------------------------------------
# Transcriber / TTS — estado (solo lectura; ver docstring del módulo)
# ---------------------------------------------------------------------------


@router.get("/services/transcriber")
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


@router.get("/services/tts")
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
