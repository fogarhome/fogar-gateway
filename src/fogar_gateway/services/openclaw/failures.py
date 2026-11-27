"""Un fallo de OpenClaw → un código estable para el cliente (#397). Stdlib, sin I/O.

Solo lee campos estructurales (docs/openclaw-protocol.md, «Failure fields over the WebSocket
(#397)»): `errorKind`, `errorDetail`/`errorObservation` (`failoverReason`,
`providerRuntimeFailureKind`, `httpStatus`), `timeoutPhase` y el `fallbackStepFromFailureReason`
de un `fallback_step` anterior del mismo run. Nunca `errorMessage`, `message`, `error`,
`providerErrorMessagePreview` ni `fallbackStepFromFailureDetail`: no deciden nada ni se
reenvían ni se registran. Lo desconocido o ausente es `orchestrator_error`.

El agente no disponible (`orchestrator_agent_unavailable`) no sale de aquí: lo decide la
política del propio gateway antes del `chat.send` (services/turns.py)."""

from collections.abc import Mapping
from typing import Any

ORCHESTRATOR_ERROR = "orchestrator_error"  # OpenClaw rejected the turn, or an internal failure
ORCHESTRATOR_AUTH_FAILED_ERROR = "orchestrator_auth_failed"
ORCHESTRATOR_MODEL_UNAVAILABLE_ERROR = "orchestrator_model_unavailable"
ORCHESTRATOR_RATE_LIMITED_ERROR = "orchestrator_rate_limited"
ORCHESTRATOR_TIMEOUT_ERROR = "orchestrator_timeout"
ORCHESTRATOR_AGENT_UNAVAILABLE_ERROR = "orchestrator_agent_unavailable"

_AUTH = ORCHESTRATOR_AUTH_FAILED_ERROR
_RATE = ORCHESTRATOR_RATE_LIMITED_ERROR
_TIMEOUT = ORCHESTRATOR_TIMEOUT_ERROR

# `failoverReason` (también `fallbackStepFromFailureReason`, que es una failover reason).
_BY_FAILOVER_REASON = {
    "auth": _AUTH,
    "auth_permanent": _AUTH,
    "session_expired": _AUTH,
    "model_not_found": ORCHESTRATOR_MODEL_UNAVAILABLE_ERROR,
    "rate_limit": _RATE,
    "overloaded": _RATE,
    "billing": _RATE,
    "timeout": _TIMEOUT,
}
_BY_RUNTIME_KIND = {
    "auth_invalid_token": _AUTH,
    "auth_refresh": _AUTH,
    "auth_scope": _AUTH,
    "timeout": _TIMEOUT,
}
_BY_HTTP_STATUS = {401: _AUTH, 403: _AUTH, 402: _RATE, 429: _RATE}
_BY_ERROR_KIND = {"rate_limit": _RATE, "timeout": _TIMEOUT}


def _lookup(table: Mapping[Any, str], value: object) -> str | None:
    # Solo str o int (no bool): un valor de otro tipo (lista, dict) no es una clave válida.
    if isinstance(value, str) or (isinstance(value, int) and not isinstance(value, bool)):
        return table.get(value)
    return None


def classify_failure(payload: Mapping[str, Any], *, fallback_reason: str | None = None) -> str:
    """El código estable de un fallo: un `chat` `state:"error"`, el `data` de un `agent`
    `phase:"error"` o un `res` `ok:false`. `fallback_reason`: la del último `fallback_step` del
    mismo run, que solo cuenta si el error final no trae señal propia (2026-11-01: el fallo sin
    credenciales llegó con un `fallback_step` y un error sin `errorObservation`)."""
    detail = payload.get("errorDetail", payload.get("errorObservation"))
    if not isinstance(detail, Mapping):
        detail = {}
    timeout_phase = payload.get("timeoutPhase")
    return (
        _lookup(_BY_FAILOVER_REASON, detail.get("failoverReason"))
        or _lookup(_BY_RUNTIME_KIND, detail.get("providerRuntimeFailureKind"))
        or _lookup(_BY_HTTP_STATUS, detail.get("httpStatus"))
        or _lookup(_BY_ERROR_KIND, payload.get("errorKind"))
        or (_TIMEOUT if isinstance(timeout_phase, str) and timeout_phase else None)
        or _lookup(_BY_FAILOVER_REASON, fallback_reason)
        or ORCHESTRATOR_ERROR
    )


def fallback_reason(data: Mapping[str, Any], previous: str | None) -> str | None:
    """La razón de fallo que deja un `fallback_step` para el resto de su run. Un paso que se
    recuperó (`succeeded`) la olvida; uno sin razón utilizable deja la anterior."""
    if data.get("fallbackStepFinalOutcome") == "succeeded":
        return None
    reason = data.get("fallbackStepFromFailureReason")
    return reason if isinstance(reason, str) else previous
