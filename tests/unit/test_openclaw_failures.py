"""#397: un fallo de OpenClaw se clasifica en un código estable leyendo solo campos
estructurales (docs/openclaw-protocol.md, «Failure fields over the WebSocket»). Los payloads
se construyen a mano con esa forma; ningún texto real."""

import pytest

from fogar_gateway.services.openclaw.failures import (
    ORCHESTRATOR_AUTH_FAILED_ERROR,
    ORCHESTRATOR_ERROR,
    ORCHESTRATOR_MODEL_UNAVAILABLE_ERROR,
    ORCHESTRATOR_RATE_LIMITED_ERROR,
    ORCHESTRATOR_TIMEOUT_ERROR,
    classify_failure,
    fallback_reason,
)

TEXT = "SECRET-INTERNAL"  # el texto libre nunca decide nada


def chat_error(**fields):
    return {"runId": "r1", "sessionKey": "sk", "seq": 3, "state": "error", "errorMessage": TEXT, **fields}


@pytest.mark.parametrize(
    "payload",
    [
        chat_error(errorDetail={"failoverReason": "auth"}),
        chat_error(errorDetail={"failoverReason": "auth_permanent"}),
        chat_error(errorDetail={"failoverReason": "session_expired"}),
        chat_error(errorDetail={"providerRuntimeFailureKind": "auth_invalid_token"}),
        chat_error(errorDetail={"providerRuntimeFailureKind": "auth_refresh"}),
        chat_error(errorDetail={"providerRuntimeFailureKind": "auth_scope"}),
        chat_error(errorDetail={"httpStatus": 401, "providerErrorMessagePreview": TEXT}),
        chat_error(errorDetail={"httpStatus": 403}),
        {"phase": "error", "error": TEXT, "errorObservation": {"failoverReason": "auth"}},
    ],
)
def test_auth_failed(payload):
    assert classify_failure(payload) == ORCHESTRATOR_AUTH_FAILED_ERROR


def test_model_unavailable():
    assert classify_failure(chat_error(errorDetail={"failoverReason": "model_not_found"})) == (
        ORCHESTRATOR_MODEL_UNAVAILABLE_ERROR
    )


@pytest.mark.parametrize(
    "payload",
    [
        chat_error(errorKind="rate_limit"),
        chat_error(errorDetail={"failoverReason": "rate_limit"}),
        chat_error(errorDetail={"failoverReason": "overloaded"}),
        chat_error(errorDetail={"failoverReason": "billing"}),
        chat_error(errorDetail={"httpStatus": 429}),
        chat_error(errorDetail={"httpStatus": 402}),
    ],
)
def test_rate_limited(payload):
    assert classify_failure(payload) == ORCHESTRATOR_RATE_LIMITED_ERROR


@pytest.mark.parametrize(
    "payload",
    [
        chat_error(errorKind="timeout"),  # visto en vivo, sin errorDetail
        chat_error(errorDetail={"failoverReason": "timeout"}),
        chat_error(errorDetail={"providerRuntimeFailureKind": "timeout"}),
        {"phase": "error", "error": TEXT, "timeoutPhase": "provider"},
    ],
)
def test_timeout(payload):
    assert classify_failure(payload) == ORCHESTRATOR_TIMEOUT_ERROR


@pytest.mark.parametrize(
    "payload",
    [
        {},
        chat_error(),
        chat_error(errorKind="refusal"),
        chat_error(errorKind="context_length"),
        chat_error(errorKind="state_contention"),
        chat_error(errorKind="unknown"),
        chat_error(errorKind="nuevo", errorDetail={"failoverReason": "nuevo", "httpStatus": 500}),
        chat_error(errorKind=["timeout"], errorDetail={"failoverReason": ["auth"], "httpStatus": True}),
        chat_error(errorDetail="auth"),
        # un `res ok:false` sin más estructura (p. ej. agente desconocido): sigue genérico
        {"type": "res", "ok": False, "error": {"code": "INVALID_REQUEST", "message": TEXT}},
    ],
)
def test_unknown_failure_is_orchestrator_error(payload):
    assert classify_failure(payload) == ORCHESTRATOR_ERROR


def test_texts_never_decide():
    assert classify_failure(chat_error(errorMessage="401 unauthorized rate_limit timeout")) == ORCHESTRATOR_ERROR


def test_a_remembered_fallback_reason_fills_in_a_bare_error():
    """2026-11-01: el fallo sin credenciales trajo `fallback_step` y un error sin errorObservation."""
    assert classify_failure(chat_error(), fallback_reason="auth") == ORCHESTRATOR_AUTH_FAILED_ERROR
    assert classify_failure(chat_error(), fallback_reason="model_not_found") == (
        ORCHESTRATOR_MODEL_UNAVAILABLE_ERROR
    )


def test_the_final_errors_own_signal_wins_over_the_remembered_one():
    payload = chat_error(errorKind="timeout")
    assert classify_failure(payload, fallback_reason="auth") == ORCHESTRATOR_TIMEOUT_ERROR


def test_fallback_reason_tracks_the_run():
    step = {"phase": "fallback_step", "fallbackStepFromModel": "m", "fallbackStepFinalOutcome": "next_fallback"}
    assert fallback_reason({**step, "fallbackStepFromFailureReason": "auth"}, None) == "auth"
    assert fallback_reason({**step, "fallbackStepFromFailureDetail": TEXT}, "auth") == "auth"
    assert fallback_reason({**step, "fallbackStepFromFailureReason": ["x"]}, "auth") == "auth"
    # un paso que se recuperó olvida el fallo anterior
    ok = {**step, "fallbackStepFinalOutcome": "succeeded", "fallbackStepFromFailureReason": "auth"}
    assert fallback_reason(ok, "auth") is None
