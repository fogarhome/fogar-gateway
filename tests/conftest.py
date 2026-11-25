"""Global test fixtures shared across unit and integration tests."""

import os
import tempfile

# Antes del primer import de fogar_gateway: Settings lee <base>/config.env al importarse, y la
# suite no debe leer ni crear nada en el ~/.fogar-gateway real (M14c). Los subprocesos lo heredan.
os.environ["FOGAR_GATEWAY_HOME"] = tempfile.mkdtemp(prefix="fogar-gw-tests-")

import pytest
from starlette.testclient import TestClient

from fogar_gateway.main import app


@pytest.fixture(autouse=True)
def _disable_single_process_lock(monkeypatch):
    """Los tests crean varias apps en un mismo proceso y flock entra en conflicto con otro
    open() del mismo fichero aunque sea del mismo proceso (M6f, #218). Además evita crear
    data/gateway.lock en el checkout. Los tests del guard lo reactivan explícitamente."""
    from fogar_gateway.core.config import settings

    monkeypatch.setattr(settings, "SINGLE_PROCESS_LOCK", False)
    # Hermético: una clave real en el entorno del desarrollador no debe filtrarse a los tests.
    monkeypatch.setattr(settings, "ELEVENLABS_API_KEY", None)


@pytest.fixture(autouse=True)
def _no_push_reply_wait(monkeypatch):
    """Un push sin texto se cierra en el acto, como antes: la espera de su respuesta en un
    `session.message` (cron, PUSH_REPLY_GRACE_S) solo la prueban los tests que la piden."""
    monkeypatch.setattr("fogar_gateway.services.channel.PUSH_REPLY_GRACE_S", 0.0)
    # Igual con la ventana en la que OpenClaw restaura su cola tras reiniciarse: con 0, lo que
    # `pendingInputs` no lista al reconectar falla en el acto (RECOVERY_WINDOW_S).
    monkeypatch.setattr("fogar_gateway.services.channel.RECOVERY_WINDOW_S", 0.0)


@pytest.fixture(autouse=True)
def _reset_gateway_stats():
    """Los contadores globales (#219) no deben filtrarse entre tests."""
    from fogar_gateway.services.stats import gateway_stats

    gateway_stats.reset()
    yield
    gateway_stats.reset()


@pytest.fixture
def test_client():
    """FastAPI TestClient con lifespan (conecta/cierra db_client)."""
    with TestClient(app) as client:
        yield client
