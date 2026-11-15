"""Auditoría 3.0, lote red: tope de cuerpo (D-3), 422 sin `input`, sin /docs (D-4) y /v1 sin
exención de origen para peticiones de navegador (D-2)."""

import pytest

from fogar_gateway.core.body_limit import MAX_BODY_BYTES
from tests.integration.conftest import ADMIN_TOKEN

ADMIN = {"x-admin-token": ADMIN_TOKEN}
TOO_BIG = b'{"name": "' + b"x" * MAX_BODY_BYTES + b'"}'


def _chunks():
    for i in range(0, len(TOO_BIG), 64 * 1024):
        yield TOO_BIG[i : i + 64 * 1024]


@pytest.mark.parametrize("path", ["/admin/clients", "/pair", "/v1/chat/completions"])
def test_chunked_body_over_cap_is_413_before_auth_or_parsing(client_untrusted, path):
    # Sin Content-Length (chunked) y sin credenciales: 413, no 422 ni 401.
    r = client_untrusted.post(path, content=_chunks(), headers={"content-type": "application/json"})
    assert r.status_code == 413
    assert r.json() == {"detail": "request body too large"}


def test_declared_length_over_cap_is_413(client_untrusted):
    r = client_untrusted.post("/admin/clients", content=TOO_BIG, headers=ADMIN)
    assert r.status_code == 413


def test_body_at_cap_reaches_the_route(client):
    body = b'{"name": "' + b"x" * (MAX_BODY_BYTES - 12) + b'"}'
    assert len(body) == MAX_BODY_BYTES
    r = client.post("/admin/clients", content=body, headers={**ADMIN, "content-type": "application/json"})
    assert r.status_code == 422  # name > 200 caracteres: lo valida la ruta, no el tope


def test_422_never_echoes_the_body(client):
    r = client.post("/pair", json={"code": "s3cr3t" * 10})
    assert r.status_code == 422
    assert "s3cr3t" not in r.text
    assert all("input" not in err for err in r.json()["detail"])


@pytest.mark.parametrize("path", ["/docs", "/redoc", "/openapi.json"])
def test_api_docs_are_not_published(client, path):
    assert client.get(path).status_code == 404


@pytest.mark.parametrize(
    "headers",
    [{"origin": "http://evil.example"}, {"sec-fetch-site": "cross-site"}, {"sec-fetch-mode": "cors"}],
)
def test_browser_request_never_gets_trusted_origin_exemption(client, headers):
    # DNS rebinding: la página llega desde un origen de confianza pero es un navegador.
    assert client.get("/v1/models").status_code == 200
    assert client.get("/v1/models", headers=headers).status_code == 401


def test_browser_request_with_bearer_still_works(client, ha_bearer_headers):
    r = client.get("/v1/models", headers={**ha_bearer_headers, "origin": "http://x"})
    assert r.status_code == 200
