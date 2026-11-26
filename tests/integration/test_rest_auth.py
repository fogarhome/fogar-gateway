"""Tests para validación de admin token (deps.py → get_admin_auth)."""


def test_missing_credentials_returns_401(client):
    """Sin X-Admin-Token ni Bearer → 401 (M20: dos formas de entrar, ninguna obligatoria)."""
    r = client.get("/system/services/orchestrator")
    assert r.status_code == 401 and r.json()["detail"] == "not_authenticated"


def test_invalid_admin_token_returns_401(client):
    """Token inválido → 401."""
    r = client.get("/system/services/orchestrator", headers={"x-admin-token": "wrong-token"})
    assert r.status_code == 401


def test_valid_admin_token_passes(client, admin_headers):
    """Token válido → request pasa."""
    r = client.get("/system/services/orchestrator", headers=admin_headers)
    assert r.status_code == 200
