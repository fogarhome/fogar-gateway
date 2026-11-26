"""Integration tests for /system/services/transcriber and /system/services/tts."""

from unittest.mock import patch


def test_get_transcriber_status_reachable(client, admin_headers):
    with patch("fogar_gateway.services.transcriber_client.TranscriberClient.ping", return_value=True):
        response = client.get("/system/services/transcriber", headers=admin_headers)
    assert response.status_code == 200
    data = response.json()
    assert data["name"] == "transcriber"
    assert data["state"] == "CONNECTED"


def test_get_transcriber_status_unreachable(client, admin_headers):
    with patch("fogar_gateway.services.transcriber_client.TranscriberClient.ping", return_value=False):
        response = client.get("/system/services/transcriber", headers=admin_headers)
    assert response.status_code == 200
    assert response.json()["state"] == "DEGRADED"


def test_get_transcriber_status_requires_admin_token(client):
    response = client.get("/system/services/transcriber")
    assert response.status_code == 401


def test_get_tts_status_default_connected(client, admin_headers):
    response = client.get("/system/services/tts", headers=admin_headers)
    assert response.status_code == 200
    data = response.json()
    assert data["name"] == "tts"
    assert data["state"] == "CONNECTED"
    assert data["reconnect_attempts"] == 0


def test_get_tts_status_requires_admin_token(client):
    response = client.get("/system/services/tts")
    assert response.status_code == 401
