"""Permisos por aparato (5.0, #410): qué se concede, el tope de cada aparato y dónde se exige."""

import json

import pytest
from sqlmodel import Session
from starlette.websockets import WebSocketDisconnect

from fogar_gateway.db.models import ClientRecord, MembershipRecord
from tests.integration.conftest import CLIENT_ID, VALID_KEY
from tests.integration.test_auth import _add_user, _bearer, _http

ADMIN = ["conversations:write", "home:admin"]
CONVERSE = ["conversations:read", "conversations:write"]
EVERYTHING = ["conversations:read", "conversations:write", "home:admin", "home:read"]
HANDSHAKE = {"token": VALID_KEY, "protocol": 5, "input_mode": "text", "output_mode": ["text"]}


@pytest.fixture
def users(db_engine):
    return {"sito": _add_user(db_engine, "sito", "owner"), "ana": _add_user(db_engine, "ana")}


@pytest.fixture
def http(mock_services, mock_registry, monkeypatch, users):
    with _http(mock_registry, monkeypatch) as c:
        yield c


def _login(http, username="sito", **extra):
    from tests.integration.test_auth import PASSWORD

    body = {"username": username, "password": PASSWORD, "device_name": "d", **extra}
    r = http.post("/auth/login", json=body)
    assert r.status_code == 201, r.text
    return r.json()


def _set_scopes(db_engine, client_id: str, scopes: list[str]) -> None:
    with Session(db_engine) as s:
        rec = s.get(ClientRecord, client_id)
        rec.scopes = json.dumps(scopes)
        s.add(rec)
        s.commit()


# --- Lo que se concede al iniciar sesión ---


def test_login_without_scopes_grants_only_conversations(http):
    body = _login(http)
    assert body["scopes"] == CONVERSE
    r = http.get("/admin/clients", headers=_bearer(body["token"]))
    assert r.status_code == 403 and r.json()["detail"] == "missing_scope"
    assert http.get("/auth/me", headers=_bearer(body["token"])).json()["scopes"] == CONVERSE


def test_login_grants_what_is_asked_within_the_role(http):
    owner = _login(http, scopes=ADMIN)
    assert owner["scopes"] == EVERYTHING
    assert http.get("/admin/clients", headers=_bearer(owner["token"])).status_code == 200
    member = _login(http, "ana", scopes=ADMIN)  # pide de más: se recorta al rol
    assert member["scopes"] == CONVERSE


def test_unknown_scope_is_422(http):
    r = http.post("/auth/login", json={
        "username": "sito", "password": "x", "device_name": "d", "scopes": ["home:root"],
    })
    assert r.status_code == 422


def test_demoting_the_person_cuts_her_devices_at_once(http, db_engine, users):
    token = _login(http, scopes=ADMIN)["token"]
    with Session(db_engine) as s:
        m = s.get(MembershipRecord, (users["sito"], "home"))
        m.role = "member"
        s.add(m)
        s.commit()
    assert http.get("/admin/clients", headers=_bearer(token)).status_code == 403
    assert http.get("/auth/me", headers=_bearer(token)).json()["scopes"] == CONVERSE
    # Lo concedido sigue guardado: si vuelve a ser administradora, lo recupera sin entrar otra vez.
    with Session(db_engine) as s:
        m = s.get(MembershipRecord, (users["sito"], "home"))
        m.role = "admin"
        s.add(m)
        s.commit()
    assert http.get("/admin/clients", headers=_bearer(token)).status_code == 200


# --- Aparatos del hogar ---


def test_household_device_never_gets_home_admin(http, admin_headers):
    r = http.post("/admin/clients", json={"name": "Tele", "scopes": ["home:admin"]},
                  headers=admin_headers)
    assert r.status_code == 422 and r.json()["detail"] == "scope_not_allowed"
    r = http.post("/admin/clients", json={"name": "Tele"}, headers=admin_headers)
    assert r.status_code == 201 and r.json()["scopes"] == CONVERSE
    r = http.patch(f"/admin/clients/{r.json()['id']}", json={"scopes": ["home:admin"]},
                   headers=admin_headers)
    assert r.status_code == 422


def test_household_device_with_read_lists_but_cannot_create(http, admin_headers):
    created = http.post("/admin/clients", json={"name": "Tablet", "scopes": ["conversations:read"]},
                        headers=admin_headers).json()
    tablet = _bearer(created["client_key"])
    assert http.get("/admin/conversations", headers=tablet).status_code == 200
    r = http.post("/admin/conversations", json={}, headers=tablet)
    assert r.status_code == 403
    assert 'scope="conversations:write"' in r.headers["www-authenticate"]
    assert http.get("/auth/me", headers=tablet).status_code == 401  # /auth es de personas


def test_admin_can_only_take_scopes_from_a_personal_device(http, admin_headers):
    device = _login(http)  # conversations:*
    path = f"/admin/clients/{device['client_id']}"
    r = http.patch(path, json={"scopes": ["conversations:write", "home:read"]}, headers=admin_headers)
    assert r.status_code == 422  # para pedir más, su persona inicia sesión otra vez
    r = http.patch(path, json={"scopes": ["conversations:read"]}, headers=admin_headers)
    assert r.status_code == 200 and r.json()["scopes"] == ["conversations:read"]


# --- Emparejamiento ---


def test_pair_grants_asked_within_the_template(http, admin_headers):
    r = http.post("/admin/pairing", json={"scopes": ["home:admin"]}, headers=admin_headers)
    assert r.status_code == 422
    code = http.post("/admin/pairing", json={"scopes": ["conversations:write", "home:read"]},
                     headers=admin_headers).json()["code"]
    body = http.post("/pair", json={"code": code, "scopes": ["home:read", "home:admin"]}).json()
    assert body["scopes"] == ["home:read"]  # lo pedido ∩ lo aprobado
    code = http.post("/admin/pairing", json={}, headers=admin_headers).json()["code"]
    assert http.post("/pair", json={"code": code}).json()["scopes"] == CONVERSE  # sin pedir


# --- WebSocket y /v1 ---


def test_handshake_without_write_closes_4011(client, db_engine):
    _set_scopes(db_engine, CLIENT_ID, ["conversations:read"])
    with client.websocket_connect("/ws/stream") as ws:
        ws.send_json(HANDSHAKE)
        with pytest.raises(WebSocketDisconnect) as exc:
            ws.receive_text()
    assert (exc.value.code, exc.value.reason) == (4011, "missing scope")


def test_v1_bearer_needs_write(client, db_engine):
    _set_scopes(db_engine, CLIENT_ID, ["conversations:read"])
    r = client.post(
        "/v1/chat/completions",
        headers={"authorization": f"Bearer {VALID_KEY}"},
        json={"model": "fogar-gateway", "messages": [{"role": "user", "content": "hola"}]},
    )
    assert r.status_code == 403 and r.json()["detail"] == "missing_scope"


def test_bearer_with_unreadable_config_is_500_not_401(client, db_engine, caplog):
    with Session(db_engine) as s:
        rec = s.get(ClientRecord, CLIENT_ID)
        rec.allowed_agents = "{roto"
        s.add(rec)
        s.commit()
    r = client.get("/admin/conversations", headers={"authorization": f"Bearer {VALID_KEY}"})
    assert r.status_code == 500 and r.json()["detail"] == "internal_error"
    assert "ilegible" in caplog.text and "{roto" not in caplog.text
