"""/admin/users (M20): usuarios del hogar, siempre con un dueño."""

import pytest
from sqlmodel import Session, select
from starlette.testclient import TestClient

from fogar_gateway.db.models import ClientRecord, ConversationRecord, UserRecord
from fogar_gateway.main import app
from fogar_gateway.services.auth import verify_password
from tests.integration.conftest import _configure_app_mocks

PW = "contraseña larga"


@pytest.fixture
def http(mock_services, mock_registry, monkeypatch):
    _configure_app_mocks(mock_registry, monkeypatch)
    with TestClient(app, client=("127.0.0.1", 50000)) as c:
        yield c


def _create(http, headers, username, role="member", **extra):
    body = {"username": username, "display_name": username.title(), "password": PW, "role": role,
            **extra}
    return http.post("/admin/users", json=body, headers=headers)


def _bearer(http, username) -> dict[str, str]:
    r = http.post("/auth/login", json={"username": username, "password": PW, "device_name": "d"})
    assert r.status_code == 201, r.text
    return {"authorization": f"Bearer {r.json()['client_key']}"}


@pytest.fixture
def owner_id(http, admin_headers):
    return _create(http, admin_headers, "sito", "owner").json()["id"]


def test_create_list_and_password_is_hashed(http, admin_headers, db_engine, owner_id):
    r = _create(http, admin_headers, "  Ana ", agent="Ana")
    assert r.status_code == 201, r.text
    ana = r.json()
    assert ana["username"] == "ana" and ana["role"] == "member" and ana["agent"] == "ana"
    assert "password" not in str(ana)
    users = http.get("/admin/users", headers=admin_headers).json()
    assert [u["username"] for u in users] == ["sito", "ana"]
    with Session(db_engine) as s:
        stored = s.get(UserRecord, ana["id"]).password_hash
    assert PW not in stored and verify_password(PW, stored)


@pytest.mark.parametrize(
    "body",
    [
        {"username": "x", "display_name": "X", "password": "corta"},  # < 8
        {"username": "con espacio", "display_name": "X", "password": PW},
        {"username": "x", "display_name": "X", "password": PW, "role": "rey"},
    ],
)
def test_invalid_input_is_422(http, admin_headers, body):
    assert http.post("/admin/users", json=body, headers=admin_headers).status_code == 422


def test_duplicate_username_is_409(http, admin_headers, owner_id):
    r = _create(http, admin_headers, "SITO")
    assert r.status_code == 409 and r.json()["detail"] == "username_taken"


def test_the_last_owner_cannot_go(http, admin_headers, owner_id):
    for patch in ({"role": "member"}, {"is_active": False}):
        r = http.patch(f"/admin/users/{owner_id}", json=patch, headers=admin_headers)
        assert r.status_code == 409 and r.json()["detail"] == "last_owner"
    r = http.delete(f"/admin/users/{owner_id}", headers=admin_headers)
    assert r.status_code == 409
    second = _create(http, admin_headers, "pilar", "owner").json()["id"]
    assert http.patch(f"/admin/users/{owner_id}", json={"role": "admin"},
                      headers=admin_headers).status_code == 200
    assert http.delete(f"/admin/users/{second}", headers=admin_headers).status_code == 409


def test_an_admin_never_touches_an_owner(http, admin_headers, owner_id):
    admin_id = _create(http, admin_headers, "admi", "admin").json()["id"]
    member_id = _create(http, admin_headers, "ana").json()["id"]
    admin = _bearer(http, "admi")
    for call in (
        lambda: http.patch(f"/admin/users/{owner_id}", json={"display_name": "X"}, headers=admin),
        lambda: http.put(f"/admin/users/{owner_id}/password", json={"password": "nueva-clave"},
                         headers=admin),
        lambda: http.delete(f"/admin/users/{owner_id}", headers=admin),
        lambda: http.patch(f"/admin/users/{member_id}", json={"role": "owner"}, headers=admin),
        lambda: _create(http, admin, "otro", "owner"),
    ):
        r = call()
        assert r.status_code == 403 and r.json()["detail"] == "owner_only"
    # Lo demás, sí.
    assert http.patch(f"/admin/users/{member_id}", json={"role": "admin"},
                      headers=admin).status_code == 200
    assert _create(http, admin, "nuevo").status_code == 201
    assert http.patch(f"/admin/users/{admin_id}", json={"display_name": "Yo"},
                      headers=_bearer(http, "sito")).status_code == 200


def test_a_member_cannot_manage_users(http, admin_headers, owner_id):
    _create(http, admin_headers, "ana")
    r = http.get("/admin/users", headers=_bearer(http, "ana"))
    assert r.status_code == 403


def test_reset_password_is_the_recovery(http, admin_headers, owner_id):
    r = http.put(f"/admin/users/{owner_id}/password", json={"password": "otra-mas-larga"},
                 headers=admin_headers)
    assert r.status_code == 204
    bad = http.post("/auth/login", json={"username": "sito", "password": PW, "device_name": "d"})
    assert bad.status_code == 401
    good = http.post("/auth/login",
                     json={"username": "sito", "password": "otra-mas-larga", "device_name": "d"})
    assert good.status_code == 201


def test_patch_rejects_null_and_unknown_fields(http, admin_headers, owner_id):
    for body in ({"display_name": None}, {"role": None}, {"password": "x" * 10}):
        r = http.patch(f"/admin/users/{owner_id}", json=body, headers=admin_headers)
        assert r.status_code == 422
    r = http.patch(f"/admin/users/{owner_id}", json={"agent": None}, headers=admin_headers)
    assert r.status_code == 200 and r.json()["agent"] is None


def test_deactivating_cuts_the_users_devices_now(http, admin_headers, owner_id, monkeypatch):
    member_id = _create(http, admin_headers, "ana").json()["id"]
    token = _bearer(http, "ana")
    revoked: list[str] = []
    monkeypatch.setattr(app.state.client_registry, "for_client",
                        lambda cid: [type("B", (), {"revoke": lambda self: revoked.append(cid)})()],
                        raising=False)
    r = http.patch(f"/admin/users/{member_id}", json={"is_active": False}, headers=admin_headers)
    assert r.status_code == 200 and len(revoked) == 1
    assert http.get("/auth/me", headers=token).status_code == 401
    r = http.post("/auth/login", json={"username": "ana", "password": PW, "device_name": "d"})
    assert r.status_code == 401


def test_deleting_a_user_takes_their_devices_and_private_conversations(
    http, admin_headers, db_engine, owner_id
):
    member_id = _create(http, admin_headers, "ana").json()["id"]
    token = _bearer(http, "ana")
    with Session(db_engine) as s:
        device = s.exec(select(ClientRecord).where(ClientRecord.user_id == member_id)).one()
        tv = ClientRecord(name="Tele", key_hash="h-tv")
        s.add(tv)
        s.commit()
        s.add(ConversationRecord(client_id=device.id, owner_user_id=member_id, agent="main",
                                 session_key="agent:main:fogar-priv", kind="created"))
        s.add(ConversationRecord(client_id=tv.id, agent="main",
                                 session_key="agent:main:fogar-hogar", kind="created"))
        s.commit()
    assert http.delete(f"/admin/users/{member_id}", headers=admin_headers).status_code == 204
    assert http.get("/auth/me", headers=token).status_code == 401
    with Session(db_engine) as s:
        assert s.get(UserRecord, member_id) is None
        assert s.exec(select(ClientRecord).where(ClientRecord.user_id == member_id)).all() == []
        keys = [c.session_key for c in s.exec(select(ConversationRecord)).all()]
    assert keys == ["agent:main:fogar-hogar"]  # la del hogar se queda
    assert http.delete(f"/admin/users/{member_id}", headers=admin_headers).status_code == 404
