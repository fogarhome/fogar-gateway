"""El dispositivo de avisos (5.0, #407): el del hogar (`PATCH /admin/home`) y el de cada persona
(`PATCH /auth/me`). Retirar el aparato lo quita de donde estuviera elegido."""

import pytest
from sqlmodel import Session
from starlette.testclient import TestClient

from fogar_gateway.db.models import ClientRecord, HomeRecord, MembershipRecord
from fogar_gateway.main import app
from tests.integration.conftest import _configure_app_mocks

PW = "contraseña larga"


@pytest.fixture
def http(mock_services, mock_registry, monkeypatch):
    _configure_app_mocks(mock_registry, monkeypatch)
    with TestClient(app, client=("127.0.0.1", 50000)) as c:
        yield c


@pytest.fixture
def people(http, admin_headers):
    ids = {}
    for username, role in (("sito", "owner"), ("ana", "member")):
        body = {"username": username, "display_name": username.title(), "password": PW,
                "role": role}
        r = http.post("/admin/users", json=body, headers=admin_headers)
        assert r.status_code == 201, r.text
        ids[username] = r.json()["id"]
    return ids


def _login(http, username: str, device_name: str = "d") -> tuple[str, dict[str, str]]:
    r = http.post("/auth/login",
                  json={"username": username, "password": PW, "device_name": device_name})
    assert r.status_code == 201, r.text
    return r.json()["client_id"], {"authorization": f"Bearer {r.json()['client_key']}"}


def _home_device(db_engine, name: str = "Tele", home_id: str = "home") -> str:
    with Session(db_engine) as s:
        rec = ClientRecord(name=name, key_hash=f"h-{name}", home_id=home_id)
        s.add(rec)
        s.commit()
        return rec.id


def _choices(db_engine, user_id: str) -> tuple[str | None, str | None]:
    """(el del hogar, el de la persona) en la BD."""
    with Session(db_engine) as s:
        home = s.get(HomeRecord, "home")
        m = s.get(MembershipRecord, (user_id, "home"))
        return home.notify_client_id, (m.notify_client_id if m else None)


def _set_both(db_engine, user_id: str, client_id: str) -> None:
    with Session(db_engine) as s:
        home = s.get(HomeRecord, "home")
        home.notify_client_id = client_id
        m = s.get(MembershipRecord, (user_id, "home"))
        m.notify_client_id = client_id
        s.add(home)
        s.add(m)
        s.commit()


def test_user_sets_own_personal_device(http, people, db_engine):
    mac, mac_auth = _login(http, "sito", "Mac")
    phone, phone_auth = _login(http, "sito", "Móvil")
    assert http.get("/auth/me", headers=mac_auth).json()["notify_client_id"] is None

    r = http.patch("/auth/me", json={"notify_client_id": phone}, headers=mac_auth)
    assert r.status_code == 200, r.text
    assert r.json()["notify_client_id"] == phone
    assert r.json()["user"]["username"] == "sito"
    # Es de la persona, no del aparato: lo ve desde cualquiera de los suyos.
    assert http.get("/auth/me", headers=phone_auth).json()["notify_client_id"] == phone
    assert _choices(db_engine, people["sito"]) == (None, phone)

    r = http.patch("/auth/me", json={}, headers=mac_auth)  # sin el campo, nada cambia
    assert r.status_code == 200 and r.json()["notify_client_id"] == phone
    r = http.patch("/auth/me", json={"notify_client_id": mac}, headers=mac_auth)
    assert r.json()["notify_client_id"] == mac
    r = http.patch("/auth/me", json={"notify_client_id": None}, headers=mac_auth)
    assert r.status_code == 200 and r.json()["notify_client_id"] is None
    assert http.patch("/auth/me", json={"x": 1}, headers=mac_auth).status_code == 422
    assert http.patch("/auth/me", json={"notify_client_id": ""}, headers=mac_auth).status_code == 422
    assert http.patch("/auth/me", json={"notify_client_id": phone}).status_code == 401


def test_user_cannot_pick_home_or_others_device(http, people, db_engine, admin_headers):
    mac, auth = _login(http, "sito", "Mac")
    old, old_auth = _login(http, "sito", "Viejo")
    off, _ = _login(http, "sito", "Apagado")
    anas, _ = _login(http, "ana")
    tv = _home_device(db_engine)
    assert http.post("/auth/logout", headers=old_auth).status_code == 204  # retirado
    r = http.patch(f"/admin/clients/{off}", json={"is_active": False}, headers=admin_headers)
    assert r.status_code == 200
    assert http.patch("/auth/me", json={"notify_client_id": mac}, headers=auth).status_code == 200

    for bad in (tv, anas, old, off, "no-existe"):
        r = http.patch("/auth/me", json={"notify_client_id": bad}, headers=auth)
        assert r.status_code == 422, bad
        assert r.json() == {"detail": "invalid_notify_device"}
    assert http.get("/auth/me", headers=auth).json()["notify_client_id"] == mac  # intacto


def test_admin_sets_home_notify_device(http, people, db_engine, admin_headers):
    tv = _home_device(db_engine)
    anas, ana_auth = _login(http, "ana")
    _, owner_auth = _login(http, "sito")
    assert http.get("/admin/home", headers=admin_headers).json()["notify_client_id"] is None

    r = http.patch("/admin/home", json={"notify_client_id": tv}, headers=admin_headers)
    assert r.status_code == 200, r.text
    assert r.json() == {"id": "home", "name": "Mi hogar", "agent": None, "notify_client_id": tv}
    assert http.get("/admin/home", headers=owner_auth).json()["notify_client_id"] == tv
    # Cualquier aparato activo del hogar, también el móvil de alguien (un aviso común).
    r = http.patch("/admin/home", json={"notify_client_id": anas}, headers=owner_auth)
    assert r.status_code == 200 and r.json()["notify_client_id"] == anas
    # Cambiar otro campo no lo toca.
    r = http.patch("/admin/home", json={"name": "Casa"}, headers=admin_headers)
    assert r.json()["notify_client_id"] == anas
    r = http.patch("/admin/home", json={"notify_client_id": None}, headers=admin_headers)
    assert r.status_code == 200 and r.json()["notify_client_id"] is None

    # Un miembro no toca el hogar, como el resto de /admin.
    r = http.patch("/admin/home", json={"notify_client_id": tv}, headers=ana_auth)
    assert r.status_code == 403 and r.json() == {"detail": "forbidden"}

    with Session(db_engine) as s:
        s.add(HomeRecord(id="h2", name="Otro"))
        s.commit()
    elsewhere = _home_device(db_engine, "Fuera", home_id="h2")
    retired = _home_device(db_engine, "Retirada")
    assert http.delete(f"/admin/clients/{retired}", headers=admin_headers).status_code == 204
    off = _home_device(db_engine, "Apagada")
    http.patch(f"/admin/clients/{off}", json={"is_active": False}, headers=admin_headers)
    for bad in (elsewhere, retired, off, "no-existe"):
        r = http.patch("/admin/home", json={"notify_client_id": bad}, headers=admin_headers)
        assert r.status_code == 422, bad
        assert r.json() == {"detail": "invalid_notify_device"}
    assert http.get("/admin/home", headers=admin_headers).json()["notify_client_id"] is None


@pytest.mark.parametrize("path", ["admin_delete", "logout", "my_devices", "delete_user"])
def test_retiring_device_clears_notify(http, people, db_engine, admin_headers, path):
    user = people["ana"]
    phone, phone_auth = _login(http, "ana", "Móvil")
    _, other_auth = _login(http, "ana", "Mac")
    _set_both(db_engine, user, phone)
    assert http.get("/auth/me", headers=other_auth).json()["notify_client_id"] == phone
    assert http.get("/admin/home", headers=admin_headers).json()["notify_client_id"] == phone

    if path == "admin_delete":
        r = http.delete(f"/admin/clients/{phone}", headers=admin_headers)
    elif path == "logout":
        r = http.post("/auth/logout", headers=phone_auth)
    elif path == "my_devices":
        r = http.delete(f"/auth/devices/{phone}", headers=other_auth)
    else:
        r = http.delete(f"/admin/users/{user}", headers=admin_headers)
    assert r.status_code == 204, r.text

    assert _choices(db_engine, user) == (None, None)
    assert http.get("/admin/home", headers=admin_headers).json()["notify_client_id"] is None
    if path != "delete_user":
        assert http.get("/auth/me", headers=other_auth).json()["notify_client_id"] is None


def test_retiring_another_device_keeps_the_choice(http, people, db_engine, admin_headers):
    user = people["sito"]
    mac, _ = _login(http, "sito", "Mac")
    _, old_auth = _login(http, "sito", "Viejo")
    _set_both(db_engine, user, mac)
    assert http.post("/auth/logout", headers=old_auth).status_code == 204
    assert _choices(db_engine, user) == (mac, mac)


def test_disabling_keeps_the_choice(http, people, db_engine, admin_headers):
    """Desactivar no es retirar: la elección queda y vuelve a valer al reactivarlo (quien reparte
    los avisos ignora un aparato inactivo)."""
    user = people["ana"]
    phone, _ = _login(http, "ana", "Móvil")
    _set_both(db_engine, user, phone)
    r = http.patch(f"/admin/clients/{phone}", json={"is_active": False}, headers=admin_headers)
    assert r.status_code == 200
    r = http.patch(f"/admin/users/{user}", json={"is_active": False}, headers=admin_headers)
    assert r.status_code == 200
    assert _choices(db_engine, user) == (phone, phone)
