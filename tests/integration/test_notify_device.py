"""El dispositivo de avisos (5.0, #407): el del hogar (`PATCH /home`) y el de cada persona
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
        r = http.post("/home/users", json=body, headers=admin_headers)
        assert r.status_code == 201, r.text
        ids[username] = r.json()["id"]
    return ids


def _login(http, username: str, device_name: str = "d") -> tuple[str, dict[str, str]]:
    r = http.post("/auth/login",
                  json={"username": username, "password": PW, "device_name": device_name,
                        "scopes": ["conversations:write", "home:admin"]})
    assert r.status_code == 201, r.text
    return r.json()["client_id"], {"authorization": f"Bearer {r.json()['token']}"}


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
    r = http.patch(f"/home/devices/{off}", json={"is_active": False}, headers=admin_headers)
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
    assert http.get("/home", headers=admin_headers).json()["notify_client_id"] is None

    r = http.patch("/home", json={"notify_client_id": tv}, headers=admin_headers)
    assert r.status_code == 200, r.text
    assert r.json() == {"id": "home", "name": "Mi hogar", "agent": None, "notify_client_id": tv}
    assert http.get("/home", headers=owner_auth).json()["notify_client_id"] == tv
    # Cualquier aparato activo del hogar, también el móvil de alguien (un aviso común).
    r = http.patch("/home", json={"notify_client_id": anas}, headers=owner_auth)
    assert r.status_code == 200 and r.json()["notify_client_id"] == anas
    # Cambiar otro campo no lo toca.
    r = http.patch("/home", json={"name": "Casa"}, headers=admin_headers)
    assert r.json()["notify_client_id"] == anas
    r = http.patch("/home", json={"notify_client_id": None}, headers=admin_headers)
    assert r.status_code == 200 and r.json()["notify_client_id"] is None

    # Un miembro no toca el hogar, como el resto de /admin.
    r = http.patch("/home", json={"notify_client_id": tv}, headers=ana_auth)
    assert r.status_code == 403 and r.json() == {"detail": "missing_scope"}

    with Session(db_engine) as s:
        s.add(HomeRecord(id="h2", name="Otro"))
        s.commit()
    elsewhere = _home_device(db_engine, "Fuera", home_id="h2")
    retired = _home_device(db_engine, "Retirada")
    assert http.delete(f"/home/devices/{retired}", headers=admin_headers).status_code == 204
    off = _home_device(db_engine, "Apagada")
    http.patch(f"/home/devices/{off}", json={"is_active": False}, headers=admin_headers)
    for bad in (elsewhere, retired, off, "no-existe"):
        r = http.patch("/home", json={"notify_client_id": bad}, headers=admin_headers)
        assert r.status_code == 422, bad
        assert r.json() == {"detail": "invalid_notify_device"}
    assert http.get("/home", headers=admin_headers).json()["notify_client_id"] is None


@pytest.mark.parametrize("path", ["admin_delete", "logout", "my_devices", "delete_user"])
def test_retiring_device_clears_notify(http, people, db_engine, admin_headers, path):
    user = people["ana"]
    phone, phone_auth = _login(http, "ana", "Móvil")
    _, other_auth = _login(http, "ana", "Mac")
    _set_both(db_engine, user, phone)
    assert http.get("/auth/me", headers=other_auth).json()["notify_client_id"] == phone
    assert http.get("/home", headers=admin_headers).json()["notify_client_id"] == phone

    if path == "admin_delete":
        r = http.delete(f"/home/devices/{phone}", headers=admin_headers)
    elif path == "logout":
        r = http.post("/auth/logout", headers=phone_auth)
    elif path == "my_devices":
        r = http.delete(f"/auth/devices/{phone}", headers=other_auth)
    else:
        r = http.delete(f"/home/users/{user}", headers=admin_headers)
    assert r.status_code == 204, r.text

    assert _choices(db_engine, user) == (None, None)
    assert http.get("/home", headers=admin_headers).json()["notify_client_id"] is None
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
    r = http.patch(f"/home/devices/{phone}", json={"is_active": False}, headers=admin_headers)
    assert r.status_code == 200
    r = http.patch(f"/home/users/{user}", json={"is_active": False}, headers=admin_headers)
    assert r.status_code == 200
    assert _choices(db_engine, user) == (phone, phone)


# ── 2c.3: el reparto lo vuelve a mirar en cada aviso ──────────────────────────────────────


def test_notify_client_for_checks_the_device_at_delivery(db_engine):
    """`notify_client_for` (lo que lee el reparto de cada aviso): el elegido solo vale si sigue
    activo, sin retirar, en el hogar y, si es personal, con su persona activa y (para una
    conversación privada) de su dueña. Desactivar no quita la elección (vuelve a valer)."""
    from datetime import UTC, datetime

    from fogar_gateway.db.models import UserRecord
    from fogar_gateway.services.notify_device import notify_client_for

    with Session(db_engine) as s:
        if s.get(HomeRecord, "home") is None:
            s.add(HomeRecord(id="home", name="Casa"))
        s.add(UserRecord(id="ana", username="ana", display_name="Ana", password_hash="x"))
        s.add(UserRecord(id="bea", username="bea", display_name="Bea", password_hash="x"))
        s.commit()
        s.add(ClientRecord(id="tv", name="Tele", key_hash="h-tv"))
        s.add(ClientRecord(id="ana-phone", name="Móvil", key_hash="h-ap", user_id="ana"))
        s.commit()
        home = s.get(HomeRecord, "home")
        home.notify_client_id = "tv"
        s.add(home)
        s.add(MembershipRecord(user_id="ana", home_id="home", role="member", notify_client_id="ana-phone"))
        s.add(MembershipRecord(user_id="bea", home_id="home", role="member", notify_client_id="ana-phone"))
        s.commit()

    def both() -> tuple[str | None, str | None, str | None]:
        with Session(db_engine) as s:
            return (
                notify_client_for(s, "home", None),
                notify_client_for(s, "home", "ana"),
                notify_client_for(s, "home", "bea"),
            )

    # Bea's choice names Ana's phone (only possible by hand): never valid for her.
    assert both() == ("tv", "ana-phone", None)
    with Session(db_engine) as s:  # another home: neither its home nor a membership there
        assert notify_client_for(s, "other-home", None) is None
        assert notify_client_for(s, "other-home", "ana") is None

    def edit(model, key, **values) -> None:
        with Session(db_engine) as s:
            rec = s.get(model, key)
            for k, v in values.items():
                setattr(rec, k, v)
            s.add(rec)
            s.commit()

    edit(ClientRecord, "tv", is_active=False)
    edit(UserRecord, "ana", is_active=False)
    assert both() == (None, None, None)
    edit(ClientRecord, "tv", is_active=True)
    edit(UserRecord, "ana", is_active=True)
    assert both() == ("tv", "ana-phone", None)  # disabling kept the choice

    edit(ClientRecord, "tv", retired_at=datetime.now(UTC))
    edit(ClientRecord, "ana-phone", retired_at=datetime.now(UTC))
    assert both() == (None, None, None)
