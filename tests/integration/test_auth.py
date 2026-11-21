"""/auth (M20): cada inicio de sesión es un dispositivo personal con su token."""

import pytest
from sqlmodel import Session, select
from starlette.testclient import TestClient

from fogar_gateway.db.models import ClientRecord, MembershipRecord, UserRecord
from fogar_gateway.main import app
from fogar_gateway.services.auth import hash_key, hash_password
from tests.integration.conftest import _configure_app_mocks

PASSWORD = "una frase bastante larga"


def _add_user(db_engine, username: str, role: str = "member", *, active: bool = True) -> str:
    with Session(db_engine) as s:
        user = UserRecord(
            username=username, display_name=username.title(),
            password_hash=hash_password(PASSWORD), is_active=active,
        )
        s.add(user)
        s.commit()
        s.add(MembershipRecord(user_id=user.id, home_id="home", role=role))
        s.commit()
        return user.id


@pytest.fixture
def users(db_engine):
    return {"sito": _add_user(db_engine, "sito", "owner"), "ana": _add_user(db_engine, "ana")}


def _http(mock_registry, monkeypatch, peer="127.0.0.1", **kw) -> TestClient:
    _configure_app_mocks(mock_registry, monkeypatch)
    return TestClient(app, client=(peer, 50000), **kw)


@pytest.fixture
def http(mock_services, mock_registry, monkeypatch, users):
    with _http(mock_registry, monkeypatch) as c:
        yield c


def _login(http, username="sito", password=PASSWORD, **extra):
    body = {"username": username, "password": password, "device_name": "Mac de Sito", **extra}
    return http.post("/auth/login", json=body)


def _bearer(key: str) -> dict[str, str]:
    return {"authorization": f"Bearer {key}"}


def test_login_creates_a_personal_device_and_returns_its_token_once(http, db_engine, users):
    r = _login(http, "SITO", client_type="app", capabilities=["screen", "mic"])
    assert r.status_code == 201, r.text
    body = r.json()
    assert body["client_key"].startswith("fgr_")
    assert body["user"] == {"id": users["sito"], "username": "sito", "display_name": "Sito",
                            "role": "owner"}
    assert body["home"] == {"id": "home", "name": "Mi hogar"}
    with Session(db_engine) as s:
        device = s.get(ClientRecord, body["client_id"])
    assert device.user_id == users["sito"] and device.home_id == "home"
    assert device.key_hash == hash_key(body["client_key"])
    assert device.capabilities == '["screen", "mic"]' and device.client_type == "app"


@pytest.mark.parametrize(
    ("username", "password"), [("sito", "mala"), ("nadie", PASSWORD), ("", PASSWORD)]
)
def test_bad_credentials_are_one_error(http, username, password, caplog):
    r = _login(http, username, password)
    assert r.status_code in (401, 422)
    if r.status_code == 401:
        assert r.json()["detail"] == "invalid_credentials"
    assert password not in caplog.text and (not username or username not in caplog.text)


def test_an_inactive_user_or_one_without_home_cannot_log_in(http, db_engine):
    _add_user(db_engine, "baja", active=False)
    with Session(db_engine) as s:
        s.add(UserRecord(username="suelto", display_name="S", password_hash=hash_password(PASSWORD)))
        s.commit()
    assert _login(http, "baja").status_code == 401
    assert _login(http, "suelto").status_code == 401


def test_repeated_failures_wait_then_a_success_resets(http):
    for _ in range(5):
        assert _login(http, password="mala").status_code == 401
    assert _login(http, password="mala").status_code == 401  # el 6.º cuenta y bloquea
    r = _login(http)  # ni con la buena mientras dura la espera
    assert r.status_code == 429 and r.json()["detail"] == "too_many_attempts"
    assert int(r.headers["retry-after"]) >= 1


def test_login_over_plain_http_from_the_lan_is_refused(mock_services, mock_registry, monkeypatch, users):
    with _http(mock_registry, monkeypatch, peer="192.168.1.30") as c:
        r = _login(c)
    assert r.status_code == 403 and r.json()["detail"] == "insecure_transport"
    with _http(mock_registry, monkeypatch, peer="100.100.1.2") as c:  # tailnet
        assert _login(c).status_code == 201


def test_me_and_devices_with_the_token(http, users):
    first = _login(http).json()
    second = _login(http, device_name="iPhone").json()
    _login(http, "ana")  # de otra persona: no sale
    r = http.get("/auth/me", headers=_bearer(second["client_key"]))
    assert r.status_code == 200
    assert r.json()["user"]["id"] == users["sito"]
    assert r.json()["device"]["id"] == second["client_id"] and r.json()["device"]["current"]
    devices = http.get("/auth/devices", headers=_bearer(first["client_key"])).json()
    assert {d["id"]: d["current"] for d in devices} == {
        first["client_id"]: True, second["client_id"]: False,
    }


@pytest.mark.parametrize(
    "headers",
    [{}, {"authorization": "Bearer nope"}, {"authorization": "Basic abc"},
     {"authorization": "Bearer "}],
)
def test_without_a_valid_token_is_401(http, headers):
    r = http.get("/auth/me", headers=headers)
    assert r.status_code == 401 and r.json()["detail"] == "not_authenticated"


def test_a_paired_home_device_has_no_account(http, admin_headers):
    key = http.post("/admin/clients", json={"name": "Tele"}, headers=admin_headers).json()["client_key"]
    assert http.get("/auth/me", headers=_bearer(key)).status_code == 401


def test_a_deactivated_user_loses_every_device_at_once(http, db_engine, users):
    key = _login(http).json()["client_key"]
    with Session(db_engine) as s:
        user = s.get(UserRecord, users["sito"])
        user.is_active = False
        s.add(user)
        s.commit()
    assert http.get("/auth/me", headers=_bearer(key)).status_code == 401


def test_logout_retires_this_device_only(http, db_engine):
    a = _login(http).json()
    b = _login(http, device_name="iPhone").json()
    assert http.post("/auth/logout", headers=_bearer(a["client_key"])).status_code == 204
    assert http.get("/auth/me", headers=_bearer(a["client_key"])).status_code == 401
    assert http.get("/auth/me", headers=_bearer(b["client_key"])).status_code == 200
    with Session(db_engine) as s:
        retired = s.get(ClientRecord, a["client_id"])
    assert retired.retired_at is not None  # retirado, no borrado (sus conversaciones siguen)


def test_retiring_a_device_of_mine_but_never_someone_elses(http):
    mine = _login(http).json()
    other = _login(http, device_name="iPad").json()
    anas = _login(http, "ana").json()
    me = _bearer(mine["client_key"])
    assert http.delete(f"/auth/devices/{anas['client_id']}", headers=me).status_code == 404
    assert http.get("/auth/me", headers=_bearer(anas["client_key"])).status_code == 200
    assert http.delete(f"/auth/devices/{other['client_id']}", headers=me).status_code == 204
    assert http.get("/auth/me", headers=_bearer(other["client_key"])).status_code == 401
    assert http.delete(f"/auth/devices/{other['client_id']}", headers=me).status_code == 404


def test_retiring_closes_the_live_sessions_right_away(http, monkeypatch):
    """Sin esperar a la revalidación: las sesiones vivas del dispositivo reciben revoke()."""
    revoked: list[str] = []

    class FakeBridge:
        def __init__(self, cid: str) -> None:
            self.cid = cid

        def revoke(self) -> None:
            revoked.append(self.cid)

    device = _login(http).json()
    registry = app.state.client_registry
    monkeypatch.setattr(registry, "for_client", lambda cid: [FakeBridge(cid)], raising=False)
    http.post("/auth/logout", headers=_bearer(device["client_key"]))
    assert revoked == [device["client_id"]]


def test_the_web_fallback_does_not_swallow_auth(http):
    assert http.get("/auth/nada").status_code == 404
    assert http.get("/auth/nada").headers["content-type"].startswith("application/json")


def test_login_never_logs_the_token(http, caplog):
    key = _login(http).json()["client_key"]
    assert key not in caplog.text and PASSWORD not in caplog.text


def test_devices_query_lists_only_live_ones(http, db_engine):
    a = _login(http).json()
    _login(http, device_name="viejo")
    http.post("/auth/logout", headers=_bearer(_login(http, device_name="x").json()["client_key"]))
    names = {d["name"] for d in http.get("/auth/devices", headers=_bearer(a["client_key"])).json()}
    assert names == {"Mac de Sito", "viejo"}
    with Session(db_engine) as s:
        assert len(s.exec(select(ClientRecord)).all()) == 3  # el retirado sigue en la BD


# ── /admin por rol (paso 4) ─────────────────────────────────────────────────


@pytest.mark.parametrize("path", ["/admin/clients", "/admin/state", "/admin/pairing"])
def test_admin_is_for_owners_and_admins(http, db_engine, path):
    _add_user(db_engine, "admi", "admin")
    owner = _bearer(_login(http).json()["client_key"])
    admin = _bearer(_login(http, "admi").json()["client_key"])
    member = _bearer(_login(http, "ana").json()["client_key"])
    assert http.get(path, headers=owner).status_code == 200
    assert http.get(path, headers=admin).status_code == 200
    r = http.get(path, headers=member)
    assert r.status_code == 403 and r.json()["detail"] == "forbidden"


def test_admin_refuses_a_paired_device_key_and_a_bad_admin_token(http, admin_headers):
    key = http.post("/admin/clients", json={"name": "Tele"}, headers=admin_headers).json()["client_key"]
    assert http.get("/admin/clients", headers=_bearer(key)).status_code == 401
    owner = _bearer(_login(http).json()["client_key"])
    # Un X-Admin-Token presente manda: si no vale, 401 aunque el Bearer sí valga.
    assert http.get("/admin/clients", headers={**owner, "x-admin-token": "mal"}).status_code == 401


def test_a_demoted_user_loses_admin_at_once(http, db_engine, users):
    owner = _bearer(_login(http).json()["client_key"])
    assert http.get("/admin/clients", headers=owner).status_code == 200
    with Session(db_engine) as s:
        m = s.get(MembershipRecord, (users["sito"], "home"))
        m.role = "member"
        s.add(m)
        s.commit()
    assert http.get("/admin/clients", headers=owner).status_code == 403
