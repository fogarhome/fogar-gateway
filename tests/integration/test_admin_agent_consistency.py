"""Consistencia de default_agent/allowed_agents y rotate-key sin 500 (#233)."""

import json
import logging

import pytest
from sqlalchemy.pool import StaticPool
from sqlmodel import Session, SQLModel, create_engine

from fogar_gateway.core.logging import fingerprint_key
from fogar_gateway.services.auth import hash_key

ADMIN_TOKEN = "test-admin-token"
HEADERS = {"x-admin-token": ADMIN_TOKEN}
TAKEN_KEY = "taken-secret-key-0123456789abcdef"


@pytest.fixture(autouse=True)
def override_admin_token(monkeypatch):
    from fogar_gateway.core import config

    monkeypatch.setattr(config.settings, "ADMIN_TOKEN", ADMIN_TOKEN)


@pytest.fixture(autouse=True)
def db_engine(monkeypatch):
    from fogar_gateway import db

    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    SQLModel.metadata.create_all(engine)
    monkeypatch.setattr(db.database, "_engine", engine)
    return engine


@pytest.fixture()
def http(db_engine):
    from starlette.testclient import TestClient

    from fogar_gateway.main import app

    with TestClient(app, raise_server_exceptions=True) as c:
        yield c


def _create(http, **body):
    return http.post("/home/devices", json={"name": "c", **body}, headers=HEADERS)


# --- POST -----------------------------------------------------------------


@pytest.mark.parametrize(
    "body",
    [
        {"default_agent": ""},
        {"default_agent": "   "},
        {"allowed_agents": [""]},
        {"allowed_agents": ["main", " "]},
    ],
)
def test_post_empty_agent_strings_are_422(http, body):
    assert _create(http, **body).status_code == 422


def test_post_default_outside_allowed_is_422(http):
    r = _create(http, default_agent="x", allowed_agents=["a", "b"])
    assert r.status_code == 422


def test_post_default_with_empty_allowed_is_422(http):
    assert _create(http, default_agent="x", allowed_agents=[]).status_code == 422


def test_post_valid_combinations_still_work(http):
    assert _create(http, default_agent="a", allowed_agents=["a", "b"]).status_code == 201
    assert _create(http, default_agent="z").status_code == 201  # allowed null = sin restricción
    assert _create(http, allowed_agents=[]).status_code == 201  # [] = denegado, sin default


# --- PATCH (estado resultante) ---------------------------------------------


def test_patch_default_checked_against_stored_allowed(http):
    cid = _create(http, allowed_agents=["a", "b"]).json()["id"]
    r = http.patch(f"/home/devices/{cid}", json={"default_agent": "x"}, headers=HEADERS)
    assert r.status_code == 422
    r = http.patch(f"/home/devices/{cid}", json={"default_agent": "a"}, headers=HEADERS)
    assert r.status_code == 200


def test_patch_allowed_checked_against_stored_default(http):
    cid = _create(http, default_agent="a").json()["id"]
    r = http.patch(f"/home/devices/{cid}", json={"allowed_agents": ["b"]}, headers=HEADERS)
    assert r.status_code == 422
    assert http.get(f"/home/devices/{cid}", headers=HEADERS).json()["allowed_agents"] is None
    r = http.patch(f"/home/devices/{cid}", json={"allowed_agents": None}, headers=HEADERS)
    assert r.status_code == 200


def test_patch_both_fields_together_use_resulting_state(http):
    cid = _create(http, default_agent="a", allowed_agents=["a"]).json()["id"]
    r = http.patch(
        f"/home/devices/{cid}",
        json={"default_agent": "b", "allowed_agents": ["b"]},
        headers=HEADERS,
    )
    assert r.status_code == 200


def test_patch_clearing_default_is_allowed(http):
    cid = _create(http, default_agent="a", allowed_agents=["a"]).json()["id"]
    r = http.patch(f"/home/devices/{cid}", json={"default_agent": None}, headers=HEADERS)
    assert r.status_code == 200


def test_patch_empty_strings_are_422(http):
    cid = _create(http).json()["id"]
    for body in ({"default_agent": ""}, {"allowed_agents": [""]}):
        assert http.patch(f"/home/devices/{cid}", json=body, headers=HEADERS).status_code == 422


# --- Filas antiguas inconsistentes (#122: solo se valida la entrada) -------


def _legacy_inconsistent(db_engine):
    from fogar_gateway.db.models import ClientRecord

    with Session(db_engine) as s:
        rec = ClientRecord(
            name="legacy",
            key_hash=hash_key("legacy-key-0123456789"),
            default_agent="x",
            allowed_agents=json.dumps(["a"]),
        )
        s.add(rec)
        s.commit()
        return rec.id


def test_legacy_inconsistent_row_still_reads_and_unrelated_patch_works(http, db_engine):
    cid = _legacy_inconsistent(db_engine)
    r = http.get(f"/home/devices/{cid}", headers=HEADERS)
    assert r.status_code == 200
    assert r.json()["default_agent"] == "x"
    r = http.patch(f"/home/devices/{cid}", json={"name": "renamed"}, headers=HEADERS)
    assert r.status_code == 200
    assert r.json()["name"] == "renamed"


def test_legacy_inconsistent_row_can_be_fixed_through_patch(http, db_engine):
    cid = _legacy_inconsistent(db_engine)
    r = http.patch(f"/home/devices/{cid}", json={"default_agent": "a"}, headers=HEADERS)
    assert r.status_code == 200


# --- rotate-key ------------------------------------------------------------


def _fake_tokens(monkeypatch, values):
    from fogar_gateway.api import home_routes as admin_routes

    it = iter(values)
    last = values[-1]
    monkeypatch.setattr(admin_routes, "new_key", lambda: next(it, last))


def test_rotate_key_collision_retries_with_new_key(http, monkeypatch):
    http.post("/home/devices", json={"name": "A", "client_key": TAKEN_KEY}, headers=HEADERS)
    cid = _create(http).json()["id"]
    _fake_tokens(monkeypatch, [TAKEN_KEY, "fresh-key-abcdefghijklmnop"])
    r = http.post(f"/home/devices/{cid}/rotate-key", headers=HEADERS)
    assert r.status_code == 200
    assert r.json()["client_key"] == "fresh-key-abcdefghijklmnop"


def test_rotate_key_persistent_collision_is_409_without_leaking_key(http, monkeypatch, caplog):
    caplog.set_level(logging.DEBUG)
    http.post("/home/devices", json={"name": "A", "client_key": TAKEN_KEY}, headers=HEADERS)
    created = _create(http).json()
    _fake_tokens(monkeypatch, [TAKEN_KEY])
    r = http.post(f"/home/devices/{created['id']}/rotate-key", headers=HEADERS)
    assert r.status_code == 409
    assert TAKEN_KEY not in r.text
    assert TAKEN_KEY not in caplog.text
    # La fila conserva su clave anterior
    again = http.get(f"/home/devices/{created['id']}", headers=HEADERS).json()
    assert again["key_fp"] == fingerprint_key(created["client_key"])


def test_agent_names_are_stored_lowercase_and_old_rows_still_match(http, db_engine):
    """Auditoría 3.0, C-1: OpenClaw usa los ids en minúsculas; "Main" se guarda como "main".
    Una fila de la 2.x con `allowed_agents: ["Main"]` sigue casando con un `default_agent`
    nuevo (ya en minúsculas) en vez de dar un 422 falso."""
    from fogar_gateway.db.models import ClientRecord

    r = _create(http, default_agent="Main", allowed_agents=["Main", "Otro"])
    assert r.status_code == 201
    assert (r.json()["default_agent"], r.json()["allowed_agents"]) == ("main", ["main", "otro"])

    cid = _create(http).json()["id"]
    with Session(db_engine) as s:
        record = s.get(ClientRecord, cid)
        assert record is not None
        record.allowed_agents = json.dumps(["Main"])
        s.add(record)
        s.commit()
    r = http.patch(f"/home/devices/{cid}", json={"default_agent": "Main"}, headers=HEADERS)
    assert (r.status_code, r.json()["default_agent"]) == (200, "main")
