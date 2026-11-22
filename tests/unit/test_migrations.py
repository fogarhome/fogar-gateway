import logging

import pytest
from sqlalchemy import create_engine, inspect

import fogar_gateway.db.models  # noqa: F401
from fogar_gateway.core.config import settings
from fogar_gateway.db import database


def _point_at(monkeypatch, db_path) -> None:
    monkeypatch.setattr(settings, "DATABASE_URL", f"sqlite:///{db_path}")
    monkeypatch.setattr(database, "CREATE_DB", True)
    database._engine = None


def test_fresh_db_upgrades_from_scratch(tmp_path, monkeypatch):
    db_path = tmp_path / "fresh.db"
    _point_at(monkeypatch, db_path)

    database.run_migrations()

    engine = create_engine(f"sqlite:///{db_path}")
    tables = inspect(engine).get_table_names()
    assert "clients" in tables
    assert "alembic_version" in tables
    columns = {c["name"] for c in inspect(engine).get_columns("clients")}
    assert {"id", "home_id", "user_id", "key_hash", "tool_calls_enabled"} <= columns
    assert {"homes", "users", "memberships", "conversations"} <= set(tables)


def test_already_versioned_db_is_a_clean_noop(tmp_path, monkeypatch):
    db_path = tmp_path / "versioned.db"
    _point_at(monkeypatch, db_path)

    database.run_migrations()
    database._engine = None  # forzar reconexión, como en un segundo arranque real

    database.run_migrations()  # no debe lanzar excepción ni duplicar nada

    engine = create_engine(f"sqlite:///{db_path}")
    assert "clients" in inspect(engine).get_table_names()


def test_run_migrations_does_not_disable_existing_loggers(tmp_path, monkeypatch):
    """migrations/env.py calls fileConfig(alembic.ini) on every run_migrations()
    call — including at real gateway startup, not just in tests. alembic.ini's
    [loggers] section only declares root/sqlalchemy/alembic, so fileConfig's
    default disable_existing_loggers=True silently disables every already-
    created fogar_gateway.* logger (main.py imports all of them before lifespan() calls
    run_migrations()) for the rest of the process's life — no application log
    line, including the security/audit ones, ever reaches stdout again."""
    db_path = tmp_path / "logging_regression.db"
    _point_at(monkeypatch, db_path)

    probe_logger = logging.getLogger("fogar_gateway.some_module_that_already_exists")
    probe_logger.disabled = False

    database.run_migrations()

    assert probe_logger.disabled is False


def test_dispose_engine_disposes_and_resets_to_none(tmp_path, monkeypatch):
    db_path = tmp_path / "dispose.db"
    _point_at(monkeypatch, db_path)

    engine = database.get_engine()
    assert database._engine is engine

    database.dispose_engine()

    assert database._engine is None


def test_dispose_engine_is_a_noop_when_never_initialized(monkeypatch):
    monkeypatch.setattr(database, "_engine", None)
    database.dispose_engine()  # must not raise
    assert database._engine is None


def _insert_client(c, client_id: str, home: str = "home", user: str | None = None) -> None:
    from sqlalchemy import text

    c.execute(text(
        "INSERT INTO clients (id, home_id, user_id, name, key_hash, is_active, created_at, "
        "stt_language, stt_vad_thold, tts_voice, tts_speed, barge_in_enabled, "
        "barge_in_min_chars, silence_timeout_s, max_silence_turns, push_enabled, "
        "tool_calls_enabled) VALUES (:id, :home, :user, 'X', :id, 1, '2026-11-21', 'es', 0, "
        "'v', 1, 1, 5, 2, 3, 1, 0)"
    ), {"id": client_id, "home": home, "user": user})


def _insert_conversation(c, conv_id: str, client_id: str, home: str = "home",
                         owner: str | None = None) -> None:
    from sqlalchemy import text

    c.execute(text(
        "INSERT INTO conversations (id, home_id, client_id, owner_user_id, agent, session_key, "
        "kind, created_at, turn_count) VALUES (:id, :home, :client, :owner, 'main', :id, "
        "'created', '2026-11-21', 0)"
    ), {"id": conv_id, "home": home, "client": client_id, "owner": owner})


def test_4_0_schema_keeps_every_conversation_with_its_home_and_device(tmp_path, monkeypatch):
    """M20: el hogar existe desde la migración; una conversación no puede quedar en otro hogar
    que su dispositivo; un dispositivo con conversaciones no se borra (se retira); borrar a un
    usuario borra sus conversaciones privadas y sus dispositivos personales."""
    from sqlalchemy import text
    from sqlalchemy.exc import IntegrityError

    _point_at(monkeypatch, tmp_path / "home.db")
    database.run_migrations()
    engine = database.get_engine()  # foreign_keys=ON, como en producción
    try:
        with engine.begin() as c:
            assert c.execute(text("SELECT id FROM homes")).scalars().all() == ["home"]
            c.execute(text("INSERT INTO homes (id, name, created_at) VALUES ('h2', 'Otro', "
                           "'2026-11-21')"))
            c.execute(text("INSERT INTO users (id, username, display_name, password_hash, "
                           "is_active, created_at) VALUES ('u1', 'sito', 'Sito', 'x', 1, "
                           "'2026-11-21')"))
            _insert_client(c, "tv")
            _insert_client(c, "mac", user="u1")
            _insert_conversation(c, "c-tv", "tv")
            _insert_conversation(c, "c-mac", "mac", owner="u1")
        with pytest.raises(IntegrityError), engine.begin() as c:  # otro hogar que su dispositivo
            _insert_conversation(c, "c-bad", "tv", home="h2")
        with pytest.raises(IntegrityError), engine.begin() as c:  # se retira, no se borra
            c.execute(text("DELETE FROM clients WHERE id = 'tv'"))
        with engine.begin() as c:
            c.execute(text("DELETE FROM users WHERE id = 'u1'"))
        with engine.connect() as c:
            assert c.execute(text("SELECT id FROM conversations")).scalars().all() == ["c-tv"]
            assert c.execute(text("SELECT id FROM clients")).scalars().all() == ["tv"]
    finally:
        database.dispose_engine()


def test_a_3x_database_is_refused_untouched(tmp_path, monkeypatch):
    """La 4.0 no convierte una BD 3.x (M20): error claro y el fichero sin tocar."""
    from sqlalchemy import text

    db_path = tmp_path / "old.db"
    old = create_engine(f"sqlite:///{db_path}")
    with old.begin() as c:
        c.execute(text("CREATE TABLE alembic_version (version_num VARCHAR(32) NOT NULL)"))
        c.execute(text("INSERT INTO alembic_version VALUES ('5b1e7c2d9a40')"))
    old.dispose()
    before = db_path.read_bytes()
    _point_at(monkeypatch, db_path)
    try:
        with pytest.raises(database.DatabaseFromOldVersion, match=r"4\.0"):
            database.run_migrations()
    finally:
        database.dispose_engine()
    assert db_path.read_bytes() == before


def test_connections_keep_foreign_keys_on_after_migrating(tmp_path, monkeypatch):
    """env.py apaga foreign_keys en la conexión de la migración y la devuelve al pool del
    engine compartido: tiene que volver a ON, o un borrado de cliente en ella se saltaría el
    ON DELETE CASCADE (el listener de `connect` no corre al reusarla)."""
    from sqlalchemy import text

    _point_at(monkeypatch, tmp_path / "fk.db")
    database.run_migrations()
    engine = database.get_engine()
    conns = [engine.connect() for _ in range(3)]  # la del pool (la de la migración) y nuevas
    try:
        assert [c.execute(text("PRAGMA foreign_keys")).scalar() for c in conns] == [1, 1, 1]
    finally:
        for c in conns:
            c.close()
        database.dispose_engine()
