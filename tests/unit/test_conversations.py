"""Conversaciones (M19): reglas del día, la tabla, la importación de antes de M19, el índice
y el reparto del push (`ClientRegistry`). services/conversations.py."""

import asyncio
from datetime import UTC, date, datetime
from unittest.mock import MagicMock

import pytest
from sqlalchemy.exc import IntegrityError
from sqlmodel import Session, SQLModel, create_engine

from fogar_gateway.core.config import settings
from fogar_gateway.db.models import ClientRecord, ConversationRecord
from fogar_gateway.services import conversations as conv_mod
from fogar_gateway.services.auth import hash_key
from fogar_gateway.services.conversations import (
    Conversation,
    ConversationClientMissing,
    ConversationNotFound,
    ConversationService,
    SessionConversation,
    TitleCache,
    conversation_day,
    history_messages,
    session_key_for,
    session_title,
)
from fogar_gateway.services.openclaw.registry import ClientRegistry


@pytest.fixture
def engine(tmp_path):
    engine = create_engine(f"sqlite:///{tmp_path / 'gateway.db'}")
    SQLModel.metadata.create_all(engine)
    with Session(engine) as s:
        for cid in ("tv", "web"):
            s.add(ClientRecord(id=cid, name=cid.upper(), key_hash=hash_key(f"key-{cid}")))
        s.commit()
    return engine


@pytest.fixture
def service(engine):
    return ConversationService(engine)


class Clock:
    def __init__(self, now: datetime) -> None:
        self.now = now

    def __call__(self) -> datetime:
        return self.now


@pytest.fixture
def clock(monkeypatch):
    c = Clock(datetime(2026, 11, 14, 12, 0))
    monkeypatch.setattr(conv_mod, "_now", c)
    return c


# ── El día ───────────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "now, day",
    [
        (datetime(2026, 11, 14, 4, 59), date(2026, 11, 13)),  # antes del corte: aún es ayer
        (datetime(2026, 11, 14, 5, 0), date(2026, 11, 14)),
        (datetime(2026, 11, 14, 23, 59), date(2026, 11, 14)),
        (datetime(2026, 11, 15, 0, 30), date(2026, 11, 14)),
    ],
)
def test_the_day_starts_at_the_configured_hour(now, day):
    assert settings.CONVERSATION_DAY_START_HOUR == 5
    assert conversation_day(now) == day


def test_day_start_hour_zero_is_midnight(monkeypatch):
    monkeypatch.setattr(settings, "CONVERSATION_DAY_START_HOUR", 0)
    assert conversation_day(datetime(2026, 11, 14, 0, 1)) == date(2026, 11, 14)


def test_day_start_hour_is_validated():
    from pydantic import ValidationError

    from fogar_gateway.core.config import Settings

    with pytest.raises(ValidationError):
        Settings(CONVERSATION_DAY_START_HOUR=24)  # type: ignore[call-arg]


def test_session_key_of_a_conversation():
    assert session_key_for("main", "abc") == "agent:main:fogar-abc"


# ── La tabla ─────────────────────────────────────────────────────────────


async def test_default_conversation_is_one_per_client_agent_and_day(service, clock):
    first = await service.default_for("tv", "main")
    assert first.kind == "default"
    assert first.day == date(2026, 11, 14)
    assert first.session_key == f"agent:main:fogar-{first.id}"
    assert await service.default_for("tv", "main") == first
    assert (await service.default_for("tv", "other")).id != first.id  # otro agente
    assert (await service.default_for("web", "main")).id != first.id  # otro cliente
    clock.now = datetime(2026, 11, 15, 5, 0)  # tras el corte del día siguiente
    tomorrow = await service.default_for("tv", "main")
    assert tomorrow.id != first.id and tomorrow.day == date(2026, 11, 15)


async def test_a_race_on_the_default_conversation_returns_the_same_row(service, engine):
    """El índice único (cliente, agente, día) resuelve dos altas a la vez."""
    day = date(2026, 11, 14)
    first = service._default_for("tv", "main", day)
    real = ConversationService._find_default(Session(engine), "tv", "main", day)
    # Como si la otra sesión no la hubiera visto al buscar: el alta choca y devuelve la suya.
    service._find_default = MagicMock(side_effect=[None, real])
    assert service._default_for("tv", "main", day).id == first.id


async def test_created_conversations_are_always_new(service):
    a, b = await service.create("web", "main"), await service.create("web", "main")
    assert a.id != b.id and a.kind == b.kind == "created" and a.day is None


async def test_a_home_conversation_opens_from_any_home_device(service):
    """M20: la del hogar se abre desde cualquier dispositivo del hogar (antes, solo desde el suyo).
    Lo privado: tests/unit/test_conversation_visibility.py."""
    web = await service.create("web", "main")
    assert (await service.get_visible(web.id, "home", None)) == web
    with pytest.raises(ConversationNotFound):
        await service.get_visible("does-not-exist", "home", None)


async def test_note_turn_counts_and_stamps(service, engine):
    conv = await service.create("web", "main")
    await service.note_turn(conv.id)
    await service.note_turn(conv.id)
    (row,) = await service.list_rows("web")
    assert row.record.turn_count == 2
    assert row.record.last_turn_at is not None
    assert row.client_name == "WEB"


async def test_list_puts_the_last_spoken_first(service):
    old, new = await service.create("web", "main"), await service.create("tv", "main")
    await service.note_turn(old.id)
    rows = await service.list_rows()
    assert [r.record.id for r in rows] == [old.id, new.id]
    assert [r.record.id for r in await service.list_rows("tv")] == [new.id]


async def test_load_fills_the_index(service, engine):
    conv = await service.create("web", "main")
    fresh = ConversationService(engine)
    assert fresh.index.get(conv.session_key) is None
    await fresh.load()
    assert fresh.index.get(conv.session_key) == conv


async def test_rows_saved_with_an_uppercase_agent_are_read_in_lowercase(service, engine, clock):
    """Auditoría 3.0 ronda 2, B-r2-1/C-r2-1: filas de antes de C-1 con el agente en mayúsculas.
    OpenClaw guarda la clave en minúsculas: el gateway las lee así y no duplica la del día."""
    record = ConversationRecord(client_id="tv", agent="Main", kind="default", day=conversation_day(), session_key="")
    record.session_key = f"agent:Main:fogar-{record.id}"
    with Session(engine, expire_on_commit=False) as s:
        s.add(record)
        s.commit()
    lower_key = f"agent:main:fogar-{record.id}"

    conv = await service.default_for("tv", "main")
    assert (conv.id, conv.agent, conv.session_key) == (record.id, "main", lower_key)
    assert (await service.get(record.id)).session_key == lower_key
    fresh = ConversationService(engine)
    await fresh.load()
    assert fresh.index.get(lower_key).id == record.id


def test_with_both_main_and_Main_the_default_is_the_one_with_history(service, engine):
    """Revisión de B-r2-1: el índice único distingue mayúsculas; se elige siempre la usada."""
    day = date(2026, 11, 14)
    with Session(engine, expire_on_commit=False) as s:
        rows = [
            ConversationRecord(client_id="tv", agent=a, kind="default", day=day, session_key=f"k-{a}", turn_count=n)
            for a, n in (("main", 0), ("Main", 3))
        ]
        s.add_all(rows)
        s.commit()
    for _ in range(3):
        assert service._default_for("tv", "main", day).id == rows[1].id


async def test_a_client_with_conversations_cannot_be_deleted(service, engine):
    """PRAGMA foreign_keys=ON en cada conexión (db/database.py): la conversación guarda siempre
    su dispositivo (M20), así que un dispositivo con conversaciones se retira, no se borra."""
    conv = await service.create("web", "main")
    with pytest.raises(IntegrityError), Session(engine) as s:
        s.delete(s.get(ClientRecord, "web"))
        s.commit()
    assert (await service.get(conv.id)) is not None


async def test_a_conversation_for_a_missing_client_fails_cleanly(service, clock):
    """La clave foránea rechaza el alta (p. ej. el cliente se borró a la vez)."""
    with pytest.raises(ConversationClientMissing):
        await service.default_for("borrado", "main")
    with pytest.raises(ConversationClientMissing):
        await service.create("borrado", "main")
    assert service.index.get("agent:main:fogar-x") is None


async def test_list_hides_default_conversations_nobody_spoke_in(service, clock):
    silent = await service.default_for("tv", "main")
    spoken = await service.default_for("web", "main")
    created = await service.create("tv", "main")  # una elegida sí sale, aunque esté vacía
    await service.note_turn(spoken.id)
    assert {r.record.id for r in await service.list_rows()} == {spoken.id, created.id}
    assert (await service.row(silent.id)) is not None  # por id, sí


async def test_titles_are_cached_and_failures_are_not():
    reads = []

    class OpenClaw:
        fail = False

        async def request(self, method, params, timeout=10.0):
            reads.append(method)
            if OpenClaw.fail:
                raise RuntimeError("down")
            return {"sessions": [{"key": "k", "derivedTitle": "Cena"}], "hasMore": False}

    cache = TitleCache()
    assert await cache.get(OpenClaw()) == {"k": "Cena"}
    assert await cache.get(OpenClaw()) == {"k": "Cena"}
    assert reads == ["sessions.list"]  # la segunda, de la caché
    cache._at = None  # caducada
    OpenClaw.fail = True
    assert await cache.get(OpenClaw()) == {}
    assert await cache.get(OpenClaw()) == {}
    assert len(reads) == 2  # un fallo se recuerda un rato: la segunda no pregunta
    cache._failed_at -= 60  # pasado TITLES_FAILURE_TTL_S
    assert await cache.get(OpenClaw()) == {}
    assert len(reads) == 3


async def test_concurrent_title_requests_share_one_read():
    started, release = asyncio.Event(), asyncio.Event()
    reads = []

    class Hanging:
        async def request(self, method, params, timeout=10.0):
            reads.append(method)
            started.set()
            await release.wait()
            raise TimeoutError()

    cache = TitleCache()
    waiters = [asyncio.create_task(cache.get(Hanging())) for _ in range(5)]
    await started.wait()
    waiters[0].cancel()  # una petición que se va no cancela la lectura de las demás
    release.set()
    results = await asyncio.gather(*waiters[1:])
    assert results == [{}] * 4 and reads == ["sessions.list"]
    assert await cache.get(Hanging()) == {} and reads == ["sessions.list"]  # fallo recordado


# ── sessions.list, paginado ──────────────────────────────────────────────


class FakeOpenClaw:
    def __init__(self, rows, page=None):
        self.rows = rows
        self.page = page
        self.calls = []

    async def request(self, method, params, timeout=10.0):
        self.calls.append((method, params))
        offset, limit = params.get("offset", 0), self.page or params["limit"]
        page = self.rows[offset:offset + limit]
        more = offset + limit < len(self.rows)
        return {"sessions": page, "hasMore": more, "nextOffset": offset + limit if more else None}


async def test_openclaw_sessions_are_listed_page_by_page_with_titles():
    rows = [{"key": f"agent:main:fogar-{i}"} for i in range(5)] + [{"nokey": 1}]
    openclaw = FakeOpenClaw(rows, page=2)
    listed = await conv_mod.list_openclaw_sessions(openclaw)
    assert [r["key"] for r in listed] == [f"agent:main:fogar-{i}" for i in range(5)]
    assert len(openclaw.calls) == 3
    assert all(p["archived"] == "all" and p["includeDerivedTitles"] for _, p in openclaw.calls)


# ── Lectura de OpenClaw ───────────────────────────────────────────────────


def test_history_is_normalised_to_role_text_and_time():
    raw = [
        {"role": "user", "content": "hola", "timestamp": 1_794_665_071_190},
        {"role": "assistant", "content": [{"type": "text", "text": "¿qué tal?"},
                                          {"type": "toolCall", "name": "x"}], "timestamp": 1},
        {"role": "assistant", "content": [{"type": "toolCall", "name": "x"}]},  # sin texto
        {"role": "toolResult", "content": "secret"},
        {"role": "custom", "customType": "run-failed-before-reply", "content": "fallo"},
        "basura",
    ]
    assert history_messages(raw) == [
        {"role": "user", "text": "hola",
         "timestamp": datetime.fromtimestamp(1_794_665_071.19, UTC)},
        {"role": "assistant", "text": "¿qué tal?", "timestamp": datetime.fromtimestamp(0.001, UTC)},
    ]
    assert history_messages(None) == []


def test_title_prefers_the_derived_one():
    assert session_title({"derivedTitle": " Luces ", "displayName": "x"}) == "Luces"
    assert session_title({"displayName": "Cena", "label": "y"}) == "Cena"
    assert session_title({"label": "z"}) == "z"
    assert session_title({"derivedTitle": "  ", "key": "k"}) is None


# ── Cambio de día de una sesión ───────────────────────────────────────────


async def test_a_default_session_moves_to_the_new_day(service, clock):
    first = await service.default_for("tv", "main")
    session = SessionConversation(first, explicit=False, service=service)
    moves = []

    async def on_move(old, new):
        moves.append((old.id, new.id))

    session.on_move = on_move
    await session.refresh()
    assert session.current == first and moves == []  # mismo día: nada
    clock.now = datetime(2026, 11, 15, 6, 0)
    await session.refresh()
    assert session.current.day == date(2026, 11, 15)
    assert moves == [(first.id, session.id)]
    assert session.id_for_key(first.session_key) == first.id  # ayer sigue en el índice


@pytest.mark.parametrize("make", ["created", "explicit_default"])
async def test_a_chosen_conversation_never_changes_day(service, clock, make):
    if make == "created":
        conv, explicit = await service.create("tv", "main"), True
    else:
        conv, explicit = await service.default_for("tv", "main"), True
    session = SessionConversation(conv, explicit=explicit, service=service)
    clock.now = datetime(2026, 11, 20, 12, 0)
    await session.refresh()
    assert session.current == conv


async def test_without_db_the_turn_stays_in_its_conversation(service, clock, caplog):
    first = await service.default_for("tv", "main")
    session = SessionConversation(first, explicit=False, service=service)
    clock.now = datetime(2026, 11, 15, 6, 0)

    async def broken(*a, **k):
        raise RuntimeError("database is locked")

    service.default_for = broken
    await session.refresh()
    assert session.current == first
    assert "database is locked" not in caplog.text


# ── Sesiones vivas y reparto del push ─────────────────────────────────────


async def test_a_day_move_frees_the_old_keys_queue_peak_and_keeps_its_value(make_bridge):
    bridge, _ = make_bridge()
    old = bridge.conversation.current
    new = Conversation("d2", old.client_id, "main", session_key_for("main", "d2"), "default", None)
    bridge._turn_registry._peak[old.session_key] = 3
    await bridge._on_conversation_move(old, new)
    assert old.session_key not in bridge._turn_registry._peak
    assert bridge._session_end_meta("completed")["turn_queue_peak"] == 3


async def test_a_session_that_lost_its_conversation_never_aborts_on_that_key(make_bridge):
    """Red de seguridad del desplazamiento: ya no aborta en la clave que otra sesión tiene."""
    from tests.fakes.bridge import push

    bridge, _ = make_bridge()
    await push(bridge, ("turn_start", bridge.conversation.session_key))
    bridge.conversation.released = True
    assert (await bridge.turns.cancel("cancelled"))[0] is True
    bridge.orchestrator.abort.assert_not_awaited()
    await bridge.close_all()


def _conv(cid, client="tv", agent="main"):
    return Conversation(cid, client, agent, session_key_for(agent, cid), "default", date(2026, 11, 14))


def test_a_conversation_is_open_in_one_session_only():
    reg = ClientRegistry()
    old, new = object(), object()
    assert reg.register("tv", old, _conv("c1")) is None
    assert reg.register("tv", new, _conv("c1")) is old  # el llamador cierra la vieja
    assert reg.holder("c1") is new
    reg.unregister("tv", old)  # el cierre tardío de la vieja no quita a la nueva
    assert reg.holder("c1") is new and reg.get("tv") is new


def test_push_goes_to_the_session_holding_its_conversation():
    reg = ClientRegistry()
    a, b = object(), object()
    reg.register("web", a, _conv("c1", "web"))
    reg.register("web", b, _conv("c2", "web"))
    assert reg.route(session_key_for("main", "c1")) is a
    assert reg.route(session_key_for("main", "c2")) is b
    assert sorted(map(id, reg.bridges())) == sorted([id(a), id(b)])


def test_push_of_a_conversation_not_open_goes_to_the_clients_last_session():
    """La conversación de ayer de la tele: su sesión ya está en la de hoy."""
    reg = ClientRegistry()
    tv = object()
    reg.index.add(_conv("yesterday"))
    reg.register("tv", tv, _conv("today"))
    assert reg.route(session_key_for("main", "yesterday")) is tv
    reg.unregister("tv", tv)
    assert reg.route(session_key_for("main", "yesterday")) is None


def test_push_of_a_key_without_a_conversation_goes_nowhere():
    reg = ClientRegistry()
    reg.register("tv", object(), _conv("today"))
    assert reg.route("agent:main:tv") is None


def test_moving_a_session_to_another_conversation():
    reg = ClientRegistry()
    tv = object()
    reg.register("tv", tv, _conv("d1"))
    reg.close("d1", tv)
    assert reg.open(_conv("d2"), tv) is None
    assert reg.holder("d1") is None and reg.holder("d2") is tv
