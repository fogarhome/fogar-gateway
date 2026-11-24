"""Conversaciones (M19) vistas desde el cliente (arnés e2e: gateway real, OpenClaw falso).

Cada test mira lo que recibe el cliente por su WebSocket (`ready.conversation`,
`turn_start.conversation`/`origin`, `turn_end.reason`, cierres 4xxx) y, cuando importa, en qué
sesión de OpenClaw acabó cada turno (`openclaw.sessions`).

Desde la 5.0 (#406) una conversación puede estar abierta en varias sesiones a la vez: cada una
recibe su par de cada turno de su canal, y el 4102 se retiró.
"""

import asyncio
import json
from datetime import datetime

import httpx
import pytest
import websockets
from sqlmodel import Session

from fogar_gateway.core.config import settings
from fogar_gateway.db.models import ClientRecord, ConversationRecord, UserRecord
from fogar_gateway.services import conversations as conv_mod
from fogar_gateway.services.auth import hash_key
from fogar_gateway.services.conversations import ConversationService, session_key_for
from tests.integration.conftest import CLIENT_ID, VALID_KEY
from tests.integration.harness.conftest import assert_turns_paired, of_type


class Clock:
    def __init__(self, now: datetime) -> None:
        self.now = now

    def __call__(self) -> datetime:
        return self.now


@pytest.fixture
def clock(monkeypatch):
    """Reloj de pared del gateway: el 14 a las 23:00 (el día empezó a las 5:00)."""
    c = Clock(datetime(2026, 11, 14, 23, 0))
    monkeypatch.setattr(conv_mod, "_now", c)
    return c


async def closed_by_gateway(client) -> websockets.ConnectionClosed:
    """Recoge lo que llegue (en `client.messages`) hasta que el gateway cierra el socket;
    devuelve el cierre."""
    while True:
        try:
            raw = await asyncio.wait_for(client.ws.recv(), timeout=10)
        except websockets.ConnectionClosed as closed:
            return closed
        client.messages.append(raw if isinstance(raw, bytes) else json.loads(raw))


async def run_turn(client, text: str, ends: int = 1) -> list:
    """`send` y espera `ends` turn_end más (el suyo, y el del turno que sustituya); devuelve
    los mensajes desde el `send`."""
    before = len(client.messages)
    seen = len(of_type(client.messages, "turn_end"))
    await client.send({"type": "send", "text": text})
    await client.collect(lambda m: len(of_type(m, "turn_end")) >= seen + ends)
    return client.messages[before:]


def said(harness, session_key: str) -> list[str]:
    """Lo que el usuario dijo en esa sesión de OpenClaw."""
    rows = harness.openclaw.sessions.get(session_key, {}).get("messages", [])
    return [m["content"] for m in rows if m["role"] == "user"]


# ── Conversación por defecto: una por cliente, agente y día ─────────────────


async def test_ready_and_turns_carry_the_default_conversation_of_the_day(harness, clock):
    client = await harness.connect()
    conv = client.ready["conversation"]
    assert conv["agent"] == "main" == client.ready["agent"]

    msgs = await run_turn(client, "hola")
    (start,) = of_type(msgs, "turn_start")
    assert (start["conversation"], start["origin"]) == (conv["id"], "user")
    assert said(harness, session_key_for("main", conv["id"])) == ["hola"]
    (end,) = of_type(msgs, "turn_end")
    assert end["reason"] == "done"


async def test_second_session_does_not_close_first(harness, clock):
    """5.0 (#406): la tele que reconecta antes de que caiga su socket viejo, o dos pestañas en la
    misma conversación: ninguna se cierra, las dos reciben `ready` y las dos siguen hablando."""
    old = await harness.connect()
    new = await harness.connect()
    assert new.ready["conversation"] == old.ready["conversation"]  # la del día, la misma

    await old.collect(timeout=0.5)  # nada de 4102: el socket viejo sigue abierto
    for client in (old, new):
        msgs = await run_turn(client, "sigo aquí", ends=1)
        assert of_type(msgs, "turn_start")[0]["conversation"] == old.ready["conversation"]["id"]
    for client in (old, new):
        await client.collect(lambda m: len(of_type(m, "turn_end")) >= 2)
        assert_turns_paired(client.messages)


async def test_both_sessions_see_the_turn(harness, clock):
    """Cada sesión adjunta recibe su propio par (su turn_id) del turno del canal, con su texto."""
    a = await harness.connect()
    b = await harness.connect()
    await run_turn(a, "hola")
    await b.collect(lambda m: bool(of_type(m, "turn_end")))
    for client in (a, b):
        (start,) = of_type(client.messages, "turn_start")
        assert (start["conversation"], start["origin"]) == (a.ready["conversation"]["id"], "user")
        assert [t["text"] for t in of_type(client.messages, "token")] == ["reply-hola"]
        assert [e["reason"] for e in of_type(client.messages, "turn_end")] == ["done"]
        assert_turns_paired(client.messages)
    assert said(harness, harness.session_key(a)) == ["hola"]  # un solo chat.send


async def test_late_session_gets_accumulated_text(harness, clock):
    """Una sesión que entra a mitad de un turno recibe su turn_start y, en un `token`, lo que el
    turno llevaba dicho; después, lo que sigue."""
    first = await harness.connect()
    await first.send({"type": "send", "text": "partial_hold"})
    await first.collect(lambda m: bool(of_type(m, "token")))

    late = await harness.connect()
    await late.collect(lambda m: bool(of_type(m, "token")))
    harness.openclaw.release.set()
    for client in (first, late):
        await client.collect(lambda m: bool(of_type(m, "turn_end")))
        assert [e["reason"] for e in of_type(client.messages, "turn_end")] == ["done"]
        assert_turns_paired(client.messages)
    assert [t["text"] for t in of_type(first.messages, "token")] == ["reply-partial ", "fin"]
    assert [t["text"] for t in of_type(late.messages, "token")] == ["reply-partial ", "fin"]
    assert of_type(late.messages, "turn_start")[0]["origin"] == "user"


async def test_closing_sender_keeps_turn_for_other(harness, clock):
    """Review Focus 1 de la 2a: dos pestañas en la misma conversación y la que envió se cierra a
    mitad de su turno. La otra lo sigue viendo hasta su turn_end, el run no se aborta y nadie
    recibe dos turn_end."""
    sender = await harness.connect()
    other = await harness.connect()
    await sender.send({"type": "send", "text": "partial_hold"})
    for client in (sender, other):
        await client.collect(lambda m: bool(of_type(m, "token")))

    await sender.ws.close()
    await asyncio.sleep(0.2)  # el gateway cierra la sesión que se fue
    harness.openclaw.release.set()
    await other.collect(lambda m: bool(of_type(m, "turn_end")))

    assert [t["text"] for t in of_type(other.messages, "token")] == ["reply-partial ", "fin"]
    assert [e["reason"] for e in of_type(other.messages, "turn_end")] == ["done"]
    assert_turns_paired(other.messages)
    # El que se fue: su turn_start y, como mucho, el turn_end de ese mismo turno (el socket lo
    # cerró él: el `closed` puede no llegarle), nunca dos.
    (start,) = of_type(sender.messages, "turn_start")
    ends = of_type(sender.messages, "turn_end")
    assert len(ends) <= 1 and all(e["turn_id"] == start["turn_id"] for e in ends)
    key = harness.session_key(other)
    assert [m for m, sk in harness.openclaw.calls if sk == key] == ["chat.send"]  # sin chat.abort


async def test_a_day_move_leaves_the_old_turn_to_the_others(harness, clock):
    """Decisión 6: una sesión que pasa a la conversación de hoy con un turno abierto en la de
    ayer cierra el suyo (`replaced`) y deja el canal; el turno sigue para la otra sesión."""
    mover = await harness.connect()
    stayer = await harness.connect(handshake={"conversation": mover.ready["conversation"]["id"]})
    yesterday = mover.ready["conversation"]["id"]
    await mover.send({"type": "send", "text": "partial_hold"})
    for client in (mover, stayer):
        await client.collect(lambda m: bool(of_type(m, "token")))

    clock.now = datetime(2026, 11, 15, 5, 30)  # pasado el corte de las 5:00
    msgs = await run_turn(mover, "hoy", ends=2)
    replaced, done = of_type(msgs, "turn_end")
    assert (replaced["reason"], done["reason"]) == ("replaced", "done")
    assert of_type(msgs, "turn_start")[0]["conversation"] != yesterday
    assert_turns_paired(mover.messages)

    harness.openclaw.release.set()
    await stayer.collect(lambda m: bool(of_type(m, "turn_end")))
    assert [e["reason"] for e in of_type(stayer.messages, "turn_end")] == ["done"]
    assert [t["text"] for t in of_type(stayer.messages, "token")] == ["reply-partial ", "fin"]
    assert [m for m, sk in harness.openclaw.calls if sk == session_key_for("main", yesterday)] == [
        "chat.send"
    ]


async def test_day_cutoff_moves_a_long_lived_session_at_its_next_turn(harness, clock):
    client = await harness.connect()
    yesterday = client.ready["conversation"]["id"]
    first = await run_turn(client, "uno")

    clock.now = datetime(2026, 11, 15, 5, 30)  # pasado el corte de las 5:00
    second = await run_turn(client, "dos")

    today = of_type(second, "turn_start")[0]["conversation"]
    assert of_type(first, "turn_start")[0]["conversation"] == yesterday
    assert today != yesterday
    assert said(harness, session_key_for("main", yesterday)) == ["uno"]
    assert said(harness, session_key_for("main", today)) == ["dos"]
    assert_turns_paired(client.messages)

    # 5.0 (decisión 7, regresión conocida hasta la 2c): un recordatorio de la conversación de
    # ayer va a su canal, en el que la tele ya no está (está en la de hoy): no le llega.
    await harness.push("se te olvidó algo", session_key=session_key_for("main", yesterday))
    await client.collect(timeout=0.5)
    assert len(of_type(client.messages, "turn_start")) == 2
    assert_turns_paired(client.messages)


async def test_push_of_a_conversation_reaches_the_session_that_has_it_open(harness, clock):
    """Dos sesiones del mismo cliente en conversaciones distintas: cada push, a la suya."""
    a = await harness.connect(handshake={"conversation": "new"})
    b = await harness.connect(handshake={"conversation": "new"})
    assert a.ready["conversation"]["id"] != b.ready["conversation"]["id"]

    await harness.push("para a", session_key=harness.session_key(a))
    await a.collect(lambda m: bool(of_type(m, "turn_end")))
    await harness.push("para b", session_key=harness.session_key(b))
    await b.collect(lambda m: bool(of_type(m, "turn_end")))

    assert [t["text"] for t in of_type(a.messages, "token")] == ["para a"]
    assert [t["text"] for t in of_type(b.messages, "token")] == ["para b"]
    assert of_type(a.messages, "turn_start")[0]["conversation"] == a.ready["conversation"]["id"]


@pytest.mark.xfail(
    strict=True,
    reason="Decisión 8 de la 5.0 PR 2: hasta la 2b.3, /v1 sigue en stream_response con su propio "
    "registro por clave y choca con el canal vivo de la conversación (409 turn_in_progress o el "
    "aborto antes de enviar del canal); la 2b.3 lo manda por el canal.",
)
async def test_v1_talks_in_a_conversation_with_a_live_channel(harness, clock):
    client = await harness.connect()
    await client.send({"type": "send", "text": "partial_hold"})
    await client.collect(lambda m: bool(of_type(m, "token")))
    body = {"model": "fogar-gateway", "messages": [{"role": "user", "content": "hola"}]}
    try:
        async with httpx.AsyncClient(
            base_url=f"http://127.0.0.1:{harness.port}",
            headers={"authorization": f"Bearer {VALID_KEY}"},
            timeout=5,
        ) as http:
            res = await http.post("/v1/chat/completions", json=body)
        assert res.status_code == 200, res.text
        assert res.json()["choices"][0]["message"]["content"] == "reply-hola"
    finally:
        harness.openclaw.release.set()
    await client.collect(lambda m: bool(of_type(m, "turn_end")))
    assert [e["reason"] for e in of_type(client.messages, "turn_end")] == ["done"]


async def test_a_new_session_that_fails_its_health_check_leaves_the_live_one_alone(harness):
    from fogar_gateway.main import app

    live = await harness.connect()

    async def down() -> bool:
        return False

    openclaw = app.state.openclaw
    openclaw.ping = down
    try:
        failed = await harness.open()
        closed = await closed_by_gateway(failed)
    finally:
        del openclaw.ping
    assert (closed.rcvd.code, closed.rcvd.reason) == (1011, "orchestrator unavailable")
    msgs = await run_turn(live, "sigo aquí")  # la viva sigue con su conversación
    assert of_type(msgs, "turn_start")[0]["conversation"] == live.ready["conversation"]["id"]


async def test_a_new_conversation_is_saved_only_if_the_session_opens(harness, db_engine):
    """Auditoría 3.0, C-3: un handshake con `"new"` que acaba en 1011 (OpenClaw caído) no deja
    una conversación vacía en `/admin/conversations` ni en el índice del push."""
    from fogar_gateway.main import app

    async def down() -> bool:
        return False

    openclaw = app.state.openclaw
    openclaw.ping = down
    try:
        for _ in range(3):
            failed = await harness.open(handshake={"conversation": "new"})
            closed = await closed_by_gateway(failed)
            assert closed.rcvd.code == 1011
    finally:
        del openclaw.ping
    assert await ConversationService(db_engine).list_rows() == []
    assert len(app.state.conversations.index) == 0

    client = await harness.connect(handshake={"conversation": "new"})
    (row,) = await ConversationService(db_engine).list_rows()
    assert row.record.id == client.ready["conversation"]["id"]
    msgs = await run_turn(client, "hola")
    assert of_type(msgs, "turn_start")[0]["conversation"] == row.record.id


async def test_a_revoked_session_ends_its_open_turn_before_the_close(harness, monkeypatch):
    monkeypatch.setattr(settings, "SESSION_REVALIDATE_INTERVAL_S", 0.1)
    client = await harness.connect()
    await client.send({"type": "send", "text": "wait-revoked"})
    await harness.openclaw.wait_held(1)
    await client.collect(lambda m: bool(of_type(m, "turn_start")))

    async with harness.admin() as admin:
        r = await admin.patch(f"/admin/clients/{CLIENT_ID}", json={"is_active": False})
        assert r.status_code == 200
    closed = await closed_by_gateway(client)
    assert (closed.rcvd.code, closed.rcvd.reason) == (4200, "credential revoked")
    assert [e["reason"] for e in of_type(client.messages, "turn_end")] == ["closed"]
    assert_turns_paired(client.messages)


# ── Conversación elegida ─────────────────────────────────────────────────────


async def test_a_chosen_conversation_never_changes_day_and_can_be_resumed(harness, clock):
    client = await harness.connect(handshake={"conversation": "new"})
    chosen = client.ready["conversation"]["id"]
    await run_turn(client, "uno")
    clock.now = datetime(2026, 11, 17, 12, 0)
    msgs = await run_turn(client, "dos")
    assert of_type(msgs, "turn_start")[0]["conversation"] == chosen
    await client.ws.close()

    again = await harness.connect(handshake={"conversation": chosen})
    assert again.ready["conversation"]["id"] == chosen
    await run_turn(again, "tres")
    assert said(harness, session_key_for("main", chosen)) == ["uno", "dos", "tres"]


async def test_a_private_conversation_of_someone_else_is_refused(harness, db_engine):
    """M20: el cliente de prueba es del hogar; la conversación privada de una persona (creada desde
    su dispositivo personal) no se abre desde él."""
    with Session(db_engine) as s:
        s.add(UserRecord(id="ana", username="ana", display_name="Ana", password_hash="x"))
        s.commit()
        s.add(ClientRecord(id="otro", name="Otro", key_hash=hash_key("otra-clave-de-cliente"),
                           user_id="ana"))
        s.commit()
    foreign = await ConversationService(db_engine).create("otro", "main")
    assert foreign.owner_user_id == "ana"

    for conversation in (foreign.id, "no-existe"):
        client = await harness.open(handshake={"conversation": conversation})
        closed = await closed_by_gateway(client)
        assert (closed.rcvd.code, closed.rcvd.reason) == (4100, "conversation not found")
        assert of_type(client.messages, "ready") == []


async def test_a_conversation_keeps_its_agent(harness):
    client = await harness.connect(handshake={"conversation": "new"})
    chosen = client.ready["conversation"]["id"]
    other = await harness.open(handshake={"conversation": chosen, "agent": "otro-agente"})
    closed = await closed_by_gateway(other)
    assert (closed.rcvd.code, closed.rcvd.reason) == (4101, "conversation agent mismatch")


async def test_an_uppercase_agent_talks_in_openclaws_lowercase_key(harness, db_engine):
    """Auditoría 3.0, C-1: OpenClaw 2026.10.5 pasa la clave entera a minúsculas y emite los
    eventos con ella. Un `default_agent` "Main" (fila de la 2.x) no puede dejar sin respuesta
    los turnos ni perder el push: el gateway usa ya el id y la clave en minúsculas."""
    with Session(db_engine) as s:
        record = s.get(ClientRecord, CLIENT_ID)
        assert record is not None
        record.default_agent = "Main"
        s.add(record)
        s.commit()

    client = await harness.connect()
    assert client.ready["agent"] == client.ready["conversation"]["agent"] == "main"
    msgs = await run_turn(client, "hola")
    assert [t["text"] for t in of_type(msgs, "token")] == ["reply-hola"]
    await harness.push("aviso")
    await client.collect(lambda m: len(of_type(m, "turn_end")) >= 2)
    assert [t["text"] for t in of_type(client.messages, "token")] == ["reply-hola", "aviso"]
    assert_turns_paired(client.messages)
    await client.ws.close()

    # Pedir "Main" en una conversación de "main" no es otro agente.
    chosen = client.ready["conversation"]["id"]
    again = await harness.connect(handshake={"conversation": chosen, "agent": "Main"})
    assert again.ready["conversation"]["id"] == chosen


async def test_a_conversation_saved_with_an_uppercase_agent_still_talks(harness, db_engine):
    """Auditoría 3.0 ronda 2, B-r2-1: una fila de antes de C-1 (agente "Main") se reabre por id
    con la clave en minúsculas, la que usa OpenClaw en sus eventos."""
    record = ConversationRecord(client_id=CLIENT_ID, agent="Main", kind="created", session_key="")
    record.session_key = f"agent:Main:fogar-{record.id}"
    with Session(db_engine, expire_on_commit=False) as s:
        s.add(record)
        s.commit()

    client = await harness.connect(handshake={"conversation": record.id})
    assert client.ready["conversation"]["agent"] == "main"
    msgs = await run_turn(client, "hola")
    assert [t["text"] for t in of_type(msgs, "token")] == ["reply-hola"]
    assert said(harness, session_key_for("main", record.id)) == ["hola"]


async def test_an_agent_id_openclaw_would_not_accept_is_an_invalid_handshake(harness, db_engine):
    """Auditoría 3.0, C-2: `:` o espacios romperían la clave `agent:{agente}:…`."""
    client = await harness.open(handshake={"agent": "x:y z/"})
    closed = await closed_by_gateway(client)
    assert closed.rcvd.code == 4000
    assert await ConversationService(db_engine).list_rows() == []


# ── turn_end.reason en cada camino ───────────────────────────────────────────


async def test_turn_end_reason_for_each_way_a_turn_ends(harness, monkeypatch):
    client = await harness.connect()

    def last_end():
        return of_type(client.messages, "turn_end")[-1]

    assert of_type(await run_turn(client, "hola"), "turn_end")[0]["reason"] == "done"
    assert of_type(await run_turn(client, "fail"), "turn_end")[0]["reason"] == "error"

    await client.send({"type": "send", "text": "wait-1"})  # se queda esperando
    await harness.openclaw.wait_held(1)
    msgs = await run_turn(client, "otra", ends=2)  # un send nuevo lo sustituye
    replaced, done = of_type(msgs, "turn_end")
    assert (replaced["reason"], done["reason"]) == ("replaced", "done")

    await client.send({"type": "send", "text": "wait-2"})
    await harness.openclaw.wait_held(2)
    await client.collect(lambda m: len(of_type(m, "turn_start")) == 5)
    await client.send({"type": "cancel"})
    await client.collect(lambda m: len(of_type(m, "turn_end")) == 5)
    assert last_end()["reason"] == "cancelled"

    monkeypatch.setattr(settings, "TURN_TIMEOUT_S", 0.3)
    assert of_type(await run_turn(client, "hold"), "turn_end")[0]["reason"] == "timeout"

    await harness.push("aviso")
    await client.collect(lambda m: len(of_type(m, "turn_end")) == 7)
    assert last_end()["reason"] == "done"
    assert of_type(client.messages, "turn_start")[-1]["origin"] == "push"
    assert of_type(client.messages, "interrupted") == []
    assert_turns_paired(client.messages)


# ── Un usuario inicia sesión y chatea (M20; antes, el panel con el ADMIN_TOKEN) ──


async def _login(harness, username: str = "sito") -> dict:
    """Alta del usuario (por la máquina) e inicio de sesión desde un dispositivo nuevo."""
    async with harness.admin() as admin:
        r = await admin.post("/admin/users", json={
            "username": username, "display_name": username.title(), "password": "contraseña larga",
        })
        assert r.status_code in (201, 409), r.text
    async with httpx.AsyncClient(base_url=f"http://127.0.0.1:{harness.port}") as http:
        r = await http.post("/auth/login", json={
            "username": username, "password": "contraseña larga", "device_name": "Web",
            "client_type": "web",
        })
        assert r.status_code == 201, r.text
        return r.json()


async def test_a_logged_in_device_chats_in_a_private_conversation(harness, caplog):
    session = await _login(harness)
    me = await harness.connect(handshake={"client_key": session["client_key"]})
    assert me.ready["session_id"].startswith(f"{session['client_id']}:")
    msgs = await run_turn(me, "hola desde la web")
    assert of_type(msgs, "turn_start")[0]["conversation"] == me.ready["conversation"]["id"]
    async with harness.admin() as admin:
        rows = (await admin.get("/admin/conversations")).json()
    (row,) = [r for r in rows if r["id"] == me.ready["conversation"]["id"]]
    assert row["owner_user_id"] == session["user"]["id"]  # privada: de su dispositivo
    assert session["client_key"] not in caplog.text


async def test_a_user_resumes_a_conversation_created_from_admin(harness):
    session = await _login(harness)
    bearer = {"authorization": f"Bearer {session['client_key']}"}
    async with httpx.AsyncClient(base_url=f"http://127.0.0.1:{harness.port}") as http:
        r = await http.post("/admin/conversations", json={}, headers=bearer)
        assert r.status_code == 201, r.text
        created = r.json()
    assert (created["client_id"], created["kind"], created["agent"]) == (
        session["client_id"], "created", "main"
    )

    me = await harness.connect(
        handshake={"client_key": session["client_key"], "conversation": created["id"]}
    )
    assert me.ready["conversation"] == {"id": created["id"], "agent": "main"}
    await run_turn(me, "hola")

    async with harness.admin() as admin:
        rows = (await admin.get("/admin/conversations",
                                params={"client_id": session["client_id"]})).json()
        history = (await admin.get(f"/admin/conversations/{created['id']}/messages")).json()
    (row,) = [r for r in rows if r["id"] == created["id"]]
    assert row["turn_count"] == 1 and row["last_turn_at"] is not None
    assert [(m["role"], m["text"]) for m in history["messages"]] == [
        ("user", "hola"),
        ("assistant", "reply-hola"),
    ]
    assert history["has_more"] is False
