"""Chat por HTTP (5.0, #412): `GET /events`, `GET /conversations/{id}`, `POST …/messages`,
`DELETE …/messages/{id}`, `POST …/cancel` y el historial con ids. TestClient con la app real y
el orquestador falso que reproduce `stream_response` por el canal."""

import json
from unittest.mock import AsyncMock

import pytest

from fogar_gateway.services import events as ev
from tests.fakes.orchestrator import OrchestratorEvent
from tests.integration.conftest import ADMIN_TOKEN, CLIENT_ID, VALID_KEY

BEARER = {"authorization": f"Bearer {VALID_KEY}"}


@pytest.fixture(autouse=True)
def short_streams(monkeypatch):
    monkeypatch.setattr(ev, "STREAM_MAX_S", 1.0)
    monkeypatch.setattr(ev, "KEEPALIVE_S", 0.2)


def _read(lines, until=None) -> list[tuple[int | None, dict]]:
    """Los eventos de un stream SSE (`lines`, su `iter_lines()`, sin comentarios ni `retry`),
    hasta `until(evento)` o el final del stream."""
    out: list[tuple[int | None, dict]] = []
    event_id = None
    for line in lines:
        if line.startswith("id: "):
            event_id = int(line[4:])
        elif line.startswith("data: "):
            data = json.loads(line[6:])
            out.append((event_id, data))
            event_id = None
            if until is not None and until(data):
                break
    return out


def _conversation(client) -> dict:
    r = client.post("/conversations", json={}, headers=BEARER)
    assert r.status_code == 201, r.text
    return r.json()


def _reply(mock_orchestrator, *tokens):
    async def stream(text, session_key=None):
        for token in tokens:
            yield OrchestratorEvent(type="token", content=token)
        yield OrchestratorEvent(type="status", content="done")

    mock_orchestrator.stream_response = stream


def test_events_needs_a_device(client):
    assert client.get("/events").status_code == 401
    r = client.get("/events", headers={"x-admin-token": ADMIN_TOKEN})
    assert r.status_code == 403 and r.json()["detail"] == "device_required"


def test_ready_first_then_a_message_and_its_turn(client, mock_orchestrator):
    conv = _conversation(client)
    _reply(mock_orchestrator, "hola", " qué tal")
    with client.stream("GET", "/events", headers=BEARER) as stream:
        lines = stream.iter_lines()
        assert next(lines) == "retry: 1000"
        ready_id, ready = None, None
        for line in lines:
            if line.startswith("id: "):
                ready_id = int(line[4:])
            if line.startswith("data: "):
                ready = json.loads(line[6:])
                break
        assert ready == {"type": "ready", "protocol": 5, "device_id": CLIENT_ID}
        r = client.post(f"/conversations/{conv['id']}/messages", json={"text": "hola"},
                        headers=BEARER)
        assert r.status_code == 202, r.text
        message_id = r.json()["message_id"]
        got = _read(lines, until=lambda e: e["type"] == "turn_end")
    kinds = [(e["type"], e.get("state") or e.get("text") or e.get("reason")) for _, e in got]
    assert ("message", "pending") in kinds or ("message", "delivered") in kinds
    assert [k for k in kinds if k[0] != "message"] == [
        ("turn_start", None), ("token", "hola"), ("token", " qué tal"), ("turn_end", "done"),
    ]
    message = next(e for _, e in got if e["type"] == "message")
    assert message["id"] == message_id and message["from"]["self"] is True
    start = next(e for _, e in got if e["type"] == "turn_start")
    assert start["conversation"] == conv["id"] and start["by"]["self"] is True
    ids = [i for i, _ in got]
    assert all(i is not None and i > ready_id for i in ids) and ids == sorted(ids)


def test_post_without_events_uses_a_transient_sender(client, mock_orchestrator):
    conv = _conversation(client)
    _reply(mock_orchestrator, "ok")
    r = client.post(f"/conversations/{conv['id']}/messages", json={"text": "hola"}, headers=BEARER)
    assert r.status_code == 202 and r.json()["message_id"]
    r = client.post(f"/conversations/{conv['id']}/messages", json={"text": "  "}, headers=BEARER)
    assert r.status_code == 422


def test_create_with_a_message_returns_its_id(client, mock_orchestrator):
    _reply(mock_orchestrator, "ok")
    r = client.post("/conversations", json={"message": "hola"}, headers=BEARER)
    assert r.status_code == 201 and r.json()["message_id"]


def test_resume_replays_and_an_unknown_id_is_a_resync(client, mock_orchestrator):
    conv = _conversation(client)
    _reply(mock_orchestrator, "uno")
    with client.stream("GET", "/events", headers=BEARER) as stream:
        lines = stream.iter_lines()
        first = _read(lines, until=lambda e: e["type"] == "ready")
        client.post(f"/conversations/{conv['id']}/messages", json={"text": "a"}, headers=BEARER)
        got = _read(lines, until=lambda e: e["type"] == "turn_end")
    after_ready = first[0][0]
    # Reanuda desde ready: lo mismo otra vez, sin un ready con id.
    with client.stream("GET", "/events", headers={**BEARER, "last-event-id": str(after_ready)}) as s:
        again = _read(s.iter_lines(), until=lambda e: e["type"] == "turn_end")
    assert again[0] == (None, {"type": "ready", "protocol": 5, "device_id": CLIENT_ID})
    assert [i for i, _ in again[1:]] == [i for i, _ in got]
    with client.stream("GET", "/events", headers={**BEARER, "last-event-id": "1"}) as s:
        resync = _read(s.iter_lines(), until=lambda e: e["type"] == "resync")
    assert [e["type"] for _, e in resync] == ["ready", "resync"]
    assert resync[1][0] is not None


def test_live_state_and_as_of(client):
    conv = _conversation(client)
    before = ev.current_id()
    r = client.get(f"/conversations/{conv['id']}", headers=BEARER)
    assert r.status_code == 200
    body = r.json()
    assert body["id"] == conv["id"] and body["turn"] is None and body["pending"] == []
    assert body["as_of"] >= before
    assert client.get("/conversations/nope", headers=BEARER).status_code == 404


@pytest.mark.parametrize(("push_enabled", "shown"), [(True, True), (False, False)])
def test_live_state_hides_a_push_the_device_does_not_get(
    client, db_engine, monkeypatch, push_enabled, shown
):
    """Como por /events: con `push_enabled=false` el aviso en curso no sale en la instantánea."""
    from types import SimpleNamespace

    from sqlmodel import Session

    from fogar_gateway.db.models import ClientRecord

    conv = _conversation(client)
    with Session(db_engine) as s:
        rec = s.get(ClientRecord, CLIENT_ID)
        rec.push_enabled = push_enabled
        s.add(rec)
        s.commit()
    turn = SimpleNamespace(
        id="t1", conversation_id=conv["id"], origin="push", by_client_id=None, by_name=None,
        session_key=None, cancel_reason=None, text="aviso",
    )
    channel = SimpleNamespace(current_turn=turn, pending_messages=lambda: [])
    monkeypatch.setattr(client.app.state.channel_hub, "get", lambda key: channel)
    body = client.get(f"/conversations/{conv['id']}", headers=BEARER).json()
    assert (body["turn"] is not None) is shown


def test_cancel_and_withdraw_without_anything_running(client):
    conv = _conversation(client)
    r = client.post(f"/conversations/{conv['id']}/cancel", headers=BEARER)
    assert r.status_code == 200 and r.json() == {"cancelled": False}
    r = client.delete(f"/conversations/{conv['id']}/messages/nope", headers=BEARER)
    assert r.status_code == 404 and r.json()["detail"] == "not_pending"
    r = client.post(f"/conversations/{conv['id']}/cancel", headers={"x-admin-token": ADMIN_TOKEN})
    assert r.status_code == 403


@pytest.mark.parametrize(
    ("open_turn", "body", "cancelled", "head"),
    [
        ("T1", {"turn_id": "T1"}, True, False),  # el turno que se ve: se para
        ("T2", {"turn_id": "T1"}, False, False),  # ya es otro: nada
        (None, {"turn_id": "T1"}, False, False),  # ya acabó: el pendiente no se toca
        (None, None, True, True),  # sin campo, como antes: el siguiente sin empezar
        ("T1", None, True, False),
    ],
)
def test_cancel_with_turn_id_only_stops_that_turn(client, monkeypatch, open_turn, body, cancelled, head):
    """«Enviar ahora» (web): si el turno acaba justo antes del POST, no para el mensaje pendiente."""
    from types import SimpleNamespace

    conv = _conversation(client)
    turn = SimpleNamespace(id=open_turn) if open_turn else None
    channel = SimpleNamespace(
        current_turn=turn,
        cancel=AsyncMock(return_value=turn),
        cancel_head=AsyncMock(return_value=True),
    )
    monkeypatch.setattr(client.app.state.channel_hub, "get", lambda key: channel)
    kwargs = {"json": body} if body is not None else {}
    r = client.post(f"/conversations/{conv['id']}/cancel", headers=BEARER, **kwargs)
    assert r.status_code == 200 and r.json() == {"cancelled": cancelled}
    assert channel.cancel_head.await_count == (1 if head else 0)
    assert channel.cancel.await_count == (1 if open_turn and (body is None or open_turn == "T1") else 0)


def test_cancel_rejects_unknown_fields(client):
    conv = _conversation(client)
    r = client.post(f"/conversations/{conv['id']}/cancel", headers=BEARER, json={"turn": "T1"})
    assert r.status_code == 422


def test_revoking_the_device_closes_its_events(client, admin_headers):
    with client.stream("GET", "/events", headers=BEARER) as stream:
        _read(stream.iter_lines(), until=lambda e: e["type"] == "ready")
    events = client.app.state.events
    assert events.device(CLIENT_ID) is not None  # sigue vivo para reanudar
    assert client.delete(f"/home/devices/{CLIENT_ID}", headers=admin_headers).status_code == 204
    assert events.device(CLIENT_ID) is None


def _connect(client, last_id: int | None = None) -> list[tuple[int | None, dict]]:
    """Un stream entero (hasta `resync` o su fin, ~1 s)."""
    headers = BEARER if last_id is None else {**BEARER, "last-event-id": str(last_id)}
    with client.stream("GET", "/events", headers=headers) as s:
        return _read(s.iter_lines(), until=lambda e: e["type"] == "resync")


def test_a_device_that_expired_resyncs_even_without_events_since(client):
    """Su `DeviceEvents` caduca sin que nadie publique un evento (solo hubo WebSocket): el búfer
    nuevo no puede garantizar lo de antes, así que el `Last-Event-ID` viejo es `resync`."""
    ready_id = _connect(client)[0][0]
    assert ready_id is not None
    client.app.state.events.close_device(CLIENT_ID)  # lo mismo que caducar a los RESUME_S
    assert [e["type"] for _, e in _connect(client, ready_id)] == ["ready", "resync"]


def test_a_fresh_device_resumes_from_its_own_ready_without_resync(client):
    ready_id = _connect(client)[0][0]
    assert [e["type"] for _, e in _connect(client, ready_id)] == ["ready"]


@pytest.mark.parametrize("change", [
    ("patch", {"is_active": False}),
    ("patch", {"allowed_agents": ["main"]}),
    ("rotate", None),
])
def test_disabling_rotating_or_new_agents_close_its_events_at_once(
    client, admin_headers, change
):
    """Como al retirarlo: su `/events` se corta ya (no a los ~25 s del stream). Con otros
    `allowed_agents`, al reconectar se rehace con la configuración nueva (y `resync`)."""
    _connect(client)
    events = client.app.state.events
    assert events.device(CLIENT_ID) is not None
    kind, body = change
    if kind == "patch":
        r = client.patch(f"/home/devices/{CLIENT_ID}", json=body, headers=admin_headers)
    else:
        r = client.post(f"/home/devices/{CLIENT_ID}/rotate-key", headers=admin_headers)
    assert r.status_code == 200, r.text
    assert events.device(CLIENT_ID) is None


def test_history_has_ids_and_pages_before_one(client, mock_orchestrator):
    conv = _conversation(client)
    rows = [
        {"role": "user" if i % 2 == 0 else "assistant", "content": f"m{i}", "timestamp": i,
         "__openclaw": {"id": f"id-{i}", "seq": i}}
        for i in range(6)
    ]
    mock_orchestrator.request = AsyncMock(return_value={"messages": rows, "hasMore": False})
    r = client.get(f"/conversations/{conv['id']}/messages", headers=BEARER)
    assert [m["id"] for m in r.json()["messages"]] == [f"id-{i}" for i in range(6)]
    r = client.get(f"/conversations/{conv['id']}/messages?before=id-4&limit=2", headers=BEARER)
    body = r.json()
    assert [m["id"] for m in body["messages"]] == ["id-2", "id-3"] and body["has_more"] is True
    r = client.get(f"/conversations/{conv['id']}/messages?before=nope", headers=BEARER)
    assert r.status_code == 404
