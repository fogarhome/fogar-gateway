"""`/v1` por el canal de su conversación (5.0 PR 2b.3, #406), visto desde fuera: lo que
recibe Home Assistant por HTTP y lo que ven las sesiones WebSocket adjuntas a esa conversación.

OpenClaw (falso) en `steer`, su modo por defecto (2026.11.7): un `chat.send` con otro run en
marcha se pliega en él. `/v1` nunca debe plegarse ni dejar que otro se pliegue en el suyo: su
respuesta es la de su propio run.
"""

import asyncio

import httpx
import pytest
from sqlmodel import Session, select

from fogar_gateway.core.config import settings
from fogar_gateway.db.models import ClientRecord, ConversationRecord
from fogar_gateway.services.auth import hash_key
from fogar_gateway.services.conversations import session_key_for
from tests.integration.harness.conftest import assert_turns_paired, audio, of_type

HA_ID = "home_assistant"
HA_NAME = "Home Assistant salón"
HA_KEY = "ha-key-0123456789"


@pytest.fixture
def ha_client(db_engine):
    """Un segundo cliente, el de Home Assistant (del hogar, como el de los tests)."""
    with Session(db_engine) as s:
        s.add(ClientRecord(id=HA_ID, name=HA_NAME, key_hash=hash_key(HA_KEY), is_active=True))
        s.commit()


def _http(harness, timeout: float = 10) -> httpx.AsyncClient:
    return httpx.AsyncClient(
        base_url=f"http://127.0.0.1:{harness.port}",
        headers={"authorization": f"Bearer {HA_KEY}"},
        timeout=timeout,
    )


def _body(text: str, stream: bool = False) -> dict:
    return {"model": "fogar-gateway", "messages": [{"role": "user", "content": text}], "stream": stream}


async def _ask(harness, text: str) -> httpx.Response:
    async with _http(harness) as http:
        return await http.post("/v1/chat/completions", json=_body(text))


def _reply(res: httpx.Response) -> str:
    assert res.status_code == 200, res.text
    return res.json()["choices"][0]["message"]["content"]


async def _ha_conversation(harness, db_engine) -> str:
    """La conversación por defecto de hoy de Home Assistant (la crea con un primer turno)."""
    assert _reply(await _ask(harness, "uno")) == "reply-uno"
    with Session(db_engine) as s:
        (row,) = s.exec(select(ConversationRecord).where(ConversationRecord.client_id == HA_ID)).all()
        return row.id


async def _watcher(harness, conversation_id: str, output_mode=("text",)):
    """Una sesión WebSocket (otro aparato) adjunta a la conversación de Home Assistant."""
    return await harness.connect(output_mode=output_mode, handshake={"conversation": conversation_id})


async def _sent_to_openclaw(harness, key: str, n: int, timeout: float = 5.0) -> None:
    async with asyncio.timeout(timeout):
        while [m for m, sk in harness.openclaw.calls if sk == key].count("chat.send") < n:
            await asyncio.sleep(0.01)


def _sends(harness, key: str) -> int:
    return [m for m, sk in harness.openclaw.calls if sk == key].count("chat.send")


def assert_socket_invariants(messages: list) -> None:
    """Por socket: un turn_end por turn_start (con su turn_seq & 0xFFFF) y, antes del turn_start
    de cada turno de usuario, el `message` `delivered` de su mensaje."""
    assert_turns_paired(messages)
    delivered = user_turns = 0
    for m in messages:
        if not isinstance(m, dict):
            continue
        if m.get("type") == "message" and m["state"] == "delivered":
            delivered += 1
        elif m.get("type") == "turn_start" and m["origin"] == "user":
            user_turns += 1
            assert delivered >= user_turns, "turn_start antes del `message` de su mensaje"


async def test_v1_message_visible_to_subscribers(harness, ha_client, db_engine):
    """Lo que entra por /v1 lo ven las sesiones de la conversación: su `message` (origen: el
    aparato de Home Assistant) y su turno (con `by`), sin audio para nadie."""
    conversation = await _ha_conversation(harness, db_engine)
    tv = await _watcher(harness, conversation, output_mode=("text", "audio"))

    assert _reply(await _ask(harness, "hola")) == "reply-hola"
    await tv.collect(lambda m: bool(of_type(m, "turn_end")))

    (message,) = of_type(tv.messages, "message")
    assert message["text"] == "hola" and message["conversation"] == conversation
    assert message["from"] == {"source": "device", "device_id": HA_ID, "name": HA_NAME, "self": False}
    assert message["state"] == "delivered"
    (start,) = of_type(tv.messages, "turn_start")
    assert (start["origin"], start["conversation"]) == ("user", conversation)
    assert start["by"] == {"device_id": HA_ID, "name": HA_NAME, "self": False}
    assert [t["text"] for t in of_type(tv.messages, "token")] == ["reply-hola"]
    assert [e["reason"] for e in of_type(tv.messages, "turn_end")] == ["done"]
    assert audio(tv.messages) == []  # el turno es de Home Assistant: no suena en la tele
    assert_socket_invariants(tv.messages)


async def test_v1_waits_for_active_turn_and_gets_its_own_reply(harness, ha_client, db_engine):
    """Con un turno en marcha en la conversación, el mensaje de /v1 espera a que termine (no se
    envía a OpenClaw antes, o `steer` lo plegaría en ese turno) y recibe solo su respuesta."""
    harness.openclaw.steer = True
    conversation = await _ha_conversation(harness, db_engine)
    key = session_key_for("main", conversation)
    tv = await _watcher(harness, conversation)
    await tv.send({"type": "send", "text": "partial_hold"})
    await tv.collect(lambda m: bool(of_type(m, "token")))

    ha = asyncio.create_task(_ask(harness, "hola"))
    await asyncio.sleep(0.3)
    assert not ha.done()
    assert _sends(harness, key) == 2  # «uno» y el de la tele: el de /v1 espera
    harness.openclaw.release.set()

    assert _reply(await ha) == "reply-hola"
    await tv.collect(lambda m: len(of_type(m, "turn_end")) >= 2)
    first, second = of_type(tv.messages, "turn_start")
    assert first["by"]["self"] is True and second["by"]["device_id"] == HA_ID
    assert [t["text"] for t in of_type(tv.messages, "token")] == ["reply-partial ", "fin", "reply-hola"]
    assert [e["reason"] for e in of_type(tv.messages, "turn_end")] == ["done", "done"]
    assert_socket_invariants(tv.messages)


async def test_v1_talks_while_nothing_else_runs_after_a_ws_send_during_its_run(
    harness, ha_client, db_engine
):
    """Un `send` del WebSocket durante el run de /v1 no se envía hasta que ese run termina
    (`steer` lo plegaría en él y cambiaría la respuesta de /v1); después va como siempre."""
    harness.openclaw.steer = True
    conversation = await _ha_conversation(harness, db_engine)
    key = session_key_for("main", conversation)
    tv = await _watcher(harness, conversation)

    ha = asyncio.create_task(_ask(harness, "hold"))
    await tv.collect(lambda m: bool(of_type(m, "turn_start")))  # el turno de /v1, abierto
    await tv.send({"type": "send", "text": "dos"})
    await asyncio.sleep(0.3)
    assert _sends(harness, key) == 2  # «uno» y el de /v1: el de la tele espera
    harness.openclaw.release.set()

    assert _reply(await ha) == "reply-hold"
    await tv.collect(lambda m: len(of_type(m, "turn_end")) >= 2)
    ha_start, tv_start = of_type(tv.messages, "turn_start")
    assert ha_start["by"]["device_id"] == HA_ID and tv_start["by"]["self"] is True
    assert [t["text"] for t in of_type(tv.messages, "token")] == ["reply-hold", "reply-dos"]
    assert [(m["text"], m["state"]) for m in of_type(tv.messages, "message")] == [
        ("hold", "delivered"),
        ("dos", "delivered"),  # enviado con la conversación ya libre: entregado, sin espera
    ]
    assert_socket_invariants(tv.messages)


async def test_two_v1_requests_go_one_after_the_other(harness, ha_client, db_engine):
    """Dos peticiones de /v1 a la vez en la misma conversación: ninguna recibe 409; la segunda
    espera a la primera y cada una recibe su respuesta."""
    harness.openclaw.steer = True
    conversation = await _ha_conversation(harness, db_engine)
    key = session_key_for("main", conversation)

    first = asyncio.create_task(_ask(harness, "hold"))
    await _sent_to_openclaw(harness, key, 2)
    second = asyncio.create_task(_ask(harness, "hola"))
    await asyncio.sleep(0.3)
    assert _sends(harness, key) == 2 and not second.done()
    harness.openclaw.release.set()

    assert (_reply(await first), _reply(await second)) == ("reply-hold", "reply-hola")


async def test_v1_client_that_leaves_aborts_only_its_run(harness, ha_client, db_engine):
    """Home Assistant corta la petición: su run (y solo él, por su runId) se aborta y su turno
    termina `cancelled` para las sesiones adjuntas, que siguen hablando."""
    conversation = await _ha_conversation(harness, db_engine)
    key = session_key_for("main", conversation)
    tv = await _watcher(harness, conversation)

    async with _http(harness, timeout=0.5) as http:
        with pytest.raises(httpx.TimeoutException):
            await http.post("/v1/chat/completions", json=_body("hold"))
    await tv.collect(lambda m: bool(of_type(m, "turn_end")))

    assert [e["reason"] for e in of_type(tv.messages, "turn_end")] == ["cancelled"]
    (abort,) = harness.openclaw.aborts
    assert abort["sessionKey"] == key and abort.get("runId")  # su run, nunca la clave entera

    await tv.send({"type": "send", "text": "sigo"})
    await tv.collect(lambda m: len(of_type(m, "turn_end")) >= 2)
    assert [t["text"] for t in of_type(tv.messages, "token")][-1] == "reply-sigo"
    assert_socket_invariants(tv.messages)


async def test_v1_timeout_ends_its_turn_and_aborts_its_run(harness, ha_client, db_engine, monkeypatch):
    """Sin progreso en TURN_TIMEOUT_S, /v1 recibe `turn_timeout` (502, como antes); su turno
    termina `timeout` con su TURN_ERROR para los adjuntos y su run se aborta por su runId."""
    conversation = await _ha_conversation(harness, db_engine)
    key = session_key_for("main", conversation)
    tv = await _watcher(harness, conversation)
    monkeypatch.setattr(settings, "TURN_TIMEOUT_S", 0.5)

    res = await _ask(harness, "hold")
    assert (res.status_code, res.json()) == (502, {"error": "turn_timeout"})
    await tv.collect(lambda m: bool(of_type(m, "turn_end")))
    (error,) = of_type(tv.messages, "error")
    assert (error["code"], error["message"]) == ("TURN_ERROR", "turn_timeout")
    assert [e["reason"] for e in of_type(tv.messages, "turn_end")] == ["timeout"]
    async with asyncio.timeout(5):
        while not harness.openclaw.aborts:
            await asyncio.sleep(0.01)
    assert [(a["sessionKey"], bool(a.get("runId"))) for a in harness.openclaw.aborts] == [(key, True)]
    assert_socket_invariants(tv.messages)


async def test_v1_stream_through_the_channel(harness, ha_client, db_engine):
    """Con `stream: true`, los fragmentos SSE son los tokens de su propio run."""
    conversation = await _ha_conversation(harness, db_engine)
    tv = await _watcher(harness, conversation)
    async with _http(harness) as http:
        res = await http.post("/v1/chat/completions", json=_body("hola", stream=True))
    assert res.status_code == 200
    assert '"content": "reply-hola"' in res.text and res.text.rstrip().endswith("data: [DONE]")
    await tv.collect(lambda m: bool(of_type(m, "turn_end")))
    assert_socket_invariants(tv.messages)
