"""G-r3-1: un surrogate suelto (`\\ud800` escapado en JSON, válido en JSON pero no en UTF-8)
venga del cliente o de OpenClaw, no deja la sesión sin salida ni la conversación ilegible."""

import json

import httpx

from tests.integration.conftest import VALID_KEY
from tests.integration.harness.conftest import assert_turns_paired, of_type, turn_ended


def tokens(messages, turn_id):
    return "".join(m["text"] for m in of_type(messages, "token") if m["turn_id"] == turn_id)


async def test_send_with_lone_surrogate_gets_a_normal_turn(harness):
    client = await harness.connect()
    await client.ws.send('{"type": "send", "text": "a\\ud800b"}')  # escapado: JSON válido
    msgs = await client.collect(turn_ended)
    (start,) = of_type(msgs, "turn_start")
    assert tokens(msgs, start["turn_id"]) == "reply-ab"

    await client.send({"type": "send", "text": "hola"})  # la sesión sigue viva
    msgs = await client.collect(lambda m: len(of_type(m, "turn_end")) >= 2)
    assert tokens(msgs, of_type(msgs, "turn_start")[1]["turn_id"]) == "reply-hola"
    assert of_type(msgs, "error") == []
    assert_turns_paired(msgs)

    async with harness.admin() as admin:
        res = await admin.get(f"/admin/conversations/{client.ready['conversation']['id']}/messages")
    assert res.status_code == 200


async def test_lone_surrogate_from_openclaw_is_dropped(harness):
    client = await harness.connect()
    await harness.push("x\ud800y")  # el falso lo escapa en JSON, como Node
    msgs = await client.collect(turn_ended)
    (start,) = of_type(msgs, "turn_start")
    assert tokens(msgs, start["turn_id"]) == "xy"

    harness.openclaw.add_session(
        harness.session_key(), messages=[{"role": "user", "content": "c\udc00d", "timestamp": 1}]
    )
    async with harness.admin() as admin:
        res = await admin.get(f"/admin/conversations/{client.ready['conversation']['id']}/messages")
    assert res.status_code == 200
    assert "cd" in [m["text"] for m in res.json()["messages"]]

    await client.send({"type": "send", "text": "hola"})
    await client.collect(lambda m: len(of_type(m, "turn_end")) >= 2)
    assert_turns_paired(client.messages)


async def test_push_with_emoji_split_between_deltas_is_delivered_once(harness):
    """B-r4-1: las mitades de un emoji en dos deltas solo-deltaText se juntan; el final con el
    texto entero no se reemite."""
    client = await harness.connect()
    oc, sk = harness.openclaw, harness.session_key()
    run_id = oc._new_run()

    def chat(**payload):
        return json.dumps(
            {"type": "event", "event": "chat", "payload": {"sessionKey": sk, "runId": run_id, **payload}}
        )

    msg = {"role": "assistant", "content": [{"type": "text", "text": "Hola "}]}
    final = {"role": "assistant", "content": [{"type": "text", "text": "Hola amigo \U0001f600 qué tal"}]}
    (ws,) = oc._conns
    await ws.send(oc._agent(sk, "start", run_id))
    await ws.send(chat(state="delta", deltaText="Hola ", message=msg))
    await ws.send(chat(state="delta", deltaText="amigo \ud83d"))
    await ws.send(chat(state="delta", deltaText="\ude00 qué tal"))
    await ws.send(chat(state="final", message=final))
    await ws.send(oc._agent(sk, "end", run_id))
    msgs = await client.collect(turn_ended)
    (start,) = of_type(msgs, "turn_start")
    assert tokens(msgs, start["turn_id"]) == "Hola amigo \U0001f600 qué tal"
    assert_turns_paired(msgs)


async def test_v1_chat_with_lone_surrogate(harness):
    # Con texto alrededor: un surrogate solo se queda en nada, y eso es un 400 (D-r6-3).
    body = '{"model": "fogar-gateway", "messages": [{"role": "user", "content": "a\\ud800b"}]%s}'
    async with httpx.AsyncClient(
        base_url=f"http://127.0.0.1:{harness.port}",
        headers={"authorization": f"Bearer {VALID_KEY}", "content-type": "application/json"},
        timeout=10,
    ) as http:
        res = await http.post("/v1/chat/completions", content=body % "")
        assert res.status_code == 200
        assert res.json()["choices"][0]["message"]["content"] == "reply-ab"

        async with http.stream("POST", "/v1/chat/completions", content=body % ', "stream": true') as res:
            assert res.status_code == 200
            chunks = [line async for line in res.aiter_lines() if line.startswith("data: {")]
    text = "".join(
        json.loads(c.removeprefix("data: "))["choices"][0]["delta"].get("content", "") for c in chunks
    )
    assert text == "reply-ab"


async def test_lone_surrogate_from_the_transcriber_does_not_break_the_session(harness):
    """G-r3-1: lo que no se sanea a la entrada (aquí la transcripción del STT) se sanea al salir:
    el mensaje llega sin el surrogate y la sesión sigue."""
    harness.transcriber.on_end = [("a\ud800b", True)]
    client = await harness.connect(input_mode="audio", output_mode=("audio", "text"))
    await client.send_audio(b"\x00" * 3200)
    await client.send({"type": "end"})
    msgs = await client.collect(lambda m: bool(of_type(m, "transcription")))
    assert of_type(msgs, "transcription")[0]["text"] == "ab"

    await client.send({"type": "send", "text": "hola"})
    msgs = await client.collect(turn_ended)
    assert tokens(msgs, of_type(msgs, "turn_start")[0]["turn_id"]) == "reply-hola"
    assert_turns_paired(msgs)
