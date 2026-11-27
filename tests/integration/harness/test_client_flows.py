"""Los flujos principales del gateway vistos desde el cliente (arnés e2e, M7a).

Cada test dice qué hace el orquestador (responde, se queda esperando, falla, se cae,
inicia un turno) y comprueba solo lo que recibe el cliente por su WebSocket.
"""

import asyncio

import httpx

from tests.integration.conftest import VALID_KEY
from tests.integration.harness.conftest import (
    assert_turns_paired,
    audio,
    cancelled,
    of_type,
    turn_ended,
)

AUDIO_FRAME = b"\x00" * 3200  # 50 ms de PCM Float32 a 16 kHz


def tokens(messages, turn_id):
    return "".join(m["text"] for m in of_type(messages, "token") if m["turn_id"] == turn_id)


def turn_errors(messages):
    return [(e["code"], e["message"], e["fatal"]) for e in of_type(messages, "error")]


async def test_text_turn_streams_the_reply(harness):
    client = await harness.connect()
    await client.send({"type": "send", "text": "hola"})
    msgs = await client.collect(turn_ended)

    (start,) = of_type(msgs, "turn_start")
    assert tokens(msgs, start["turn_id"]) == "reply-hola"
    assert of_type(msgs, "error") == []
    assert_turns_paired(msgs)


async def test_voice_turn_transcribes_and_speaks_the_reply(harness):
    harness.transcriber.on_end = [("enciende la luz", True)]
    client = await harness.connect(input_mode="audio", output_mode=("audio", "text"))
    assert client.ready["live_capabilities"]["transcriber"] is True
    assert client.ready["live_capabilities"]["tts"] is True

    await client.send_audio(AUDIO_FRAME)
    await client.send({"type": "end"})
    msgs = await client.collect(lambda m: bool(of_type(m, "transcription")))
    (transcription,) = of_type(msgs, "transcription")
    assert transcription["text"] == "enciende la luz"
    assert of_type(msgs, "turn_start") == []  # el cliente decide cuándo enviarla

    pcm = harness.tts.chunk * harness.tts.chunks_per_reply
    await client.send({"type": "send", "text": transcription["text"]})
    msgs = await client.collect(
        lambda m: turn_ended(m) and sum(len(f) - 3 for f in audio(m)) >= len(pcm)
    )

    (start,) = of_type(msgs, "turn_start")
    assert tokens(msgs, start["turn_id"]) == "reply-enciende la luz"
    frames = audio(msgs)
    assert all(f[0] == 0xA1 and int.from_bytes(f[1:3], "big") == start["turn_seq"] for f in frames)
    assert b"".join(f[3:] for f in frames) == pcm
    assert_turns_paired(msgs)


async def test_two_phrases_in_a_row_each_get_their_final(harness):
    """A-r5-1: el transcriber cierra 1000 tras cada final (como el real); el gateway reabre y
    la segunda frase recibe su propia final, no `phrase_interrupted`."""
    harness.transcriber.on_end = [("enciende la luz", True)]
    client = await harness.connect(input_mode="audio", output_mode=("text",))
    for n in (1, 2):
        await _until(lambda n=n: len(harness.transcriber.configs) == n)  # su conexión ya tiene `ready`
        for _ in range(4):
            await client.send_audio(AUDIO_FRAME)
            await asyncio.sleep(0.05)
        await client.send({"type": "end"})
        await client.collect(lambda m, n=n: len(of_type(m, "transcription")) == n)

    assert [t["text"] for t in of_type(client.messages, "transcription")] == ["enciende la luz"] * 2
    assert of_type(client.messages, "status") == []  # ningún phrase_interrupted


async def _until(cond, timeout: float = 5.0) -> None:
    async with asyncio.timeout(timeout):
        while not cond():
            await asyncio.sleep(0.01)


async def test_speaking_over_a_running_turn_never_interrupts_it(harness):
    """5.0 (#406): el barge-in sale del gateway. Hablar encima solo trae la transcripción; el
    turno sigue hasta que el cliente lo corta de forma explícita (`cancel`)."""
    harness.transcriber.on_audio = [("espera un momento", False), ("espera un momento", True)]
    client = await harness.connect(input_mode="audio", output_mode=("text",))
    assert "barge_in" not in client.ready["live_capabilities"]
    assert "barge_in" not in client.ready["requested_capabilities"]

    await client.send({"type": "send", "text": "hold"})  # el orquestador se queda esperando
    await client.collect(lambda m: bool(of_type(m, "turn_start")))
    await client.send_audio(AUDIO_FRAME)
    msgs = await client.collect(lambda m: bool(of_type(m, "transcription")))
    msgs = await client.collect(timeout=0.3)
    assert of_type(msgs, "transcription_partial")[0]["text"] == "espera un momento"
    assert of_type(msgs, "transcription")[0]["text"] == "espera un momento"
    assert of_type(msgs, "turn_end") == []  # ni la parcial ni la final cortan nada

    await client.send({"type": "cancel"})
    msgs = await client.collect(lambda m: bool(cancelled(m)))
    (start,) = of_type(msgs, "turn_start")
    (end,) = of_type(msgs, "turn_end")
    assert end == {"type": "turn_end", "turn_id": start["turn_id"], "reason": "cancelled"}
    assert_turns_paired(msgs)


async def test_orchestrator_push_reaches_the_client_as_one_turn(harness):
    client = await harness.connect()
    await harness.push("tienes una reunión en 5 minutos")
    await client.collect(turn_ended)
    msgs = await client.collect(timeout=0.3)  # nada más detrás: ningún turno duplicado

    (start,) = of_type(msgs, "turn_start")
    assert len(of_type(msgs, "turn_end")) == 1
    assert tokens(msgs, start["turn_id"]) == "tienes una reunión en 5 minutos"
    assert_turns_paired(msgs)


async def test_orchestrator_dropping_mid_turn_ends_the_turn_with_a_stable_error(harness):
    client = await harness.connect()
    await client.send({"type": "send", "text": "drop"})  # responde a medias y se cae

    def orchestrator_down(m):
        return [
            s
            for s in of_type(m, "status")
            if s["service"] == "orchestrator" and s["state"] in ("unavailable", "reconnecting")
        ]

    msgs = await client.collect(lambda m: turn_ended(m) and bool(orchestrator_down(m)))

    (start,) = of_type(msgs, "turn_start")
    assert turn_errors(msgs) == [("TURN_ERROR", "orchestrator_disconnected", False)]
    assert of_type(msgs, "error")[0]["turn_id"] == start["turn_id"]
    assert_turns_paired(msgs)


async def test_orchestrator_failure_ends_the_turn_and_the_session_goes_on(harness):
    client = await harness.connect()
    await client.send({"type": "send", "text": "fail"})  # el orquestador rechaza el turno
    msgs = await client.collect(turn_ended)

    (start,) = of_type(msgs, "turn_start")
    assert turn_errors(msgs) == [("TURN_ERROR", "orchestrator_error", False)]
    assert of_type(msgs, "error")[0]["turn_id"] == start["turn_id"]
    leaked = [t for t in client.raw_texts if "provider exploded" in t or "UNAVAILABLE" in t]
    assert leaked == []

    await client.send({"type": "send", "text": "otra vez"})
    msgs = await client.collect(lambda m: len(of_type(m, "turn_end")) == 2)
    second = of_type(msgs, "turn_start")[1]
    assert tokens(msgs, second["turn_id"]) == "reply-otra vez"
    assert_turns_paired(msgs)


async def test_agent_failing_after_accepting_ends_the_turn_at_once(harness):
    # #300: OpenClaw acepta el turno y luego el agente falla (chat state:error).
    client = await harness.connect()
    await client.send({"type": "send", "text": "agent_error"})
    msgs = await client.collect(turn_ended, timeout=3)

    (start,) = of_type(msgs, "turn_start")
    assert turn_errors(msgs) == [("TURN_ERROR", "orchestrator_error", False)]
    assert of_type(msgs, "error")[0]["turn_id"] == start["turn_id"]
    assert [t for t in client.raw_texts if "No API key" in t] == []

    await client.send({"type": "send", "text": "otra vez"})
    msgs = await client.collect(lambda m: len(of_type(m, "turn_end")) == 2)
    second = of_type(msgs, "turn_start")[1]
    assert tokens(msgs, second["turn_id"]) == "reply-otra vez"
    assert_turns_paired(msgs)


async def test_failing_push_still_ends_its_turn(harness):
    # #300: un push cuyo agente falla acaba en agent phase:error, no en end.
    client = await harness.connect()
    await harness.push("recordatorio", end=False)
    await client.collect(lambda m: bool(of_type(m, "turn_start")))
    await harness.push_fail()
    msgs = await client.collect(turn_ended, timeout=3)

    assert len(of_type(msgs, "turn_end")) == 1
    assert_turns_paired(msgs)


async def test_v1_without_stream_aborts_the_turn_when_the_client_leaves(harness):
    """D-r6-1: HA corta (timeout) un /v1 sin stream: el turno se aborta en OpenClaw y la
    siguiente petición no espera a ese turno hasta TURN_TIMEOUT_S."""
    body = {"model": "fogar-gateway", "messages": [{"role": "user", "content": "wait"}]}
    async with httpx.AsyncClient(
        base_url=f"http://127.0.0.1:{harness.port}",
        headers={"authorization": f"Bearer {VALID_KEY}"},
        timeout=10,
    ) as http:
        request = asyncio.create_task(http.post("/v1/chat/completions", json=body))
        await harness.openclaw.wait_held(1)
        request.cancel()  # el cliente se va: httpx cierra la conexión
        await asyncio.wait({request})
        [(_, sk)] = harness.openclaw.calls
        async with asyncio.timeout(5):
            while ("chat.abort", sk) not in harness.openclaw.calls:
                await asyncio.sleep(0.02)
        body["messages"][0]["content"] = "hola"
        res = await http.post("/v1/chat/completions", json=body)
    assert res.status_code == 200, res.text
    assert res.json()["choices"][0]["message"]["content"] == "reply-hola"
