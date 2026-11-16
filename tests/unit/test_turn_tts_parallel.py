"""Auditoría 3.0, E-2: el TTS se conecta a la vez que se pide la respuesta al orquestador.

Un fogar-speaker colgado (connect tarda hasta su timeout) no retrasa el texto; lo que llega
mientras conecta se le manda en orden en cuanto conecta.
"""

import asyncio
from unittest.mock import AsyncMock, MagicMock

from tests.fakes.bridge import sent, start_turn, tokens


def _stream():
    fed: list[str] = []
    stream = MagicMock()
    stream.send_text_chunk = AsyncMock(side_effect=lambda t: fed.append(t))
    stream.end = AsyncMock()
    stream.close = AsyncMock()

    async def audio():
        return
        yield b""

    stream.get_audio_stream = audio
    return stream, fed


def _slow_tts(stream, release: asyncio.Event):
    tts = MagicMock()
    tts.resolve_voice = MagicMock(return_value="v")

    async def connect(**kw):
        await release.wait()
        return stream

    tts.connect = connect
    return tts


async def test_text_does_not_wait_for_a_slow_tts_connect(make_bridge):
    release = asyncio.Event()
    stream, fed = _stream()
    bridge, ws = make_bridge(output_mode=("audio", "text"), tts=_slow_tts(stream, release))
    bridge.orchestrator.stream_response = tokens("a", "b")

    task = await start_turn(bridge)
    for _ in range(50):
        await asyncio.sleep(0.01)
        await sent(bridge)
        if len(ws.of("token")) == 2:
            break
    assert [m["text"] for m in ws.of("token")] == ["a", "b"]  # sin esperar al TTS
    assert not task.done()  # el turno espera a la voz, que aún no conectó

    release.set()
    await asyncio.wait_for(task, 2)
    assert fed == ["a", "b"]  # lo que llegó antes de conectar, en orden
    stream.end.assert_awaited_once()
    stream.close.assert_awaited()
    assert (await sent(bridge))[-1]["type"] == "turn_end"


async def test_cancel_while_tts_connects_cancels_the_connect(make_bridge):
    release = asyncio.Event()
    stream, _ = _stream()
    bridge, _ws = make_bridge(output_mode=("audio", "text"), tts=_slow_tts(stream, release))
    bridge.orchestrator.stream_response = tokens("a")

    task = await start_turn(bridge)
    await asyncio.sleep(0.05)  # el texto ya salió; el turno espera a la voz
    task.cancel()  # barge-in
    await asyncio.wait({task}, timeout=2)
    assert task.cancelled()  # no esperó a que el TTS conectase
    release.set()
    await asyncio.sleep(0.01)
    stream.close.assert_not_awaited()  # la conexión se canceló: no quedó ningún stream abierto
    assert [m["type"] for m in await sent(bridge)].count("turn_end") == 1
