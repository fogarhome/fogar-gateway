"""Un `cancel` justo después de `tts.connect()` no deja el stream huérfano."""

import asyncio
from unittest.mock import AsyncMock, MagicMock

from tests.fakes.bridge import blocking


async def test_cancel_right_after_tts_connect_closes_the_stream(make_bridge):
    stream = MagicMock()
    stream.close = AsyncMock()
    tts = MagicMock()
    tts.name = "elevenlabs"
    tts.resolve_voice = MagicMock(return_value="v")
    tts.connect = AsyncMock(return_value=stream)
    bridge, _ws = make_bridge(output_mode=("audio", "text"), tts=tts)
    # E-2: el TTS conecta a la vez que el orquestador contesta; este no termina el turno antes.
    bridge.orchestrator.stream_response = blocking()
    in_record = asyncio.Event()

    async def blocking_record(name, **kw):
        if name == "tts_start":
            in_record.set()
            await asyncio.sleep(3600)

    bridge.tracker.record = blocking_record
    await bridge.turns.start_user_turn("hola")
    task = bridge.turns.user_task
    await asyncio.wait_for(in_record.wait(), 2)
    assert (await bridge.turns.cancel("cancelled"))[0]
    await asyncio.wait_for(bridge.turns.idle(), 2)
    assert task.cancelled()
    stream.close.assert_awaited_once()
