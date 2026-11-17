"""TTSClient (fogar-speaker): socket muerto a mitad de síntesis (E-r6-3) y PCM16 impar (E-r6-4)."""

import asyncio
import json
import logging

import websockets

from fogar_gateway.services.tts_client import TTSClient


async def _serve(frames: list[bytes], abort: bool):
    async def handler(ws):
        await ws.recv()  # auth
        await ws.send(json.dumps({"type": "auth_ok"}))
        for f in frames:
            await ws.send(f)
        if abort:
            ws.transport.abort()  # el speaker se cae a mitad, sin cierre WS
        else:
            await ws.send(json.dumps({"type": "done"}))
        await ws.wait_closed()

    return await websockets.serve(handler, "127.0.0.1", 0)


async def _client(server) -> TTSClient:
    c = TTSClient(f"127.0.0.1:{server.sockets[0].getsockname()[1]}", "tok", "t")
    await c.connect()
    return c


async def test_dead_speaker_warns_once_and_keeps_the_audio_that_arrived(caplog):
    server = await _serve([b"\x01\x00" * 4, b"\x02\x00" * 4], abort=True)
    try:
        c = await _client(server)
        chunks = await asyncio.wait_for(_collect(c), 2)
        assert b"".join(chunks) == b"\x01\x00" * 4 + b"\x02\x00" * 4
        with caplog.at_level(logging.WARNING):
            for _ in range(50):
                await c.send_text_chunk("hola")  # no relanza
            await c.end()
        warns = [r for r in caplog.records if r.levelno >= logging.WARNING]
        assert len(warns) == 1, [r.getMessage() for r in warns]
        assert "hola" not in warns[0].getMessage()
        await c.close()
    finally:
        server.close()
        await server.wait_closed()


async def test_odd_frames_are_realigned_to_pcm16_pairs():
    server = await _serve([b"\x01", b"\x02\x03\x04", b"\x05\x06\x07"], abort=False)
    try:
        c = await _client(server)
        chunks = await asyncio.wait_for(_collect(c), 2)
        assert all(len(ch) % 2 == 0 and ch for ch in chunks)
        assert b"".join(chunks) == b"\x01\x02\x03\x04\x05\x06"  # el byte final sin pareja se tira
        await c.close()
    finally:
        server.close()
        await server.wait_closed()


async def _collect(c: TTSClient) -> list[bytes]:
    return [x async for x in c.get_audio_stream()]
