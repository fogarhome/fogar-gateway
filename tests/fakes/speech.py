"""Titular mínimo de proveedores de voz para construir un `FogarBridge` en tests."""

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

from fogar_gateway.core.config import settings
from fogar_gateway.services.speech.factory import build_stt_provider


def speech_providers(tts=None, stt=None) -> SimpleNamespace:
    """Como `SpeechProviders`: `.tts`/`.stt`."""
    return SimpleNamespace(
        tts=tts if tts is not None else AsyncMock(),
        stt=stt if stt is not None else build_stt_provider(settings),
    )


class FakeTtsStream:
    def __init__(self):
        self.text = []
        self.ended = False

    async def send_text_chunk(self, text):
        self.text.append(text)

    async def end(self):
        self.ended = True

    async def get_audio_stream(self):
        while not self.ended:
            await asyncio.sleep(0.01)
        yield b"\x01\x02" * 8

    async def close(self):
        pass


class FakeTts:
    name = "fake"

    def __init__(self, up=True):
        self.up = up
        self.streams = []

    def resolve_voice(self, configured):
        return configured

    async def connect(self, voice, speed, client_id):
        if not self.up:
            return None
        stream = FakeTtsStream()
        self.streams.append(stream)
        return stream
