"""Voz sin el WebSocket de conversación (5.0 5c, #412): `/speech/dictation`, `speak` por
`/events` y la voz bajo demanda (`…/messages/{id}/speech`). STT y TTS falsos."""

import asyncio
import base64
import json
import math
import struct
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from pydantic import SecretStr
from sqlmodel import Session
from starlette.websockets import WebSocketDisconnect

from fogar_gateway.api import dictation_routes
from fogar_gateway.core.config import settings
from fogar_gateway.db.models import ClientRecord
from fogar_gateway.services import events as ev
from fogar_gateway.services.speech.elevenlabs.stt import ElevenLabsSttProvider
from tests.fakes.elevenlabs_stt import FakeElevenLabsStt
from tests.fakes.orchestrator import OrchestratorEvent
from tests.fakes.speech import FakeTts, FakeTtsStream
from tests.integration.conftest import CLIENT_ID, VALID_KEY

BEARER = {"authorization": f"Bearer {VALID_KEY}"}
HELLO = {"protocol": 5, "token": VALID_KEY}


class FakeSttSession:
    def __init__(self, callbacks, answer=True):
        self.callbacks = callbacks
        self.answer = answer
        self.audio = 0
        self.closed = False

    async def start(self):
        pass

    async def send_audio(self, frame):
        self.audio += len(frame)
        await self.callbacks.on_transcription("ho", False)

    async def send_end(self):
        if self.answer:
            await self.callbacks.on_transcription("hola", True)

    async def run(self):
        pass

    async def close(self):
        self.closed = True


class FakeStt:
    def __init__(self, name="fake", answer=True):
        self.name = name
        self.answer = answer
        self.sessions = []

    def open_session(self, *, client, params, callbacks):
        session = FakeSttSession(callbacks, self.answer)
        self.sessions.append(session)
        return session


@pytest.fixture
def voice(client, monkeypatch):
    monkeypatch.setattr(settings, "AUDIO_PACING_ENABLED", False)
    monkeypatch.setattr(ev, "STREAM_MAX_S", 1.5)
    stt, tts = FakeStt(), FakeTts()
    client.app.state.speech = SimpleNamespace(stt=stt, tts=tts)
    client.app.state.events.tts = lambda: tts
    return SimpleNamespace(client=client, stt=stt, tts=tts)


# --- Dictado -------------------------------------------------------------------------


def test_dictation_partials_final_and_close_1000(voice):
    with voice.client.websocket_connect("/speech/dictation") as ws:
        ws.send_json(HELLO)
        ws.send_bytes(b"\x00" * 64)
        assert ws.receive_json() == {"type": "transcription_partial", "text": "ho"}
        ws.send_json({"type": "end"})
        assert ws.receive_json() == {"type": "transcription", "text": "hola"}
        with pytest.raises(WebSocketDisconnect) as exc:
            ws.receive_text()
    assert exc.value.code == 1000
    assert voice.stt.sessions[0].audio == 64 and voice.stt.sessions[0].closed


def test_dictation_with_stt_off_is_4901(voice):
    voice.client.app.state.speech = SimpleNamespace(stt=FakeStt(name="off"), tts=voice.tts)
    with voice.client.websocket_connect("/speech/dictation") as ws:
        ws.send_json(HELLO)
        with pytest.raises(WebSocketDisconnect) as exc:
            ws.receive_text()
    assert (exc.value.code, exc.value.reason) == (4901, "speech unavailable")


def test_dictation_with_a_phrase_left_without_its_final_is_4901(voice, monkeypatch):
    monkeypatch.setattr(dictation_routes, "FINAL_S", 0.1)
    voice.client.app.state.speech = SimpleNamespace(stt=FakeStt(answer=False), tts=voice.tts)
    with voice.client.websocket_connect("/speech/dictation") as ws:
        ws.send_json(HELLO)
        ws.send_bytes(b"\x00" * 8)
        assert ws.receive_json()["type"] == "transcription_partial"  # una frase a medias
        ws.send_json({"type": "end"})
        with pytest.raises(WebSocketDisconnect) as exc:
            ws.receive_text()
    assert exc.value.code == 4901


def test_dictation_without_speech_ends_1000(voice, monkeypatch):
    """Una toma sin habla no es «voz no disponible»: 1000 sin `transcription`."""
    monkeypatch.setattr(dictation_routes, "SETTLE_S", 0.05)
    voice.client.app.state.speech = SimpleNamespace(stt=FakeStt(answer=False), tts=voice.tts)
    with voice.client.websocket_connect("/speech/dictation") as ws:
        ws.send_json(HELLO)
        ws.send_json({"type": "end"})
        with pytest.raises(WebSocketDisconnect) as exc:
            ws.receive_text()
    assert exc.value.code == 1000


def _speech(samples=1600):
    return struct.pack(f"<{samples}f", *[0.1 * math.sin(i * 0.3) for i in range(samples)])


def _quiet(samples=1600):
    return struct.pack(f"<{samples}f", *([0.0] * samples))


def test_dictation_with_elevenlabs_closing_the_phrase_before_end_is_1000(voice, monkeypatch):
    """ElevenLabs ya cerró la frase (su VAD/compuerta) antes de `end`: su `send_end` no hace
    nada y no llega otra final. Antes, 4901 a los FINAL_S; ahora 1000, sin repetir la final."""
    from fogar_gateway.services.speech.elevenlabs import stt as stt_mod

    monkeypatch.setattr(stt_mod, "DRAIN_S", 0.3)
    monkeypatch.setattr(dictation_routes, "FINAL_S", 1.0)
    monkeypatch.setattr(dictation_routes, "SETTLE_S", 0.2)
    fake = FakeElevenLabsStt(
        api_key="fake-el-key", on_audio=[("hola", False)], on_end=[("hola qué tal", True)]
    )
    portal = voice.client.portal
    portal.call(fake.start)
    try:
        provider = ElevenLabsSttProvider(settings.model_copy(update={
            "ELEVENLABS_API_KEY": SecretStr("fake-el-key"),
            "ELEVENLABS_BASE_URL": fake.url,
            "STT_AUDIO_GATE": "energy",
        }))
        voice.client.app.state.speech = SimpleNamespace(stt=provider, tts=voice.tts)
        got = []
        with voice.client.websocket_connect("/speech/dictation") as ws:
            ws.send_json(HELLO)
            for frame in [_speech()] * 10 + [_quiet()] * 17:  # > hangover: la compuerta confirma
                ws.send_bytes(frame)
            while not got or got[-1]["type"] != "transcription":
                got.append(ws.receive_json())
            ws.send_json({"type": "end"})
            with pytest.raises(WebSocketDisconnect) as exc:
                ws.receive_text()
        assert exc.value.code == 1000
        assert [e["text"] for e in got if e["type"] == "transcription"] == ["hola qué tal"]
        assert fake.commits == 1
    finally:
        portal.call(fake.stop)


def test_a_second_dictation_of_the_device_closes_the_first(voice):
    with voice.client.websocket_connect("/speech/dictation") as first:
        first.send_json(HELLO)
        first.send_bytes(b"\x00" * 8)
        first.receive_json()  # ya está registrado
        with voice.client.websocket_connect("/speech/dictation") as second:
            second.send_json(HELLO)
            with pytest.raises(WebSocketDisconnect) as exc:
                first.receive_text()
            assert exc.value.code == 4901
            second.send_bytes(b"\x00" * 8)
            assert second.receive_json()["type"] == "transcription_partial"


def test_revoking_the_device_closes_its_dictation_4200(voice, admin_headers):
    with voice.client.websocket_connect("/speech/dictation") as ws:
        ws.send_json(HELLO)
        ws.send_bytes(b"\x00" * 8)
        ws.receive_json()
        r = voice.client.patch(
            f"/home/devices/{CLIENT_ID}", json={"is_active": False}, headers=admin_headers
        )
        assert r.status_code == 200, r.text
        with pytest.raises(WebSocketDisconnect) as exc:
            ws.receive_text()
    assert (exc.value.code, exc.value.reason) == (4200, "credential revoked")
    assert dictation_routes._live == {}


@pytest.mark.parametrize(("hello", "code"), [
    ({"protocol": 5, "token": "nope"}, 4010),
    ({"protocol": 4, "token": VALID_KEY}, 4003),
    ({"protocol": 5}, 4000),
])
def test_dictation_handshake_closes(voice, hello, code):
    with voice.client.websocket_connect("/speech/dictation") as ws:
        ws.send_json(hello)
        with pytest.raises(WebSocketDisconnect) as exc:
            ws.receive_text()
    assert exc.value.code == code


def test_dictation_needs_write(voice, db_engine):
    with Session(db_engine) as s:
        rec = s.get(ClientRecord, CLIENT_ID)
        rec.scopes = json.dumps(["conversations:read"])
        s.add(rec)
        s.commit()
    with voice.client.websocket_connect("/speech/dictation") as ws:
        ws.send_json(HELLO)
        with pytest.raises(WebSocketDisconnect) as exc:
            ws.receive_text()
    assert exc.value.code == 4011


# --- Voz de la respuesta por /events ---------------------------------------------------


def test_speak_sends_audio_events_before_turn_end(voice, mock_orchestrator):
    client = voice.client
    conv = client.post("/conversations", json={}, headers=BEARER).json()

    async def stream(text, session_key=None):
        yield OrchestratorEvent(type="token", content="hola")
        yield OrchestratorEvent(type="status", content="done")

    mock_orchestrator.stream_response = stream
    got: list[dict] = []
    with client.stream("GET", "/events", headers=BEARER) as s:
        lines = s.iter_lines()
        for line in lines:
            if line.startswith("data: ") and json.loads(line[6:])["type"] == "ready":
                break
        r = client.post(f"/conversations/{conv['id']}/messages",
                        json={"text": "hola", "speak": True}, headers=BEARER)
        assert r.status_code == 202
        for line in lines:
            if line.startswith("data: "):
                got.append(json.loads(line[6:]))
                if got[-1]["type"] == "turn_end":
                    break
    kinds = [e["type"] for e in got if e["type"] != "message"]
    assert kinds.index("audio") < kinds.index("turn_end")
    audio = next(e for e in got if e["type"] == "audio")
    assert base64.b64decode(audio["data"]) and audio["conversation"] == conv["id"]
    assert voice.tts.streams[0].text == ["hola"] and voice.tts.streams[0].ended


def test_without_speak_there_is_no_audio(voice, mock_orchestrator):
    client = voice.client
    conv = client.post("/conversations", json={}, headers=BEARER).json()

    async def stream(text, session_key=None):
        yield OrchestratorEvent(type="token", content="hola")
        yield OrchestratorEvent(type="status", content="done")

    mock_orchestrator.stream_response = stream
    with client.stream("GET", "/events", headers=BEARER) as s:
        lines = s.iter_lines()
        types = []
        for line in lines:
            if line.startswith("data: "):
                kind = json.loads(line[6:])["type"]
                if kind == "ready":
                    client.post(f"/conversations/{conv['id']}/messages", json={"text": "x"},
                                headers=BEARER)
                types.append(kind)
                if kind == "turn_end":
                    break
    assert "audio" not in types and voice.tts.streams == []


# --- Voz bajo demanda ------------------------------------------------------------------


def _history(mock_orchestrator):
    rows = [
        {"role": "user", "content": "hola", "timestamp": 1, "__openclaw": {"id": "u1"}},
        {"role": "assistant", "content": "buenas", "timestamp": 2, "__openclaw": {"id": "a1"}},
    ]
    mock_orchestrator.request = AsyncMock(return_value={"messages": rows, "hasMore": False})


def test_speech_of_an_answer_streams_its_audio(voice, mock_orchestrator):
    _history(mock_orchestrator)
    conv = voice.client.post("/conversations", json={}, headers=BEARER).json()
    r = voice.client.post(f"/conversations/{conv['id']}/messages/a1/speech", headers=BEARER)
    assert r.status_code == 200 and r.content
    assert r.headers["content-type"].startswith("audio/L16")
    assert voice.tts.streams[0].text == ["buenas"]


def test_speech_of_a_user_message_or_without_tts(voice, mock_orchestrator):
    _history(mock_orchestrator)
    conv = voice.client.post("/conversations", json={}, headers=BEARER).json()
    path = f"/conversations/{conv['id']}/messages"
    assert voice.client.post(f"{path}/u1/speech", headers=BEARER).status_code == 404
    assert voice.client.post(f"{path}/nope/speech", headers=BEARER).status_code == 404
    voice.client.app.state.speech = SimpleNamespace(stt=voice.stt, tts=FakeTts(up=False))
    r = voice.client.post(f"{path}/a1/speech", headers=BEARER)
    assert r.status_code == 503 and r.json()["detail"] == "tts_unavailable"


def test_speech_on_demand_closes_a_tts_that_fails_half_way(voice, mock_orchestrator):
    """Conectado y caído al mandarle el texto: 503 y el stream (de pago) cerrado."""
    _history(mock_orchestrator)
    conv = voice.client.post("/conversations", json={}, headers=BEARER).json()

    class Broken(FakeTtsStream):
        async def send_text_chunk(self, text):
            raise RuntimeError("upstream")

    voice.tts.connect = AsyncMock(return_value=(broken := Broken()))
    r = voice.client.post(f"/conversations/{conv['id']}/messages/a1/speech", headers=BEARER)
    assert r.status_code == 503 and r.json()["detail"] == "tts_unavailable"
    assert broken.closed


def test_speech_on_demand_gives_up_on_a_stalled_tts(voice, mock_orchestrator, monkeypatch):
    """Un TTS que deja de dar audio no deja la respuesta (ni su conexión) abierta sin tope."""
    monkeypatch.setattr(settings, "PUSH_TTS_DRAIN_TIMEOUT_S", 0.2)
    _history(mock_orchestrator)
    conv = voice.client.post("/conversations", json={}, headers=BEARER).json()

    class Stalled(FakeTtsStream):
        async def get_audio_stream(self):
            yield b"\x01\x02"
            await asyncio.sleep(60)
            yield b"never"

    voice.tts.connect = AsyncMock(return_value=(stalled := Stalled()))
    r = voice.client.post(f"/conversations/{conv['id']}/messages/a1/speech", headers=BEARER)
    assert r.status_code == 200 and r.content == b"\x01\x02"
    assert stalled.closed


def test_dictation_stt_failure_closes_1011(voice):
    class Broken(FakeSttSession):
        async def send_audio(self, frame):
            raise RuntimeError("upstream")

    class BrokenStt(FakeStt):
        def open_session(self, *, client, params, callbacks):
            return Broken(callbacks)

    voice.client.app.state.speech = SimpleNamespace(stt=BrokenStt(), tts=voice.tts)
    with voice.client.websocket_connect("/speech/dictation") as ws:
        ws.send_json(HELLO)
        ws.send_bytes(b"\x00" * 8)
        with pytest.raises(WebSocketDisconnect) as exc:
            ws.receive_text()
    assert (exc.value.code, exc.value.reason) == (1011, "internal error")
