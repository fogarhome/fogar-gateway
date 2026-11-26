"""Voz sin el WebSocket de conversación (5.0 5c, #412): `/speech/dictation`, `speak` por
`/events` y la voz bajo demanda (`…/messages/{id}/speech`). STT y TTS falsos."""

import base64
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sqlmodel import Session
from starlette.websockets import WebSocketDisconnect

from fogar_gateway.api import dictation_routes
from fogar_gateway.core.config import settings
from fogar_gateway.db.models import ClientRecord
from fogar_gateway.services import events as ev
from tests.fakes.orchestrator import OrchestratorEvent
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
            import asyncio

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


def test_dictation_without_a_final_is_4901(voice, monkeypatch):
    monkeypatch.setattr(dictation_routes, "FINAL_S", 0.1)
    voice.client.app.state.speech = SimpleNamespace(stt=FakeStt(answer=False), tts=voice.tts)
    with voice.client.websocket_connect("/speech/dictation") as ws:
        ws.send_json(HELLO)
        ws.send_json({"type": "end"})
        with pytest.raises(WebSocketDisconnect) as exc:
            ws.receive_text()
    assert exc.value.code == 4901


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
