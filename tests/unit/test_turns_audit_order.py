"""Auditoría 3.0 (A-1..A-4): orden turn_start/turn_end, audio tras cancelar y cierre por política."""

import asyncio
from unittest.mock import AsyncMock, MagicMock

from fogar_gateway.core.config import settings
from tests.fakes.bridge import RecWS, blocking, sent, start_turn, tokens, wait_until


class ClosingWS(RecWS):
    def __init__(self):
        super().__init__()
        self.closed: tuple | None = None

    async def close(self, code=1000, reason=""):
        self.closed = (code, reason)


def tts_provider(stream):
    tts = MagicMock()
    tts.name = "fake"
    tts.resolve_voice = MagicMock(return_value="v")
    tts.connect = AsyncMock(return_value=stream)
    return tts


def tts_stream(audio):
    stream = MagicMock()
    stream.send_text_chunk = AsyncMock()
    stream.end = AsyncMock()
    stream.close = AsyncMock()
    stream.get_audio_stream = audio
    return stream


async def no_audio():
    return
    yield b""


def turn_marks(ws) -> list[tuple[str, str]]:
    return [(m["type"], m["turn_id"]) for m in ws.sent if isinstance(m, dict)
            and m["type"] in ("turn_start", "turn_end")]


async def test_turn_end_of_a_finished_push_precedes_the_next_turn_start(make_bridge):
    """A-1: mientras se libera el TTS de un push que terminó, un `send` no abre su turno antes
    del turn_end del push."""
    closing, release_close = asyncio.Event(), asyncio.Event()
    stream = tts_stream(no_audio)

    async def slow_close():
        closing.set()
        await release_close.wait()

    stream.close = slow_close
    bridge, ws = make_bridge(output_mode=("text", "audio"), tts=tts_provider(stream))
    bridge.orchestrator.stream_response = tokens("hola")
    for kind, arg in (("turn_start", "agent:main:u"), ("turn_end", "agent:main:u")):
        bridge.enqueue_push(kind, arg)
    await asyncio.wait_for(closing.wait(), 2)
    send = asyncio.create_task(bridge.turns.start_user_turn("x"))
    await asyncio.sleep(0.05)
    release_close.set()
    await asyncio.wait_for(send, 2)
    await asyncio.wait({bridge.turns.user_task}, timeout=2)
    await bridge.push_idle()
    await sent(bridge)
    assert turn_marks(ws) == [
        ("turn_start", "t-1"), ("turn_end", "t-1"), ("turn_start", "t-2"), ("turn_end", "t-2"),
    ]


async def test_no_audio_leaves_once_the_turn_is_cancelled(make_bridge, monkeypatch):
    """A-2: tras `cancel` no sale audio del turno aunque cerrar su TTS tarde."""
    monkeypatch.setattr(settings, "AUDIO_PACING_ENABLED", False)

    async def endless_audio():
        while True:
            yield b"\x00\x01" * 160
            await asyncio.sleep(0.002)

    stream = tts_stream(endless_audio)

    async def slow_end():
        await asyncio.sleep(0.3)

    stream.end = slow_end
    bridge, ws = make_bridge(output_mode=("text", "audio"), tts=tts_provider(stream))
    bridge.orchestrator.stream_response = blocking()
    await start_turn(bridge)
    await wait_until(lambda: any(isinstance(m, bytes) for m in ws.sent))
    frames = []
    real = bridge._sender.send_bytes

    async def spy(data):
        frames.append(data)
        await real(data)

    bridge._sender.send_bytes = spy
    await bridge.turns.cancel("interrupted")
    assert frames == []


async def test_a_displaced_session_never_sends_a_lone_turn_end(make_bridge):
    """A-3: un push que esperaba al turno del usuario no se abre en una sesión que se cierra."""
    bridge, ws = make_bridge(ClosingWS())
    started = asyncio.Event()
    bridge.orchestrator.stream_response = blocking(started)
    await start_turn(bridge)
    await asyncio.wait_for(started.wait(), 2)
    bridge.enqueue_push("turn_start", "agent:main:u")
    await asyncio.sleep(0.02)  # el worker queda esperando al turno del usuario
    await bridge.displace()
    await asyncio.wait_for(bridge._policy_close, 2)
    starts = {i for t, i in turn_marks(ws) if t == "turn_start"}
    ends = [i for t, i in turn_marks(ws) if t == "turn_end"]
    assert ends == ["t-1"] and all(i in starts for i in ends)
    assert ws.closed is not None and ws.closed[0] == 4102


async def test_run_after_a_displacement_spawns_nothing_and_keeps_4102(make_bridge):
    """A-4: desplazada antes de run() (durante claim_conversation), run() no deja tareas vivas y
    el cierre sigue siendo 4102 con su status, aunque routes.py llame a close_all(status=error)."""
    bridge, ws = make_bridge(ClosingWS())
    await bridge.displace()
    await asyncio.wait_for(bridge._policy_close, 2)  # el cierre terminó antes de run()
    run = asyncio.create_task(bridge.run())
    await asyncio.wait({run}, timeout=1)
    leaked = [t.get_name() for t in bridge.tasks if not t.done()]
    for t in [*bridge.tasks, run]:  # sin el arreglo: que el test falle, no que se cuelgue
        t.cancel()
    assert run.done() and leaked == []
    await bridge.close_all(status="error")  # el finally de routes.py
    assert ws.closed == (4102, "conversation opened elsewhere")
    statuses = [c.kwargs.get("status") for c in bridge.tracker.close.await_args_list]
    assert statuses and "error" not in statuses

