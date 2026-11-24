"""Auditoría 3.0 (A-1..A-4): orden turn_start/turn_end, audio tras cancelar y cierre por política."""

import asyncio
from unittest.mock import AsyncMock, MagicMock

from fogar_gateway.core.config import settings
from tests.fakes.bridge import RecWS, blocking, enqueue_push, sent, start_turn, tokens, wait_until


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
        enqueue_push(bridge, kind, arg)
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
    enqueue_push(bridge, "turn_start", "agent:main:u")
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



class GatedClosingWS(ClosingWS):
    """Un cliente lento: su escritura espera a `gate`, así lo encolado sigue en la cola."""

    def __init__(self):
        super().__init__()
        self.gate = asyncio.Event()

    async def send_json(self, p):
        await self.gate.wait()
        self.sent.append(p)


async def test_a_displaced_slow_client_gets_the_whole_pair_or_nothing(make_bridge):
    """A-r2-1: con el turn_start aún en la cola al desplazar, no sale su turn_end solo."""
    bridge, ws = make_bridge(GatedClosingWS())
    bridge.orchestrator.stream_response = blocking()
    await bridge._sender.send_json({"type": "status", "service": "tts", "state": "ok"})
    await start_turn(bridge)  # su turn_start espera detrás del status
    displace = asyncio.create_task(bridge.displace())
    await asyncio.sleep(0.05)
    ws.gate.set()
    await asyncio.wait_for(displace, 2)
    await asyncio.wait_for(bridge._policy_close, 2)
    assert turn_marks(ws) == [("turn_start", "t-1"), ("turn_end", "t-1")]


async def test_turn_end_of_a_cancelled_turn_does_not_wait_for_the_tts_close(make_bridge, monkeypatch):
    """A-r2-2: tras cancel("interrupted") el turn_end sale enseguida, sin esperar a cerrar el
    TTS; después, ni audio ni otro turn_end."""
    monkeypatch.setattr(settings, "AUDIO_PACING_ENABLED", False)

    async def endless_audio():
        while True:
            yield b"\x00\x01" * 160
            await asyncio.sleep(0.002)

    stream = tts_stream(endless_audio)
    release_close = asyncio.Event()

    async def slow_close():
        await release_close.wait()

    stream.close = slow_close
    bridge, ws = make_bridge(output_mode=("text", "audio"), tts=tts_provider(stream))
    bridge.orchestrator.stream_response = blocking()
    await start_turn(bridge)
    await wait_until(lambda: any(isinstance(m, bytes) for m in ws.sent))
    cancel = asyncio.create_task(bridge.turns.cancel("interrupted"))
    try:
        await wait_until(lambda: ws.of("turn_end"), timeout=1.0)
    finally:
        release_close.set()
        await asyncio.wait_for(cancel, 2)
    await sent(bridge)
    ends = [i for i, m in enumerate(ws.sent) if isinstance(m, dict) and m["type"] == "turn_end"]
    assert len(ends) == 1 and ws.sent[ends[0]]["reason"] == "interrupted"
    assert not any(isinstance(m, bytes) for m in ws.sent[ends[0]:])


async def test_a_send_replaced_before_it_starts_still_gets_its_pair(make_bridge):
    """G-r2-1: dos `send` seguidos → dos pares; el del primero, `replaced`."""
    bridge, ws = make_bridge()
    bridge.orchestrator.stream_response = tokens("ok")
    await bridge.turns.start_user_turn("uno")
    await bridge.turns.start_user_turn("dos")
    await asyncio.wait({bridge.turns.user_task}, timeout=2)
    await sent(bridge)
    assert turn_marks(ws) == [
        ("turn_start", "t-1"), ("turn_end", "t-1"), ("turn_start", "t-2"), ("turn_end", "t-2"),
    ]
    assert [m["reason"] for m in ws.of("turn_end")] == ["replaced", "done"]
    assert {m["conversation"] for m in ws.of("turn_start")} == {bridge.conversation.id}


async def test_a_send_cancelled_before_it_starts_still_gets_its_pair(make_bridge):
    """G-r2-1: `send` y `cancel` seguidos → un par `cancelled`."""
    bridge, ws = make_bridge()
    bridge.orchestrator.stream_response = tokens("ok")
    await bridge.turns.start_user_turn("uno")
    assert await bridge.turns.cancel("cancelled") == (True, "t-1")
    await sent(bridge)
    assert turn_marks(ws) == [("turn_start", "t-1"), ("turn_end", "t-1")]
    assert ws.of("turn_end")[0]["reason"] == "cancelled"


async def test_turn_end_of_a_turn_cancelled_while_its_audio_plays_is_not_late(make_bridge, monkeypatch):
    """A-r2-2 (revisión): OpenClaw ya terminó y el audio suena (drain): tras cancel el turn_end
    sale sin esperar a cerrar el TTS; después, ni audio ni otro turn_end."""
    monkeypatch.setattr(settings, "AUDIO_PACING_ENABLED", False)

    async def endless_audio():
        while True:
            yield b"\x00\x01" * 160
            await asyncio.sleep(0.002)

    stream = tts_stream(endless_audio)
    release_close = asyncio.Event()

    async def slow_close():
        await release_close.wait()

    stream.close = slow_close
    bridge, ws = make_bridge(output_mode=("text", "audio"), tts=tts_provider(stream))
    bridge.orchestrator.stream_response = tokens("hola")
    await start_turn(bridge)
    await wait_until(lambda: any(isinstance(m, bytes) for m in ws.sent))
    await asyncio.sleep(0.1)  # el stream terminó: el turno está drenando el audio
    cancel = asyncio.create_task(bridge.turns.cancel("interrupted"))
    try:
        await wait_until(lambda: ws.of("turn_end"), timeout=1.0)
    finally:
        release_close.set()
        await asyncio.wait_for(cancel, 2)
    await sent(bridge)
    ends = [i for i, m in enumerate(ws.sent) if isinstance(m, dict) and m["type"] == "turn_end"]
    assert len(ends) == 1 and ws.sent[ends[0]]["reason"] == "interrupted"
    assert not any(isinstance(m, bytes) for m in ws.sent[ends[0]:])


async def test_the_pair_of_an_unstarted_send_does_not_count_as_a_turn(make_bridge):
    """G-r2-1 (revisión): el par de un send que no llegó a OpenClaw no suma en la conversación."""
    bridge, ws = make_bridge()
    bridge.orchestrator.stream_response = tokens("ok")
    noted = []
    bridge.conversation.note_turn = noted.append
    await bridge.turns.start_user_turn("uno")
    await bridge.turns.start_user_turn("dos")
    await asyncio.wait({bridge.turns.user_task}, timeout=2)
    await sent(bridge)
    assert len(ws.of("turn_start")) == 2 and noted == [bridge.conversation.id]


async def test_let_go_gives_an_unstarted_send_its_pair(make_bridge):
    """G-r2-1 (revisión): desplazada o revocada, un send aceptado cuya tarea aún no corrió
    recibe su par `closed` (let_go)."""
    bridge, ws = make_bridge()
    bridge.orchestrator.stream_response = blocking()
    await bridge.turns.start_user_turn("uno")  # su tarea aún no corrió
    await bridge.turns.let_go(1.0)
    await sent(bridge)
    assert turn_marks(ws) == [("turn_start", "t-1"), ("turn_end", "t-1")]
    assert ws.of("turn_end")[0]["reason"] == "closed"


async def test_close_all_gives_an_unstarted_send_its_pair(make_bridge):
    """A-r3-1: un send aceptado cuya tarea aún no corrió y close_all → su par `closed`."""
    bridge, ws = make_bridge()
    bridge.orchestrator.stream_response = blocking()
    await bridge.turns.start_user_turn("uno")  # su tarea aún no corrió
    await bridge.close_all()
    await sent(bridge)
    assert turn_marks(ws) == [("turn_start", "t-1"), ("turn_end", "t-1")]
    assert ws.of("turn_end")[0]["reason"] == "closed"
