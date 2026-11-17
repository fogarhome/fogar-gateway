"""E-r6-1: un transcriber que deja de leer no cuelga la sesión.

fogar-transcriber colgado (o su host caído con el TCP abierto) deja de leer: sin plazo,
`ws.send` se queda en drain y con él el bucle de entrada del cliente, que ya no atiende
`cancel`, `send` ni la desconexión. El envío está acotado: al vencer se aborta el socket y
el wrapper reconecta (RECONNECTING → `status` al cliente). Un fallo de STT degrada, no cierra.
"""

import asyncio
import json
import time
from unittest.mock import AsyncMock

import pytest
from websockets.utils import accept_key

from fogar_gateway.services import transcriber_client
from fogar_gateway.services.reconnection import ConnectionState
from fogar_gateway.services.speech.ports import SttCallbacks, SttParams
from fogar_gateway.services.transcriber_reconnecting import ReconnectingTranscriberClient
from tests.fakes.bridge import sent, tokens, wait_until

SEND_TIMEOUT = 0.3
FRAME = b"\x00" * 65536


@pytest.fixture(autouse=True)
def _short_send_timeout(monkeypatch):
    monkeypatch.setattr(transcriber_client, "SEND_TIMEOUT_S", SEND_TIMEOUT)


@pytest.fixture
async def hung_transcriber():
    """Servidor asyncio crudo: acepta la primera conexión (upgrade WS + `ready`) y deja de leer
    el socket, como un proceso colgado con el TCP abierto; las siguientes las corta sin más.
    (Un servidor `websockets` reanuda la lectura por su cuenta: no sirve para atascar.)"""
    release = asyncio.Event()
    transports = []

    async def handle(reader, writer):
        if transports:
            writer.transport.abort()
            return
        transports.append(writer.transport)
        head = await reader.readuntil(b"\r\n\r\n")
        key = next(
            line.split(b":", 1)[1].strip().decode()
            for line in head.split(b"\r\n")
            if line.lower().startswith(b"sec-websocket-key:")
        )
        writer.write(
            b"HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n"
            b"Sec-WebSocket-Accept: " + accept_key(key).encode() + b"\r\n\r\n"
        )
        ready = json.dumps({"type": "ready", "session_id": "s1"}).encode()
        writer.write(bytes([0x81, len(ready)]) + ready)  # trama de texto sin máscara
        writer.transport.pause_reading()  # ni siquiera lee la config: nadie lee nada más
        await release.wait()
        writer.transport.abort()

    server = await asyncio.start_server(handle, "127.0.0.1", 0)
    try:
        yield server.sockets[0].getsockname()[1]
    finally:
        release.set()
        server.close()
        await server.wait_closed()


def _wrap(port: int, callbacks: SttCallbacks | None = None) -> ReconnectingTranscriberClient:
    cbs = callbacks or SttCallbacks(
        on_transcription=AsyncMock(), on_warning=AsyncMock(),
        on_status=AsyncMock(), on_incomplete=AsyncMock(),
    )
    return ReconnectingTranscriberClient(
        url=f"127.0.0.1:{port}", client_id="c1", callbacks=cbs,
        params=SttParams(language="es", vad_threshold=0.0),
        initial_backoff=30.0,  # tras el rechazo se queda en RECONNECTING durante el test
    )


async def test_send_audio_to_a_hung_transcriber_is_bounded_and_reconnects(hung_transcriber):
    w = _wrap(hung_transcriber)
    states: list[ConnectionState] = []
    await w.start()
    w.on_state_change = states.append
    run = asyncio.create_task(w.run())
    try:
        slowest = 0.0
        async with asyncio.timeout(15):  # sin el arreglo, send_audio no vuelve nunca
            for _ in range(2000):  # llena los búferes del socket hasta que el envío se atasca
                t0 = time.monotonic()
                await w.send_audio(FRAME)
                slowest = max(slowest, time.monotonic() - t0)
                if not w._client._is_ready:
                    break
        assert not w._client._is_ready, "el socket nunca se llenó"
        assert slowest < SEND_TIMEOUT + 1.0
        await wait_until(lambda: ConnectionState.RECONNECTING in states, timeout=3)
        assert w.state == ConnectionState.RECONNECTING
    finally:
        await w.close()
        await asyncio.wait([run], timeout=2)


async def test_session_with_a_hung_transcriber_still_handles_cancel_send_and_disconnect(
    hung_transcriber, make_bridge
):
    """De punta a punta en el bridge: con el transcriber colgado, `cancel`, `send` y la
    desconexión se procesan a tiempo y la sesión no acaba en error."""
    bridge, ws = make_bridge(input_mode="audio", output_mode=("text",))
    bridge.orchestrator.stream_response = tokens("ok")
    w = _wrap(
        hung_transcriber,
        SttCallbacks(
            on_transcription=bridge._on_transcription,
            on_warning=bridge._on_transcriber_warning,
            on_status=bridge._on_transcriber_status,
            on_incomplete=bridge._on_transcriber_incomplete,
        ),
    )
    bridge.transcriber = w
    await w.start()
    w.on_state_change = bridge._on_transcriber_state_change
    run = asyncio.create_task(w.run())
    for _ in range(2000):  # ~130 MB: más de lo que absorbe cualquier búfer de loopback
        ws.inbox.put_nowait({"bytes": FRAME})
    ws.inbox.put_nowait({"text": json.dumps({"type": "cancel"})})
    ws.inbox.put_nowait({"text": json.dumps({"type": "send", "text": "hola"})})
    ws.inbox.put_nowait({"type": "websocket.disconnect", "code": 1000})
    loop = asyncio.create_task(bridge._client_input_loop())
    try:
        done, _ = await asyncio.wait([loop], timeout=SEND_TIMEOUT + 5)
        assert done, "el bucle de entrada se quedó colgado en el transcriber"
        assert loop.exception() is None
        assert bridge.turns.client_gone
        await wait_until(lambda: "turn_end" in ws.types(), timeout=3)
        await wait_until(
            lambda: any(m.get("service") == "transcriber" for m in ws.of("status")), timeout=3
        )
        out = await sent(bridge)
        assert {"type": "status", "service": "transcriber", "state": "reconnecting"} in [
            {k: m[k] for k in ("type", "service", "state")} for m in out if isinstance(m, dict)
            and m.get("type") == "status"
        ]
        assert ws.types().count("turn_start") == ws.types().count("turn_end") == 1
        assert bridge._close_reason != "error"
    finally:
        loop.cancel()
        t0 = time.monotonic()
        await bridge.close_all()
        assert time.monotonic() - t0 < 3
        await asyncio.wait([run], timeout=2)
