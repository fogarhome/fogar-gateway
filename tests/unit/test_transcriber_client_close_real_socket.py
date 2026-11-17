"""G-01 (#214): TranscriberClient.close() must really close the socket.

Uses a real `websockets` server on port 0: the old fake exposed `.closed`, an
attribute that does not exist in `websockets.asyncio`, which hid the bug.
"""

import asyncio
import json
from unittest.mock import MagicMock

import pytest
import websockets

from fogar_gateway.services.transcriber_client import TranscriberClient


async def test_close_closes_real_socket_and_server_sees_it():
    closed = asyncio.Event()

    async def handler(ws):
        await ws.recv()  # config handshake
        await ws.send(json.dumps({"type": "ready", "session_id": "s1"}))
        try:
            async for _ in ws:
                pass
        finally:
            closed.set()

    async with websockets.serve(handler, "127.0.0.1", 0) as server:
        port = server.sockets[0].getsockname()[1]
        client = TranscriberClient(url=f"127.0.0.1:{port}", client_id="c1")
        await client.connect()
        assert client._is_ready

        await client.close()

        await asyncio.wait_for(closed.wait(), timeout=2.0)
        assert client.ws.state.name == "CLOSED"


async def test_close_logs_failure_instead_of_swallowing(caplog):
    class BrokenWS:
        state = None  # no quedó cerrado: se aborta el transporte (A-r7-1)
        transport = MagicMock()

        async def close(self):
            raise OSError("boom")

    client = TranscriberClient(url="x:1", client_id="c1")
    client.ws = BrokenWS()
    with caplog.at_level("WARNING"):
        await client.close()  # must not raise
    assert any("OSError" in r.getMessage() for r in caplog.records)
    BrokenWS.transport.abort.assert_called_once()


async def test_close_cancelled_on_a_peer_that_does_not_read_aborts_the_socket():
    """A-r7-1: close() espera el Close del peer hasta el close_timeout (10 s); si el Teardown
    lo cancela antes, el socket no se queda abierto: se aborta el transporte."""
    from websockets.utils import accept_key

    release = asyncio.Event()

    async def handle(reader, writer):
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
        writer.write(bytes([0x81, len(ready)]) + ready)
        writer.transport.pause_reading()  # no lee ni contesta al Close
        await release.wait()
        writer.transport.abort()

    server = await asyncio.start_server(handle, "127.0.0.1", 0)
    try:
        port = server.sockets[0].getsockname()[1]
        client = TranscriberClient(url=f"127.0.0.1:{port}", client_id="c1")
        await client.connect()
        task = asyncio.create_task(client.close())
        await asyncio.sleep(0.2)
        assert not task.done()  # esperando el Close del peer
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert client.ws.transport.is_closing()
        await asyncio.sleep(0.05)
        assert client.ws.state.name == "CLOSED"
    finally:
        release.set()
        server.close()
        await server.wait_closed()


async def test_connect_to_a_peer_that_never_sends_ready_fails_within_ready_timeout_plus_1s(
    monkeypatch,
):
    """E-r8-1: un transcriber que acepta el upgrade y calla (ni ready ni Close) hace fallar
    connect() en ~TRANSCRIBER_READY_TIMEOUT_S + 1 s, no + 10 s (close_timeout por defecto)."""
    from websockets.utils import accept_key

    from fogar_gateway.core.config import settings

    monkeypatch.setattr(settings, "TRANSCRIBER_READY_TIMEOUT_S", 0.2)
    release = asyncio.Event()

    async def handle(reader, writer):
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
        writer.transport.pause_reading()  # ni ready ni respuesta al Close
        await release.wait()
        writer.transport.abort()

    server = await asyncio.start_server(handle, "127.0.0.1", 0)
    try:
        port = server.sockets[0].getsockname()[1]
        client = TranscriberClient(url=f"127.0.0.1:{port}", client_id="c1")
        loop = asyncio.get_running_loop()
        t0 = loop.time()
        with pytest.raises(TimeoutError):
            await asyncio.wait_for(client.connect(), timeout=5)
        assert loop.time() - t0 < 2.5
    finally:
        release.set()
        server.close()
        await server.wait_closed()


async def test_close_survives_ws_reset_by_a_cancelled_connect():
    """E-r8-2: un connect cancelado pone self.ws a None mientras close() espera: el finally
    usa el socket que tomó al empezar (sin AttributeError) y lo aborta."""
    client = TranscriberClient(url="x:1", client_id="c1")

    class WS:
        state = None
        transport = MagicMock()

        async def close(self):
            client.ws = None

    ws = WS()
    client.ws = ws
    await client.close()
    ws.transport.abort.assert_called_once()
