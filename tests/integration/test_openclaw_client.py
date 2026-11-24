import asyncio
import json
import logging
import uuid
from unittest.mock import patch

import pytest

from fogar_gateway.services.channel import ChannelHub
from fogar_gateway.services.openclaw.client import (
    ORCHESTRATOR_HANDSHAKE_FAILED_ERROR,
    OpenClawClient,
    OrchestratorHandshakeError,
    OrchestratorStartingError,
    _startup_retry_after_s,
)
from fogar_gateway.services.openclaw.dispatcher import FrameDispatcher
from fogar_gateway.services.openclaw.reconnecting import ReconnectingOpenClawClient
from fogar_gateway.services.openclaw.registry import TurnRegistry
from fogar_gateway.services.orchestration import OrchestratorError
from fogar_gateway.services.reconnection import ConnectionState
from tests.fakes.openclaw import FakeOpenClawServer

HELLO_OK_PAYLOAD = {
    "type": "hello-ok",
    "protocol": 4,
    "server": {"version": "2026.6.6", "connId": "test-conn"},
    "policy": {"tickIntervalMs": 30000, "maxPayload": 26214400, "maxBufferedBytes": 0},
    "snapshot": {
        "defaultAgentId": "main",
        "agents": [
            {"agentId": "main", "name": "Main Agent", "isDefault": True, "heartbeat": {}},
            {"agentId": "assistant", "name": "Fogar Voice", "isDefault": False, "heartbeat": {}},
        ],
        "sessionDefaults": {"defaultAgentId": "main"},
    },
    "auth": {"role": "operator", "scopes": ["operator.read", "operator.write"]},
}

# Matches OpenClaw server 2026.6.11+: hello-ok's snapshot no longer embeds the
# agent roster at all — it must be fetched separately via agents.list.
HELLO_OK_PAYLOAD_NO_SNAPSHOT_AGENTS = {
    "type": "hello-ok",
    "protocol": 4,
    "server": {"version": "2026.6.11", "connId": "test-conn-2"},
    "policy": {"tickIntervalMs": 30000, "maxPayload": 26214400, "maxBufferedBytes": 0},
    "snapshot": {
        "presence": {},
        "health": {},
        "stateVersion": 1,
        "uptimeMs": 1000,
        "sessionDefaults": {"defaultAgentId": "main"},
    },
    "auth": {"role": "operator", "scopes": ["operator.read", "operator.write"]},
}

AGENTS_LIST_PAYLOAD = {
    "defaultId": "main",
    "mainKey": "main",
    "scope": "per-sender",
    "agents": [
        {"id": "main", "name": "Main Agent"},
        {"id": "assistant", "name": "Fogar Voice"},
        {"id": "ci-tester", "name": "CI Tester"},
    ],
}


class SmartFakeWS:
    """Queue-backed fake WebSocket that auto-responds to OpenClaw protocol v4.

    chat_responses: {sessionKey → [list of deltaText strings]}
    """

    def __init__(
        self,
        chat_responses: dict | None = None,
        chat_response_delay: float = 0.0,
        hello_ok_payload: dict | None = None,
        agents_list_payload: dict | None = None,
        tool_calls: dict | None = None,
    ):
        self.chat_responses: dict[str, list[str]] = chat_responses or {}
        self.chat_response_delay: float = chat_response_delay  # delay between chunks (in seconds)
        self.hello_ok_payload = hello_ok_payload or HELLO_OK_PAYLOAD
        self.agents_list_payload = agents_list_payload or AGENTS_LIST_PAYLOAD
        self.tool_calls: dict[str, list[dict]] = tool_calls or {}
        # Flip to False to make agents.list answer ok:false (roster fetch failure, #151).
        self.agents_list_ok: bool = True
        # Flip to False to leave every chat.send without its ack.
        self.ack_chat_send: bool = True
        self._to_client: asyncio.Queue = asyncio.Queue()
        self._from_client: asyncio.Queue = asyncio.Queue()
        self.sent_frames: list[dict] = []
        self.closed = False
        self._handler: asyncio.Task | None = None

    async def start(self):
        self._handler = asyncio.create_task(self._auto_respond())

    async def recv(self) -> str:
        msg = await self._to_client.get()
        if msg is None:
            raise Exception("connection closed")
        return msg

    async def send(self, data: str) -> None:
        frame = json.loads(data)
        self.sent_frames.append(frame)
        await self._from_client.put(data)

    async def close(self) -> None:
        self.closed = True
        await self._to_client.put(None)
        if self._handler:
            self._handler.cancel()

    def __await__(self):
        # Allows `await websockets.connect(...)` when patched with return_value=fake_ws
        async def _noop():
            return self

        return _noop().__await__()

    def __aiter__(self):
        return self

    async def __anext__(self) -> str:
        msg = await self._to_client.get()
        if msg is None:
            raise StopAsyncIteration
        return msg

    async def _auto_respond(self):
        # 1. Send challenge
        await self._to_client.put(
            json.dumps(
                {
                    "type": "event",
                    "event": "connect.challenge",
                    "payload": {"nonce": "test-nonce", "ts": 1234567890},
                }
            )
        )

        while True:
            raw = await self._from_client.get()
            frame = json.loads(raw)
            method = frame.get("method")
            req_id = frame.get("id", "")
            params = frame.get("params", {})

            if method == "connect":
                await self._to_client.put(
                    json.dumps(
                        {
                            "type": "res",
                            "id": req_id,
                            "ok": True,
                            "payload": self.hello_ok_payload,
                        }
                    )
                )

            elif method == "agents.list":
                await self._to_client.put(
                    json.dumps(
                        {
                            "type": "res",
                            "id": req_id,
                            "ok": self.agents_list_ok,
                            "payload": self.agents_list_payload if self.agents_list_ok else {},
                            **({} if self.agents_list_ok else {"error": "overloaded"}),
                        }
                    )
                )

            elif method == "sessions.subscribe":
                await self._to_client.put(
                    json.dumps(
                        {
                            "type": "res",
                            "id": req_id,
                            "ok": True,
                            "payload": {"subscribed": True},
                        }
                    )
                )

            elif method == "chat.send" and not self.ack_chat_send:
                pass

            elif method == "chat.send":
                sk = params.get("sessionKey", "")
                run_id = str(uuid.uuid4())
                chunks = self.chat_responses.get(sk, ["Hello"])

                await self._to_client.put(
                    json.dumps(
                        {
                            "type": "res",
                            "id": req_id,
                            "ok": True,
                            "payload": {"runId": run_id, "status": "started"},
                        }
                    )
                )
                if self.chat_response_delay > 0:
                    await asyncio.sleep(self.chat_response_delay)
                for tool_data in self.tool_calls.get(sk, []):
                    await self._to_client.put(
                        json.dumps(
                            {
                                "type": "event",
                                "event": "session.tool",
                                "payload": {"sessionKey": sk, "runId": run_id, "data": tool_data},
                            }
                        )
                    )
                for i, chunk in enumerate(chunks):
                    is_last = i == len(chunks) - 1
                    await self._to_client.put(
                        json.dumps(
                            {
                                "type": "event",
                                "event": "chat",
                                "payload": {
                                    "runId": run_id,
                                    "sessionKey": sk,
                                    "seq": i + 1,
                                    "state": "final" if is_last else "delta",
                                    "deltaText": chunk,
                                },
                            }
                        )
                    )
                    if self.chat_response_delay > 0:
                        await asyncio.sleep(self.chat_response_delay)

            elif method == "health":
                await self._to_client.put(
                    json.dumps(
                        {
                            "type": "res",
                            "id": req_id,
                            "ok": True,
                            "payload": {"ok": True},
                        }
                    )
                )

            elif method == "chat.abort":
                pass


def make_client(fake_ws: SmartFakeWS) -> OpenClawClient:
    turn_reg = TurnRegistry()
    dispatcher = FrameDispatcher(turn_reg, ChannelHub())
    client = OpenClawClient("127.0.0.1", 18789, "test-token", turn_reg, dispatcher)
    return client


@pytest.fixture
def fake_ws():
    return SmartFakeWS({"agent:main:client-a": ["Hola ", "mundo"]})


async def connected_client(fake_ws: SmartFakeWS) -> OpenClawClient:
    client = make_client(fake_ws)
    await fake_ws.start()
    with patch("websockets.connect", return_value=fake_ws):
        await client.connect()
    return client


@pytest.mark.asyncio
async def test_connect_returns_gateway_info(fake_ws):
    client = await connected_client(fake_ws)
    assert client.gateway_info is not None
    assert client.gateway_info.default_agent_id == "main"
    assert client.gateway_info.has_agent("assistant")
    await client.close()


@pytest.mark.asyncio
async def test_connect_ignores_an_interleaved_event_with_a_dirty_routing_key():
    """B-r5-1 (revisión): un evento ajeno con sessionKey sucia antes de la respuesta de
    agents.list se ignora; no aborta el connect."""

    class DirtyEventWS(SmartFakeWS):
        async def send(self, data: str) -> None:
            if json.loads(data).get("method") == "agents.list":
                dirty = {"type": "event", "event": "chat", "payload": {"sessionKey": "k\ud800"}}
                await self._to_client.put(json.dumps(dirty))
            await super().send(data)

    client = await connected_client(DirtyEventWS(hello_ok_payload=HELLO_OK_PAYLOAD_NO_SNAPSHOT_AGENTS))
    assert client.gateway_info.has_agent("assistant")
    await client.close()


@pytest.mark.asyncio
async def test_connect_populates_agents_via_agents_list_when_snapshot_lacks_agents():
    """OpenClaw server 2026.6.11+ no longer embeds the agent roster in
    hello-ok's snapshot — it must be fetched via a separate agents.list call
    right after connecting, or has_agent() silently rejects every agent."""
    fake_ws = SmartFakeWS(
        {"agent:main:client-a": ["Hola"]},
        hello_ok_payload=HELLO_OK_PAYLOAD_NO_SNAPSHOT_AGENTS,
    )
    client = await connected_client(fake_ws)
    assert client.gateway_info.has_agent("main")
    assert client.gateway_info.has_agent("assistant")
    assert client.gateway_info.has_agent("ci-tester")
    assert client.gateway_info.default_agent_id == "main"
    await client.close()


@pytest.mark.asyncio
async def test_reconnect_keeps_last_known_roster_when_agents_list_fails():
    """#151: a transient agents.list failure on a *re*connect must not wipe the
    roster (has_agent() would then reject every named agent with 1008)."""
    fake_ws = SmartFakeWS(hello_ok_payload=HELLO_OK_PAYLOAD_NO_SNAPSHOT_AGENTS)
    client = await connected_client(fake_ws)
    assert client.gateway_info.has_agent("ci-tester")

    fake_ws2 = SmartFakeWS(hello_ok_payload=HELLO_OK_PAYLOAD_NO_SNAPSHOT_AGENTS)
    fake_ws2.agents_list_ok = False
    await fake_ws2.start()
    with patch("websockets.connect", return_value=fake_ws2):
        info = await client.connect()

    assert info.has_agent("main")
    assert info.has_agent("assistant")
    assert info.has_agent("ci-tester")
    assert client.gateway_info is info
    await client.close()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "bad", [None, [], {"agents": None}, {"agents": "main"}, {"agents": ["main"]}]
)
async def test_reconnect_with_malformed_agents_list_connects_and_keeps_roster(bad):
    """Auditoría 3.0, B-1: un agents.list `ok:true` mal formado (payload null, `agents` que no
    es una lista de objetos) contaba como roster y rompía el connect para siempre
    (AttributeError/TypeError en cada reintento). Ahora vale como `ok:false`: conecta y
    conserva el roster anterior (#151)."""
    fake_ws = SmartFakeWS(hello_ok_payload=HELLO_OK_PAYLOAD_NO_SNAPSHOT_AGENTS)
    client = await connected_client(fake_ws)

    fake_ws2 = SmartFakeWS(hello_ok_payload=HELLO_OK_PAYLOAD_NO_SNAPSHOT_AGENTS)
    fake_ws2.agents_list_payload = bad
    await fake_ws2.start()
    with patch("websockets.connect", return_value=fake_ws2):
        info = await client.connect()

    assert info.agents == ["main", "assistant", "ci-tester"]
    await client.close()


@pytest.mark.asyncio
async def test_agents_list_entries_without_a_string_id_are_skipped():
    fake_ws = SmartFakeWS(
        hello_ok_payload=HELLO_OK_PAYLOAD_NO_SNAPSHOT_AGENTS,
        agents_list_payload={"defaultId": None, "agents": [{"id": "main"}, {"name": "x"}, {"id": 3}, "y"]},
    )
    client = await connected_client(fake_ws)
    assert client.gateway_info.agents == ["main"]
    assert client.gateway_info.default_agent_id == "main"
    await client.close()


@pytest.mark.asyncio
async def test_first_connect_with_failed_agents_list_has_empty_roster():
    """No previous roster to fall back to: behaviour unchanged (empty, warning)."""
    fake_ws = SmartFakeWS(hello_ok_payload=HELLO_OK_PAYLOAD_NO_SNAPSHOT_AGENTS)
    fake_ws.agents_list_ok = False
    client = await connected_client(fake_ws)
    assert client.gateway_info.agents == []
    await client.close()


@pytest.mark.asyncio
async def test_reconnect_replaces_roster_when_agents_list_succeeds():
    fake_ws = SmartFakeWS(hello_ok_payload=HELLO_OK_PAYLOAD_NO_SNAPSHOT_AGENTS)
    client = await connected_client(fake_ws)

    fake_ws2 = SmartFakeWS(
        hello_ok_payload=HELLO_OK_PAYLOAD_NO_SNAPSHOT_AGENTS,
        agents_list_payload={"defaultId": "main", "agents": [{"id": "main", "name": "Main"}]},
    )
    await fake_ws2.start()
    with patch("websockets.connect", return_value=fake_ws2):
        info = await client.connect()

    assert set(info.agents) == {"main"}
    await client.close()


@pytest.mark.asyncio
async def test_connect_sends_sessions_subscribe(fake_ws):
    client = await connected_client(fake_ws)
    methods = [f["method"] for f in fake_ws.sent_frames]
    assert "sessions.subscribe" in methods
    await client.close()


@pytest.mark.asyncio
async def test_send_returns_the_run_id_of_its_ack(fake_ws):
    """5.0 (#406): `send` waits only for the ack and returns its runId; `chat.send` carries a
    flat `sessionKey` and no `queueMode` unless asked (untried live, 2b.3)."""
    client = await connected_client(fake_ws)
    run_id = await client.send("hola", "agent:main:client-a")
    (chat_send,) = [f for f in fake_ws.sent_frames if f.get("method") == "chat.send"]
    assert isinstance(run_id, str) and run_id
    assert chat_send["params"]["sessionKey"] == "agent:main:client-a"
    assert "session" not in chat_send["params"]
    assert "queueMode" not in chat_send["params"]
    await client.close()


@pytest.mark.asyncio
async def test_abort_run_frame_schema(fake_ws):
    """`chat.abort` of one run: flat `sessionKey` and its `runId`, never the whole key."""
    client = await connected_client(fake_ws)
    await client.abort_run("agent:main:client-a", "run-1", timeout=0.05)  # never answered
    (abort,) = [f for f in fake_ws.sent_frames if f.get("method") == "chat.abort"]
    assert abort["params"] == {"sessionKey": "agent:main:client-a", "runId": "run-1"}
    await client.close()


@pytest.mark.asyncio
async def test_reconnect_fails_a_send_waiting_for_its_ack():
    """#103/G-04: a reconnect (here, forced) tears the old connection down; a `chat.send`
    still waiting for its ack fails with `orchestrator_disconnected` instead of hanging."""
    silent_ws = SmartFakeWS()
    silent_ws.ack_chat_send = False
    client = await connected_client(silent_ws)
    send = asyncio.create_task(client.send("hola", "agent:main:client-a"))
    await asyncio.sleep(0.01)  # chat.send on the wire, no ack

    reconnect_ws = SmartFakeWS()
    await reconnect_ws.start()
    with patch("websockets.connect", return_value=reconnect_ws):
        await client.connect()

    with pytest.raises(OrchestratorError, match="orchestrator_disconnected"):
        await asyncio.wait_for(send, timeout=1.0)
    await client.close()


@pytest.mark.asyncio
async def test_ping_returns_true_when_connected(fake_ws):
    client = await connected_client(fake_ws)
    result = await client.ping()
    assert result is True
    await client.close()


@pytest.mark.asyncio
async def test_ping_returns_false_when_not_connected():
    client = make_client(SmartFakeWS())
    result = await client.ping()
    assert result is False


@pytest.mark.asyncio
async def test_on_disconnect_called_on_ws_drop(fake_ws):
    client = await connected_client(fake_ws)
    disconnected = []
    client.on_disconnect = lambda: disconnected.append(True)
    await fake_ws.close()
    await asyncio.sleep(0.05)
    assert disconnected == [True]


@pytest.mark.asyncio
async def test_connect_frame_schema(fake_ws):
    """connect frame must carry protocol range, auth token, and operator role."""
    client = await connected_client(fake_ws)
    connect_frames = [f for f in fake_ws.sent_frames if f.get("method") == "connect"]
    assert len(connect_frames) == 1
    p = connect_frames[0]["params"]
    assert p["minProtocol"] == 3
    assert p["maxProtocol"] == 4
    assert p["role"] == "operator"
    assert "token" in p["auth"]
    assert p["client"]["mode"] == "backend"
    await client.close()


# ── OpenClaw ≥2026.11 arrancando: UNAVAILABLE / startup-sidecars ───


def real_ws_client(port: int) -> OpenClawClient:
    turns = TurnRegistry()
    return OpenClawClient("127.0.0.1", port, "tok", turns, FrameDispatcher(turns, ChannelHub()))


@pytest.mark.parametrize(
    ("host", "uri"),
    [
        ("::1", "ws://[::1]:18789"),
        ("[::1]", "ws://[::1]:18789"),  # ya entre corchetes a mano (instalación manual): igual que antes
        ("127.0.0.1", "ws://127.0.0.1:18789"),
    ],
)
def test_ipv6_host_is_bracketed_in_uri(host, uri):
    turns = TurnRegistry()
    client = OpenClawClient(host, 18789, "tok", turns, FrameDispatcher(turns, ChannelHub()))
    assert client._uri == uri


@pytest.mark.parametrize(
    ("host", "uri"),
    [("green-house.example.ts.net", "wss://green-house.example.ts.net:443"), ("::1", "wss://[::1]:443")],
)
def test_tls_uses_wss(host, uri):
    turns = TurnRegistry()
    client = OpenClawClient(host, 443, "tok", turns, FrameDispatcher(turns, ChannelHub()), tls=True)
    assert client._uri == uri


@pytest.mark.asyncio
async def test_connect_retries_on_startup_sidecars():
    async with FakeOpenClawServer(startup_pending=1) as server:
        client = real_ws_client(server.port)
        loop = asyncio.get_running_loop()
        started = loop.time()
        info = await client.connect()
        try:
            assert info.default_agent_id == "main"
            assert server.connections == 2  # el segundo handshake va en un socket nuevo
            assert loop.time() - started >= 0.09  # esperó: retryAfterMs 50 → suelo de 100 ms
        finally:
            await client.close()


_SIDECARS = {"code": "UNAVAILABLE", "retryable": True, "details": {"reason": "startup-sidecars"}}


@pytest.mark.parametrize(
    ("error", "wait_s"),
    [
        ({**_SIDECARS, "retryAfterMs": 500}, 0.5),
        ({**_SIDECARS, "retryAfterMs": 60_000}, 5.0),  # tope de 5 s por espera
        ({**_SIDECARS, "retryAfterMs": 0}, 0.1),  # suelo: no martillear a OpenClaw
        (_SIDECARS, 0.5),  # sin retryAfterMs: 500 ms, como su cliente de referencia
        ({**_SIDECARS, "retryAfterMs": True}, 0.5),
        ({**_SIDECARS, "retryAfterMs": float("nan")}, 0.5),
        ({**_SIDECARS, "retryAfterMs": 10**400}, 5.0),  # int enorme: sin OverflowError
        ({**_SIDECARS, "retryable": False}, None),
        ({**_SIDECARS, "details": {"reason": "authenticated-profile-unavailable"}}, None),
        ({**_SIDECARS, "code": "UNAUTHORIZED"}, None),
        ({"code": "UNAVAILABLE", "retryable": True, "retryAfterMs": 500}, None),
        (None, None),
    ],
)
def test_startup_retry_after_only_for_startup_sidecars(error, wait_s):
    assert _startup_retry_after_s(error) == wait_s


@pytest.mark.asyncio
@pytest.mark.parametrize("retry_after_ms", [50, 5000])  # 5000: la espera se recorta a lo que queda
async def test_connect_gives_up_after_budget(monkeypatch, caplog, retry_after_ms):
    caplog.set_level(logging.DEBUG, logger="fogar_gateway")
    monkeypatch.setattr("fogar_gateway.services.openclaw.client._STARTUP_RETRY_BUDGET_S", 0.3)
    async with FakeOpenClawServer(startup_pending=10**6, startup_retry_after_ms=retry_after_ms) as server:
        client = real_ws_client(server.port)
        loop = asyncio.get_running_loop()
        started = loop.time()
        with pytest.raises(OrchestratorHandshakeError) as excinfo:
            await client.connect()
        assert 0.3 <= loop.time() - started < 1.0  # agota el presupuesto, sin pasarse
        # Distinguible de un token rechazado (verify_openclaw), con el mismo código estable de hoy.
        assert excinfo.type is OrchestratorStartingError
        assert excinfo.value.stable_code == ORCHESTRATOR_HANDSHAKE_FAILED_ERROR
        assert server.connections > 1
        assert client._ws is None
        assert "retry shortly" not in caplog.text  # nunca el texto de OpenClaw
        retry_levels = [r.levelno for r in caplog.records if "startup-sidecars" in r.getMessage()]
        assert retry_levels.count(logging.INFO) == 1  # solo el primer reintento; el resto, DEBUG


@pytest.mark.asyncio
async def test_close_during_startup_wait_cancels_retry():
    """close() del wrapper corta la espera de retryAfterMs: no queda socket ni otro intento."""
    async with FakeOpenClawServer(startup_pending=1, startup_retry_after_ms=5000) as server:
        inner = real_ws_client(server.port)
        roc = ReconnectingOpenClawClient(inner, name="openclaw", initial_backoff=60.0)
        roc.trigger_reconnect()
        async with asyncio.timeout(2):
            while not (server.connections == 1 and not server._conns and inner._ws is None):
                await asyncio.sleep(0.005)
        async with asyncio.timeout(1):
            await roc.close()
        assert server.connections == 1
        assert inner._ws is None


@pytest.mark.asyncio
async def test_connect_twice_closes_and_cancels_previous_connection():
    """Issue #103: connect() must tear down the previous ws/listener/keepalive
    before establishing a new one. Otherwise the old listener keeps dispatching
    frames from the old socket to the shared dispatcher, causing false
    error_all events on healthy sessions, and the old socket/keepalive leak."""
    fake_ws1 = SmartFakeWS({"agent:main:client-a": ["Hola"]})
    client = await connected_client(fake_ws1)
    old_listener_task = client._listener_task
    old_keepalive_task = client._keepalive_task
    assert not old_listener_task.done()

    disconnected = []
    client.on_disconnect = lambda: disconnected.append(True)

    fake_ws2 = SmartFakeWS({"agent:main:client-a": ["Hola"]})
    await fake_ws2.start()
    with patch("websockets.connect", return_value=fake_ws2):
        await client.connect()

    assert fake_ws1.closed, "previous socket must be closed on reconnect"
    assert old_listener_task.done(), "previous listener task must be cancelled"
    assert old_keepalive_task.done(), "previous keepalive task must be cancelled"
    assert client._listener_task is not old_listener_task
    assert not client._listener_task.done()

    # A forced reconnect legitimately replaces a healthy connection — cancelling
    # the old listener must not be mistaken for an unexpected drop, or
    # ReconnectingOpenClawClient would flip back into RECONNECTING right after
    # a successful forced reconnect.
    await asyncio.sleep(0.05)
    assert disconnected == []

    await client.close()


@pytest.mark.asyncio
async def test_concurrent_connect_calls_are_serialized():
    """Two connect() calls racing (e.g. an admin-triggered reconnect racing the
    background reconnect loop) must not interleave their handshakes — a
    _connect_lock serializes them so exactly one final ws/listener survives,
    with the other cleanly closed rather than orphaned."""
    client = make_client(SmartFakeWS())
    fake_ws_a = SmartFakeWS({"agent:main:client-a": ["Hola"]})
    fake_ws_b = SmartFakeWS({"agent:main:client-a": ["Hola"]})
    await fake_ws_a.start()
    await fake_ws_b.start()

    with patch("websockets.connect", side_effect=[fake_ws_a, fake_ws_b]):
        await asyncio.wait_for(
            asyncio.gather(client.connect(), client.connect()),
            timeout=1.0,
        )

    assert [fake_ws_a.closed, fake_ws_b.closed].count(True) == 1, (
        "exactly one of the two sockets must be closed as the loser of the race"
    )
    assert client._ws in (fake_ws_a, fake_ws_b)
    assert not client._listener_task.done()

    await client.close()


@pytest.mark.asyncio
async def test_admin_trigger_reconnect_coalesces_with_real_background_reconnect(fake_ws):
    """End-to-end version of the issue #103 coalescing guarantee: a REAL
    background reconnect (started by ReconnectingOpenClawClient after an
    unexpected drop) that is still mid-handshake must not be joined by a
    second, concurrent websocket handshake when an admin trigger_reconnect()
    call races it — it must coalesce onto the one in-flight attempt."""
    inner = await connected_client(fake_ws)
    roc = ReconnectingOpenClawClient(inner, "openclaw")  # rewires inner.on_disconnect

    reconnect_ws = SmartFakeWS({"agent:main:client-a": ["Hola"]})
    await reconnect_ws.start()
    connect_attempted = asyncio.Event()
    release_connect = asyncio.Event()
    connect_calls = 0

    async def gated_connect(uri, **kwargs):
        nonlocal connect_calls
        connect_calls += 1
        if connect_calls == 1:
            connect_attempted.set()
            await release_connect.wait()
        return reconnect_ws

    with patch("websockets.connect", side_effect=gated_connect):
        await fake_ws.close()  # simulate an unexpected drop → background reconnect starts
        await connect_attempted.wait()  # background reconnect is now mid-handshake

        roc.trigger_reconnect()

        release_connect.set()
        await asyncio.sleep(0.05)

    assert connect_calls == 1, "admin trigger must coalesce, not open a second socket"
    assert roc.state == ConnectionState.CONNECTED
    await inner.close()
