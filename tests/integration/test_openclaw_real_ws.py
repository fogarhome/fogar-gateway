"""Frontera con OpenClaw contra un servidor `websockets` REAL (issue #213, M6a).

Los dobles de `SmartFakeWS` reprodujeron el diseño del cliente y ocultaron bugs
(G-03/G-04/G-05). Aquí el servidor es un `websockets.serve` en el puerto 0 y el cliente
es el `OpenClawClient` de producción, con dos sesiones concurrentes sobre la misma
conexión multiplexada.
"""

import asyncio
import json
import logging
from dataclasses import dataclass

import pytest

from fogar_gateway.services.channel import ChannelError, ChannelHub, ChannelToken
from fogar_gateway.services.conversations import ConversationIndex
from fogar_gateway.services.openclaw.client import OpenClawClient
from fogar_gateway.services.openclaw.dispatcher import FrameDispatcher
from fogar_gateway.services.openclaw.registry import TurnRegistry
from fogar_gateway.services.orchestration import OrchestratorError
from tests.fakes.bridge import keyed_conversation
from tests.fakes.openclaw import MIB, FakeOpenClawServer

SK_A = "agent:main:client-a"
SK_B = "agent:main:client-b"


def make_client(port, token="tok"):
    turns = TurnRegistry()
    hub = ChannelHub(turn_registry=turns)
    dispatcher = FrameDispatcher(turns, hub)
    client = OpenClawClient("127.0.0.1", port, token, turns, dispatcher)
    hub.orchestrator = client
    return client, turns


@dataclass
class Event:
    type: str  # token | error | status
    content: str


def _hub(client) -> ChannelHub:
    inner = getattr(client, "_client", client)  # ReconnectingOpenClawClient → its client
    return inner._dispatcher._channels


async def run_turn(client, sk, text):
    """A turn through its key's channel, as /v1 runs it since 5.0 PR 2b.3 (`send_exclusive`):
    the tokens and errors of its own run, then `done` if it ended well."""
    _, agent, client_id = sk.split(":")
    channel = _hub(client).get_or_create(keyed_conversation(client_id, agent))
    events: list[Event] = []

    def sink(event) -> None:
        if isinstance(event, ChannelToken):
            events.append(Event("token", event.text))
        elif isinstance(event, ChannelError):
            events.append(Event("error", event.code))

    outcome = await channel.send_exclusive(text, by_client_id=client_id, by_name=client_id, sink=sink)
    if outcome.error is not None:
        if outcome.error not in errors(events):
            events.append(Event("error", outcome.error))
    elif not outcome.cut:
        events.append(Event("status", "done"))
    return events


async def _sent(server, sk, n=1):
    """Wait until `n` `chat.send` of `sk` reached the server."""
    for _ in range(250):
        if [m for m, key in server.calls if key == sk].count("chat.send") >= n:
            return
        await asyncio.sleep(0.02)
    raise AssertionError(f"no chat.send for {sk}")


def _no_turn(client, sk) -> bool:
    channel = _hub(client).get(sk)
    return channel is None or not channel.user_turn_running()


def tokens(events):
    return "".join(e.content for e in events if e.type == "token")


def errors(events):
    return [e.content for e in events if e.type == "error"]


@pytest.fixture
async def server():
    async with FakeOpenClawServer() as srv:
        yield srv


async def _two_sessions(server, text_a, text_b, release_after=None):
    """B queda en `hold`; A ejecuta `text_a`. Devuelve (events_a, events_b, client, turns)."""
    client, turns = make_client(server.port)
    await client.connect()
    task_b = asyncio.create_task(run_turn(client, SK_B, text_b))
    await _sent(server, SK_B)
    events_a = await asyncio.wait_for(run_turn(client, SK_A, text_a), 5)
    return events_a, task_b, client, turns


# ── G-03 ──────────────────────────────────────────────────────────


async def test_malformed_frames_do_not_affect_concurrent_session(server):
    events_a, task_b, client, turns = await _two_sessions(server, "malformed", "hold")
    try:
        server.release.set()
        events_b = await asyncio.wait_for(task_b, 5)
        assert errors(events_a) == [] and tokens(events_a) == "reply-malformed"
        assert errors(events_b) == [] and tokens(events_b) == "reply-hold"
        assert server.connections == 1
    finally:
        await client.close()


async def test_malformed_frame_warning_does_not_leak_content(server, caplog):
    caplog.set_level(logging.WARNING)
    events_a, task_b, client, _ = await _two_sessions(server, "malformed", "hold")
    try:
        server.release.set()
        await asyncio.wait_for(task_b, 5)
        assert "this is not json" not in caplog.text
        assert any("malformed" in r.getMessage().lower() for r in caplog.records)
    finally:
        await client.close()


# ── G-04 ──────────────────────────────────────────────────────────


async def test_clean_close_errors_pending_turns_fast_and_frees_session_key(server):
    client, turns = make_client(server.port)
    await client.connect()
    task_b = asyncio.create_task(run_turn(client, SK_B, "hold"))
    await _sent(server, SK_B)
    try:
        events_a = await asyncio.wait_for(run_turn(client, SK_A, "close"), 3)
        events_b = await asyncio.wait_for(task_b, 3)
        assert errors(events_a) == ["orchestrator_disconnected"]
        assert errors(events_b) == ["orchestrator_disconnected"]
        assert _no_turn(client, SK_B) and _no_turn(client, SK_A)
    finally:
        server.release.set()
        await client.close()


async def test_listen_errors_use_stable_message_not_raw_exception():
    turns = TurnRegistry()
    client = OpenClawClient("x", 1, "t", turns, FrameDispatcher(turns, ChannelHub()))

    class Boom:
        def __aiter__(self):
            return self

        async def __anext__(self):
            raise RuntimeError("secret internal detail")

    client._ws = Boom()
    ack = asyncio.get_running_loop().create_future()
    client._send_futures["r"] = ack  # a chat.send waiting for its ack
    await client._listen()
    with pytest.raises(OrchestratorError, match=r"^orchestrator_disconnected$"):
        ack.result()


# ── G-05 ──────────────────────────────────────────────────────────


async def test_two_mib_frame_does_not_drop_connection_or_other_session(server):
    events_a, task_b, client, _ = await _two_sessions(server, "big", "hold")
    try:
        server.release.set()
        events_b = await asyncio.wait_for(task_b, 5)
        assert errors(events_a) == [] and len(tokens(events_a)) == 2 * MIB
        assert errors(events_b) == [] and tokens(events_b) == "reply-hold"
        assert server.connections == 1
    finally:
        await client.close()


async def test_frame_larger_than_default_follows_announced_max_payload():
    hello = {
        "type": "hello-ok",
        "protocol": 4,
        "server": {"version": "9", "connId": "c"},
        "policy": {"tickIntervalMs": 30000, "maxPayload": 40 * MIB},
        "snapshot": {"sessionDefaults": {"defaultAgentId": "main"}},
    }
    async with FakeOpenClawServer(hello_payload=hello, big_bytes=30 * MIB) as srv:
        client, _ = make_client(srv.port)
        await client.connect()
        try:
            events = await asyncio.wait_for(run_turn(client, SK_A, "big"), 10)
            assert errors(events) == [] and len(tokens(events)) == 30 * MIB
        finally:
            await client.close()


def test_frame_limit_clamps():
    from fogar_gateway.services.openclaw.client import _frame_limit

    assert _frame_limit(25 * MIB) == 25 * MIB
    assert _frame_limit(10**12) == 64 * MIB
    assert _frame_limit(10) == 1 * MIB
    assert _frame_limit(None) == 25 * MIB
    assert _frame_limit("junk") == 25 * MIB


# ── G-47 ──────────────────────────────────────────────────────────


async def test_ok_true_res_other_than_started_does_not_end_turn(server):
    client, _ = make_client(server.port)
    await client.connect()
    try:
        events = await asyncio.wait_for(run_turn(client, SK_A, "accepted"), 5)
        assert tokens(events) == "reply-accepted"
        assert errors(events) == []
    finally:
        await client.close()


# ── G-48 ──────────────────────────────────────────────────────────


async def test_hello_ok_server_and_protocol_are_logged_once(server, caplog):
    caplog.set_level(logging.INFO, logger="fogar_gateway.services.openclaw.client")
    client, _ = make_client(server.port)
    await client.connect()
    try:
        recs = [r.getMessage() for r in caplog.records if "hello-ok" in r.getMessage()]
        assert len(recs) == 1
        assert "2026.6.11" in recs[0] and "protocol=4" in recs[0]
    finally:
        await client.close()


# ── Observabilidad de la caída (revisión de #226) ─────────────────


async def test_1009_close_logs_code_but_no_frame_content(server, caplog):
    caplog.set_level(logging.DEBUG, logger="fogar_gateway.services.openclaw.client")
    client, _ = make_client(server.port)
    await client.connect()
    try:
        events = await asyncio.wait_for(run_turn(client, SK_A, "huge"), 10)
        assert errors(events) == ["orchestrator_disconnected"]
        assert "1009" in caplog.text
        assert "SECRETCONTENT" not in caplog.text
    finally:
        await client.close()


async def test_handler_error_logs_traceback_without_exception_message(caplog):
    caplog.set_level(logging.DEBUG, logger="fogar_gateway.services.openclaw.client")
    turns = TurnRegistry()
    client = OpenClawClient("x", 1, "t", turns, FrameDispatcher(turns, ChannelHub()))

    class Boom:
        def __aiter__(self):
            return self

        async def __anext__(self):
            raise RuntimeError("secret internal detail")

    client._ws = Boom()
    await client._listen()
    assert "secret internal detail" not in caplog.text
    assert "RuntimeError" in caplog.text
    assert "__anext__" in caplog.text  # traceback frames (code lines) are present


# ── Reconexión tras cierre limpio (hueco de cobertura de #226) ────


async def test_clean_close_fires_on_disconnect_after_connection_lost_and_new_connection_is_clean(
    server,
):
    from fogar_gateway.services.openclaw.reconnecting import ReconnectingOpenClawClient
    from fogar_gateway.services.reconnection import ConnectionState

    client, turns = make_client(server.port)
    order = []
    dispatcher = client._dispatcher
    real_lost = dispatcher.connection_lost
    dispatcher.connection_lost = lambda msg: (order.append(("connection_lost", msg)), real_lost(msg))[1]
    roc = ReconnectingOpenClawClient(client, "oc", initial_backoff=0.05, max_backoff=0.1)
    _hub(client).orchestrator = roc
    real_on_disc = client.on_disconnect
    client.on_disconnect = lambda: (order.append(("on_disconnect", None)), real_on_disc())[1]
    await roc.connect()
    try:
        events = await asyncio.wait_for(run_turn(roc, SK_A, "close"), 3)
        assert errors(events) == ["orchestrator_disconnected"]
        assert order[:2] == [("connection_lost", "orchestrator_disconnected"), ("on_disconnect", None)]
        for _ in range(100):
            if roc.state == ConnectionState.CONNECTED and server.connections == 2:
                break
            await asyncio.sleep(0.05)
        assert server.connections == 2 and roc.state == ConnectionState.CONNECTED
        calls_before = len(order)
        events = await asyncio.wait_for(run_turn(roc, SK_A, "ok"), 3)
        assert errors(events) == [] and tokens(events) == "reply-ok"
        await asyncio.sleep(0.1)
        assert [o for o in order[calls_before:] if o[0] == "connection_lost"] == []
        assert [o for o in order if o[0] == "connection_lost"] == [
            ("connection_lost", "orchestrator_disconnected")
        ]
    finally:
        await roc.close()


# ── Escenarios del arnés e2e (M7a) ────────────────────────────────


async def test_fail_ends_turn_with_stable_orchestrator_error(server):
    client, _ = make_client(server.port)
    await client.connect()
    try:
        events = await asyncio.wait_for(run_turn(client, SK_A, "fail"), 5)
        assert errors(events) == ["orchestrator_error"] and tokens(events) == ""
    finally:
        await client.close()


async def test_agent_error_after_ack_ends_turn_with_stable_error(server, caplog):
    # #300: el chat state:error termina el turno ya, sin esperar a turn_timeout.
    # B-r4-2: `websockets` a DEBUG volcaría las tramas (el connect con el token): connect lo fija.
    caplog.set_level(logging.DEBUG)
    for name in ("websockets", "websockets.client"):
        caplog.set_level(logging.DEBUG, logger=name)
    client, turns = make_client(server.port, token="SECRET-OC-TOKEN")
    await client.connect()
    try:
        events = await asyncio.wait_for(run_turn(client, SK_A, "agent_error"), 3)
        assert errors(events) == ["orchestrator_error"] and tokens(events) == ""
        assert _no_turn(client, SK_A)
        assert "No API key" not in caplog.text
        assert "SECRET-OC-TOKEN" not in caplog.text
    finally:
        await client.close()


async def test_drop_mid_turn_delivers_delta_then_disconnect_error(server):
    client, turns = make_client(server.port)
    await client.connect()
    try:
        events = await asyncio.wait_for(run_turn(client, SK_A, "drop"), 5)
        assert tokens(events) == "reply-drop"
        assert errors(events) == ["orchestrator_disconnected"]
        assert _no_turn(client, SK_A)
    finally:
        await client.close()


def _route_pushes_to(client, fake) -> None:
    """The pushes of client-a's conversation reach `fake` (5.0: through its channel, which
    the dispatcher finds by the conversation index)."""
    conversation = keyed_conversation("client-a")
    hub = client._dispatcher._channels
    hub.index = ConversationIndex()
    hub.index.add(conversation)
    hub.channel_for(conversation.session_key).enqueue_push = fake.enqueue_push


async def test_push_reaches_registered_bridge_in_order(server):
    class FakeBridge:
        def __init__(self):
            self.calls = []

        def enqueue_push(self, kind, payload, run_id=None):
            self.calls.append((kind, payload))

    client, _ = make_client(server.port)
    bridge = FakeBridge()
    _route_pushes_to(client, bridge)
    await client.connect()
    try:
        await server.push(SK_A, "hola")
        for _ in range(100):
            if len(bridge.calls) >= 4:
                break
            await asyncio.sleep(0.02)
        assert [k for k, _ in bridge.calls] == ["turn_start", "chat", "chat", "turn_end"]
        assert bridge.calls[1][1]["deltaText"] == "hola" and bridge.calls[2][1]["state"] == "final"
    finally:
        await client.close()


async def test_push_without_open_connection_raises(server):
    with pytest.raises(RuntimeError):
        await server.push(SK_A, "x")


# ── Cableado de ChatTextTracker en el canal (tramas de producción) ──
# Los casos del tracker viven en tests/unit/test_chat_text.py.

TEXT = "SECRETREPLY"


def _msg(text):
    return {"role": "assistant", "content": [{"type": "text", "text": text}], "timestamp": 1}


RUN = "run-scripted"  # 2026.10.5: every `chat` event carries its run's `runId` (#219)


def _ev(sk, **kw):
    payload = {"sessionKey": sk, "runId": RUN, **kw}
    return json.dumps({"type": "event", "event": "chat", "payload": payload})


def _binary(sk, **kw):
    """Trama binaria con los surrogates crudos en UTF-8 (sin escapes)."""
    frame = {"type": "event", "event": "chat", "payload": {"sessionKey": sk, "runId": RUN, **kw}}
    return json.dumps(frame, ensure_ascii=False).encode("utf-8", "surrogatepass")


def _delta(sk, text, seq, replace=False):
    kw = {"state": "delta", "seq": seq, "deltaText": text, "message": _msg(text)}
    if replace:
        kw["replace"] = True
    return _ev(sk, **kw)


def _final(sk, text, seq=11):
    return _ev(sk, state="final", seq=seq, stopReason="stop", message=_msg(text))


class Scripted2026_10_5(FakeOpenClawServer):
    async def _chat_send(self, ws, rid, params, run_id=None):
        sk, scenario = params["sessionKey"], params["message"]
        await ws.send(self._res(rid, {"runId": RUN, "status": "started"}))
        if scenario == "captured":
            for phase, seq in (
                ("preparing_workspace", 1),
                ("preparing_context", 2),
                ("starting_model", 4),
            ):
                await ws.send(_ev(sk, state="status", phase=phase, seq=seq))
            await ws.send(_delta(sk, "1, 2, 3,", 6))
            await ws.send(_delta(sk, "", 9, replace=True))
            await ws.send(_final(sk, "1, 2, 3, 4, 5"))
        elif scenario == "split_emoji":  # B-r4-1: solo el 1.er delta trae `message`
            await ws.send(_delta(sk, "Hola ", 1))
            await ws.send(_ev(sk, state="delta", seq=2, deltaText="amigo \ud83d"))
            await ws.send(_ev(sk, state="delta", seq=3, deltaText="\ude00 qué tal"))
            await ws.send(_final(sk, "Hola amigo \U0001f600 qué tal"))
        elif scenario == "split_emoji_binary":  # revisión r4: snapshots en tramas binarias
            for text in ("Hola amigo \ud83d", "Hola amigo \U0001f600 qué tal"):
                await ws.send(_binary(sk, state="delta", seq=1, message=_msg(text)))
            await ws.send(_binary(sk, state="final", seq=2, message=_msg("Hola amigo \U0001f600 qué tal")))
        elif scenario == "multi_message":
            await ws.send(_delta(sk, "Voy a buscar. ", 1))
            await ws.send(_final(sk, f"Resultado: {TEXT}"))


@pytest.fixture
async def scripted_server():
    async with Scripted2026_10_5() as srv:
        yield srv


async def _turn(server, scenario):
    client, _ = make_client(server.port)
    await client.connect()
    try:
        return await run_turn(client, SK_A, scenario)
    finally:
        await client.close()


def _toks(events):
    return [e.content for e in events if e.type == "token"]


async def test_captured_sequence_delivers_full_text_then_done(scripted_server):
    events = await _turn(scripted_server, "captured")
    assert _toks(events) == ["1, 2, 3,", " 4, 5"]
    assert events[-1].type == "status" and events[-1].content == "done"
    assert errors(events) == []


async def test_emoji_split_between_deltas_is_delivered_once(scripted_server):
    events = await _turn(scripted_server, "split_emoji")
    assert "".join(_toks(events)) == "Hola amigo \U0001f600 qué tal"
    assert errors(events) == []


async def test_emoji_split_in_binary_snapshots_is_delivered_once(scripted_server):
    events = await _turn(scripted_server, "split_emoji_binary")
    assert "".join(_toks(events)) == "Hola amigo \U0001f600 qué tal"
    assert errors(events) == []


async def test_diverging_final_emits_full_final_and_warns_without_content(scripted_server, caplog):
    caplog.set_level(logging.DEBUG)
    events = await _turn(scripted_server, "multi_message")
    assert _toks(events) == ["Voy a buscar. ", f"Resultado: {TEXT}"]
    assert events[-1].content == "done"
    assert any("diverges" in r.getMessage() for r in caplog.records)
    assert TEXT not in caplog.text
    assert "Voy a buscar" not in caplog.text


# ── 7e: el `aborted` de un run abortado no llega a nadie (G-14) ──


async def test_abort_absorbs_its_aborted_frame(server):
    class FakeBridge:
        def __init__(self):
            self.calls = []

        def enqueue_push(self, kind, payload, run_id=None):
            self.calls.append((kind, payload))

    client, turns = make_client(server.port)
    bridge = FakeBridge()
    _route_pushes_to(client, bridge)
    await client.connect()
    try:
        await server.push(SK_A, "aviso", end=False)
        for _ in range(100):
            if len(bridge.calls) >= 2:
                break
            await asyncio.sleep(0.02)
        # 5.0: only by its runId, tombstoned first (as the channel does): its `aborted` and
        # lifecycle `end` are dropped by the dispatcher.
        turns.abort_run("run-1")
        await asyncio.wait_for(client.abort_run(SK_A, "run-1"), 2)
        await asyncio.sleep(0.05)

        assert server.aborts == [{"sessionKey": SK_A, "runId": "run-1"}]
        assert not [p for k, p in bridge.calls if k == "chat" and p.get("state") == "aborted"]
        events = await asyncio.wait_for(run_turn(client, SK_A, "hola"), 5)
        assert tokens(events) == "reply-hola" and not errors(events)
        # #285: the answer named the aborted run; its late frames are dropped from now on
        assert turns.is_run_aborted("run-1")
    finally:
        await client.close()


# ── #285: correlación por runId ──


async def test_aborted_turn_frames_never_reach_the_next_turn(server):
    """A turn cancelled mid-run (barge-in) is aborted; the reply of its run that was
    already on its way is dropped instead of closing the next turn (G-14)."""
    client, turns = make_client(server.port)
    await client.connect()
    try:
        first = asyncio.create_task(run_turn(client, SK_A, "wait"))
        await server.wait_held(1)
        await asyncio.sleep(0.05)  # the ack (with run-1) arrived
        first.cancel()
        await asyncio.gather(first, return_exceptions=True)
        second = asyncio.create_task(run_turn(client, SK_A, "wait again"))
        await server.wait_held(2)
        await server.finish_next()  # the reply of the aborted run arrives late
        await server.finish_next()
        events = await asyncio.wait_for(second, 5)
        assert tokens(events) == "reply-wait again" and not errors(events)
        assert turns.is_run_aborted("run-1") and not turns.is_run_aborted("run-2")
    finally:
        await client.close()


async def test_terminal_chat_of_another_run_does_not_end_the_turn(server):
    """#285: once the turn knows its run (from the ack), a `final` of another run on the
    same session key does not close it; its own `final` does."""
    client, turns = make_client(server.port)
    await client.connect()
    try:
        task = asyncio.create_task(run_turn(client, SK_A, "wait"))
        await server.wait_held(1)
        await asyncio.sleep(0.05)
        other = {"sessionKey": SK_A, "runId": "run-other", "state": "final", "deltaText": "x"}
        await client._dispatcher.dispatch({"type": "event", "event": "chat", "payload": other})
        await asyncio.sleep(0.05)
        assert not task.done()
        await server.finish_next()
        events = await asyncio.wait_for(task, 5)
        assert tokens(events) == "reply-wait" and not errors(events)
    finally:
        await client.close()
