"""Messages go to OpenClaw's queue (5.0 PR 2b.1, #406).

Two real sessions (`FogarBridge`) of two devices attached to one conversation's channel, a real
`OpenClawClient` and `FrameDispatcher`, and the OpenClaw fake in `queue` mode (2026.11.7,
`followup`: one run at a time per key; a run drained from the queue gets no lifecycle `start`
and its `final` carries no `message`; docs/openclaw-protocol.md «C3 queue behaviour»).

What the plan asks of 2b.1: a `send` during a turn never aborts anything (it waits, `pending`);
every accepted message reaches every attached session as `message` (with `self`); `interrupt`
aborts the active run only; `withdraw` aborts that pending run only; `cancel` from any session
ends the turn for all; frames before the ack wait for it; `turn_start.by`; a late session gets
the pending messages; one turn_end per turn_start on every socket.
"""

import asyncio
import json

import pytest

from fogar_gateway.models.schemas import Client
from fogar_gateway.services.channel import ChannelHub
from fogar_gateway.services.conversations import ConversationIndex, SessionConversation
from fogar_gateway.services.openclaw.client import OpenClawClient
from fogar_gateway.services.openclaw.dispatcher import FrameDispatcher
from fogar_gateway.services.openclaw.registry import ClientRegistry, TurnRegistry
from tests.fakes.bridge import keyed_conversation, sent, wait_until
from tests.fakes.openclaw import FakeOpenClawServer, SendOnlyOpenClawWS

CONVERSATION = keyed_conversation("shared", id="conv-q")
SK = CONVERSATION.session_key
NAMES = {"tv": "Tele del salón", "mac": "Mac de Ana", "web": "Web"}


@pytest.fixture
async def openclaw():
    async with FakeOpenClawServer(queue=True) as server:
        yield server


@pytest.fixture
async def hub(openclaw):
    index = ConversationIndex()
    index.add(CONVERSATION)
    turns = TurnRegistry()
    hub = ChannelHub(index=index, turn_registry=turns)
    client = OpenClawClient("127.0.0.1", openclaw.port, "tok", turns, FrameDispatcher(turns, hub))
    hub.orchestrator = client
    await client.connect()
    yield hub
    await hub.aclose(1.0)
    await client.close()


@pytest.fixture
def session(make_bridge, hub):
    """`session(client_id) -> (bridge, ws)`: a device's session attached to the shared
    conversation's channel."""

    def _make(client_id, **overrides):
        return make_bridge(
            client=Client(id=client_id, key_hash="k", is_active=True, name=NAMES[client_id]),
            client_id=client_id,
            orchestrator=hub.orchestrator,
            turn_registry=hub.turn_registry,
            channel_hub=hub,
            client_registry=ClientRegistry(hub.index),
            conversation=SessionConversation(CONVERSATION, explicit=True),
            **overrides,
        )

    return _make


def pairs(ws) -> list[tuple]:
    """(type, turn_id, origin or reason) of every turn_start/turn_end the client got."""
    return [
        (m["type"], m["turn_id"], m.get("origin") or m.get("reason"))
        for m in ws.sent
        if isinstance(m, dict) and m["type"] in ("turn_start", "turn_end")
    ]


def messages(ws) -> list[tuple]:
    """(id, text, state, self) of every `message` the client got."""
    return [(m["id"], m["text"], m["state"], m["from"]["self"]) for m in ws.of("message")]


def texts(ws, turn_id) -> str:
    return "".join(m["text"] for m in ws.of("token") if m["turn_id"] == turn_id)


def assert_paired(ws) -> None:
    """Per socket: one turn_end per turn_start, never two turns open at once."""
    open_turn = None
    for kind, turn_id, _ in pairs(ws):
        if kind == "turn_start":
            assert open_turn is None, f"{turn_id} opened with {open_turn} still open"
            open_turn = turn_id
        else:
            assert turn_id == open_turn, f"turn_end of {turn_id} without its turn_start"
            open_turn = None
    assert open_turn is None, f"{open_turn} was never closed"


async def settle(*bridges) -> None:
    """The channel's worker and each session's queue and sender, drained."""
    for bridge in bridges:
        channel = bridge.channel_hub.get(SK)
        if channel is not None:
            await channel.push_idle()
        await sent(bridge)


async def turns_ended(ws, n: int) -> None:
    await wait_until(lambda: len(ws.of("turn_end")) >= n, timeout=5)


# ── A message during a turn waits in OpenClaw's queue ─────────────────────


async def test_send_during_turn_does_not_abort(session, openclaw):
    tv, tv_ws = session("tv")
    mac, mac_ws = session("mac")
    await tv.turns.start_user_turn("hold-a")
    run_a = await openclaw.wait_running(SK)
    await wait_until(lambda: tv_ws.of("token"))

    await mac.turns.start_user_turn("b")  # A is running: B goes to the queue, nothing aborted
    await openclaw.wait_queued(SK, 1)
    await settle(tv, mac)
    assert openclaw.aborts == []
    assert [m for m, _ in openclaw.calls] == ["chat.send", "chat.send"]
    (run_b,) = openclaw.queued[SK]
    for ws in (tv_ws, mac_ws):
        assert pairs(ws) == [("turn_start", "t-1", "user")]  # A goes on, not replaced
        assert [(i, s) for i, _, s, _ in messages(ws)] == [(run_a, "delivered"), (run_b, "pending")]

    openclaw.release_run(run_a)
    await turns_ended(tv_ws, 2), await turns_ended(mac_ws, 2)
    await settle(tv, mac)
    for ws in (tv_ws, mac_ws):
        assert pairs(ws) == [
            ("turn_start", "t-1", "user"), ("turn_end", "t-1", "done"),
            ("turn_start", "t-2", "user"), ("turn_end", "t-2", "done"),
        ]  # fmt: skip
        assert texts(ws, "t-1") == "reply-hold-a"
        # B ran drained from the queue: no lifecycle `start`, its `final` without `message`.
        assert texts(ws, "t-2") == "reply-b"
        assert [(i, s) for i, _, s, _ in messages(ws)][-1] == (run_b, "delivered")
        assert ws.of("error") == []
        assert_paired(ws)
    assert openclaw.aborts == []


async def test_message_echo_to_all_with_self_flag(session, openclaw):
    tv, tv_ws = session("tv")
    mac, mac_ws = session("mac")
    await tv.turns.start_user_turn("hola")
    await turns_ended(tv_ws, 1), await turns_ended(mac_ws, 1)
    await settle(tv, mac)

    for ws, me in ((tv_ws, True), (mac_ws, False)):
        (message,) = ws.of("message")
        assert message == {
            "type": "message",
            "id": message["id"],
            "conversation": "conv-q",
            "text": "hola",
            "from": {"source": "device", "device_id": "tv", "name": "Tele del salón", "self": me},
            "state": "delivered",
        }
        # The echo goes before the turn that answers it, through the session's own queue.
        types = ws.types(messages=True)
        assert types.index("message") < types.index("turn_start")
        (start,) = ws.of("turn_start")
        assert start["by"] == {"device_id": "tv", "name": "Tele del salón", "self": me}
    assert tv_ws.of("message")[0]["id"] == mac_ws.of("message")[0]["id"]


async def test_message_text_never_reaches_the_logs(session, openclaw, caplog):
    import logging

    caplog.set_level(logging.DEBUG)
    tv, tv_ws = session("tv")
    await tv.turns.start_user_turn("SECRETO-del-usuario")
    await turns_ended(tv_ws, 1)
    await settle(tv)
    logged = "\n".join(r.getMessage() for r in caplog.records)
    assert "SECRETO" not in logged
    assert "text_len=19" in logged


# ── Interrupt, withdraw, cancel ───────────────────────────────────────────


async def test_interrupt_aborts_only_active_run(session, openclaw):
    tv, tv_ws = session("tv")
    mac, mac_ws = session("mac")
    await tv.turns.start_user_turn("hold-a")
    run_a = await openclaw.wait_running(SK)
    await mac.turns.start_user_turn("b")
    await openclaw.wait_queued(SK, 1)
    (run_b,) = openclaw.queued[SK]

    await tv.turns.start_user_turn("c", interrupt=True)  # «send now»
    await turns_ended(tv_ws, 3), await turns_ended(mac_ws, 3)
    await settle(tv, mac)

    assert openclaw.aborts == [{"sessionKey": SK, "runId": run_a}]  # only A, never the key
    for ws in (tv_ws, mac_ws):
        assert pairs(ws) == [
            ("turn_start", "t-1", "user"), ("turn_end", "t-1", "interrupted"),
            ("turn_start", "t-2", "user"), ("turn_end", "t-2", "done"),
            ("turn_start", "t-3", "user"), ("turn_end", "t-3", "done"),
        ]  # fmt: skip
        # B (queued) was not touched: it runs before C, as OpenClaw's queue says.
        assert texts(ws, "t-2") == "reply-b"
        assert texts(ws, "t-3") == "reply-c"
        assert [s for i, _, s, _ in messages(ws) if i == run_b] == ["pending", "delivered"]
        assert_paired(ws)


async def test_withdraw_aborts_queued_run_only(session, openclaw):
    tv, tv_ws = session("tv")
    mac, mac_ws = session("mac")
    await tv.turns.start_user_turn("hold-a")
    run_a = await openclaw.wait_running(SK)
    await mac.turns.start_user_turn("b")
    await openclaw.wait_queued(SK, 1)
    (run_b,) = openclaw.queued[SK]

    assert await tv.turns.withdraw(run_b)  # any device may withdraw a pending message
    assert not await mac.turns.withdraw(run_b)  # already withdrawn: nothing happens
    assert not await mac.turns.withdraw(run_a)  # delivered, not pending: nothing happens
    await settle(tv, mac)
    assert openclaw.aborts == [{"sessionKey": SK, "runId": run_b}]
    for ws in (tv_ws, mac_ws):
        assert [s for i, _, s, _ in messages(ws) if i == run_b] == ["pending", "withdrawn"]

    openclaw.release_run(run_a)  # A was not touched
    await turns_ended(tv_ws, 1), await turns_ended(mac_ws, 1)
    await asyncio.sleep(0.1)
    await settle(tv, mac)
    for ws in (tv_ws, mac_ws):
        assert pairs(ws) == [("turn_start", "t-1", "user"), ("turn_end", "t-1", "done")]
        assert texts(ws, "t-1") == "reply-hold-a"
        assert_paired(ws)


async def test_withdraw_from_the_wire(session, openclaw):
    """`{"type":"withdraw","message_id":...}` reaches the channel; a malformed one is
    INVALID_MESSAGE and the session goes on."""
    tv, tv_ws = session("tv")
    mac, mac_ws = session("mac")
    await tv.turns.start_user_turn("hold-a")
    run_a = await openclaw.wait_running(SK)
    await tv.turns.start_user_turn("b")
    await openclaw.wait_queued(SK, 1)
    (run_b,) = openclaw.queued[SK]

    for frame in ({"type": "withdraw"}, {"type": "withdraw", "message_id": 3},
                  {"type": "withdraw", "message_id": run_b}):  # fmt: skip
        mac_ws.inbox.put_nowait({"type": "websocket.message", "text": json.dumps(frame)})
    mac_ws.inbox.put_nowait({"type": "websocket.disconnect"})
    await mac._client_input_loop()
    await settle(tv, mac)

    assert [e["code"] for e in mac_ws.of("error")] == ["INVALID_MESSAGE", "INVALID_MESSAGE"]
    assert openclaw.aborts == [{"sessionKey": SK, "runId": run_b}]
    assert [s for i, _, s, _ in messages(tv_ws) if i == run_b] == ["pending", "withdrawn"]
    openclaw.release_run(run_a)
    await turns_ended(tv_ws, 1)


async def test_cancel_from_any_subscriber_ends_turn_for_all(session, openclaw):
    tv, tv_ws = session("tv")
    mac, mac_ws = session("mac")
    await tv.turns.start_user_turn("hold-a")
    run_a = await openclaw.wait_running(SK)
    await wait_until(lambda: mac_ws.of("turn_start"))

    assert await mac.turns.cancel("cancelled") == (True, "t-1")  # «Parar» on the other device
    await settle(tv, mac)

    assert openclaw.aborts == [{"sessionKey": SK, "runId": run_a}]
    for ws in (tv_ws, mac_ws):
        assert pairs(ws) == [("turn_start", "t-1", "user"), ("turn_end", "t-1", "cancelled")]
        assert_paired(ws)


async def test_an_interrupt_from_the_wire_is_a_bool(session, openclaw):
    tv, tv_ws = session("tv")
    tv_ws.inbox.put_nowait({
        "type": "websocket.message", "text": json.dumps({"type": "send", "text": "x", "interrupt": "yes"}),
    })  # fmt: skip
    tv_ws.inbox.put_nowait({"type": "websocket.disconnect"})
    await tv._client_input_loop()
    await settle(tv)
    assert [e["code"] for e in tv_ws.of("error")] == ["INVALID_MESSAGE"]
    assert openclaw.calls == []


# ── Order between devices ─────────────────────────────────────────────────


async def test_two_sends_from_two_devices_go_in_arrival_order(session, openclaw):
    tv, tv_ws = session("tv")
    mac, mac_ws = session("mac")
    await asyncio.gather(tv.turns.start_user_turn("uno"), mac.turns.start_user_turn("dos"))
    await turns_ended(tv_ws, 2), await turns_ended(mac_ws, 2)
    await settle(tv, mac)

    said = [m["content"] for m in openclaw.sessions[SK]["messages"] if m["role"] == "user"]
    assert said == ["uno", "dos"]
    for ws in (tv_ws, mac_ws):
        assert ws.of("error") == []  # no turn_in_progress between devices
        assert [m["by"]["device_id"] for m in ws.of("turn_start")] == ["tv", "mac"]
        assert [texts(ws, t) for t in ("t-1", "t-2")] == ["reply-uno", "reply-dos"]
        assert_paired(ws)


# ── A session that attaches later ─────────────────────────────────────────


async def test_late_attach_gets_pending_messages(session, openclaw):
    tv, tv_ws = session("tv")
    await tv.turns.start_user_turn("hold-a")
    run_a = await openclaw.wait_running(SK)
    await wait_until(lambda: tv_ws.of("token"))
    await tv.turns.start_user_turn("b")
    await openclaw.wait_queued(SK, 1)
    (run_b,) = openclaw.queued[SK]
    await settle(tv)

    web, web_ws = session("web")  # joins mid-turn, with B still waiting
    await settle(web)
    assert pairs(web_ws) == [("turn_start", "t-1", "user")]
    assert texts(web_ws, "t-1") == "reply-hold-a"  # the turn's text so far, in one token
    assert messages(web_ws) == [(run_b, "b", "pending", False)]  # only what still waits
    assert web_ws.of("turn_start")[0]["by"] == {"device_id": "tv", "name": "Tele del salón", "self": False}

    openclaw.release_run(run_a)
    await turns_ended(web_ws, 2)
    await settle(tv, web)
    assert [s for i, _, s, _ in messages(web_ws)] == ["pending", "delivered"]
    assert_paired(web_ws)
    assert_paired(tv_ws)


# ── Origin before the ack (decision 3) ────────────────────────────────────


async def test_frames_held_before_the_ack_go_to_the_right_sender(make_bridge):
    """Frames of a run nobody knows yet, while a `chat.send` waits for its ack, are held: the
    ack's runId says whose they are. The sender's run gets its own (its turn, its `by`);
    another run's, still unowned after the ack, is a push."""
    turns = TurnRegistry()
    index = ConversationIndex()
    index.add(CONVERSATION)
    hub = ChannelHub(index=index, turn_registry=turns)
    dispatcher = FrameDispatcher(turns, hub)
    client = OpenClawClient("127.0.0.1", 1, "tok", turns, dispatcher)
    hub.orchestrator = client
    client._ws = ws = SendOnlyOpenClawWS()
    tv, tv_ws = make_bridge(
        client=Client(id="tv", key_hash="k", is_active=True, name="Tele"), client_id="tv",
        orchestrator=client, turn_registry=turns, channel_hub=hub,
        client_registry=ClientRegistry(index), conversation=SessionConversation(CONVERSATION, explicit=True),
    )  # fmt: skip

    def event(name, payload):
        return {"type": "event", "event": name, "payload": {"sessionKey": SK, **payload}}

    sending = asyncio.create_task(tv.turns.start_user_turn("hola"))
    await wait_until(lambda: ws.ids("chat.send"))
    # Before the ack: a frame of the sender's run and one of another run, both unknown yet.
    await dispatcher.dispatch(event("chat", {"runId": "run-u", "state": "delta", "deltaText": "mi"}))
    await dispatcher.dispatch(event("agent", {"runId": "run-x", "stream": "lifecycle", "data": {"phase": "start"}}))
    (req,) = ws.ids("chat.send")
    await client._handle_raw_frame(
        json.dumps({"type": "res", "id": req, "ok": True, "payload": {"runId": "run-u"}})
    )
    await asyncio.wait_for(sending, 2)
    await dispatcher.dispatch(event("chat", {"runId": "run-u", "state": "final"}))
    await asyncio.wait_for(tv.turns.user_task, 2)
    await dispatcher.dispatch(event("chat", {"runId": "run-x", "state": "delta", "deltaText": "aviso"}))
    await dispatcher.dispatch(event("agent", {"runId": "run-x", "stream": "lifecycle", "data": {"phase": "end"}}))
    await hub.get(SK).push_idle()
    await sent(tv)

    starts = tv_ws.of("turn_start")
    assert [(s["origin"], s["by"]) for s in starts] == [
        ("user", {"device_id": "tv", "name": "Tele", "self": True}), ("push", None),
    ]
    assert texts(tv_ws, "t-1") == "mi"
    assert texts(tv_ws, "t-2") == "aviso"
    assert_paired(tv_ws)
    await tv.close_all()
    await hub.aclose(1.0)


async def test_a_lost_connection_ends_the_runs_with_a_stable_error(session, openclaw, hub, monkeypatch):
    """Every run of the channel still waiting when OpenClaw drops ends for every session with
    TURN_ERROR `orchestrator_disconnected`: the open one at once, and a queued one (its own
    pair) once no reconnection proved it still queued within RECONNECT_GRACE_S (2b.2)."""
    from fogar_gateway.services import channel as channel_module

    monkeypatch.setattr(channel_module, "RECONNECT_GRACE_S", 0.2)
    tv, tv_ws = session("tv")
    mac, mac_ws = session("mac")
    await tv.turns.start_user_turn("hold-a")
    await openclaw.wait_running(SK)
    await wait_until(lambda: mac_ws.of("token"))
    await mac.turns.start_user_turn("b")
    await openclaw.wait_queued(SK, 1)

    for conn in list(openclaw._conns):
        await conn.close(1011)
    await turns_ended(tv_ws, 2), await turns_ended(mac_ws, 2)
    await settle(tv, mac)
    for ws in (tv_ws, mac_ws):
        assert [e["message"] for e in ws.of("error")] == ["orchestrator_disconnected"] * 2
        assert [r for t, _, r in pairs(ws) if t == "turn_end"] == ["error", "error"]
        assert_paired(ws)


# ── Channel-level edges (review of 2b.1) ──────────────────────────────────


class _Orchestrator:
    """`send` acks with `r1`, `r2`… once `ack` is set (set by default); records aborts."""

    def __init__(self, *, acked: bool = True) -> None:
        self.ack = asyncio.Event()
        if acked:
            self.ack.set()
        self.sent = 0
        self.aborted: list[str] = []

    async def send(self, text, session_key, *, queue_mode=None):
        await self.ack.wait()
        self.sent += 1
        return f"r{self.sent}"

    async def abort_run(self, session_key, run_id):
        self.aborted.append(run_id)

    async def abort(self, session_key):
        self.aborted.append("THE WHOLE KEY")


class _Subscriber:
    conversation_id = "conv-q"
    wants_audio = False
    wants_push = True
    wants_progress = False
    tracker = None

    def __init__(self) -> None:
        self.client = Client(id="tv", key_hash="k", is_active=True, name="TV")
        self.got: list[tuple] = []

    def agent_allowed(self, session_key):
        return True

    def on_turn_start(self, turn):
        self.got.append(("start", turn.origin, turn.run_id))

    def on_event(self, turn, event):
        self.got.append(("event", getattr(event, "code", event.kind)))

    def on_turn_end(self, turn, reason):
        self.got.append(("end", reason))

    def on_message(self, message):
        self.got.append(("message", message.id, message.state))


def _channel(orchestrator):
    index = ConversationIndex()
    index.add(CONVERSATION)
    turns = TurnRegistry()
    hub = ChannelHub(index=index, turn_registry=turns, orchestrator=orchestrator)
    channel = hub.get_or_create(CONVERSATION)
    subscriber = _Subscriber()
    channel.subscribe(subscriber)
    return hub, channel, subscriber, FrameDispatcher(turns, hub)


def _event(name, payload):
    return {"type": "event", "event": name, "payload": {"sessionKey": SK, **payload}}


async def test_held_frames_wait_for_the_ack_of_a_send_cancelled_before_it():
    """A `chat.send` cancelled while it waits for its ack (close_all cancels the session's
    sends; 2b.3 review I3): what was held keeps waiting for the ack, which says whose it is.
    Another run's frames (here, a push) are then released; the sender's own run is
    tombstoned and aborted by its runId, never left running unowned."""
    orchestrator = _Orchestrator(acked=False)
    hub, channel, subscriber, dispatcher = _channel(orchestrator)
    sending = asyncio.create_task(channel.send_user_message(subscriber, "hola"))
    await wait_until(lambda: channel._acks == 1)
    await dispatcher.dispatch(_event("agent", {"runId": "run-x", "stream": "lifecycle", "data": {"phase": "start"}}))
    await dispatcher.dispatch(_event("chat", {"runId": "run-x", "state": "delta", "deltaText": "aviso"}))
    assert len(channel._held) == 2
    sending.cancel()
    await asyncio.gather(sending, return_exceptions=True)
    assert channel._acks == 1 and len(channel._held) == 2  # still waiting for the ack

    orchestrator.ack.set()  # the ack names r1: run-x was a push
    await wait_until(lambda: channel._acks == 0)
    await dispatcher.dispatch(_event("agent", {"runId": "run-x", "stream": "lifecycle", "data": {"phase": "end"}}))
    await channel.push_idle()
    await wait_until(lambda: orchestrator.aborted == ["r1"])
    assert subscriber.got == [("start", "push", "run-x"), ("event", "token"), ("end", "done")]
    assert hub.turn_registry.is_run_aborted("r1")
    assert channel.idle is False  # still subscribed
    await hub.aclose(1.0)


@pytest.mark.parametrize("exclusive", [False, True], ids=["ws", "v1"])
async def test_a_send_cancelled_before_its_ack_leaves_no_orphan_run(exclusive):
    """2b.3 review I3, WebSocket and /v1: the sender leaves before the ack; when the ack
    arrives its run is tombstoned and aborted by its runId, and its frames (held before the
    ack, or after it) reach nobody, not even as a push."""
    orchestrator = _Orchestrator(acked=False)
    hub, channel, subscriber, dispatcher = _channel(orchestrator)
    if exclusive:
        sending = asyncio.create_task(
            channel.send_exclusive("hola", by_client_id="ha", by_name="HA", sink=lambda e: None)
        )
    else:
        sending = asyncio.create_task(channel.send_user_message(subscriber, "hola"))
    await wait_until(lambda: channel._acks == 1)
    await dispatcher.dispatch(_event("agent", {"runId": "r1", "stream": "lifecycle", "data": {"phase": "start"}}))
    sending.cancel()
    await asyncio.gather(sending, return_exceptions=True)

    orchestrator.ack.set()
    await wait_until(lambda: orchestrator.aborted == ["r1"])
    await dispatcher.dispatch(_event("chat", {"runId": "r1", "state": "delta", "deltaText": "huérfano"}))
    await dispatcher.dispatch(_event("chat", {"runId": "r1", "state": "final"}))
    await channel.push_idle()
    assert subscriber.got == []
    assert channel._acks == 0 and channel._held == [] and not channel._runs
    await hub.aclose(1.0)


async def test_a_held_run_status_is_its_run_s_first_event():
    """During the ack wait, an `agent` `run_status` of a run nobody knows yet is held too (not
    dropped): it is the first event of the sender's run and opens its turn."""
    hub, channel, subscriber, dispatcher = _channel(_Orchestrator(acked=False))
    sending = asyncio.create_task(channel.send_user_message(subscriber, "hola"))
    await wait_until(lambda: channel._acks == 1)
    await dispatcher.dispatch(_event("agent", {"runId": "r1", "stream": "run_status", "data": {"phase": "queued"}}))
    hub.orchestrator.ack.set()
    await asyncio.wait_for(sending, 2)
    await channel.push_idle()
    assert subscriber.got == [("message", "r1", "delivered"), ("start", "user", "r1")]
    await hub.aclose(1.0)


async def test_a_queued_head_that_never_starts_times_out_alone(monkeypatch):
    """Decision 2: two messages whose runs never send anything. Each, when nothing is ahead
    of it, gets its pair with TURN_ERROR turn_timeout; only its own runId is aborted."""
    from fogar_gateway.core.config import settings

    monkeypatch.setattr(settings, "TURN_TIMEOUT_S", 0.2)
    orchestrator = _Orchestrator()
    hub, channel, subscriber, _ = _channel(orchestrator)
    await channel.send_user_message(subscriber, "a")
    await channel.send_user_message(subscriber, "b")
    await wait_until(lambda: not channel._runs, timeout=3)
    await wait_until(lambda: len(orchestrator.aborted) == 2)

    assert subscriber.got == [
        ("message", "r1", "delivered"), ("message", "r2", "pending"),
        ("start", "user", "r1"), ("event", "turn_timeout"), ("end", "timeout"),
        ("message", "r2", "delivered"),
        ("start", "user", "r2"), ("event", "turn_timeout"), ("end", "timeout"),
    ]  # fmt: skip
    assert orchestrator.aborted == ["r1", "r2"]
    await hub.aclose(1.0)


async def test_an_ack_after_the_channel_closed_aborts_its_run_and_registers_nothing():
    """Shutdown while a `chat.send` waits for its ack: the late ack's run is aborted (only it)
    and never registered in the closed channel (nothing waits for it forever)."""
    orchestrator = _Orchestrator(acked=False)
    hub, channel, subscriber, _ = _channel(orchestrator)
    sending = asyncio.create_task(channel.send_user_message(subscriber, "hola"))
    await wait_until(lambda: channel._acks == 1)
    await hub.aclose(1.0)
    orchestrator.ack.set()
    assert await asyncio.wait_for(sending, 2) is None
    assert channel._runs == {}
    assert orchestrator.aborted == ["r1"]
    assert hub.turn_registry.is_run_aborted("r1")


# ── 2b.3 review, round 2: «send now» cuts once (N1), a cancel right after a send (N2) ──


def _wire(ws, payload) -> None:
    ws.inbox.put_nowait({"type": "websocket.message", "text": json.dumps(payload)})


async def test_wire_send_now_aborts_only_the_open_run(session, openclaw):
    """N1: `{"type":"send","interrupt":true}` on the wire cuts the run open when it is read
    (A) and nothing else: B, another device's message queued behind A, then runs as its own
    turn; the new message goes after it. Exactly one `chat.abort`."""
    tv, tv_ws = session("tv")
    mac, mac_ws = session("mac")
    await tv.turns.start_user_turn("hold-a")
    run_a = await openclaw.wait_running(SK)
    await mac.turns.start_user_turn("hold-b")
    await openclaw.wait_queued(SK, 1)
    (run_b,) = openclaw.queued[SK]

    loop = asyncio.create_task(tv._client_input_loop())
    _wire(tv_ws, {"type": "send", "text": "c", "interrupt": True})
    await wait_until(lambda: openclaw.running.get(SK) == run_b, timeout=5)
    await tv.turns.sends_idle()
    await asyncio.sleep(0.2)
    assert [a["runId"] for a in openclaw.aborts] == [run_a]
    openclaw.release_run(run_b)
    await turns_ended(mac_ws, 3)
    await settle(tv, mac)

    assert [a["runId"] for a in openclaw.aborts] == [run_a]
    assert [r for _, _, r in pairs(mac_ws) if r not in ("user", "push")] == ["interrupted", "done", "done"]
    assert texts(mac_ws, "t-3") == "reply-c"
    states: dict[str, list[str]] = {}
    for _, text, state, _ in messages(mac_ws):
        states.setdefault(text, []).append(state)
    assert states["hold-a"] == ["delivered"]
    assert states["hold-b"] == ["pending", "delivered"]  # never withdrawn
    assert states["c"][-1] == "delivered" and "withdrawn" not in states["c"]
    tv_ws.inbox.put_nowait({"type": "websocket.disconnect"})
    await loop
    assert_paired(tv_ws)
    assert_paired(mac_ws)


async def test_a_cancel_right_after_a_send_stops_that_message(session, openclaw):
    """N2: `send` and `cancel` read back to back (no turn open yet): the message never plays.
    Whether the cancel finds it still waiting here (it is not sent) or already in `chat.send`
    (its run is aborted by its runId when acked), nothing of it reaches the wire: no
    `message`, no turn. A later `send` is not affected."""
    tv, tv_ws = session("tv")
    loop = asyncio.create_task(tv._client_input_loop())
    _wire(tv_ws, {"type": "send", "text": "hold-x"})
    _wire(tv_ws, {"type": "cancel"})
    await wait_until(lambda: tv_ws.inbox.empty())
    await tv.turns.sends_idle()
    await asyncio.sleep(0.3)
    assert SK not in openclaw.running and not openclaw.queued.get(SK)
    sent_runs = [m for m, sk in openclaw.calls if sk == SK].count("chat.send")
    assert len(openclaw.aborts) == sent_runs  # sent → aborted by its runId; else nothing
    assert all(a.get("runId") for a in openclaw.aborts)
    await settle(tv)
    assert tv_ws.of("message") == [] and pairs(tv_ws) == []

    _wire(tv_ws, {"type": "send", "text": "dos"})
    await turns_ended(tv_ws, 1)
    await settle(tv)
    assert texts(tv_ws, "t-1") == "reply-dos"
    assert [(text, state) for _, text, state, _ in messages(tv_ws)] == [("dos", "delivered")]
    tv_ws.inbox.put_nowait({"type": "websocket.disconnect"})
    await loop
    assert_paired(tv_ws)


async def test_a_cancel_while_a_send_waits_for_its_ack_aborts_it_unannounced():
    """N2, the in-`chat.send` case made exact: the send is past the lock and waits for its ack
    when the cancel covers it; on the ack its run is tombstoned and aborted, never announced,
    and what was held for it reaches nobody."""
    from fogar_gateway.services.channel import SendTicket

    orchestrator = _Orchestrator(acked=False)
    hub, channel, subscriber, dispatcher = _channel(orchestrator)
    ticket = SendTicket()
    sending = asyncio.create_task(channel.send_user_message(subscriber, "hola", ticket=ticket))
    await wait_until(lambda: channel._acks == 1)
    assert ticket.sending
    await dispatcher.dispatch(_event("agent", {"runId": "r1", "stream": "lifecycle", "data": {"phase": "start"}}))
    ticket.cut = True  # what TurnController.cancel_sends does
    orchestrator.ack.set()
    assert await asyncio.wait_for(sending, 2) is None
    await dispatcher.dispatch(_event("chat", {"runId": "r1", "state": "delta", "deltaText": "no"}))
    await channel.push_idle()
    assert orchestrator.aborted == ["r1"] and hub.turn_registry.is_run_aborted("r1")
    assert subscriber.got == [] and not channel._runs and channel._held == []
    assert await asyncio.wait_for(channel.send_user_message(subscriber, "otra"), 2) is not None
    await hub.aclose(1.0)
