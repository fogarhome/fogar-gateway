"""Queued, steered and start-less runs (5.0 PR 2b.2, #406; OpenClaw 2026.11.7).

Frame sequences copied (structure only, no real text) from docs/openclaw-protocol.md «C3 queue
behaviour and failure fields»:

- `steer` (the default): B's ack is `started`; later B's user `session.message` (`messageId` M,
  `idempotencyKey` `"<B>:user"`) and another copy of M with `__openclaw.steerTargetRunId: A`;
  then B's `chat` `final` `seq: 1` with no `message` and no prior event of B. The answer goes
  on under A.
- `followup`: a run drained from the queue gets no lifecycle `start` (an `agent`
  `stream:"error"` `seq gap` instead): `session.message`, `run_status`, `chat` status,
  lifecycle `model`, deltas, a `final` without `message`, lifecycle `end`.

Channel-level tests feed the frames through a real `FrameDispatcher` into a real
`ConversationChannel` with a recording subscriber; the steer test runs two real sessions
against the OpenClaw fake (`steer=True`) and checks the per-socket invariants.
"""

import asyncio
import logging

import pytest

from fogar_gateway.models.schemas import Client
from fogar_gateway.services import channel as channel_module
from fogar_gateway.services.channel import ChannelHub, pending_run_ids
from fogar_gateway.services.conversations import ConversationIndex, SessionConversation
from fogar_gateway.services.openclaw.client import OpenClawClient
from fogar_gateway.services.openclaw.dispatcher import FrameDispatcher
from fogar_gateway.services.openclaw.registry import ClientRegistry, TurnRegistry
from tests.fakes.bridge import keyed_conversation, sent, wait_until
from tests.fakes.openclaw import FakeOpenClawServer

CONVERSATION = keyed_conversation("shared", id="conv-r")
SK = CONVERSATION.session_key
NAMES = {"tv": "Tele del salón", "mac": "Mac de Ana"}


# ── Channel-level harness ──────────────────────────────────────────────────


class _Orchestrator:
    """`send` acks with `r1`, `r2`… once `ack` is set (set by default); `request` answers
    `chat.history` with `history` (or raises when it is an exception); records aborts."""

    def __init__(self, *, acked: bool = True, history=None) -> None:
        self.ack = asyncio.Event()
        if acked:
            self.ack.set()
        self.sent = 0
        self.aborted: list[str] = []
        self.history = history
        self.reads: list[tuple[str, dict]] = []

    async def send(self, text, session_key, *, queue_mode=None):
        await self.ack.wait()
        self.sent += 1
        return f"r{self.sent}"

    async def abort_run(self, session_key, run_id):
        self.aborted.append(run_id)

    async def abort(self, session_key):
        self.aborted.append("THE WHOLE KEY")

    async def request(self, method, params, timeout=10.0):
        self.reads.append((method, params))
        if isinstance(self.history, Exception):
            raise self.history
        return self.history or {}


class _Subscriber:
    conversation_id = "conv-r"
    wants_audio = False
    wants_push = True
    wants_progress = True
    tracker = None

    def __init__(self, client_id="tv") -> None:
        self.client = Client(id=client_id, key_hash="k", is_active=True, name=client_id.upper())
        self.got: list[tuple] = []

    def agent_allowed(self, session_key):
        return True

    def on_turn_start(self, turn):
        self.got.append(("start", turn.origin, turn.run_id))

    def on_event(self, turn, event):
        if event.kind == "token":
            self.got.append(("token", event.text))
        elif event.kind == "error":
            self.got.append(("error", event.code))
        else:
            self.got.append((event.kind,))

    def on_turn_end(self, turn, reason):
        self.got.append(("end", reason))

    def on_message(self, message):
        self.got.append(("message", message.id, message.state))


def _channel(orchestrator=None):
    orchestrator = orchestrator if orchestrator is not None else _Orchestrator()
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


def lifecycle(run_id, phase, **data):
    return _event("agent", {"runId": run_id, "stream": "lifecycle", "data": {"phase": phase, **data}})


def seq_gap(run_id):
    data = {"reason": "seq gap", "expected": 3, "received": 4}
    return _event("agent", {"runId": run_id, "stream": "error", "data": data})


def run_status(run_id):
    return _event("agent", {"runId": run_id, "stream": "run_status", "data": {"status": "running"}})


def status(run_id, phase="starting_model"):
    return _event("chat", {"runId": run_id, "state": "status", "phase": phase})


def delta(run_id, text):
    return _event("chat", {"runId": run_id, "state": "delta", "deltaText": text})


def final(run_id, message=None, seq=None):
    payload = {"runId": run_id, "state": "final"}
    if seq is not None:
        payload["seq"] = seq
    if message is not None:
        payload["message"] = {"role": "assistant", "content": [{"type": "text", "text": message}]}
    return _event("chat", payload)


def session_message(oc_id, *, run_id=None, steer=None, role="user"):
    """A `session.message` of `oc_id`; `run_id` links it (`idempotencyKey`), `steer` marks it
    absorbed by that run. The content is a placeholder: user text is never read."""
    meta = {"id": oc_id, "seq": 7}
    if steer is not None:
        meta["steerTargetRunId"] = steer
    message = {"role": role, "content": "x", "__openclaw": meta}
    if run_id is not None:
        message["idempotencyKey"] = f"{run_id}:user"
    return _event("session.message", {"message": message, "messageId": oc_id, "messageSeq": 7})


async def feed(dispatcher, channel, *frames):
    for frame in frames:
        await dispatcher.dispatch(frame)
    await channel.push_idle()


async def _a_running_and_b_pending(channel, subscriber, dispatcher):
    """r1 (A) sent and opened by its `start`; r2 (B) sent while A runs: pending."""
    await channel.send_user_message(subscriber, "a")
    await feed(dispatcher, channel, lifecycle("r1", "start"), delta("r1", "ho"))
    await channel.send_user_message(subscriber, "b")
    await channel.push_idle()
    assert subscriber.got == [
        ("message", "r1", "delivered"), ("start", "user", "r1"), ("token", "ho"),
        ("message", "r2", "pending"),
    ]  # fmt: skip
    subscriber.got.clear()


# ── steer: a message absorbed into the active turn ────────────────────────


@pytest.fixture
async def steer_hub():
    async with FakeOpenClawServer() as server:
        server.steer = True
        index = ConversationIndex()
        index.add(CONVERSATION)
        turns = TurnRegistry()
        hub = ChannelHub(index=index, turn_registry=turns)
        client = OpenClawClient("127.0.0.1", server.port, "tok", turns, FrameDispatcher(turns, hub))
        hub.orchestrator = client
        await client.connect()
        yield server, hub
        await hub.aclose(1.0)
        await client.close()


def _session(make_bridge, hub, client_id):
    return make_bridge(
        client=Client(id=client_id, key_hash="k", is_active=True, name=NAMES[client_id]),
        client_id=client_id,
        orchestrator=hub.orchestrator,
        turn_registry=hub.turn_registry,
        channel_hub=hub,
        client_registry=ClientRegistry(hub.index),
        conversation=SessionConversation(CONVERSATION, explicit=True),
    )


def pairs(ws) -> list[tuple]:
    return [
        (m["type"], m["turn_id"], m.get("origin") or m.get("reason"))
        for m in ws.sent
        if isinstance(m, dict) and m["type"] in ("turn_start", "turn_end")
    ]


def assert_socket_invariants(ws) -> None:
    """Per socket: one turn_end per turn_start, never two open, turn_seq masked, and every
    `message` before the turn_start of its own run's turn."""
    open_turn = None
    for kind, turn_id, _ in pairs(ws):
        if kind == "turn_start":
            assert open_turn is None
            open_turn = turn_id
        else:
            assert turn_id == open_turn
            open_turn = None
    assert open_turn is None
    for start in ws.of("turn_start"):
        assert 0 <= start["turn_seq"] <= 0xFFFF


async def test_steered_message_is_absorbed_into_active_turn(make_bridge, steer_hub):
    """B sent from the Mac while the TV's turn runs, in `steer`: both see B `pending` then
    `delivered` (once); the answer stays inside the TV's turn (its `by`, its audio), and B's
    empty `final` opens and closes nothing."""
    server, hub = steer_hub
    tv, tv_ws = _session(make_bridge, hub, "tv")
    mac, mac_ws = _session(make_bridge, hub, "mac")
    await tv.turns.start_user_turn("partial_hold")
    await wait_until(lambda: tv_ws.of("token") and mac_ws.of("token"))
    channel = hub.get(SK)
    sender = channel.current_turn.audio_session

    await mac.turns.start_user_turn("b")
    await sent(mac)
    (run_b,) = {m["id"] for m in mac_ws.of("message") if m["text"] == "b"}
    await wait_until(lambda: run_b not in channel._runs)
    await channel.push_idle()
    await sent(tv), await sent(mac)
    for ws in (tv_ws, mac_ws):
        assert [m["state"] for m in ws.of("message") if m["id"] == run_b] == ["pending", "delivered"]
    assert run_b not in channel._runs  # B's empty `final` ended it
    assert channel.current_turn is not None and channel.current_turn.audio_session is sender

    server.release.set()
    await wait_until(lambda: tv_ws.of("turn_end") and mac_ws.of("turn_end"))
    await channel.push_idle()
    await sent(tv), await sent(mac)
    for ws, me in ((tv_ws, True), (mac_ws, False)):
        assert pairs(ws) == [("turn_start", "t-1", "user"), ("turn_end", "t-1", "done")]
        assert ws.of("turn_start")[0]["by"] == {"device_id": "tv", "name": "Tele del salón", "self": me}
        assert "".join(t["text"] for t in ws.of("token")) == "reply-partial fin"
        assert [m["state"] for m in ws.of("message") if m["id"] == run_b] == ["pending", "delivered"]
        assert ws.of("error") == []
        assert_socket_invariants(ws)
    assert server.aborts == []
    await tv.close_all(), await mac.close_all()


async def test_session_message_copies_update_by_oc_message_id():
    """Copies of one `session.message` are not discarded as duplicates: the link
    (`idempotencyKey`) comes on one, the steer mark on another; B is `delivered` once, and a
    repeated mark changes nothing. Other roles and unknown links are ignored."""
    hub, channel, subscriber, dispatcher = _channel()
    await _a_running_and_b_pending(channel, subscriber, dispatcher)

    await feed(
        dispatcher, channel,
        session_message("m-9", role="assistant", steer="r1"),  # not a user message
        session_message("m-1", run_id="r2"),  # the link, no mark yet
        session_message("m-1"),  # a status repeat: nothing new
        session_message("m-2", steer="r1"),  # a mark for a message never linked
    )  # fmt: skip
    assert subscriber.got == []
    await feed(dispatcher, channel, session_message("m-1", steer="r1"))  # the mark, unlinked copy
    assert subscriber.got == [("message", "r2", "delivered")]
    assert channel._runs["r2"].state == "absorbed"
    await feed(dispatcher, channel, session_message("m-1", steer="r1"), final("r2", seq=1))
    await feed(dispatcher, channel, delta("r1", "la"), final("r1"), lifecycle("r1", "end"))
    assert subscriber.got == [
        ("message", "r2", "delivered"), ("token", "la"), ("end", "done"),
    ]  # fmt: skip
    assert channel._runs == {}
    await hub.aclose(1.0)


async def test_an_absorbed_run_ends_with_the_turn_that_absorbed_it():
    """If B's empty `final` never comes, B ends when A's turn ends: it never opens a turn of
    its own nor keeps the channel alive (no `turn_timeout` for it)."""
    hub, channel, subscriber, dispatcher = _channel()
    await _a_running_and_b_pending(channel, subscriber, dispatcher)
    await feed(dispatcher, channel, session_message("m-1", run_id="r2", steer="r1"))
    await feed(dispatcher, channel, final("r1"))
    assert subscriber.got == [("message", "r2", "delivered"), ("end", "done")]
    assert channel._runs == {}
    await hub.aclose(1.0)


async def test_a_steer_mark_held_while_its_ack_waits_still_counts():
    """B's `session.message` may arrive before B's ack (decision 3): it waits with what is
    held and is read once the ack names B."""
    orchestrator = _Orchestrator()
    hub, channel, subscriber, dispatcher = _channel(orchestrator)
    await channel.send_user_message(subscriber, "a")
    await feed(dispatcher, channel, lifecycle("r1", "start"))
    orchestrator.ack.clear()
    sending = asyncio.create_task(channel.send_user_message(subscriber, "b"))
    await wait_until(lambda: channel._acks == 1)
    await dispatcher.dispatch(session_message("m-1", run_id="r2", steer="r1"))
    assert len(channel._held) == 1
    orchestrator.ack.set()
    await asyncio.wait_for(sending, 2)
    await feed(dispatcher, channel, final("r2", seq=1))
    assert subscriber.got[-2:] == [("message", "r2", "pending"), ("message", "r2", "delivered")]
    assert "r2" not in channel._runs
    await hub.aclose(1.0)


# ── followup: a queued run with no lifecycle `start` ──────────────────────


async def test_queued_run_opens_on_first_event_without_start():
    """The observed followup sequence for B: no `start` (a `seq gap` notice), `run_status`
    first. B turns `delivered` when its own run opens, and its turn has its origin."""
    hub, channel, subscriber, dispatcher = _channel()
    await _a_running_and_b_pending(channel, subscriber, dispatcher)
    await feed(dispatcher, channel, final("r1", message="hola"), lifecycle("r1", "end"))
    assert subscriber.got == [("token", "la"), ("end", "done")]
    subscriber.got.clear()

    await feed(
        dispatcher, channel,
        session_message("m-2", run_id="r2"), seq_gap("r2"),
    )  # fmt: skip
    assert subscriber.got == []  # neither the transcript append nor the notice opens it
    await feed(
        dispatcher, channel,
        run_status("r2"), status("r2"), lifecycle("r2", "model"), delta("r2", "qué"),
        lifecycle("r2", "finishing"), final("r2"), lifecycle("r2", "end"),
    )  # fmt: skip
    assert subscriber.got == [
        ("message", "r2", "delivered"), ("start", "user", "r2"), ("progress",), ("token", "qué"),
        ("end", "done"),
    ]  # fmt: skip
    await hub.aclose(1.0)


async def test_seq_gap_is_not_an_error(caplog):
    """`agent` `stream:"error"` `seq gap` is OpenClaw's notice, not a failure: logged at DEBUG
    only, it opens nothing (an own queued run, or a push)."""
    hub, channel, subscriber, dispatcher = _channel()
    await channel.send_user_message(subscriber, "a")
    subscriber.got.clear()
    with caplog.at_level(logging.DEBUG):
        await feed(dispatcher, channel, seq_gap("r1"), seq_gap("run-x"))
    assert subscriber.got == []
    assert channel.current_turn is None and channel._push is None
    assert [r for r in caplog.records if r.levelno >= logging.WARNING] == []
    assert any("seq gap" in r.getMessage() for r in caplog.records)
    await hub.aclose(1.0)


async def test_final_without_message_keeps_streamed_text():
    """A drained run's `final` carries no `message`: the turn closes with what the deltas
    brought, nothing added, nothing lost."""
    hub, channel, subscriber, dispatcher = _channel()
    await channel.send_user_message(subscriber, "a")
    await feed(dispatcher, channel, seq_gap("r1"), delta("r1", "ho"), delta("r1", "la"), final("r1"))
    assert subscriber.got == [
        ("message", "r1", "delivered"), ("start", "user", "r1"), ("token", "ho"), ("token", "la"),
        ("end", "done"),
    ]  # fmt: skip
    assert channel.current_turn is None
    await hub.aclose(1.0)


async def test_push_without_start_still_opens():
    """A push drained from OpenClaw's queue (no `start`): its first event (`run_status`) opens
    it; its text comes in the deltas; lifecycle `end` closes it. A late frame of its run does
    not reopen it."""
    hub, channel, subscriber, dispatcher = _channel()
    await feed(
        dispatcher, channel,
        seq_gap("run-x"), run_status("run-x"), status("run-x"), delta("run-x", "aviso"),
        final("run-x"), lifecycle("run-x", "end"), run_status("run-x"),
    )  # fmt: skip
    assert subscriber.got == [("start", "push", "run-x"), ("progress",), ("token", "aviso"), ("end", "done")]
    await hub.aclose(1.0)


async def test_a_push_without_start_waits_for_the_open_user_turn():
    """One turn at a time: a push whose first event arrives during a user turn opens after
    it, in order."""
    hub, channel, subscriber, dispatcher = _channel()
    await channel.send_user_message(subscriber, "a")
    await feed(dispatcher, channel, lifecycle("r1", "start"))
    await feed(dispatcher, channel, delta("run-x", "aviso"), lifecycle("run-x", "end"))
    await feed(dispatcher, channel, final("r1", message="ok"))
    assert subscriber.got == [
        ("message", "r1", "delivered"), ("start", "user", "r1"), ("token", "ok"), ("end", "done"),
        ("start", "push", "run-x"), ("token", "aviso"), ("end", "done"),
    ]  # fmt: skip
    await hub.aclose(1.0)


# ── Decision 4: the terminal of a run that never opened ───────────────────


@pytest.mark.parametrize(
    ("frames", "expected"),
    [
        pytest.param(
            [final("r2", seq=1)], [("message", "r2", "delivered"), ("end", "done")],
            id="final-without-message",
        ),
        pytest.param(
            [final("r2", message="modo cambiado")],
            [("end", "done"), ("message", "r2", "delivered"), ("start", "user", "r2"),
             ("token", "modo cambiado"), ("end", "done")],
            id="final-with-message",
        ),
        pytest.param(
            [_event("chat", {"runId": "r2", "state": "aborted", "stopReason": "rpc", "seq": 1}),
             lifecycle("r2", "end", aborted=True)],
            [("message", "r2", "withdrawn"), ("end", "done")],
            id="aborted",
        ),
        pytest.param(
            [_event("chat", {"runId": "r2", "state": "error", "errorKind": "rate_limit",
                             "errorMessage": "SECRET"})],
            [("end", "done"), ("message", "r2", "delivered"), ("start", "user", "r2"),
             ("error", "orchestrator_rate_limited"), ("end", "error")],
            id="chat-error",
        ),
        pytest.param(
            [lifecycle("r2", "error", timeoutPhase="provider", error="SECRET"),
             _event("chat", {"runId": "r2", "state": "error", "errorKind": "unknown"})],
            [("end", "done"), ("message", "r2", "delivered"), ("start", "user", "r2"),
             ("error", "orchestrator_timeout"), ("end", "error")],
            id="lifecycle-error",
        ),
    ],
)  # fmt: skip
async def test_terminal_of_a_run_that_never_opened(frames, expected, caplog):
    """B, pending behind A, ends without any event of its own. Its terminal arrives while A
    still runs: what opens nothing happens at once; what opens a turn waits for A's end (one
    turn at a time)."""
    hub, channel, subscriber, dispatcher = _channel()
    await _a_running_and_b_pending(channel, subscriber, dispatcher)
    with caplog.at_level(logging.DEBUG):
        await feed(dispatcher, channel, *frames)
        await feed(dispatcher, channel, final("r1"))
    assert subscriber.got == expected
    assert channel._runs == {} and channel.current_turn is None
    assert "SECRET" not in caplog.text
    await hub.aclose(1.0)


# ── Reconnect: `pendingInputs` ─────────────────────────────────────────────


def _history(*queued, in_flight=None):
    items = [{"id": f"pi-{r}", "acceptedAt": 0, "state": "queued", "message": "x", "runId": r, "queued": True}
             for r in queued]  # fmt: skip
    history = {"messages": [], "pendingInputs": {"items": items, "total": len(items), "queuedCount": len(items)}}
    if in_flight is not None:
        history["inFlightRun"] = {"runId": in_flight}
    return history


def test_pending_run_ids_reads_defensively():
    assert pending_run_ids(_history("r2", in_flight="r1")) == {"r1", "r2"}
    cancelled = _history("r3")
    cancelled["pendingInputs"]["items"][0]["state"] = "cancelled"
    assert pending_run_ids(cancelled) == set()
    assert pending_run_ids({"messages": []}) is None  # no pendingInputs: nothing proven
    assert pending_run_ids({"pendingInputs": {"items": "nope"}}) is None
    assert pending_run_ids({"pendingInputs": {"items": [None, {"state": "queued"}, {"queued": True, "runId": "r9"}]}}) == {"r9"}


async def test_pending_inputs_on_reconnect_keep_what_is_still_queued():
    """OpenClaw drops: A (its turn open) fails at once; B and C (queued) wait. On reconnect
    `pendingInputs` still lists B: B stays pending (and later runs normally); C is gone and
    gets its pair with `orchestrator_disconnected`."""
    orchestrator = _Orchestrator(history=_history("r2"))
    hub, channel, subscriber, dispatcher = _channel(orchestrator)
    await _a_running_and_b_pending(channel, subscriber, dispatcher)
    await channel.send_user_message(subscriber, "c")
    await channel.push_idle()
    subscriber.got.clear()

    dispatcher.connection_lost("orchestrator_disconnected")
    await channel.push_idle()
    assert subscriber.got == [("error", "orchestrator_disconnected"), ("end", "error")]
    assert channel._runs["r2"].unconfirmed and channel._runs["r3"].unconfirmed
    subscriber.got.clear()

    hub.reconnected()
    await wait_until(lambda: orchestrator.reads)
    await wait_until(lambda: "r3" not in channel._runs)
    await channel.push_idle()
    assert orchestrator.reads == [("chat.history", {"sessionKey": SK, "limit": 1})]
    assert subscriber.got == [
        ("message", "r3", "delivered"), ("start", "user", "r3"), ("error", "orchestrator_disconnected"),
        ("end", "error"),
    ]  # fmt: skip
    assert channel._runs["r2"].message == "pending" and not channel._runs["r2"].unconfirmed
    subscriber.got.clear()

    await feed(dispatcher, channel, delta("r1", "tarde"), final("r1"))  # A's late frames: dropped
    await feed(dispatcher, channel, seq_gap("r2"), delta("r2", "b"), final("r2"))
    assert subscriber.got == [
        ("message", "r2", "delivered"), ("start", "user", "r2"), ("token", "b"), ("end", "done"),
    ]  # fmt: skip
    await hub.aclose(1.0)


@pytest.mark.parametrize("history", [RuntimeError("down"), {"messages": []}], ids=["unreadable", "no-pending"])
async def test_pending_inputs_that_prove_nothing_end_the_queued_runs(history):
    orchestrator = _Orchestrator(history=history)
    hub, channel, subscriber, dispatcher = _channel(orchestrator)
    await _a_running_and_b_pending(channel, subscriber, dispatcher)
    dispatcher.connection_lost("orchestrator_disconnected")
    await channel.push_idle()
    subscriber.got.clear()
    hub.reconnected()
    await wait_until(lambda: not channel._runs)
    await channel.push_idle()
    assert subscriber.got == [
        ("message", "r2", "delivered"), ("start", "user", "r2"), ("error", "orchestrator_disconnected"),
        ("end", "error"),
    ]  # fmt: skip
    await hub.aclose(1.0)


async def test_pending_inputs_against_the_fake_after_a_drop(make_bridge):
    """With the OpenClaw fake in `queue` mode: after a (simulated) drop, the reconnection reads
    the real `chat.history` shape; B, still in OpenClaw's queue, stays pending and later gets
    its own turn on every socket; A's open turn ended with the drop."""
    async with FakeOpenClawServer(queue=True) as server:
        index = ConversationIndex()
        index.add(CONVERSATION)
        turns = TurnRegistry()
        hub = ChannelHub(index=index, turn_registry=turns)
        dispatcher = FrameDispatcher(turns, hub)
        client = OpenClawClient("127.0.0.1", server.port, "tok", turns, dispatcher)
        hub.orchestrator = client
        await client.connect()
        try:
            tv, tv_ws = _session(make_bridge, hub, "tv")
            await tv.turns.start_user_turn("hold-a")
            await server.wait_running(SK)
            await wait_until(lambda: tv_ws.of("token"))
            await tv.turns.start_user_turn("b")
            await server.wait_queued(SK, 1)
            (run_b,) = server.queued[SK]

            dispatcher.connection_lost("orchestrator_disconnected")
            await wait_until(lambda: tv_ws.of("turn_end"))
            hub.reconnected()
            await wait_until(lambda: ("chat.history", {"sessionKey": SK, "limit": 1}) in server.reads)
            channel = hub.get(SK)
            await wait_until(lambda: not channel._runs[run_b].unconfirmed)

            server.release.set()
            await wait_until(lambda: len(tv_ws.of("turn_end")) == 2, timeout=5)
            await channel.push_idle()
            await sent(tv)
            assert [e["message"] for e in tv_ws.of("error")] == ["orchestrator_disconnected"]
            assert [r for t, _, r in pairs(tv_ws) if t == "turn_end"] == ["error", "done"]
            assert [m["state"] for m in tv_ws.of("message") if m["id"] == run_b] == ["pending", "delivered"]
            assert "".join(t["text"] for t in tv_ws.of("token") if t["turn_id"] == "t-2") == "reply-b"
            assert_socket_invariants(tv_ws)
            await tv.close_all()
        finally:
            await hub.aclose(1.0)
            await client.close()


async def test_without_a_reconnection_queued_runs_fail_after_the_grace(monkeypatch):
    monkeypatch.setattr(channel_module, "RECONNECT_GRACE_S", 0.1)
    hub, channel, subscriber, dispatcher = _channel()
    await _a_running_and_b_pending(channel, subscriber, dispatcher)
    dispatcher.connection_lost("orchestrator_disconnected")
    await wait_until(lambda: not channel._runs)
    await channel.push_idle()
    assert subscriber.got == [
        ("error", "orchestrator_disconnected"), ("end", "error"),
        ("message", "r2", "delivered"), ("start", "user", "r2"), ("error", "orchestrator_disconnected"),
        ("end", "error"),
    ]  # fmt: skip
    await hub.aclose(1.0)


async def test_the_reconnecting_client_tells_the_channels_it_is_back():
    from unittest.mock import AsyncMock

    from fogar_gateway.services.openclaw.reconnecting import ReconnectingOpenClawClient

    inner = AsyncMock(spec=OpenClawClient)
    inner.on_disconnect = None
    inner.gateway_info = None
    roc = ReconnectingOpenClawClient(inner, "test", initial_backoff=0.01)
    await roc.connect()
    calls = []
    roc.on_connected = lambda: calls.append("back")
    inner.on_disconnect()
    await wait_until(lambda: calls == ["back"])
    await roc.close()


# ── #286: an open push's frames are not held behind a slow ack ────────────


async def test_an_open_push_does_not_time_out_while_a_send_waits_for_its_ack(monkeypatch):
    """A push is open when a `chat.send` goes out and its ack is slow. The push's frames keep
    coming: they are not held for the ack (they are the push's, never the sender's), so the
    push's #286 clock sees them and it does not time out."""
    from fogar_gateway.core.config import settings

    monkeypatch.setattr(settings, "TURN_TIMEOUT_S", 0.3)
    orchestrator = _Orchestrator(acked=False)
    hub, channel, subscriber, dispatcher = _channel(orchestrator)
    await feed(dispatcher, channel, lifecycle("run-x", "start"))
    sending = asyncio.create_task(channel.send_user_message(subscriber, "b"))
    await wait_until(lambda: channel._acks == 1)
    for _ in range(6):  # 0.6 s of push progress, longer than TURN_TIMEOUT_S
        await dispatcher.dispatch(delta("run-x", "."))
        await asyncio.sleep(0.1)
    assert channel._held == []
    orchestrator.ack.set()
    await asyncio.wait_for(sending, 2)
    await feed(dispatcher, channel, lifecycle("run-x", "end"))
    assert ("error", "turn_timeout") not in subscriber.got
    assert subscriber.got[:2] == [("start", "push", "run-x"), ("token", ".")]
    assert ("end", "done") in subscriber.got
    assert orchestrator.aborted == []
    await hub.aclose(1.0)
