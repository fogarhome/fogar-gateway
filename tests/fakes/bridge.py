"""One way to build a FogarBridge in tests (#270).

The bridge sends through a real `QueuedSender`, the production path: what a
test reads from `RecWS.sent` is what a client would have received. Writes
happen on the sender's own task, so read them with `await sent(bridge)`, which
drains the queue first.
"""

import asyncio
import time
from unittest.mock import AsyncMock, MagicMock

from fogar_gateway.core.session_key import make_session_key
from fogar_gateway.models.schemas import Client, ClientConfig, Handshake
from fogar_gateway.services.bridge import FogarBridge
from fogar_gateway.services.conversations import Conversation, SessionConversation
from fogar_gateway.services.openclaw.registry import ClientRegistry, TurnRegistry
from fogar_gateway.services.outbound import ClientGone, QueuedSender
from tests.fakes.orchestrator import OrchestratorEvent, wire_stream_orchestrator
from tests.fakes.speech import speech_providers


class RecWS:
    """A client socket that records everything sent to it (json dicts and audio bytes)."""

    def __init__(self):
        self.sent: list = []
        self.inbox: asyncio.Queue = asyncio.Queue()

    async def send_json(self, p):
        self.sent.append(p)

    async def send_bytes(self, b):
        self.sent.append(b)

    async def receive(self):
        return await self.inbox.get()

    def types(self, messages: bool = False):
        """The types of the JSON messages sent, in order. The `message` echo of each user
        message (5.0 PR 2b) only with `messages=True`: most tests look at the turns."""
        return [
            m["type"]
            for m in self.sent
            if isinstance(m, dict) and (messages or m["type"] != "message")
        ]

    def of(self, type_):
        return [m for m in self.sent if isinstance(m, dict) and m["type"] == type_]


def fake_tracker():
    t = MagicMock()
    t.record = AsyncMock()
    t.close = AsyncMock()
    return t


def make_bridge(
    ws=None,
    *,
    client_id="u",
    output_mode=("text",),
    input_mode="text",
    tts=None,
    stt=None,
    transcriber=None,
    register=False,
    attach=True,
    notify=True,
    **overrides,
):
    """Return `(bridge, ws)`. Any `FogarBridge` argument can be overridden by name.

    Must run inside the event loop (the `QueuedSender` starts its writer task).
    `register=True` adds the bridge to its `client_registry`, as `routes.py` does. The bridge
    is attached to its conversation's channel (what `claim_conversation` does before `ready`);
    `attach=False` leaves it out.

    5.0 PR 2c.3: with its own hub (no `channel_hub` given) and `notify=True`, the bridge's
    client is its conversation's notify device (the push audio plays there, as before 2c);
    `notify=False` leaves the conversation without one.
    """
    ws = ws if ws is not None else RecWS()
    kwargs = dict(
        client=Client(id=client_id, key_hash="k", is_active=True),
        config=ClientConfig(),
        client_ws=ws,
        orchestrator=AsyncMock(),
        speech=speech_providers(tts=tts, stt=stt),
        tracker=fake_tracker(),
        handshake=Handshake(client_key="k", input_mode=input_mode, output_mode=list(output_mode)),
        client_registry=ClientRegistry(),
        turn_registry=TurnRegistry(),
        conversation=fixed_conversation(client_id),
    )
    kwargs.update(overrides)
    if "sender" not in kwargs:  # not setdefault: it would start a writer task nobody closes
        kwargs["sender"] = QueuedSender(ws)
    bridge = FogarBridge(**kwargs)
    # 5.0 PR 2b: the channel sends and reads frames; a mock's `stream_response` plays them.
    wire_stream_orchestrator(bridge.channel_hub.orchestrator, bridge.channel_hub)
    bridge.transcriber = transcriber
    if bridge._own_hub:
        registry = kwargs["client_registry"]

        def _sessions_of(cid: str) -> list:
            live = [b.turns for b in registry.for_client(cid)]
            if not live and cid == bridge.client_id:
                live = [bridge.turns]  # not registered (`register=False`): still the device's session
            return live

        bridge.channel_hub.sessions_of = _sessions_of
        if notify:
            bridge.channel_hub.notify_device = notify_device_is(client_id)
    if register:
        kwargs["client_registry"].register(bridge.client_id, bridge, bridge.conversation.current)
    if attach:
        bridge.turns.attach()
    return bridge, ws


CONVERSATION_ID = "conv-1"


def notify_device_is(client_id: str | None):
    """A `ChannelHub.notify_device` for tests: every conversation's notify device is
    `client_id` (None: none)."""

    async def _lookup(conversation) -> str | None:
        return client_id

    return _lookup


def fixed_conversation(client_id: str = "u", agent: str = "main", **kwargs) -> SessionConversation:
    """A session conversation that never changes day, on the key `agent:{agent}:{client_id}`,
    so push tests keep their `agent:main:<client>` keys."""
    conv = keyed_conversation(client_id, agent, kwargs.pop("id", CONVERSATION_ID))
    return SessionConversation(conv, explicit=True, **kwargs)


def keyed_conversation(client_id: str, agent: str = "main", id: str | None = None) -> Conversation:
    """A conversation on the key `agent:{agent}:{client_id}`: register it with the session so
    a push on that key routes to it (only indexed keys are routed)."""
    return Conversation(
        id=id or f"conv-{client_id}",
        client_id=client_id,
        agent=agent,
        session_key=make_session_key(agent, client_id),
        kind="created",
    )


async def sent(bridge) -> list:
    """Everything the client has received so far, after draining the sender.

    The session paints its turns on its own worker (5.0: the channel only enqueues there), so
    this waits for that queue first.

    Raises `ClientGone` if the sender broke: an empty list must mean "nothing was
    sent", not "the sender dropped it". With a broken sender, read `ws.sent`."""
    await asyncio.wait_for(bridge.turns.idle(), 5.0)
    await bridge._sender.flush()
    return bridge.client_ws.sent


async def start_turn(bridge, text: str = "x") -> asyncio.Future:
    """Start a user turn as a `send` does (5.0 PR 2b: queued behind the one in flight); return
    a future done once its run ends."""
    await bridge.turns.start_user_turn(text)
    task = bridge.turns.user_task
    assert task is not None, "the turn did not start"
    return task


async def run_turn(bridge, text: str = "x", timeout: float = 5.0) -> None:
    """Run one user turn to its end (its turn_end included)."""
    task = await start_turn(bridge, text)
    done, _ = await asyncio.wait({task}, timeout=timeout)
    if not done:
        task.cancel()
        raise AssertionError(f"the turn did not end in {timeout}s")
    await asyncio.wait_for(bridge.turns.idle(), timeout)


def enqueue_push(bridge, kind: str, arg, run_id: str | None = None) -> None:
    """A push event `(kind, arg)` for the channel the bridge is attached to (what
    FrameDispatcher does with `channel_for(sessionKey).enqueue_push`)."""
    channel = bridge.channel
    assert channel is not None, "the bridge is not attached to a channel"
    channel.enqueue_push(kind, arg, run_id)


async def push(bridge, *events) -> None:
    """Feed OpenClaw push events `(kind, arg)` through the channel's push worker and wait
    until the channel and the session handled them all."""
    for kind, arg in events:
        enqueue_push(bridge, kind, arg)
    await bridge.push_idle()


def push_reply(*deltas: str, sk: str = "sk", end: bool = True) -> list[tuple[str, object]]:
    """The events of a push reply: agent start, one `chat` per delta and (if `end`) agent end."""
    events: list[tuple[str, object]] = [("turn_start", sk)]
    events += [("chat", {"sessionKey": sk, "deltaText": d}) for d in deltas]
    if end:
        events.append(("turn_end", sk))
    return events


def tokens(*tokens):
    """An orchestrator `stream_response` that yields these tokens and ends."""

    async def _s(*a, **k):
        for t in tokens:
            yield OrchestratorEvent(type="token", content=t)

    return _s


def blocking(started: asyncio.Event | None = None):
    """An orchestrator `stream_response` that emits one token then waits until cancelled."""

    async def _s(*a, **k):
        yield OrchestratorEvent(type="token", content="hi")
        if started:
            started.set()
        await asyncio.sleep(3600)

    return _s


async def wait_until(pred, timeout=2.0):
    end = time.monotonic() + timeout
    while not pred():
        if time.monotonic() > end:
            raise AssertionError("condition not reached")
        await asyncio.sleep(0.005)


class GoneSender:
    """A sender whose client already left: every send raises `ClientGone`, as a broken
    `QueuedSender` does. Use it as `make_bridge(sender=GoneSender())`; pass another
    exception to reach the bridge's generic send-failure branches."""

    def __init__(self, error: type[Exception] = ClientGone):
        self._error = error

    async def send_json(self, payload: dict) -> None:
        raise self._error("cliente no disponible")

    async def send_bytes(self, data: bytes) -> None:
        raise self._error("cliente no disponible")

    async def flush(self) -> None:
        raise self._error("cliente no disponible")

    async def aclose(self, timeout: float) -> None:
        return None

    def queue_stats(self) -> dict:
        return {"pending_msgs": 0, "pending_bytes": 0, "peak_msgs": 0, "peak_bytes": 0}

    def restrict(self, allowed: frozenset[str]) -> None:
        return None


class DirectSender:
    """Passthrough: writes straight to the socket, no queue. For tests of the
    `PipelineTracker` alone; the bridge uses the real `QueuedSender`."""

    def __init__(self, ws):
        self._ws = ws

    async def send_json(self, payload: dict) -> None:
        await self._ws.send_json(payload)

    async def send_bytes(self, data: bytes) -> None:
        await self._ws.send_bytes(data)

    async def flush(self) -> None:
        return None

    async def aclose(self, timeout: float) -> None:
        return None

    def queue_stats(self) -> dict:
        return {"pending_msgs": 0, "pending_bytes": 0, "peak_msgs": 0, "peak_bytes": 0}

    def restrict(self, allowed: frozenset[str]) -> None:
        return None
