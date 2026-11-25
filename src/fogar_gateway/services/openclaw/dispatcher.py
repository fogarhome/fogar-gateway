import logging
from typing import TYPE_CHECKING, Protocol

from fogar_gateway.services.openclaw.registry import TurnRegistry

if TYPE_CHECKING:
    from fogar_gateway.services.channel import ConversationChannel

logger = logging.getLogger(__name__)


def _as_dict(value: object) -> dict:
    """`payload`/`data` may arrive as null or a non-object: treat that as empty (G-03)."""
    return value if isinstance(value, dict) else {}


def _run_id(payload: dict) -> str | None:
    run_id = payload.get("runId")
    return run_id if isinstance(run_id, str) else None


def push_route(event: str, payload: dict) -> tuple[str, object] | None:
    """What a frame of a run nobody owns is for the push of its key: `(kind, arg)` for
    `ConversationChannel.enqueue_push`, or None (nothing to do). The single mapping, for the
    dispatcher and for what a channel held until an ack (5.0 PR 2b.2).

    Since 2b.2 a push opens on its run's **first event**, not only on lifecycle `start` (a run
    drained from OpenClaw's queue never gets one; docs/openclaw-protocol.md, «C3 queue
    behaviour»): a `chat` status or delta, a `session.tool` start/result, an `agent` lifecycle
    `start`/`model`/`fallback_step` or a `run_status`. Lifecycle `end`/`error` close it. The
    other `agent` streams (`item`, `tool`, `assistant`, `thinking`, the `error` stream of a
    `seq gap` notice…) open and close nothing: an `item` `end` must not close a push."""
    if event == "chat":
        return "chat", payload
    data = _as_dict(payload.get("data"))
    if event == "session.tool":
        return ("tool", data) if data.get("phase") in ("start", "result") else None
    if event != "agent":
        return None
    stream = payload.get("stream")
    if stream == "run_status":
        return "open", payload.get("sessionKey")
    if stream is not None and stream != "lifecycle":
        return None
    phase = data.get("phase")
    if phase == "fallback_step":
        return "fallback", data
    if phase == "start":
        return "turn_start", payload.get("sessionKey")
    if phase == "model":
        # Opens like `start` but, unlike it, never reopens a push its run already closed.
        return "open", payload.get("sessionKey")
    if phase == "end":
        return "turn_end", payload.get("sessionKey")
    if phase == "error":  # #300; its `data`, which the push classifies (#397)
        return "turn_error", data
    return None


def _taken(
    channel: "ConversationChannel | None", event: str, payload: dict, *, hold: bool = False
) -> bool:
    """The channel took the frame: one of its own runs (or, with `hold`, one it holds until a
    `chat.send` ack names its run). `is True`: a stand-in channel never takes anything."""
    return channel is not None and channel.take_frame(event, payload, hold=hold) is True


class Channels(Protocol):
    """Where a key's frames go: `ChannelHub` (services/channel.py)."""

    def channel_for(self, session_key: str) -> "ConversationChannel | None": ...

    def connection_lost(self, code: str) -> None: ...


class FrameDispatcher:
    """Routes incoming OpenClaw frames to the correct queue or channel.

    Knows about TurnRegistry (the tombstones) and the conversation channels (5.0, #406).
    Does not touch WebSocket or TTS. A frame of a run a channel sent
    (`ConversationChannel.take_frame`; a WebSocket message or, since 5.0 PR 2b.3, a /v1 one)
    goes to that channel; a frame of a run no one knows yet while its channel waits for a
    `chat.send` ack is held by the channel (decision 3 of the 5.0 PR 2 plan); everything else
    is a push of the key's conversation's channel (`channel_for`: the conversation from the
    index, or the live channel of the key, created if none is live), opened by its run's first
    event (`push_route`, 2b.2). A `user` `session.message` goes to the channel too
    (`take_session_message`: ids and `steer`'s mark). A key no channel has goes nowhere.

    Frames of a run the gateway aborted (`runId` tombstoned in TurnRegistry, #285) are
    dropped before any routing: they never reach the next turn nor come back as a push.

    Nothing is awaited here: this runs inside OpenClaw's _listen loop, so anything slow
    would stall every session sharing the connection (issue #130 S3a). The routing decision
    is taken synchronously, and only the resulting work is queued on the channel's worker.
    """

    def __init__(self, turn_registry: TurnRegistry, channels: Channels) -> None:
        self._turns = turn_registry
        self._channels = channels
        # Frame/event kinds already reported as unhandled — each is logged at INFO
        # once (issue #136), so protocol drift is visible without a per-frame flood.
        self._unknown_seen: set[str] = set()

    async def dispatch(self, frame: dict) -> None:
        ftype = frame.get("type")
        if ftype == "res":
            await self._handle_res(frame)
        elif ftype == "event":
            await self._handle_event(frame)
        else:
            self._note_unhandled(f"frame type {ftype!r}")

    def _note_unhandled(self, what: str) -> None:
        """Log an unhandled frame/event kind once at INFO, then at DEBUG. Only the
        kind is logged, never the payload (it may carry user content)."""
        if what in self._unknown_seen:
            logger.debug("unhandled OpenClaw %s", what)
            return
        self._unknown_seen.add(what)
        logger.info(
            "unhandled OpenClaw %s (first occurrence; further ones are logged at DEBUG) — "
            "possible protocol drift, see docs/openclaw-protocol.md",
            what,
        )

    async def _handle_res(self, frame: dict) -> None:
        # Every request the gateway sends (`chat.send`, `chat.abort`, a read, a ping) gets its
        # `res` by id in OpenClawClient; one that reaches the dispatcher came after its caller
        # stopped waiting. A late `chat.abort` answer still names the runs it aborted (#285).
        if frame.get("ok") is False:
            return
        self._turns.note_abort_result(_as_dict(frame.get("payload")))

    def _of_aborted_run(self, payload: dict) -> bool:
        run_id = payload.get("runId")
        if self._turns.is_run_aborted(run_id):
            logger.debug("frame of an aborted run dropped (sk=%s)", payload.get("sessionKey"))
            return True
        if self._turns.is_run_finished(run_id):
            # A channel's own run that already ended (m5): never a push, even once its
            # channel is gone.
            logger.debug("frame of a finished run dropped (sk=%s)", payload.get("sessionKey"))
            return True
        return False

    async def _handle_event(self, frame: dict) -> None:
        event = frame.get("event", "")
        payload = _as_dict(frame.get("payload"))
        if event == "chat":
            await self._handle_chat(payload)
        elif event == "agent":
            await self._handle_agent_lifecycle(payload)
        elif event == "session.tool":
            await self._handle_session_tool(payload)
        elif event == "session.message":
            self._handle_session_message(payload)
        else:
            self._note_unhandled(f"event {event!r}")

    async def _handle_chat(self, payload: dict) -> None:
        sk = payload.get("sessionKey")
        if sk is None or self._of_aborted_run(payload):
            return
        channel = self._channels.channel_for(sk)
        if channel is None or _taken(channel, "chat", payload, hold=True):
            return
        self._push(channel, "chat", payload)

    async def _handle_agent_lifecycle(self, payload: dict) -> None:
        sk = payload.get("sessionKey")
        if sk is None or self._of_aborted_run(payload):
            return
        channel = self._channels.channel_for(sk)
        if channel is None or _taken(channel, "agent", payload, hold=True):
            # A run the channel sent: every stream of it is the channel's (it opens its turn
            # on the run's first event, not on `start`: docs/openclaw-protocol.md, C3; a
            # `fallback_step` is kept for classifying its error, #397). Or held while a
            # `chat.send` of the channel waits for its ack (any stream: a `run_status` or a
            # lifecycle `model` may be its run's first event).
            return
        data = _as_dict(payload.get("data"))
        if payload.get("stream") == "error" and data.get("reason") == "seq gap":
            # OpenClaw's own notice where a drained run's lifecycle `start` should be
            # (2026.11.7): not a failure, it opens and closes nothing.
            logger.debug("agent seq gap notice (sk=%s)", sk)
        self._push(channel, "agent", payload)

    def _push(self, channel: "ConversationChannel", event: str, payload: dict) -> None:
        """A frame nobody owns: the push of its key (`push_route`). An `end`/`error` is always
        forwarded: the push end no-ops when no push is open (#84). While a turn of the channel
        is open (a /v1 one too, #112) a push's opener waits in the channel's worker."""
        route = push_route(event, payload)
        if route is None:
            return
        kind, arg = route
        channel.enqueue_push(kind, arg, run_id=_run_id(payload))

    def _handle_session_message(self, payload: dict) -> None:
        """`session.message` (2026.11.7): a transcript append. Only a `user` one matters: it
        links an OpenClaw message id to the `chat.send` that sent it (`idempotencyKey`
        `"<runId>:user"`) and, on a repeated copy, says that `steer` folded it into the active
        run (`__openclaw.steerTargetRunId`). It goes to the key's channel, in order with its
        frames (`ConversationChannel.take_session_message`). It carries user text: never
        logged. Not drift."""
        sk = payload.get("sessionKey")
        message = _as_dict(payload.get("message"))
        if not isinstance(sk, str) or message.get("role") != "user":
            return
        channel = self._channels.channel_for(sk)
        take = getattr(channel, "take_session_message", None) if channel is not None else None
        if callable(take):
            take(payload)

    async def _handle_session_tool(self, payload: dict) -> None:
        sk = payload.get("sessionKey")
        if sk is None or self._of_aborted_run(payload):
            return
        data = _as_dict(payload.get("data"))
        if data.get("phase") not in ("start", "result"):
            return
        channel = self._channels.channel_for(sk)
        if channel is None or _taken(channel, "session.tool", payload, hold=True):
            return
        self._push(channel, "session.tool", payload)

    def connection_lost(self, code: str) -> None:
        """The OpenClaw connection dropped (`OpenClawClient._listen`): every channel ends the
        runs it was waiting for with `code` (a stable one, `orchestrator_disconnected`)."""
        lost = getattr(self._channels, "connection_lost", None)
        if lost is not None:
            lost(code)
