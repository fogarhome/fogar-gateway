"""The channel owns its conversation's OpenClaw runs (5.0 PR 2a.2, #406).

Two real sessions (`FogarBridge`) attached to one conversation's channel, sharing a hub and
an OpenClaw fake (the orchestrator mock's `stream_response`, as in `tests/fakes/bridge.py`):
each session gets its own turn_start/turn_end pair of every channel turn, the audio goes to
the sender only, and a session leaving never aborts the run.
"""

import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest

from fogar_gateway.models.schemas import ClientConfig
from fogar_gateway.services.channel import ChannelHub
from fogar_gateway.services.conversations import ConversationIndex, SessionConversation
from fogar_gateway.services.openclaw.registry import ClientRegistry, TurnRegistry
from fogar_gateway.services.protocol import OrchestratorEvent
from tests.fakes.bridge import blocking, keyed_conversation, sent, tokens, wait_until

CONVERSATION = keyed_conversation("shared", id="conv-s")
SK = CONVERSATION.session_key


@pytest.fixture
async def hub():
    """One hub for both sessions, as the app's: its orchestrator is an OpenClaw fake."""
    index = ConversationIndex()
    index.add(CONVERSATION)
    counted: list[str] = []
    hub = ChannelHub(
        index=index, orchestrator=AsyncMock(), turn_registry=TurnRegistry(),
        note_turn=counted.append,
    )  # fmt: skip
    hub.counted = counted
    yield hub
    await hub.aclose(1.0)


@pytest.fixture
def session(make_bridge, hub):
    """`session(client_id, **overrides) -> (bridge, ws)`: a session attached to the shared
    conversation's channel."""

    def _make(client_id, **overrides):
        overrides.setdefault("client_registry", ClientRegistry(hub.index))
        return make_bridge(
            client_id=client_id,
            orchestrator=hub.orchestrator,
            turn_registry=hub.turn_registry,
            channel_hub=hub,
            conversation=SessionConversation(CONVERSATION, explicit=True),
            **overrides,
        )

    return _make


def _tts(chunks=(b"\x00\x01",)):
    stream = MagicMock()
    stream.send_text_chunk = AsyncMock()
    stream.end = AsyncMock()
    stream.close = AsyncMock()

    async def audio():
        for chunk in chunks:
            yield chunk

    stream.get_audio_stream = audio
    provider = MagicMock()
    provider.name = "fake"
    provider.resolve_voice = MagicMock(return_value="v")
    provider.connect = AsyncMock(return_value=stream)
    return provider, stream


def pairs(ws) -> list[tuple]:
    """(type, turn_id, origin or reason) of every turn_start/turn_end the client got."""
    return [
        (m["type"], m["turn_id"], m.get("origin") or m.get("reason"))
        for m in ws.sent
        if isinstance(m, dict) and m["type"] in ("turn_start", "turn_end")
    ]


def texts(ws) -> list[str]:
    return [m["text"] for m in ws.of("token")]


async def push(channel, *events):
    for kind, arg in events:
        channel.enqueue_push(kind, arg)
    await channel.push_idle()


# ── The user turn ──────────────────────────────────────────────────────────


async def test_user_turn_reaches_two_subscribers(session, hub):
    tv, tv_ws = session("tv")
    mac, mac_ws = session("mac")
    hub.orchestrator.stream_response = tokens("ho", "la")
    assert tv.channel is mac.channel and len(tv.channel.subscribers()) == 2

    await tv.turns.start_user_turn("hola")
    await asyncio.wait_for(tv.turns.user_task, 2)
    await sent(tv), await sent(mac)

    for ws in (tv_ws, mac_ws):
        assert pairs(ws) == [("turn_start", "t-1", "user"), ("turn_end", "t-1", "done")]
        assert texts(ws) == ["ho", "la"]
        assert ws.of("turn_start")[0]["conversation"] == "conv-s"
    hub.orchestrator.abort.assert_not_awaited()


async def test_each_subscriber_numbers_its_own_turns(session, hub):
    tv, tv_ws = session("tv")
    hub.orchestrator.stream_response = tokens("a")
    await tv.turns.start_user_turn("uno")
    await asyncio.wait_for(tv.turns.user_task, 2)
    mac, mac_ws = session("mac")  # attached later: its first turn is its t-1
    await mac.turns.start_user_turn("dos")
    await asyncio.wait_for(mac.turns.user_task, 2)
    await sent(tv), await sent(mac)

    assert [i for t, i, _ in pairs(tv_ws) if t == "turn_start"] == ["t-1", "t-2"]
    assert [i for t, i, _ in pairs(mac_ws) if t == "turn_start"] == ["t-1"]
    assert [m["turn_seq"] for m in mac_ws.of("turn_start")] == [1]


async def test_tts_only_for_sender(session, hub):
    tv_tts, tv_stream = _tts()
    mac_tts, _ = _tts()
    tv, tv_ws = session("tv", output_mode=("text", "audio"), tts=tv_tts)
    mac, mac_ws = session("mac", output_mode=("text", "audio"), tts=mac_tts)
    hub.orchestrator.stream_response = tokens("hola")

    await tv.turns.start_user_turn("hola")
    await asyncio.wait_for(tv.turns.user_task, 2)
    await sent(tv), await sent(mac)

    tv_tts.connect.assert_awaited_once()
    tv_stream.send_text_chunk.assert_awaited_once_with("hola")
    mac_tts.connect.assert_not_awaited()
    assert any(isinstance(m, bytes) for m in tv_ws.sent)
    assert not any(isinstance(m, bytes) for m in mac_ws.sent)
    assert texts(mac_ws) == ["hola"]  # the other one sees the text, without voice


async def test_note_turn_once_per_channel_turn(session, hub):
    tv, _ = session("tv")
    mac, _ = session("mac")
    hub.orchestrator.stream_response = tokens("a")
    await tv.turns.start_user_turn("hola")
    await asyncio.wait_for(tv.turns.user_task, 2)
    await push(tv.channel, ("turn_start", SK), ("chat", {"deltaText": "aviso"}), ("turn_end", SK))
    await sent(tv), await sent(mac)

    assert hub.counted == ["conv-s", "conv-s"]  # one user turn + one push, not per session


# ── Push ──────────────────────────────────────────────────────────────────


async def test_push_reaches_attached_subscribers(session, hub):
    tv, tv_ws = session("tv")
    mac, mac_ws = session("mac")
    await push(tv.channel, ("turn_start", SK), ("chat", {"deltaText": "aviso"}), ("turn_end", SK))
    await sent(tv), await sent(mac)

    for ws in (tv_ws, mac_ws):
        assert pairs(ws) == [("turn_start", "t-1", "push"), ("turn_end", "t-1", "done")]
        assert texts(ws) == ["aviso"]


async def test_push_skips_subscriber_with_push_disabled(session, hub):
    tv, tv_ws = session("tv")
    mac, mac_ws = session("mac", config=ClientConfig(push_enabled=False))
    await push(tv.channel, ("turn_start", SK), ("chat", {"deltaText": "aviso"}), ("turn_end", SK))
    await sent(tv), await sent(mac)

    assert pairs(tv_ws) == [("turn_start", "t-1", "push"), ("turn_end", "t-1", "done")]
    assert mac_ws.sent == []


async def test_push_audio_goes_to_the_last_attached_audio_subscriber(session, hub):
    """2a: the most recently attached session that wants audio plays the push (2c replaces
    it by the notification device)."""
    tv_tts, _ = _tts()
    mac_tts, _ = _tts()
    tv, tv_ws = session("tv", output_mode=("text", "audio"), tts=tv_tts)
    mac, mac_ws = session("mac", output_mode=("text", "audio"), tts=mac_tts)
    await push(tv.channel, ("turn_start", SK), ("chat", {"deltaText": "aviso"}), ("turn_end", SK))
    await sent(tv), await sent(mac)

    mac_tts.connect.assert_awaited_once()
    tv_tts.connect.assert_not_awaited()
    assert texts(tv_ws) == texts(mac_ws) == ["aviso"]


_TOOL = {"phase": "start", "name": "exec", "toolCallId": "c1", "args": {"command": "ls"}}


async def test_tool_calls_only_to_enabled_subscriber(session, hub):
    tv, tv_ws = session("tv", config=ClientConfig(tool_calls_enabled=True))
    mac, mac_ws = session("mac", config=ClientConfig(tool_calls_enabled=False))
    await push(tv.channel, ("turn_start", SK), ("tool", _TOOL), ("turn_end", SK))
    await sent(tv), await sent(mac)

    assert [m["name"] for m in tv_ws.of("tool_call")] == ["exec"]
    assert mac_ws.of("tool_call") == []
    assert len(mac_ws.of("turn_end")) == 1


# ── One turn_end per turn_start per subscriber ────────────────────────────


async def test_cancel_ends_the_turn_once_for_every_subscriber(session, hub):
    tv, tv_ws = session("tv")
    mac, mac_ws = session("mac")
    started = asyncio.Event()
    hub.orchestrator.stream_response = blocking(started)
    await tv.turns.start_user_turn("hola")
    await asyncio.wait_for(started.wait(), 2)

    assert await mac.turns.cancel("cancelled") == (True, "t-1")  # a session cancels what it sees
    assert await tv.turns.cancel("cancelled") == (False, None)
    await sent(tv), await sent(mac)

    for ws in (tv_ws, mac_ws):
        assert pairs(ws) == [("turn_start", "t-1", "user"), ("turn_end", "t-1", "cancelled")]


async def test_a_replacing_send_ends_the_turn_once_for_every_subscriber(session, hub):
    tv, tv_ws = session("tv")
    mac, mac_ws = session("mac")
    started = asyncio.Event()
    hub.orchestrator.stream_response = blocking(started)
    await tv.turns.start_user_turn("uno")
    await asyncio.wait_for(started.wait(), 2)
    hub.orchestrator.stream_response = tokens("dos")
    await mac.turns.start_user_turn("dos")
    await asyncio.wait_for(mac.turns.user_task, 2)
    await sent(tv), await sent(mac)

    for ws in (tv_ws, mac_ws):
        assert pairs(ws) == [
            ("turn_start", "t-1", "user"), ("turn_end", "t-1", "replaced"),
            ("turn_start", "t-2", "user"), ("turn_end", "t-2", "done"),
        ]  # fmt: skip


async def test_an_error_ends_the_turn_once_for_every_subscriber(session, hub):
    tv, tv_ws = session("tv")
    mac, mac_ws = session("mac")

    async def failing(*a, **k):
        yield OrchestratorEvent(type="token", content="a")
        yield OrchestratorEvent(type="error", content="orchestrator_error")

    hub.orchestrator.stream_response = failing
    await tv.turns.start_user_turn("hola")
    await asyncio.wait_for(tv.turns.user_task, 2)
    await sent(tv), await sent(mac)

    for ws in (tv_ws, mac_ws):
        assert ws.types() == ["turn_start", "token", "error", "turn_end"]
        assert ws.of("error")[0]["message"] == "orchestrator_error"
        assert ws.of("turn_end")[0]["reason"] == "error"


# ── A subscriber leaving (decision 2) ─────────────────────────────────────


async def test_a_subscriber_leaving_mid_turn_does_not_abort_the_run(session, hub):
    """Review focus 1 of 2a: the sender closes during its own turn. It ends its own turn
    (`closed`); the other session keeps seeing the turn to its end; the run is not aborted."""
    tv, tv_ws = session("tv")
    mac, mac_ws = session("mac")
    started, go_on = asyncio.Event(), asyncio.Event()

    async def stream(*a, **k):
        yield OrchestratorEvent(type="token", content="uno ")
        started.set()
        await go_on.wait()
        yield OrchestratorEvent(type="token", content="dos")

    hub.orchestrator.stream_response = stream
    await tv.turns.start_user_turn("hola")
    task = tv.turns.user_task
    await asyncio.wait_for(started.wait(), 2)

    await asyncio.wait_for(tv.close_all(), 2)
    assert not task.done(), "the sender leaving cancelled the channel's turn"
    go_on.set()
    await asyncio.wait_for(task, 2)
    await sent(mac)

    assert pairs(tv_ws) == [("turn_start", "t-1", "user"), ("turn_end", "t-1", "closed")]
    assert pairs(mac_ws) == [("turn_start", "t-1", "user"), ("turn_end", "t-1", "done")]
    assert texts(mac_ws) == ["uno ", "dos"]
    hub.orchestrator.abort.assert_not_awaited()
    assert mac.channel.subscribers() == [mac.turns]


async def test_a_channel_nobody_uses_is_dropped_once_its_turn_ends(session, hub):
    tv, _ = session("tv")
    go_on = asyncio.Event()

    async def stream(*a, **k):
        await go_on.wait()
        yield OrchestratorEvent(type="token", content="x")

    hub.orchestrator.stream_response = stream
    await tv.turns.start_user_turn("hola")
    task = tv.turns.user_task
    await asyncio.wait_for(tv.close_all(), 2)
    assert hub.get(SK) is not None  # its turn still runs
    go_on.set()
    await asyncio.wait_for(task, 2)
    await wait_until(lambda: hub.get(SK) is None)
    assert hub.channel_for(SK) is not None  # a later push gets a fresh one from the index
