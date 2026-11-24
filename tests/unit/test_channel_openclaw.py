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
from tests.fakes.bridge import blocking, keyed_conversation, sent, tokens, wait_until
from tests.fakes.orchestrator import OrchestratorEvent

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


async def test_an_interrupting_send_ends_the_turn_once_for_every_subscriber(session, hub):
    """5.0 PR 2b: only «send now» (`interrupt`) ends another device's turn (`interrupted`); a
    plain `send` queues (tests/unit/test_channel_queue.py)."""
    tv, tv_ws = session("tv")
    mac, mac_ws = session("mac")
    started = asyncio.Event()
    hub.orchestrator.stream_response = blocking(started)
    await tv.turns.start_user_turn("uno")
    await asyncio.wait_for(started.wait(), 2)
    hub.orchestrator.stream_response = tokens("dos")
    await mac.turns.start_user_turn("dos", interrupt=True)
    await asyncio.wait_for(mac.turns.user_task, 2)
    await sent(tv), await sent(mac)

    for ws in (tv_ws, mac_ws):
        assert pairs(ws) == [
            ("turn_start", "t-1", "user"), ("turn_end", "t-1", "interrupted"),
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


# ── Several sessions in one conversation (2a.3) ───────────────────────────


async def test_a_late_session_gets_the_accumulated_text_without_audio(session, hub):
    """A session that attaches mid-turn gets the turn's turn_start and one `token` with what the
    turn said so far, then the rest; never the audio (it is the sender's)."""
    tv, tv_ws = session("tv")
    tts, stream = _tts()
    started, go_on = asyncio.Event(), asyncio.Event()

    async def stream_response(*a, **k):
        yield OrchestratorEvent(type="token", content="uno ")
        yield OrchestratorEvent(type="token", content="dos ")
        started.set()
        await go_on.wait()
        yield OrchestratorEvent(type="token", content="tres")

    hub.orchestrator.stream_response = stream_response
    await tv.turns.start_user_turn("hola")
    task = tv.turns.user_task
    await asyncio.wait_for(started.wait(), 2)

    mac, mac_ws = session("mac", output_mode=("text", "audio"), tts=tts)
    go_on.set()
    await asyncio.wait_for(task, 2)
    await sent(mac)
    await sent(tv)

    assert pairs(mac_ws) == [("turn_start", "t-1", "user"), ("turn_end", "t-1", "done")]
    assert texts(mac_ws) == ["uno dos ", "tres"]
    assert not any(isinstance(m, bytes) for m in mac_ws.sent)
    tts.connect.assert_not_awaited()
    assert texts(tv_ws) == ["uno ", "dos ", "tres"]


async def test_a_late_session_without_text_gets_no_catch_up(session, hub):
    tv, _ = session("tv")
    started, go_on = asyncio.Event(), asyncio.Event()

    async def stream_response(*a, **k):
        yield OrchestratorEvent(type="token", content="uno")
        started.set()
        await go_on.wait()

    hub.orchestrator.stream_response = stream_response
    await tv.turns.start_user_turn("hola")
    task = tv.turns.user_task
    await asyncio.wait_for(started.wait(), 2)
    mute, mute_ws = session("mute", output_mode=("status",))
    go_on.set()
    await asyncio.wait_for(task, 2)
    await sent(mute)
    assert pairs(mute_ws) == [("turn_start", "t-1", "user"), ("turn_end", "t-1", "done")]
    assert mute_ws.of("token") == []


async def test_a_home_device_never_attaches_to_a_private_conversation(make_bridge, hub):
    """M20: a device attaches only to a conversation visible to it (`visible_to`): never to a
    private one of a person (from a home device or another person's), never to another home's."""
    private = keyed_conversation("ana-web", id="conv-private")
    object.__setattr__(private, "owner_user_id", "ana")
    other_home = keyed_conversation("vecino", id="conv-other-home")
    object.__setattr__(other_home, "home_id", "otro-hogar")
    for conversation in (private, other_home):
        hub.index.add(conversation)
        bridge, _ = make_bridge(
            client_id="tv", orchestrator=hub.orchestrator, turn_registry=hub.turn_registry,
            channel_hub=hub, client_registry=ClientRegistry(hub.index),
            conversation=SessionConversation(conversation, explicit=True),
        )  # fmt: skip
        assert bridge.channel is None
        assert hub.get(conversation.session_key) is None
        await bridge.close_all()


# ── Who may cut a turn (2a.3, L2) ─────────────────────────────────────────


async def test_a_voice_client_s_barge_in_never_cuts_another_session_s_turn(session, hub):
    """A voice client hearing the TV: its barge-in and its final transcription only cut a turn
    it sent; the TV's turn goes on for everyone, not aborted."""
    tv, tv_ws = session("tv")
    voice, voice_ws = session("voice")
    started = asyncio.Event()
    hub.orchestrator.stream_response = blocking(started)
    await tv.turns.start_user_turn("hola")
    await asyncio.wait_for(started.wait(), 2)

    await voice._on_transcription("una frase larga", False)  # barge-in partial
    await voice._on_transcription("una frase larga", True)  # final
    await sent(voice)
    assert not tv.turns.user_task.done()
    assert hub.get(SK).current_turn is not None
    assert pairs(voice_ws) == [("turn_start", "t-1", "user")]

    await tv._on_transcription("otra frase larga", False)  # its own barge-in does cut it
    await sent(tv), await sent(voice)
    assert pairs(tv_ws)[-1] == ("turn_end", "t-1", "interrupted")
    assert pairs(voice_ws)[-1] == ("turn_end", "t-1", "interrupted")


async def test_a_barge_in_on_a_push_cuts_only_this_session_s_copy(session, hub):
    tv, tv_ws = session("tv")
    voice, voice_ws = session("voice")
    await push(tv.channel, ("turn_start", SK), ("chat", {"deltaText": "aviso"}))
    await sent(tv), await sent(voice)

    await voice._on_transcription("una frase larga", False)
    await push(tv.channel, ("chat", {"deltaText": " y más"}), ("turn_end", SK))
    await sent(tv), await sent(voice)

    assert pairs(voice_ws) == [("turn_start", "t-1", "push"), ("turn_end", "t-1", "interrupted")]
    assert texts(voice_ws) == ["aviso"]
    assert pairs(tv_ws) == [("turn_start", "t-1", "push"), ("turn_end", "t-1", "done")]
    assert texts(tv_ws) == ["aviso", " y más"]


async def test_a_client_cancel_stops_the_turn_for_everyone(session, hub):
    """C3: «Parar» from any session stops the conversation's turn (the client `cancel`)."""
    tv, tv_ws = session("tv")
    mac, mac_ws = session("mac")
    started = asyncio.Event()
    hub.orchestrator.stream_response = blocking(started)
    await tv.turns.start_user_turn("hola")
    await asyncio.wait_for(started.wait(), 2)
    assert (await mac.turns.cancel("cancelled"))[0]
    await sent(tv), await sent(mac)
    for ws in (tv_ws, mac_ws):
        assert pairs(ws) == [("turn_start", "t-1", "user"), ("turn_end", "t-1", "cancelled")]


async def test_another_person_s_device_never_attaches_to_a_private_conversation(make_bridge, hub):
    """M20: Ana's private conversation is not opened from Bea's personal device."""
    from fogar_gateway.models.schemas import Client

    private = keyed_conversation("ana-web", id="conv-ana")
    object.__setattr__(private, "owner_user_id", "ana")
    hub.index.add(private)
    bea, _ = make_bridge(
        client=Client(id="bea-web", key_hash="k", is_active=True, user_id="bea"),
        client_id="bea-web", orchestrator=hub.orchestrator, turn_registry=hub.turn_registry,
        channel_hub=hub, client_registry=ClientRegistry(hub.index),
        conversation=SessionConversation(private, explicit=True),
    )  # fmt: skip
    ana, _ = make_bridge(
        client=Client(id="ana-web", key_hash="k", is_active=True, user_id="ana"),
        client_id="ana-web", orchestrator=hub.orchestrator, turn_registry=hub.turn_registry,
        channel_hub=hub, client_registry=ClientRegistry(hub.index),
        conversation=SessionConversation(private, explicit=True),
    )  # fmt: skip
    assert bea.channel is None
    assert ana.channel is not None and ana.channel.subscribers() == [ana.turns]
    await bea.close_all()
    await ana.close_all()


async def test_a_send_with_an_agent_not_allowed_leaves_the_channel_alone(session, hub):
    """#397 in a shared conversation: a session whose `allowed_agents` no longer has this agent
    gets its own pair with TURN_ERROR orchestrator_agent_unavailable; the open turn is not
    replaced, nothing is aborted, and the other session sees nothing of it."""
    tv, tv_ws = session("tv")
    mac, mac_ws = session("mac")
    started = asyncio.Event()
    hub.orchestrator.stream_response = blocking(started)
    await tv.turns.start_user_turn("hola")
    task = tv.turns.user_task
    await asyncio.wait_for(started.wait(), 2)

    mac.turns.allowed_agents = ["otro"]
    await mac.turns.start_user_turn("prohibido")
    await sent(mac), await sent(tv)

    assert not task.done()
    assert pairs(tv_ws) == [("turn_start", "t-1", "user")]
    assert tv_ws.of("error") == []
    # On its own socket the refused send leaves the turn it showed behind (one open turn per
    # socket, #202), as any `send` does; in the channel that turn goes on.
    assert pairs(mac_ws) == [
        ("turn_start", "t-1", "user"), ("turn_end", "t-1", "replaced"),
        ("turn_start", "t-2", "user"), ("turn_end", "t-2", "error"),
    ]  # fmt: skip
    assert [e["message"] for e in mac_ws.of("error")] == ["orchestrator_agent_unavailable"]
