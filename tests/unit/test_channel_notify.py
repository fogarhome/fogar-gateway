"""Where a push goes (5.0 PR 2c.3, #407): its text to the attached sessions that may see its
conversation, its audio only to the conversation's notify device; with nobody attached, its
text too goes to the notify device's session, as a push of another conversation, FIFO behind
whatever turn that session has open (#202), never on top.

Real sessions (`FogarBridge`) on one hub and one `ClientRegistry`, as in the app; the notify
device lookup (the DB in the app, `db_client.notify_device`) is a dict here. Every test checks
each socket's pairs (`assert_pairs`: one `turn_end` per `turn_start`, `turn_seq` & 0xFFFF,
never two turns open at once).
"""

import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest

from fogar_gateway.core.config import settings
from fogar_gateway.models.schemas import Client, ClientConfig
from fogar_gateway.services.channel import ChannelHub, ChannelTurn
from fogar_gateway.services.conversations import (
    Conversation,
    ConversationIndex,
    SessionConversation,
)
from fogar_gateway.services.openclaw.registry import ClientRegistry, TurnRegistry
from tests.fakes.bridge import blocking, keyed_conversation, sent, tokens, wait_until

HOME = keyed_conversation("home-conv", id="conv-home")  # a home conversation (no owner)
OTHER = keyed_conversation("other-conv", id="conv-other")  # another home conversation
PRIVATE = Conversation(
    id="conv-private",
    client_id="ana-phone",
    agent="main",
    session_key="agent:main:fogar-conv-private",
    kind="created",
    owner_user_id="ana",
)
ANA_OTHER = Conversation(
    id="conv-ana-2",
    client_id="ana-phone",
    agent="main",
    session_key="agent:main:fogar-conv-ana-2",
    kind="created",
    owner_user_id="ana",
)
ALL = (HOME, OTHER, PRIVATE, ANA_OTHER)


@pytest.fixture
async def hub():
    index = ConversationIndex()
    for conversation in ALL:
        index.add(conversation)
    registry = ClientRegistry(index)
    notify: dict[str, str] = {}  # conversation id -> notify device's client id

    async def lookup(conversation: Conversation) -> str | None:
        return notify.get(conversation.id)

    hub = ChannelHub(
        index=index,
        orchestrator=AsyncMock(),
        turn_registry=TurnRegistry(),
        notify_device=lookup,
        sessions_of=lambda cid: [b.turns for b in registry.for_client(cid)],
    )
    hub.registry = registry
    hub.notify = notify
    yield hub
    await hub.aclose(1.0)


@pytest.fixture
def session(make_bridge, hub):
    """`session(client_id, conversation, user_id=None, **overrides) -> (bridge, ws)`: a live
    session of `client_id` (registered, attached to `conversation`'s channel)."""

    def _make(client_id, conversation=HOME, *, user_id=None, **overrides):
        return make_bridge(
            client_id=client_id,
            client=Client(id=client_id, key_hash="k", is_active=True, user_id=user_id),
            orchestrator=hub.orchestrator,
            turn_registry=hub.turn_registry,
            channel_hub=hub,
            client_registry=hub.registry,
            register=True,
            conversation=SessionConversation(conversation, explicit=True),
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


def turns(ws) -> list[tuple]:
    """(type, turn_id, origin or reason, conversation) of every turn_start/turn_end."""
    return [
        (m["type"], m["turn_id"], m.get("origin") or m.get("reason"), m.get("conversation"))
        for m in ws.sent
        if isinstance(m, dict) and m["type"] in ("turn_start", "turn_end")
    ]


def texts(ws) -> list[str]:
    return [m["text"] for m in ws.of("token")]


def audio(ws) -> list[bytes]:
    return [m for m in ws.sent if isinstance(m, bytes)]


def assert_pairs(ws) -> None:
    """Per socket: one turn_end per turn_start, after it; `turn_seq` & 0xFFFF; never two
    turns open at once (#202); every token/audio inside an open turn."""
    open_turn: str | None = None
    seen: set[str] = set()
    for m in ws.sent:
        if isinstance(m, bytes):
            assert open_turn is not None, "audio outside a turn"
            continue
        kind = m["type"]
        if kind == "turn_start":
            assert open_turn is None, f"{m['turn_id']} opened over {open_turn}"
            assert m["turn_id"] not in seen
            assert m["turn_seq"] == int(m["turn_id"].removeprefix("t-")) & 0xFFFF
            open_turn = m["turn_id"]
            seen.add(open_turn)
        elif kind == "turn_end":
            assert m["turn_id"] == open_turn, f"turn_end {m['turn_id']} while {open_turn} open"
            open_turn = None
        elif kind in ("token", "tool_call", "turn_progress"):
            assert m["turn_id"] == open_turn
    assert open_turn is None, f"{open_turn} never ended"


async def push(hub, conversation, *texts_, end=True):
    channel = hub.channel_for(conversation.session_key)
    assert channel is not None
    sk = conversation.session_key
    channel.enqueue_push("turn_start", sk)
    for text in texts_:
        channel.enqueue_push("chat", {"sessionKey": sk, "deltaText": text})
    if end:
        channel.enqueue_push("turn_end", sk)
    await channel.push_idle()
    return channel


async def push_end(hub, conversation):
    channel = hub.channel_for(conversation.session_key)
    channel.enqueue_push("turn_end", conversation.session_key)
    await channel.push_idle()


async def settle(*bridges):
    for bridge in bridges:
        await sent(bridge)


# ── text to the attached, audio to the notify device ─────────────────────────


async def test_push_text_to_attached_audio_to_notify_device(session, hub):
    """The notify device's session is in another conversation: it still gets the push (its
    audio), as a push of the push's conversation; the attached sessions get its text and no
    audio."""
    tv_tts, _ = _tts()
    mac_tts, _ = _tts()
    speaker_tts, _ = _tts()
    tv, tv_ws = session("tv", HOME, output_mode=("text", "audio"), tts=tv_tts)
    mac, mac_ws = session("mac", HOME, output_mode=("text", "audio"), tts=mac_tts)
    speaker, speaker_ws = session("speaker", OTHER, output_mode=("text", "audio"), tts=speaker_tts)
    hub.notify[HOME.id] = "speaker"

    await push(hub, HOME, "aviso")
    await settle(tv, mac, speaker)

    for ws in (tv_ws, mac_ws, speaker_ws):
        assert turns(ws) == [
            ("turn_start", "t-1", "push", "conv-home"),
            ("turn_end", "t-1", "done", None),
        ]
        assert texts(ws) == ["aviso"]
        assert_pairs(ws)
    speaker_tts.connect.assert_awaited_once()
    tv_tts.connect.assert_not_awaited()
    mac_tts.connect.assert_not_awaited()
    assert audio(speaker_ws) and not audio(tv_ws) and not audio(mac_ws)
    # A guest of that push only: it never attached to the other channel.
    assert speaker.channel is not None and speaker.channel.conversation.id == "conv-other"
    home_channel = hub.get(HOME.session_key)
    assert home_channel is not None and speaker.turns not in home_channel.subscribers()


async def test_attached_notify_device_plays_and_text_only_one_is_not_invited(session, hub):
    """The notify device attached plays the push there; a notify device without audio that is
    not attached gets nothing while others are attached (it would only be there for the
    audio)."""
    tv_tts, _ = _tts()
    tv, tv_ws = session("tv", HOME, output_mode=("text", "audio"), tts=tv_tts)
    hub.notify[HOME.id] = "tv"
    await push(hub, HOME, "uno")
    await settle(tv)
    tv_tts.connect.assert_awaited_once()
    assert audio(tv_ws)

    reader, reader_ws = session("reader", OTHER, output_mode=("text",))
    hub.notify[HOME.id] = "reader"
    await push(hub, HOME, "dos")
    await settle(tv, reader)
    assert reader_ws.sent == []
    assert texts(tv_ws) == ["uno", "dos"]
    assert_pairs(tv_ws)


async def test_push_to_notify_device_when_nobody_attached(session, hub):
    """Nobody attached: the text goes to the notify device's session (text only here), with
    its conversation in turn_start. Another device's session gets nothing."""
    tv, tv_ws = session("tv", OTHER, output_mode=("text",))
    mac, mac_ws = session("mac", OTHER, output_mode=("text",))
    hub.notify[HOME.id] = "tv"

    await push(hub, HOME, "recordatorio")
    await settle(tv, mac)

    assert turns(tv_ws) == [
        ("turn_start", "t-1", "push", "conv-home"),
        ("turn_end", "t-1", "done", None),
    ]
    assert texts(tv_ws) == ["recordatorio"]
    assert mac_ws.sent == []
    assert_pairs(tv_ws)


async def test_push_without_notify_device_is_silent(session, hub):
    """No notify device: the attached sessions get the text, nobody plays it; nobody attached
    → nobody gets anything (no turn_start), the run goes on in OpenClaw (no abort)."""
    tv_tts, _ = _tts()
    tv, tv_ws = session("tv", HOME, output_mode=("text", "audio"), tts=tv_tts)
    other, other_ws = session("other", OTHER, output_mode=("text", "audio"))
    await push(hub, HOME, "aviso")
    await settle(tv, other)
    assert texts(tv_ws) == ["aviso"]
    tv_tts.connect.assert_not_awaited()
    assert_pairs(tv_ws)

    lonely = keyed_conversation("lonely", id="conv-lonely")  # nobody has it open
    hub.index.add(lonely)
    await push(hub, lonely, "nadie")
    await settle(tv, other)
    assert other_ws.sent == [] and texts(tv_ws) == ["aviso"]
    hub.orchestrator.abort_run.assert_not_awaited()


async def test_notify_device_session_not_live_or_push_disabled_is_skipped(session, hub):
    """The notify device chosen but not connected, or connected with push off: nobody plays
    the push; the attached get its text."""
    tv, tv_ws = session("tv", HOME, output_mode=("text", "audio"))
    hub.notify[HOME.id] = "absent"
    await push(hub, HOME, "uno")
    off, off_ws = session("off", OTHER, output_mode=("text", "audio"), config=ClientConfig(push_enabled=False))
    hub.notify[HOME.id] = "off"
    await push(hub, HOME, "dos")
    await settle(tv, off)
    assert texts(tv_ws) == ["uno", "dos"]
    assert off_ws.sent == []
    assert_pairs(tv_ws)


async def test_notify_device_with_two_sessions_prefers_the_one_with_the_conversation(session, hub):
    """Two live sessions of the notify device: the one that has the conversation open plays
    it, even if the other is more recent; with neither attached, the most recent."""
    first_tts, _ = _tts()
    second_tts, _ = _tts()
    first, first_ws = session("tv", HOME, output_mode=("text", "audio"), tts=first_tts)
    second, second_ws = session("tv", OTHER, output_mode=("text", "audio"), tts=second_tts)
    hub.notify[HOME.id] = "tv"
    hub.notify[ANA_OTHER.id] = "tv"  # (not visible to a home device: never delivered)
    hub.notify[OTHER.id] = "tv"

    await push(hub, HOME, "a")
    await settle(first, second)
    first_tts.connect.assert_awaited_once()
    second_tts.connect.assert_not_awaited()
    assert texts(first_ws) == ["a"] and second_ws.sent == []

    # A conversation neither has open (nobody attached): the most recent session.
    third_conv = keyed_conversation("third", id="conv-third")
    hub.index.add(third_conv)
    hub.notify[third_conv.id] = "tv"
    await push(hub, third_conv, "b")
    await settle(first, second)
    assert texts(second_ws) == ["b"]
    assert texts(first_ws) == ["a"]
    for ws in (first_ws, second_ws):
        assert_pairs(ws)


# ── privacy (M20; Review Focus 4) ───────────────────────────────────────────


async def test_private_push_never_reaches_home_device(session, hub):
    """A private conversation's push never reaches a home device nor another person's, text
    or audio, even if the notify lookup named one (defence in depth: `visible_to` on every
    delivery path). Its owner's device gets it."""
    tv_tts, _ = _tts()
    tv, tv_ws = session("tv", HOME, output_mode=("text", "audio"), tts=tv_tts)
    bea, bea_ws = session("bea-phone", HOME, user_id="bea", output_mode=("text", "audio"))
    for wrong in ("tv", "bea-phone"):
        hub.notify[PRIVATE.id] = wrong
        await push(hub, PRIVATE, "secreto")
    await settle(tv, bea)
    assert tv_ws.sent == [] and bea_ws.sent == []
    tv_tts.connect.assert_not_awaited()

    ana_tts, _ = _tts()
    ana, ana_ws = session("ana-phone", ANA_OTHER, user_id="ana", output_mode=("text", "audio"), tts=ana_tts)
    hub.notify[PRIVATE.id] = "ana-phone"
    await push(hub, PRIVATE, "para ana")
    await settle(tv, bea, ana)
    assert texts(ana_ws) == ["para ana"]
    assert ana_ws.of("turn_start")[0]["conversation"] == "conv-private"
    ana_tts.connect.assert_awaited_once()
    assert tv_ws.sent == [] and bea_ws.sent == []
    assert_pairs(ana_ws)


# ── FIFO behind the session's open turn (#202) ──────────────────────────────


async def test_foreign_push_waits_behind_open_turn(session, hub):
    """The notify device's session is busy with its own turn in another conversation: the
    push's turn_start comes after that turn's turn_end, never on top."""
    speaker, ws = session("speaker", OTHER, output_mode=("text",))
    hub.notify[HOME.id] = "speaker"
    started = asyncio.Event()
    hub.orchestrator.stream_response = blocking(started)
    await speaker.turns.start_user_turn("hola")
    await asyncio.wait_for(started.wait(), 2)

    await push(hub, HOME, "aviso")
    await settle(speaker)
    assert turns(ws) == [("turn_start", "t-1", "user", "conv-other")]

    assert await speaker.turns.cancel("cancelled") == (True, "t-1")
    await settle(speaker)
    assert turns(ws) == [
        ("turn_start", "t-1", "user", "conv-other"),
        ("turn_end", "t-1", "cancelled", None),
        ("turn_start", "t-2", "push", "conv-home"),
        ("turn_end", "t-2", "done", None),
    ]
    assert texts(ws)[-1] == "aviso"
    assert_pairs(ws)


async def test_own_turn_waits_behind_a_foreign_push(session, hub):
    """And the other way round: a turn of its own conversation that opens while the foreign
    push is open on the session waits for that push's turn_end."""
    speaker, ws = session("speaker", OTHER, output_mode=("text",))
    hub.notify[HOME.id] = "speaker"
    await push(hub, HOME, "aviso", end=False)
    await settle(speaker)
    assert turns(ws) == [("turn_start", "t-1", "push", "conv-home")]

    hub.orchestrator.stream_response = tokens("respuesta")
    await speaker.turns.start_user_turn("hola")
    await asyncio.wait_for(speaker.turns.user_task, 2)
    await settle(speaker)
    assert turns(ws) == [("turn_start", "t-1", "push", "conv-home")]

    await push_end(hub, HOME)
    await settle(speaker)
    assert turns(ws) == [
        ("turn_start", "t-1", "push", "conv-home"),
        ("turn_end", "t-1", "done", None),
        ("turn_start", "t-2", "user", "conv-other"),
        ("turn_end", "t-2", "done", None),
    ]
    assert texts(ws) == ["aviso", "respuesta"]
    assert_pairs(ws)


# ── how a foreign push ends on the session ──────────────────────────────────


async def test_cancel_of_a_foreign_push_cuts_only_its_own_copy(session, hub):
    """`cancel` from the session that got the push as a guest cuts its copy only (it is not
    attached to that conversation); the attached sessions keep the push."""
    tv, tv_ws = session("tv", HOME, output_mode=("text",))
    speaker_tts, _ = _tts()
    speaker, speaker_ws = session("speaker", OTHER, output_mode=("text", "audio"), tts=speaker_tts)
    hub.notify[HOME.id] = "speaker"
    channel = await push(hub, HOME, "uno", end=False)
    await settle(tv, speaker)

    assert await speaker.turns.cancel("cancelled") == (True, "t-1")
    channel.enqueue_push("chat", {"sessionKey": HOME.session_key, "deltaText": "dos"})
    await push_end(hub, HOME)
    await settle(tv, speaker)

    assert turns(speaker_ws) == [
        ("turn_start", "t-1", "push", "conv-home"),
        ("turn_end", "t-1", "cancelled", None),
    ]
    assert texts(speaker_ws) == ["uno"]
    assert texts(tv_ws) == ["uno", "dos"]
    assert turns(tv_ws)[-1] == ("turn_end", "t-1", "done", None)
    hub.orchestrator.abort_run.assert_not_awaited()
    for ws in (tv_ws, speaker_ws):
        assert_pairs(ws)


async def test_attached_cancel_ends_the_foreign_copy_too(session, hub):
    """An attached session's `cancel` stops the push for everyone, the guest too."""
    tv, tv_ws = session("tv", HOME, output_mode=("text",))
    speaker_tts, _ = _tts()
    speaker, speaker_ws = session("speaker", OTHER, output_mode=("text", "audio"), tts=speaker_tts)
    hub.notify[HOME.id] = "speaker"
    await push(hub, HOME, "uno", end=False)
    await settle(tv, speaker)
    assert await tv.turns.cancel("cancelled") == (True, "t-1")
    await settle(tv, speaker)
    for ws in (tv_ws, speaker_ws):
        assert turns(ws)[-1] == ("turn_end", "t-1", "cancelled", None)
        assert_pairs(ws)


async def test_foreign_push_times_out_on_the_session(session, hub, monkeypatch):
    """#286: a push without progress ends with `timeout` for its guest too."""
    monkeypatch.setattr(settings, "TURN_TIMEOUT_S", 0.2)
    speaker, ws = session("speaker", OTHER, output_mode=("text",))
    hub.notify[HOME.id] = "speaker"
    await push(hub, HOME, "uno", end=False)
    await wait_until(lambda: len(ws.of("turn_end")) == 1, timeout=3)
    await settle(speaker)
    assert turns(ws)[-1] == ("turn_end", "t-1", "timeout", None)
    assert_pairs(ws)


async def test_session_closing_with_a_foreign_push_open_ends_it_closed(session, hub):
    tv, tv_ws = session("tv", HOME, output_mode=("text",))
    speaker_tts, _ = _tts()
    speaker, speaker_ws = session("speaker", OTHER, output_mode=("text", "audio"), tts=speaker_tts)
    hub.notify[HOME.id] = "speaker"
    channel = await push(hub, HOME, "uno", end=False)
    await settle(tv, speaker)
    assert turns(speaker_ws) == [("turn_start", "t-1", "push", "conv-home")]
    await speaker.close_all()
    assert turns(speaker_ws)[-1] == ("turn_end", "t-1", "closed", None)
    assert_pairs(speaker_ws)
    # The push goes on for the attached session, and the guest is gone from it.
    channel.enqueue_push("chat", {"sessionKey": HOME.session_key, "deltaText": "dos"})
    await push_end(hub, HOME)
    await settle(tv)
    assert texts(tv_ws) == ["uno", "dos"]
    assert texts(speaker_ws) == ["uno"]
    assert_pairs(tv_ws)


async def test_session_closing_with_a_foreign_push_waiting_never_shows_it(session, hub):
    speaker, ws = session("speaker", OTHER, output_mode=("text",))
    hub.notify[HOME.id] = "speaker"
    started = asyncio.Event()
    hub.orchestrator.stream_response = blocking(started)
    await speaker.turns.start_user_turn("hola")
    await asyncio.wait_for(started.wait(), 2)
    await push(hub, HOME, "aviso", end=False)
    await settle(speaker)

    await speaker.close_all()
    assert turns(ws) == [
        ("turn_start", "t-1", "user", "conv-other"),
        ("turn_end", "t-1", "closed", None),
    ]
    assert_pairs(ws)
    await push_end(hub, HOME)  # nothing left to deliver it to
    assert turns(ws)[-1] == ("turn_end", "t-1", "closed", None)


# ── review fixes (2c.3) ─────────────────────────────────────────────────────


async def test_a_session_still_in_its_handshake_never_gets_a_push(session, hub):
    """I1: a session of the notify device that is registered but has not claimed its
    conversation yet (before `ready`) gets nothing, not even as the most recent one; the
    push goes to the device's session that is ready."""
    ready, ready_ws = session("tv", OTHER, output_mode=("text",))
    early, early_ws = session("tv", OTHER, output_mode=("text",), attach=False)
    assert hub.registry.get("tv") is early  # the most recent
    hub.notify[HOME.id] = "tv"

    await push(hub, HOME, "aviso")
    await settle(ready, early)
    assert early_ws.sent == []
    assert texts(ready_ws) == ["aviso"]
    assert_pairs(ready_ws)

    ready.turns.stop_intake()  # closing: the only claimed one is gone
    await push(hub, HOME, "otro")
    await settle(early)
    assert early_ws.sent == []


async def test_interrupting_send_drops_foreign_pushes_still_waiting(session, hub):
    """M1: «send now» cuts the foreign push on screen and drops the ones waiting behind it
    (no pair: they never opened), so the reply does not wait behind them."""
    third = keyed_conversation("third", id="conv-third")
    hub.index.add(third)
    speaker, ws = session("speaker", OTHER, output_mode=("text",))
    hub.notify[HOME.id] = "speaker"
    hub.notify[third.id] = "speaker"
    await push(hub, HOME, "uno", end=False)
    await push(hub, third, "dos", end=False)
    await settle(speaker)
    assert turns(ws) == [("turn_start", "t-1", "push", "conv-home")]

    hub.orchestrator.stream_response = tokens("respuesta")
    await speaker.turns.start_user_turn("ya", interrupt=True)
    await asyncio.wait_for(speaker.turns.user_task, 2)
    await settle(speaker)
    assert [t for t in turns(ws) if t[0] == "turn_start"] == [
        ("turn_start", "t-1", "push", "conv-home"),
        ("turn_start", "t-3", "user", "conv-other"),
    ]
    assert ("turn_end", "t-1", "interrupted", None) in turns(ws)
    assert texts(ws) == ["uno", "respuesta"]
    assert_pairs(ws)
    hub.orchestrator.abort_run.assert_not_awaited()  # cut locally only


async def test_unindexed_key_push_never_goes_to_the_notify_device(session, hub):
    """M2: a live channel on a key no conversation has (the /v1 trusted-origin `…:ha` key)
    reaches only its attached sessions: no notify device, no guest."""
    ha = Conversation(id="ha", client_id="ha", agent="main", session_key="agent:main:ha", kind="created")
    hub.get_or_create(ha)  # live, not indexed
    speaker, ws = session("speaker", OTHER, output_mode=("text", "audio"))
    hub.notify["ha"] = "speaker"
    await push(hub, ha, "de ha")
    await settle(speaker)
    assert ws.sent == []


async def test_early_phase_reaches_the_notify_device_with_nobody_attached(session, hub):
    """M4: a startup phase of a run, noted while nobody was known to see it (another push
    open for no one, no notify device read yet), is kept and given to whoever gets its push
    (here the notify device's session, as a guest)."""
    speaker, ws = session("speaker", OTHER, output_mode=("text", "status"))
    answers = iter([None, "speaker"])  # r1's read: none; r2's: the speaker

    async def lookup(conversation):
        return next(answers)

    hub.notify_device = lookup
    channel = hub.channel_for(HOME.session_key)
    sk = HOME.session_key
    channel.enqueue_push("turn_start", sk, "r1")  # open for nobody
    channel.enqueue_push(
        "chat", {"sessionKey": sk, "runId": "r2", "state": "status", "phase": "preparing_workspace"}, "r2"
    )
    channel.enqueue_push("turn_end", sk, "r1")
    channel.enqueue_push("turn_start", sk, "r2")
    channel.enqueue_push("turn_end", sk, "r2")
    await channel.push_idle()
    await settle(speaker)
    assert turns(ws) == [("turn_start", "t-1", "push", "conv-home"), ("turn_end", "t-1", "done", None)]
    assert [m["turn_id"] for m in ws.of("turn_progress")][:1] == ["t-1"]
    assert_pairs(ws)


# ── final 2c review ──────────────────────────────────────────────────────────


async def test_guest_drops_a_foreign_push_it_cannot_see(session, hub):
    """Third defence: a foreign push whose conversation this session may not see (a private one
    on a home device) is dropped in `on_turn_start`, with no pair, even if a channel let it
    through; a visible one still opens."""
    tv, tv_ws = session("tv", OTHER, output_mode=("text",))
    await settle(tv)
    hidden = ChannelTurn(
        id="x1", conversation_id=PRIVATE.id, origin="push", by_client_id=None,
        session_key=PRIVATE.session_key, conversation=PRIVATE,
    )
    tv.turns.on_turn_start(hidden)
    tv.turns.on_turn_end(hidden, "done")
    await settle(tv)
    assert tv_ws.sent == []

    visible = ChannelTurn(
        id="x2", conversation_id=HOME.id, origin="push", by_client_id=None,
        session_key=HOME.session_key, conversation=HOME,
    )
    tv.turns.on_turn_start(visible)
    tv.turns.on_turn_end(visible, "done")
    await settle(tv)
    assert [m[0] for m in turns(tv_ws)] == ["turn_start", "turn_end"]
    assert_pairs(tv_ws)


async def test_notify_device_read_uses_the_indexed_conversation(session, hub):
    """`_read_notify_device` asks for the conversation the index holds for the key (its home
    and owner as stored), not the object the channel was created with."""
    unstamped = Conversation(
        id=PRIVATE.id, client_id="ana-phone", agent="main",
        session_key=PRIVATE.session_key, kind="created",  # owner missing
    )
    hub.get_or_create(unstamped)
    asked: list[Conversation] = []

    async def lookup(conversation: Conversation) -> str | None:
        asked.append(conversation)
        return None

    hub.notify_device = lookup
    await push(hub, PRIVATE, "hola")
    assert asked and asked[0] is PRIVATE and asked[0].owner_user_id == "ana"
