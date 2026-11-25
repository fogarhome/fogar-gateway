"""Final review of 5.0 PR 2b (#406): «Parar» between turns (m1), TURN_QUEUE_MAX overflow (m3)
and no ghost push from a dropped channel's finished runs (m5).

A real channel and dispatcher with the scripted orchestrator of test_channel_queue.py (acks
`r1`, `r2`… and records aborts) and a recording subscriber.
"""

import asyncio

import pytest

from fogar_gateway.services.channel import ChannelHub
from fogar_gateway.services.conversations import ConversationIndex
from fogar_gateway.services.openclaw.dispatcher import FrameDispatcher
from fogar_gateway.services.openclaw.registry import TurnRegistry
from tests.fakes.bridge import keyed_conversation, wait_until
from tests.unit.test_channel_queue import SK, _channel, _event, _Orchestrator, _Subscriber


def _delta(run_id, text="x"):
    return _event("chat", {"runId": run_id, "state": "delta", "deltaText": text})


def _final(run_id):
    return _event("chat", {"runId": run_id, "state": "final"})


async def _a_then_b_queued(orchestrator):
    """r1 runs and ends; r2 (sent during r1) is next and still `pending`: no event of it yet."""
    hub, channel, subscriber, dispatcher = _channel(orchestrator)
    await channel.send_user_message(subscriber, "a")
    await channel.send_user_message(subscriber, "b")
    await dispatcher.dispatch(_delta("r1"))
    await dispatcher.dispatch(_final("r1"))
    await channel.push_idle()
    assert channel.current_turn is None
    assert channel._runs["r2"].message == "pending"
    return hub, channel, subscriber, dispatcher


# ── m1: «Parar» and «enviar ahora» between two turns ──────────────────────


@pytest.mark.parametrize("how", ["cancel", "interrupt"])
async def test_stopping_between_turns_takes_the_next_queued_run(how):
    """No turn open, the next message still `pending` (OpenClaw already runs it, its first
    event has not come): a `cancel` (or a `send` with `interrupt`) aborts it by its runId and
    its message is `withdrawn`; its late frames open nothing."""
    orchestrator = _Orchestrator()
    hub, channel, subscriber, dispatcher = await _a_then_b_queued(orchestrator)

    if how == "cancel":
        assert await channel.cancel_head("cancelled")
    else:
        await channel.send_user_message(subscriber, "c", interrupt=True)
    assert orchestrator.aborted == ["r2"]
    assert ("message", "r2", "withdrawn") in subscriber.got
    await dispatcher.dispatch(_delta("r2", "tarde"))
    await channel.push_idle()
    assert ("start", "user", "r2") not in subscriber.got
    if how == "interrupt":
        assert ("message", "r3", "delivered") in subscriber.got  # nothing ahead of it now
    await hub.aclose(1.0)


async def test_withdraw_still_takes_only_a_pending_message():
    """`withdraw` is unchanged: a delivered head is not withdrawn by it."""
    orchestrator = _Orchestrator()
    hub, channel, subscriber, _ = _channel(orchestrator)
    await channel.send_user_message(subscriber, "a")  # delivered, its run not started
    assert not await channel.withdraw("r1")
    assert orchestrator.aborted == []
    await hub.aclose(1.0)


# ── m3: TURN_QUEUE_MAX ───────────────────────────────────────────────────────


async def test_a_run_overflowing_while_it_waits_for_the_open_turn_fails_and_is_aborted(monkeypatch):
    """r2's frames arrive while r1's turn is open and do not fit: r2 fails with TURN_ERROR
    `internal_error` once r1's turn ends (its turn opens and closes), it is aborted by its
    runId, and r1 is untouched. Nothing hangs until TURN_TIMEOUT_S."""
    from fogar_gateway.core.config import settings

    monkeypatch.setattr(settings, "TURN_QUEUE_MAX", 3)
    orchestrator = _Orchestrator()
    hub, channel, subscriber, dispatcher = _channel(orchestrator)
    await channel.send_user_message(subscriber, "a")
    await channel.send_user_message(subscriber, "b")
    await dispatcher.dispatch(_delta("r1"))
    for _ in range(6):
        await dispatcher.dispatch(_delta("r2"))
    await dispatcher.dispatch(_final("r2"))  # buried: dropped before routing
    await channel.push_idle()
    await wait_until(lambda: orchestrator.aborted == ["r2"])

    await dispatcher.dispatch(_final("r1"))
    await channel.push_idle()
    assert subscriber.got[-6:] == [
        ("event", "token"), ("end", "done"),
        ("message", "r2", "delivered"),
        ("start", "user", "r2"), ("event", "internal_error"), ("end", "error"),
    ]  # fmt: skip
    assert not channel._runs and not channel._deferred
    assert orchestrator.aborted == ["r2"]
    await hub.aclose(1.0)


async def test_a_run_whose_held_frames_overflow_fails_on_its_ack(monkeypatch):
    """Frames held while a `chat.send` waits for its ack do not fit: the ack names the run,
    which fails with `internal_error` (never painted half) and is aborted by its runId. Its
    terminal frame was never dropped."""
    from fogar_gateway.core.config import settings

    monkeypatch.setattr(settings, "TURN_QUEUE_MAX", 2)
    orchestrator = _Orchestrator(acked=False)
    hub, channel, subscriber, dispatcher = _channel(orchestrator)
    sending = asyncio.create_task(channel.send_user_message(subscriber, "hola"))
    await wait_until(lambda: channel._acks == 1)
    for _ in range(4):
        await dispatcher.dispatch(_delta("r1"))
    await dispatcher.dispatch(_final("r1"))
    assert len(channel._held) == 3  # two deltas and the final, kept past the cap

    orchestrator.ack.set()
    await asyncio.wait_for(sending, 2)
    await channel.push_idle()
    await wait_until(lambda: orchestrator.aborted == ["r1"])
    assert subscriber.got == [
        ("message", "r1", "delivered"),
        ("start", "user", "r1"), ("event", "internal_error"), ("end", "error"),
    ]  # fmt: skip
    assert not channel._runs and channel._held == []
    await hub.aclose(1.0)


async def test_held_overflow_of_a_push_keeps_its_end(monkeypatch):
    """Held frames that turn out to be a push's: what did not fit is lost, but its end was
    kept, so the push opens and closes (it never waits for its #286 budget)."""
    from fogar_gateway.core.config import settings

    monkeypatch.setattr(settings, "TURN_QUEUE_MAX", 2)
    orchestrator = _Orchestrator(acked=False)
    hub, channel, subscriber, dispatcher = _channel(orchestrator)
    sending = asyncio.create_task(channel.send_user_message(subscriber, "hola"))
    await wait_until(lambda: channel._acks == 1)
    await dispatcher.dispatch(_event("agent", {"runId": "run-p", "stream": "lifecycle", "data": {"phase": "start"}}))
    for _ in range(3):
        await dispatcher.dispatch(_delta("run-p"))
    await dispatcher.dispatch(_event("agent", {"runId": "run-p", "stream": "lifecycle", "data": {"phase": "end"}}))
    orchestrator.ack.set()  # r1: run-p was a push
    await asyncio.wait_for(sending, 2)
    await channel.push_idle()
    pushes = [g for g in subscriber.got if g[0] in ("start", "end")]
    assert pushes[:2] == [("start", "push", "run-p"), ("end", "done")]
    assert orchestrator.aborted == []
    await hub.aclose(1.0)


# ── m5: no ghost push from a dropped channel ────────────────────────────────


async def test_a_late_frame_of_a_dropped_channel_s_finished_run_opens_no_push():
    """A trusted-origin /v1 channel (its key is not indexed, nobody subscribes): its run ends,
    the channel is dropped, a new /v1 request makes a new channel on the key, and a late
    `run_status`/delta of the old run arrives. It is dropped before routing: no push opens
    (which would make the new request wait for calm until TURN_TIMEOUT_S)."""
    ha = keyed_conversation("ha", id="conv-ha")
    turns = TurnRegistry()
    hub = ChannelHub(index=ConversationIndex(), turn_registry=turns, orchestrator=_Orchestrator())
    dispatcher = FrameDispatcher(turns, hub)
    channel = hub.get_or_create(ha)

    def ev(name, payload):
        return {"type": "event", "event": name, "payload": {"sessionKey": ha.session_key, **payload}}

    v1 = asyncio.create_task(
        channel.send_exclusive("hola", by_client_id="ha", by_name="HA", sink=lambda e: None)
    )
    await wait_until(lambda: "r1" in channel._runs)
    await dispatcher.dispatch(ev("chat", {"runId": "r1", "state": "delta", "deltaText": "hola"}))
    await dispatcher.dispatch(ev("chat", {"runId": "r1", "state": "final"}))
    outcome = await asyncio.wait_for(v1, 2)
    assert outcome.error is None
    await wait_until(lambda: hub.get(ha.session_key) is None)  # dropped when idle

    again = hub.get_or_create(ha)  # the next /v1 request
    await dispatcher.dispatch(ev("agent", {"runId": "r1", "stream": "run_status", "data": {"phase": "done"}}))
    await dispatcher.dispatch(ev("chat", {"runId": "r1", "state": "delta", "deltaText": "tarde"}))
    await again.push_idle()
    assert again.current_turn is None and again._push is None
    assert turns.is_run_finished("r1")
    await hub.aclose(1.0)


async def test_a_finished_run_s_late_frame_reaches_no_subscriber_of_an_indexed_key():
    """The same with an indexed conversation and a session that attaches after the drop."""
    orchestrator = _Orchestrator()
    hub, channel, subscriber, dispatcher = _channel(orchestrator)
    await channel.send_user_message(subscriber, "a")
    await dispatcher.dispatch(_delta("r1"))
    await dispatcher.dispatch(_final("r1"))
    await channel.push_idle()
    channel.unsubscribe(subscriber)
    assert hub.drop_if_idle(SK)

    late = _Subscriber()
    hub.channel_for(SK).subscribe(late)
    await dispatcher.dispatch(_event("agent", {"runId": "r1", "stream": "run_status", "data": {"phase": "done"}}))
    await hub.get(SK).push_idle()
    assert late.got == []
    await hub.aclose(1.0)
