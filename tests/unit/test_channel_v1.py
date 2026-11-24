"""`ConversationChannel.send_exclusive`, the /v1 path (5.0 PR 2b.3), and its review fixes.

A real channel and dispatcher, a scripted orchestrator (`_Orchestrator` of
test_channel_queue.py: acks `r1`, `r2`… and records aborts) and a recording subscriber.
"""

import asyncio

import pytest

from tests.fakes.bridge import wait_until
from tests.unit.test_channel_queue import _channel, _event, _Orchestrator


def _v1(channel, tokens=None, **kwargs):
    sink = (lambda e: tokens.append(e)) if tokens is not None else (lambda e: None)
    return asyncio.create_task(
        channel.send_exclusive("hola", by_client_id="ha", by_name="HA", sink=sink, **kwargs)
    )


def _push_start(run_id):
    return _event("agent", {"runId": run_id, "stream": "lifecycle", "data": {"phase": "start"}})


def _absorbed(run_id, target):
    """`session.message` of `run_id`'s user message, folded by `steer` into `target`."""
    message = {"role": "user", "content": "hola", "idempotencyKey": f"{run_id}:user",
               "__openclaw": {"id": "m-1", "steerTargetRunId": target}}
    return _event("session.message", {"message": message, "messageId": "m-1"})


async def test_v1_absorbed_by_a_push_that_started_after_it_is_turn_absorbed():
    """Review I2: a push run that starts after /v1's `chat.send` absorbs /v1's message
    (`steer`): /v1's own run never opens a turn. /v1 gets `turn_absorbed`, never an empty
    answer; its message stays `delivered`, nothing is aborted."""
    orchestrator = _Orchestrator()
    hub, channel, subscriber, dispatcher = _channel(orchestrator)
    v1 = _v1(channel)
    await wait_until(lambda: "r1" in channel._runs)
    await dispatcher.dispatch(_push_start("run-p"))
    await dispatcher.dispatch(_absorbed("r1", "run-p"))
    await dispatcher.dispatch(_event("agent", {"runId": "run-p", "stream": "lifecycle", "data": {"phase": "end"}}))
    outcome = await asyncio.wait_for(v1, 2)
    assert (outcome.error, outcome.cut) == ("turn_absorbed", False)
    assert ("message", "r1", "withdrawn") not in subscriber.got
    assert orchestrator.aborted == []
    await hub.aclose(1.0)


async def test_cutting_an_absorbed_v1_run_neither_withdraws_nor_aborts_it():
    """Review M3: /v1 leaves while its message, absorbed into the open push, waits for that
    turn to end: no `withdrawn` after `delivered`, and the absorbing run is not aborted."""
    orchestrator = _Orchestrator()
    hub, channel, subscriber, dispatcher = _channel(orchestrator)
    v1 = _v1(channel)
    await wait_until(lambda: "r1" in channel._runs)
    await dispatcher.dispatch(_push_start("run-p"))
    await dispatcher.dispatch(_absorbed("r1", "run-p"))
    await channel.push_idle()
    assert channel._runs["r1"].state == "absorbed"
    v1.cancel()
    await asyncio.gather(v1, return_exceptions=True)
    assert [m for m in subscriber.got if m[0] == "message"] == [("message", "r1", "delivered")]
    assert orchestrator.aborted == []
    assert not channel._runs
    await hub.aclose(1.0)


async def test_a_failing_on_accepted_cuts_the_v1_run():
    """Review M4: if `on_accepted` raises, the run does not go on alone: it is aborted by its
    runId and the lock is released (a later message is sent)."""
    orchestrator = _Orchestrator()
    hub, channel, subscriber, _ = _channel(orchestrator)

    async def broken():
        raise RuntimeError("contador roto")

    with pytest.raises(RuntimeError):
        await asyncio.wait_for(_v1(channel, on_accepted=broken), 2)
    assert orchestrator.aborted == ["r1"]
    assert hub.turn_registry.is_run_aborted("r1")
    assert not channel._lock.locked()
    assert await asyncio.wait_for(channel.send_user_message(subscriber, "otra"), 2) is not None
    await hub.aclose(1.0)


async def test_v1_gives_up_with_turn_timeout_if_the_channel_never_calms(monkeypatch):
    """Review M1: the calm wait is capped by TURN_TIMEOUT_S: /v1 answers `turn_timeout` and
    nothing is sent."""
    from fogar_gateway.core.config import settings

    monkeypatch.setattr(settings, "TURN_TIMEOUT_S", 0.2)
    orchestrator = _Orchestrator()
    hub, channel, _, _ = _channel(orchestrator)
    channel.open_turn("user", by_client_id="tv", by_name="TV")  # a turn that never ends
    outcome = await asyncio.wait_for(_v1(channel), 2)
    assert outcome.error == "turn_timeout"
    assert orchestrator.sent == 0
    assert not channel._lock.locked() and channel._senders == 0
    await hub.aclose(1.0)


async def test_v1_gives_up_with_turn_timeout_behind_the_lock(monkeypatch):
    """Review M1: waiting for the send lock counts too (behind another /v1 that never ends)."""
    from fogar_gateway.core.config import settings

    monkeypatch.setattr(settings, "TURN_TIMEOUT_S", 0.2)
    orchestrator = _Orchestrator()
    hub, channel, _, _ = _channel(orchestrator)
    await channel._lock.acquire()  # someone holds it
    try:
        outcome = await asyncio.wait_for(_v1(channel), 2)
    finally:
        channel._lock.release()
    assert outcome.error == "turn_timeout" and orchestrator.sent == 0
    await hub.aclose(1.0)
