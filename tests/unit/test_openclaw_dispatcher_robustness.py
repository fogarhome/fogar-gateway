"""FrameDispatcher: res semantics (G-47) and null payload/data tolerance (G-03), #213."""

import pytest

from fogar_gateway.services.channel import ChannelHub
from fogar_gateway.services.openclaw.dispatcher import FrameDispatcher
from fogar_gateway.services.openclaw.registry import TurnRegistry

SK = "agent:main:client-a"


def make():
    turns = TurnRegistry()
    return FrameDispatcher(turns, ChannelHub()), turns


async def test_a_stray_res_is_tolerated_and_a_late_abort_answer_tombstones_its_runs():
    """Every request gets its `res` by id in OpenClawClient (5.0 PR 2b.3: no turn queue per
    key); one that reaches the dispatcher came late. Odd ones are dropped; a late `chat.abort`
    answer still tombstones the runs it names (#285)."""
    dispatcher, turns = make()
    await dispatcher.dispatch({"type": "res", "id": "req-1", "ok": False, "error": {"code": "x"}})
    await dispatcher.dispatch({"type": "res", "id": "req-1", "ok": True, "payload": None})
    await dispatcher.dispatch({"type": "res", "id": "req-2", "ok": True, "payload": {"status": "accepted"}})
    assert not turns.is_run_aborted("run-1")
    late = {"ok": True, "aborted": True, "runIds": ["run-1"]}
    await dispatcher.dispatch({"type": "res", "id": "abort-1", "ok": True, "payload": late})
    assert turns.is_run_aborted("run-1")


@pytest.mark.parametrize("event", ["chat", "agent", "session.tool"])
async def test_null_payload_or_data_is_tolerated(event):
    dispatcher, _ = make()
    await dispatcher.dispatch({"type": "event", "event": event, "payload": None})
    payload = {"sessionKey": SK, "data": None}
    await dispatcher.dispatch({"type": "event", "event": event, "payload": payload})
