"""A mock orchestrator's `stream_response` played as OpenClaw runs (5.0 PR 2b, #406).

Since 2b the channel no longer reads `stream_response` (since 2b.3 the gateway has none, /v1
included): it sends (`orchestrator.send` → the ack's runId) and reads the run's frames from
the dispatcher (`ConversationChannel.take_frame`). Most session tests still describe the reply as a
`stream_response` (`tests/fakes/bridge.py`: `tokens`, `blocking`, …); `wire_stream_orchestrator`
gives such a mock a `send` and an `abort_run` that play it as OpenClaw would:

- `send(text, sk)` acks at once with a new runId and runs the stream in a task, one run at a
  time per key (OpenClaw's queue, `followup`: a run waits for the one before it to end).
- The run: an `agent` lifecycle `start`, then each event as its frame (a token is a `chat`
  delta, a tool call a `session.tool`, a phase a `chat` status), then a `chat` `final` without
  `message`. An `error` event ends it with `fail_run(code)` (its stable code as it is), an
  exception with `internal_error` (crashed). Each frame is handled by the channel before the
  stream goes on, so a test that waits on the stream sees its effect.
- `abort_run(sk, run_id)` cancels that run's task (queued or running); nothing more of it
  arrives (the channel tombstoned it already).

The mock keeps its own `stream_response` (the reply, as `OrchestratorEvent`s) and assertions;
`send`/`abort_run` are AsyncMocks too (`send.await_args_list`,
`abort_run.assert_awaited_once_with(...)`).
"""

import asyncio
import itertools
from dataclasses import dataclass
from typing import Literal
from unittest.mock import AsyncMock, NonCallableMock

from fogar_gateway.services.openclaw.models import ToolCallEvent
from fogar_gateway.services.orchestration import OrchestratorError
from fogar_gateway.services.turn_progress import ProgressPhase


@dataclass
class OrchestratorEvent:
    """One event of a mock's `stream_response`: how a test describes a reply. Until 5.0 PR
    2b.3 it was the gateway's own (`OpenClawClient.stream_response`); since every turn goes
    through its channel it only lives here, played as OpenClaw frames (module docstring)."""

    type: Literal["token", "status", "error", "tool_call", "progress"]
    content: str = ""
    tool_call: ToolCallEvent | None = None
    phase: ProgressPhase | None = None  # `progress`: in the gateway's vocabulary (#254)

_RUNS = itertools.count(1)

# A gateway phase → an OpenClaw `chat` status phase that maps back to it.
_STATUS = {"preparing": "preparing_workspace", "thinking": "starting_model", "tool": "preparing_workspace"}


class _StreamRuns:
    def __init__(self, orchestrator, hub) -> None:
        self._orchestrator = orchestrator
        self._hub = hub
        self._tasks: dict[str, asyncio.Task] = {}
        self._last: dict[str, asyncio.Task] = {}

    async def send(self, text: str, session_key: str, *, queue_mode=None) -> str:
        run_id = f"stream-run-{next(_RUNS)}"
        previous = self._last.get(session_key)
        task = asyncio.create_task(self._run(text, session_key, run_id, previous), name="fake_run")
        self._tasks[run_id] = task
        self._last[session_key] = task
        task.add_done_callback(lambda t: self._tasks.pop(run_id, None))
        return run_id

    async def abort_run(self, session_key: str, run_id: str) -> None:
        task = self._tasks.get(run_id)
        if task is not None and not task.done():
            task.cancel()

    async def _deliver(self, session_key: str, event: str, payload: dict) -> None:
        channel = self._hub.channel_for(session_key)
        if channel is None:
            return
        payload = {"sessionKey": session_key, **payload}
        if not channel.take_frame(event, payload, hold=True):
            return  # not (or no longer) the channel's run
        await channel.push_idle()

    async def _fail(self, session_key: str, run_id: str, code: str, crashed: bool = False) -> None:
        channel = self._hub.channel_for(session_key)
        if channel is not None:
            channel.fail_run(run_id, code, crashed=crashed)
            await channel.push_idle()

    async def _run(self, text, session_key, run_id, previous) -> None:
        if previous is not None and not previous.done():
            await asyncio.wait({previous})
        # The ack (the caller's `send`) registers the run first, as OpenClaw's ack comes before
        # the run's events (a few loop turns: the channel shields its `send`, 2b.3).
        for _ in range(20):
            await asyncio.sleep(0)
            channel = self._hub.channel_for(session_key)
            if channel is None or run_id in channel._runs or run_id in channel._done_runs:
                break
        frame = {"runId": run_id}
        await self._deliver(session_key, "agent", {**frame, "stream": "lifecycle", "data": {"phase": "start"}})
        try:
            stream = self._orchestrator.stream_response(text=text, session_key=session_key)
            if asyncio.iscoroutine(stream):  # a bare AsyncMock: not a stream (as before 2b)
                stream.close()
                raise TypeError("stream_response is not an async iterator")
            async for event in stream:
                if event.type == "token":
                    await self._deliver(session_key, "chat", {**frame, "state": "delta", "deltaText": event.content})
                elif event.type == "tool_call" and event.tool_call is not None:
                    await self._deliver(session_key, "session.tool", {**frame, "data": _tool_data(event.tool_call)})
                elif event.type == "progress" and event.phase is not None:
                    status = _STATUS.get(event.phase, "preparing_workspace")
                    await self._deliver(session_key, "chat", {**frame, "state": "status", "phase": status})
                elif event.type == "error":
                    await self._fail(session_key, run_id, event.content)
                    return
        except asyncio.CancelledError:
            raise
        except OrchestratorError as e:
            await self._fail(session_key, run_id, str(e))
            return
        except Exception:
            await self._fail(session_key, run_id, "internal_error", crashed=True)
            return
        await self._deliver(session_key, "chat", {**frame, "state": "final"})


def _tool_data(tool_call) -> dict:
    data = {"phase": tool_call.phase, "name": tool_call.name, "toolCallId": tool_call.tool_call_id}
    if tool_call.phase == "result":
        result = tool_call.result
        data["result"] = {"content": [{"type": "text", "text": result}] if result is not None else []}
        data["isError"] = tool_call.is_error
    else:
        data["args"] = tool_call.args
    return data


def wire_stream_orchestrator(orchestrator, hub) -> None:
    """Give a mock orchestrator a `send`/`abort_run` that play its `stream_response` into
    `hub`'s channels (see the module docstring). A real client, or a mock already wired, is
    left alone."""
    if not isinstance(orchestrator, NonCallableMock) or vars(orchestrator).get("_stream_runs"):
        return
    runs = _StreamRuns(orchestrator, hub)
    orchestrator._stream_runs = runs
    orchestrator.send = AsyncMock(side_effect=runs.send)
    orchestrator.abort_run = AsyncMock(side_effect=runs.abort_run)
