from typing import Protocol, runtime_checkable


@runtime_checkable
class OrchestratorProtocol(Protocol):
    """What the gateway needs from the orchestrator (OpenClaw). Since 5.0 PR 2b.3 every turn,
    WebSocket or /v1, goes through its conversation's channel (services/channel.py): `send`
    and `abort_run` by `runId`; the reply comes as frames through the dispatcher. Nothing
    aborts a whole `sessionKey`."""

    async def connect(self) -> None: ...
    async def close(self) -> None: ...
    async def ping(self) -> bool: ...

    async def send(self, text: str, session_key: str, *, queue_mode: str | None = None) -> str:
        """`chat.send` without waiting for the reply: the ack's `runId`. Raises
        `OrchestratorError` with a stable code (services/orchestration.py) if it fails."""
        ...

    async def abort_run(self, session_key: str, run_id: str) -> None:
        """`chat.abort` of that run only (a queued one too). Never raises an Exception."""
        ...

    async def request(self, method: str, params: dict, timeout: float = ...) -> dict: ...
