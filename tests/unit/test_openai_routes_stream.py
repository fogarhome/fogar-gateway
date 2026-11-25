"""/v1/chat/completions: cancelación y estado de sesión (#221).

Se llama al handler directamente para controlar el cierre del stream SSE.
"""

import asyncio
from types import SimpleNamespace
from unittest.mock import MagicMock

from fogar_gateway.api.openai_routes import HACaller, _ChatCompletionRequest, chat_completions
from fogar_gateway.services.channel import ChannelHub
from fogar_gateway.services.protocol import OrchestratorProtocol
from fogar_gateway.services.session_registry import SessionRegistry
from tests.fakes.orchestrator import OrchestratorEvent, wire_stream_orchestrator


def _orchestrator(abort_delay: float = 0.0):
    """A turn that never ends; its `chat.abort` (the cut of /v1's run, 5.0 PR 2b.3) takes
    `abort_delay`."""

    async def stream_response(text, session_key=None):
        yield OrchestratorEvent(type="token", content="hola")
        await asyncio.Event().wait()  # el turno no termina nunca

    orchestrator = MagicMock(spec=OrchestratorProtocol)
    orchestrator.gateway_info = None
    orchestrator.stream_response = stream_response
    hub = ChannelHub(orchestrator=orchestrator)
    wire_stream_orchestrator(orchestrator, hub)
    play_abort = orchestrator.abort_run.side_effect

    async def slow_abort(session_key, run_id):
        await asyncio.sleep(abort_delay)  # un chat.abort lento
        await play_abort(session_key, run_id)

    orchestrator.abort_run.side_effect = slow_abort
    return orchestrator, hub


def _request(abort_delay: float = 0.0) -> tuple[SimpleNamespace, SessionRegistry]:
    orchestrator, hub = _orchestrator(abort_delay)
    registry = SessionRegistry()
    state = SimpleNamespace(openclaw=orchestrator, session_registry=registry, channel_hub=hub)
    return SimpleNamespace(app=SimpleNamespace(state=state)), registry


def _body(stream: bool) -> _ChatCompletionRequest:
    return _ChatCompletionRequest(messages=[{"role": "user", "content": "hola"}], stream=stream)


async def test_cancelled_stream_closes_session_as_error():
    """G-26: un stream cortado (cliente que se va) no es un turno completado."""
    request, registry = _request()
    resp = await chat_completions(request, _body(True), HACaller(None, None))
    gen = resp.body_iterator
    await gen.__anext__()
    await gen.aclose()

    [session] = registry.get_all()
    assert session.status == "error"


async def test_cancelled_non_stream_closes_session_as_error():
    request, registry = _request()
    task = asyncio.create_task(chat_completions(request, _body(False), HACaller(None, None)))
    for _ in range(5):
        await asyncio.sleep(0)
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)

    [session] = registry.get_all()
    assert session.status == "error"


async def test_stream_close_does_not_swallow_caller_cancellation():
    """Si cancelan a quien cierra el stream mientras espera al corte del turno, la cancelación
    sigue."""
    request, _ = _request(abort_delay=0.2)
    resp = await chat_completions(request, _body(True), HACaller(None, None))
    gen = resp.body_iterator
    await gen.__anext__()
    closer = asyncio.create_task(gen.aclose())
    await asyncio.sleep(0.05)
    closer.cancel()
    await asyncio.gather(closer, return_exceptions=True)
    assert closer.cancelled()
    await asyncio.sleep(0.25)  # deja terminar el turno
