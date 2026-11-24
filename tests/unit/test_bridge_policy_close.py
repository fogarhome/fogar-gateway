"""Cierre por política (4200 revocada, 4020 agente retirado; M19): una sola vez por sesión, y
desde que empieza no sale nada más que el turn_end (G-18). Desde la 5.0 (#406) la sesión se
cierra sola: deja el canal de su conversación sin abortar nada, y el turno sigue para las demás
sesiones adjuntas. (El 4102 «conversation opened elsewhere» se retiró.)"""

import asyncio
from unittest.mock import AsyncMock

from fogar_gateway.services.channel import ChannelHub
from fogar_gateway.services.conversations import SessionConversation
from fogar_gateway.services.openclaw.registry import ClientRegistry, TurnRegistry
from fogar_gateway.services.outbound import QueuedSender
from fogar_gateway.services.protocol import OrchestratorEvent
from tests.fakes.bridge import RecWS, blocking, keyed_conversation, sent, start_turn


class GatedWS(RecWS):
    """Un socket cuya escritura espera a `gate`: así lo encolado sigue en la cola."""

    def __init__(self):
        super().__init__()
        self.gate = asyncio.Event()
        self.gate.set()
        self.closed: tuple | None = None

    async def send_json(self, p):
        await self.gate.wait()
        self.sent.append(p)

    async def send_bytes(self, b):
        await self.gate.wait()
        self.sent.append(b)

    async def close(self, code=1000, reason=""):
        self.closed = (code, reason)


async def test_queued_output_is_dropped_once_restricted():
    ws = GatedWS()
    sender = QueuedSender(ws)
    ws.gate.clear()
    await sender.send_bytes(b"\xa1\x00\x01audio")
    await sender.send_json({"type": "token", "turn_id": "t-1", "text": "x"})
    sender.restrict(frozenset({"turn_end"}))
    await sender.send_bytes(b"\xa1\x00\x01more")
    await sender.send_json({"type": "status", "service": "tts", "state": "degraded"})
    await sender.send_json({"type": "turn_end", "turn_id": "t-1", "reason": "closed"})
    ws.gate.set()
    await sender.flush()
    # Lo que aún no había salido tampoco sale: solo el turn_end.
    assert ws.sent == [{"type": "turn_end", "turn_id": "t-1", "reason": "closed"}]
    await sender.aclose(1.0)


async def test_a_revoked_session_sends_only_its_turn_end_then_4200(make_bridge):
    ws = GatedWS()
    bridge, _ = make_bridge(ws)

    async def chatty(*a, **k):
        while True:  # tokens sin parar: lo que siga saliendo después de revocar se vería
            yield OrchestratorEvent(type="token", content="t")
            await asyncio.sleep(0.001)

    bridge.orchestrator.stream_response = chatty
    await start_turn(bridge)
    await asyncio.sleep(0.05)
    restrict = bridge._sender.restrict
    at = {}

    def spy(allowed):
        at["n"] = len(ws.sent)
        restrict(allowed)

    bridge._sender.restrict = spy
    await bridge._revoke_session()
    after = ws.sent[at["n"]:]
    assert [m for m in after if not (isinstance(m, dict) and m["type"] == "turn_end")] == []
    assert after == [{"type": "turn_end", "turn_id": "t-1", "reason": "closed"}]
    assert ws.closed == (4200, "credential revoked")


async def test_policy_close_runs_once_and_later_callers_wait_for_it(make_bridge):
    ws = GatedWS()
    bridge, _ = make_bridge(ws)
    started = asyncio.Event()
    bridge.orchestrator.stream_response = blocking(started)
    await start_turn(bridge)
    await started.wait()

    revoke = asyncio.create_task(bridge._revoke_session())
    await asyncio.sleep(0)
    later = bridge.revoke()  # ya cerrándose: espera a ese, no abre otro
    await revoke
    await asyncio.wait_for(later, 1.0)
    assert later is bridge._policy_close
    assert ws.closed == (4200, "credential revoked")
    assert [m["reason"] for m in ws.of("turn_end")] == ["closed"]
    assert bridge.tracker.record.await_args_list.count((("session_revoked",), {})) == 1


async def test_the_close_goes_on_even_if_recording_fails(make_bridge):
    ws = GatedWS()
    bridge, _ = make_bridge(ws)
    bridge.tracker.record = AsyncMock(side_effect=RuntimeError("tracker roto"))
    await asyncio.wait_for(bridge._revoke_session(), 1.0)
    assert ws.closed == (4200, "credential revoked")


async def test_revoked_session_closes_alone_without_abort(make_bridge):
    """5.0 (#406): dos sesiones en la misma conversación; se revoca la que envió el turno. Ella
    cierra lo suyo (`closed`) y recibe su 4200; la otra sigue viendo el turno hasta su fin, nadie
    recibe dos turn_end y el run no se aborta."""
    hub = ChannelHub(orchestrator=AsyncMock(), turn_registry=TurnRegistry())
    conversation = keyed_conversation("shared", id="conv-s")

    def session(client_id, ws=None):
        return make_bridge(
            ws, client_id=client_id, orchestrator=hub.orchestrator,
            turn_registry=hub.turn_registry, channel_hub=hub, client_registry=ClientRegistry(),
            conversation=SessionConversation(conversation, explicit=True),
        )  # fmt: skip

    tv, tv_ws = session("tv", GatedWS())
    mac, mac_ws = session("mac")
    started, go_on = asyncio.Event(), asyncio.Event()

    async def stream(*a, **k):
        yield OrchestratorEvent(type="token", content="uno ")
        started.set()
        await go_on.wait()
        yield OrchestratorEvent(type="token", content="dos")

    hub.orchestrator.stream_response = stream
    task = await start_turn(tv, "hola")
    await asyncio.wait_for(started.wait(), 2)

    await asyncio.wait_for(tv._revoke_session(), 2)
    assert tv_ws.closed == (4200, "credential revoked")
    assert [(m["type"], m.get("reason")) for m in tv_ws.sent if m["type"].startswith("turn_")] == [
        ("turn_start", None), ("turn_end", "closed"),
    ]
    assert not task.done(), "the revoked sender cancelled the channel's turn"
    hub.orchestrator.abort.assert_not_awaited()

    go_on.set()
    await asyncio.wait_for(task, 2)
    await sent(mac)
    assert [(m["type"], m.get("reason")) for m in mac_ws.of("turn_start") + mac_ws.of("turn_end")] == [
        ("turn_start", None), ("turn_end", "done"),
    ]
    assert [m["text"] for m in mac_ws.of("token")] == ["uno ", "dos"]
    hub.orchestrator.abort.assert_not_awaited()
    assert mac.channel.subscribers() == [mac.turns]
    await mac.close_all()
    await hub.aclose(1.0)
