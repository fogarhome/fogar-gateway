from unittest.mock import AsyncMock

from fogar_gateway.services.openclaw.registry import ClientRegistry, TurnRegistry


def test_client_registry_register_get():
    reg = ClientRegistry()
    bridge = object()
    reg.register("hab_sito", bridge)
    assert reg.get("hab_sito") is bridge


def test_client_registry_unregister():
    reg = ClientRegistry()
    bridge = object()
    reg.register("hab_sito", bridge)
    reg.unregister("hab_sito", bridge)
    assert reg.get("hab_sito") is None


def test_client_registry_unregister_stale_bridge_keeps_newer():
    """Regression for #113: a reconnect race registers a new bridge for the
    same client_id (overwriting the old one). When the OLD bridge later closes
    and calls unregister(), it must not evict the NEW bridge that now owns that
    client_id — otherwise the live session stops receiving push events and
    status broadcasts."""
    reg = ClientRegistry()
    bridge_a = object()
    bridge_b = object()
    reg.register("client-x", bridge_a)
    reg.register("client-x", bridge_b)  # new session overwrites the old one

    # Old bridge (A) tears down late and unregisters itself.
    reg.unregister("client-x", bridge_a)

    # The new bridge (B) must survive.
    assert reg.get("client-x") is bridge_b


def test_client_registry_unregister_is_noop_for_any_superseded_owner():
    """After multiple reconnects (A→B→C), a late unregister() from ANY former
    owner (A or B) must be a no-op — only the current owner (C) can evict."""
    reg = ClientRegistry()
    bridge_a, bridge_b, bridge_c = object(), object(), object()
    reg.register("client-x", bridge_a)
    reg.register("client-x", bridge_b)
    reg.register("client-x", bridge_c)

    reg.unregister("client-x", bridge_a)  # oldest, long gone
    reg.unregister("client-x", bridge_b)  # superseded too
    assert reg.get("client-x") is bridge_c

    reg.unregister("client-x", bridge_c)  # current owner tears down
    assert reg.get("client-x") is None


def test_client_registry_missing_returns_none():
    reg = ClientRegistry()
    assert reg.get("nonexistent") is None


async def test_broadcast_status_delivers_to_all_registered_bridges():
    reg = ClientRegistry()
    bridge_a = AsyncMock()
    bridge_b = AsyncMock()
    reg.register("a", bridge_a)
    reg.register("b", bridge_b)

    await reg.broadcast_status("orchestrator", "restored")

    bridge_a.notify_service_status.assert_awaited_once_with("orchestrator", "restored")
    bridge_b.notify_service_status.assert_awaited_once_with("orchestrator", "restored")


async def test_broadcast_status_one_failure_does_not_block_others():
    reg = ClientRegistry()
    bridge_a = AsyncMock()
    bridge_a.notify_service_status.side_effect = RuntimeError("disconnected")
    bridge_b = AsyncMock()
    reg.register("a", bridge_a)
    reg.register("b", bridge_b)

    await reg.broadcast_status("orchestrator", "unavailable")  # must not raise

    bridge_b.notify_service_status.assert_awaited_once_with("orchestrator", "unavailable")


def test_an_owed_abort_is_taken_once_and_settled_by_an_abort_answer():
    turns = TurnRegistry()
    turns.owe_abort("sk", "r1")
    turns.owe_abort("sk", "r2")
    assert turns.take_owed_abort("r1") == "sk"
    assert turns.take_owed_abort("r1") is None
    turns.note_abort_result({"ok": True, "aborted": True, "runIds": ["r2"]})
    assert turns.take_owed_abort("r2") is None


def test_an_ended_push_is_remembered_until_a_new_start():
    turns = TurnRegistry()
    turns.end_push("r1")
    assert turns.push_ended("r1") and not turns.push_ended("r2") and not turns.push_ended(None)
    turns.reopen_push("r1")
    assert not turns.push_ended("r1")
