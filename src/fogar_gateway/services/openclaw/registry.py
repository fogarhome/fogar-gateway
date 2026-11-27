import logging
import time
from typing import Any

from fogar_gateway.services.conversations import Conversation, ConversationIndex
from fogar_gateway.services.reconnection import describe_error

logger = logging.getLogger(__name__)


# How long, after the gateway aborts a run (a cancel, an interrupting `send`, a withdrawn
# message, a timeout, a cut push, a /v1 client that left), frames carrying that `runId` are
# dropped (#285). A runId is never reused, so a long tombstone drops nothing else: it covers
# frames already in flight when chat.abort went out and a run that keeps streaming for a
# while after it.
RUN_ABORT_TOMBSTONE_S = 300.0
# The same memory for a run a conversation channel sent that already ended there (final 2b
# review, m5): a late frame of it (a `run_status`, a stray delta) is dropped before routing,
# so it never opens a push, even after its channel was dropped (`ChannelHub.drop_if_idle`).
RUN_FINISHED_MEMORY_S = RUN_ABORT_TOMBSTONE_S


class TurnRegistry:
    """The tombstones of the runs the gateway aborted (#285), shared by the dispatcher (which
    drops their frames before any routing), the conversation channels (which tombstone a run
    before its `chat.abort`) and the OpenClaw client (the `runIds` a `chat.abort` answer names).

    Since 5.0 PR 2b.3 nothing registers a turn per `sessionKey` here: every run, /v1's too,
    belongs to its conversation's channel (services/channel.py)."""

    def __init__(self) -> None:
        # runId -> deadline; see RUN_ABORT_TOMBSTONE_S.
        self._aborted_runs: dict[str, float] = {}
        # runId -> deadline; see RUN_FINISHED_MEMORY_S.
        self._finished_runs: dict[str, float] = {}
        # Pushes cerrados por su lifecycle `end`/`error` (runId -> deadline): un evento tardío
        # de su run no abre otro push, tampoco en un canal nuevo de la clave tras
        # `ChannelHub.drop_if_idle`; solo un lifecycle `start` nuevo lo reabre (#84).
        self._ended_pushes: dict[str, float] = {}
        # Aborts debidos (runId -> (sessionKey, deadline)): un `chat.abort` enviado sin conexión
        # o tras reconectar puede no haber llegado a nada (OpenClaw restaura su cola ~13 s
        # después de aceptar conexiones). El primer frame de ese run lo aborta otra vez.
        self._owed_aborts: dict[str, tuple[str, float]] = {}

    def abort_run(self, run_id: str) -> None:
        _prune(self._aborted_runs)
        self._aborted_runs[run_id] = time.monotonic() + RUN_ABORT_TOMBSTONE_S

    def finish_run(self, run_id: str) -> None:
        """A channel's own run ended there: its late frames are not a push (m5)."""
        _prune(self._finished_runs)
        self._finished_runs[run_id] = time.monotonic() + RUN_FINISHED_MEMORY_S

    def is_run_finished(self, run_id: object) -> bool:
        """True while frames of this run (a channel's own, already ended) must be dropped."""
        return isinstance(run_id, str) and _alive(self._finished_runs, run_id)

    def end_push(self, run_id: str) -> None:
        """A push closed by its lifecycle `end`/`error`: its late frames open nothing (#84)."""
        _prune(self._ended_pushes)
        self._ended_pushes[run_id] = time.monotonic() + RUN_FINISHED_MEMORY_S

    def push_ended(self, run_id: object) -> bool:
        return isinstance(run_id, str) and _alive(self._ended_pushes, run_id)

    def reopen_push(self, run_id: str) -> None:
        """A new lifecycle `start` of the run: it may open a push again (#84)."""
        self._ended_pushes.pop(run_id, None)

    def owe_abort(self, session_key: str, run_id: str) -> None:
        """The `chat.abort` of a tombstoned run may not have reached OpenClaw: abort it again
        when one of its frames shows up (`take_owed_abort`)."""
        self._owed_aborts = {
            r: v for r, v in self._owed_aborts.items() if v[1] > time.monotonic()
        }
        self._owed_aborts[run_id] = (session_key, time.monotonic() + RUN_ABORT_TOMBSTONE_S)
        # Su lápida pudo vencer durante una caída larga: sin ella su frame no llegaría aquí.
        self.abort_run(run_id)

    def take_owed_abort(self, run_id: object) -> str | None:
        """The session key of an owed abort of this run (once), or None."""
        if not isinstance(run_id, str):
            return None
        owed = self._owed_aborts.pop(run_id, None)
        return owed[0] if owed is not None and owed[1] > time.monotonic() else None

    def note_abort_result(self, payload: object) -> None:
        """A `chat.abort` answer (`{ok, aborted, runIds}`) names the runs it aborted."""
        if not isinstance(payload, dict) or payload.get("aborted") is not True:
            return
        run_ids = payload.get("runIds")
        if isinstance(run_ids, list):
            for run_id in run_ids:
                if isinstance(run_id, str):
                    self.abort_run(run_id)
                    self._owed_aborts.pop(run_id, None)  # ya no se debe

    def is_run_aborted(self, run_id: object) -> bool:
        """True while frames of this run (aborted by the gateway) must be dropped."""
        return isinstance(run_id, str) and _alive(self._aborted_runs, run_id)


def _alive(deadlines: dict[str, float], key: str) -> bool:
    deadline = deadlines.get(key)
    if deadline is None:
        return False
    if deadline <= time.monotonic():
        del deadlines[key]
        return False
    return True


def _prune(deadlines: dict[str, float]) -> None:
    now = time.monotonic()
    for key in [k for k, deadline in deadlines.items() if deadline <= now]:
        del deadlines[key]


class ClientRegistry:
    """Sesiones vivas de cada cliente (#113): los avisos de estado (`broadcast_status`) y la
    revocación (`for_client`). Desde la 5.0 (#406) una conversación puede estar abierta en
    varias sesiones a la vez: cada una se adjunta al canal de su conversación
    (services/channel.py), que reparte sus turnos y sus push; aquí no hay nada por
    conversación (el 4102 «conversation opened elsewhere» se retiró).

    Las identidades se comparan con `is` (#113): el cierre tardío de una sesión vieja nunca
    quita a la nueva.
    """

    def __init__(self, index: ConversationIndex | None = None) -> None:
        self.index = index if index is not None else ConversationIndex()
        self._clients: dict[str, list[Any]] = {}  # client_id -> sesiones vivas, la última al final

    def register(self, client_id: str, bridge: Any, conversation: Conversation | None = None) -> None:
        """Registra la sesión; con `conversation`, la indexa (el push de su `sessionKey` llega
        a su canal)."""
        live = self._clients.setdefault(client_id, [])
        if not any(b is bridge for b in live):
            live.append(bridge)
        if conversation is not None:
            self.index.add(conversation)

    def unregister(self, client_id: str, expected_bridge: Any) -> None:
        live = self._clients.get(client_id)
        if live is not None:
            live[:] = [b for b in live if b is not expected_bridge]
            if not live:
                del self._clients[client_id]

    def get(self, client_id: str) -> Any | None:
        """La sesión viva más reciente del cliente."""
        live = self._clients.get(client_id)
        return live[-1] if live else None

    def for_client(self, client_id: str) -> list[Any]:
        """Las sesiones vivas de un cliente (una copia)."""
        return list(self._clients.get(client_id, ()))

    def bridges(self) -> list[Any]:
        return [b for live in self._clients.values() for b in live]

    async def broadcast_status(self, service: str, state: str) -> None:
        for bridge in self.bridges():
            try:
                await bridge.notify_service_status(service, state)
            except Exception as e:  # noqa: BLE001 — frontera: una sesión rota no bloquea al resto
                logger.debug("broadcast_status: notify failed (%s)", describe_error(e))
