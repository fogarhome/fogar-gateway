import asyncio
import logging
from collections import deque

from fogar_gateway.services.reconnection import (
    ConnectionState,
    ServiceHealth,
    ServiceStatus,
    describe_error,
    health_attr,
    reconnect_with_backoff,
    traceback_no_message,
)
from fogar_gateway.services.speech.ports import SttCallbacks, SttParams
from fogar_gateway.services.teardown import Teardown
from fogar_gateway.services.transcriber_client import TranscriberClient

logger = logging.getLogger(__name__)

# Warning code sent to the client when a reconnect cut the phrase in flight (E-r3-1).
PHRASE_INTERRUPTED = "phrase_interrupted"
# Audio held between our `end` and the next `ready` (~5 s of 20 ms frames): covers a final
# slowed by a saturated GPU (3 s) plus the reopen.
HELD_MAX = 250


class ReconnectingTranscriberClient:
    """Wraps TranscriberClient with exponential-backoff reconnection.

    One instance per audio session: it is the fogar `SttSession` (built by
    `FogarSttProvider.open_session()`). Delegates send_audio/send_end/close to the inner
    client unchanged, so bridge.py's call sites don't need to know reconnection exists.
    """

    state = health_attr("state")
    on_state_change = health_attr("on_state_change")
    _last_error = health_attr("last_error")

    def __init__(
        self,
        url: str,
        client_id: str,
        callbacks: SttCallbacks,
        params: SttParams,
        token: str = "",
        initial_backoff: float = 1.0,
        max_backoff: float = 60.0,
        max_duration: float = 300.0,
    ) -> None:
        self._client = TranscriberClient(url=url, client_id=client_id)
        self._client_id = client_id
        self._callbacks = callbacks
        self._initial_backoff = initial_backoff
        self._max_backoff = max_backoff
        self._max_duration = max_duration
        self._health = ServiceHealth("transcriber", ConnectionState.DEGRADED)
        self._closed = False
        self._run_task: asyncio.Task | None = None
        self._language = params.language
        self._token = token
        self._vad_thold = params.vad_threshold
        # E-r3-1 review: a drop mid-phrase loses its head; its tail must not pass as an order.
        self._phrase_open = False  # audio sent since the last final
        self._lost = False  # this phrase lost audio: its final (and partials) are withheld
        # fogar-transcriber stops reading a socket at our `end` (handleEnd, then close 1000): audio
        # from then on is the next phrase and waits here for the new connection (E-r4-1, #358).
        # None stands for an `end` that arrived while the held frames were being replayed.
        self._held: deque[bytes | None] = deque(maxlen=HELD_MAX)
        self._held_cut = False  # the oldest held frames were dropped

    @property
    def last_transcript_at(self) -> float | None:
        return self._client._last_transcription_at

    def _set_state(self, state: ConnectionState) -> None:
        self._health.set_state(state)

    async def start(self) -> None:
        """Attempts the initial connection. Never raises: on failure this
        just leaves state=RECONNECTING for run() to pick up in the background."""
        if self._closed:
            return
        try:
            await self._client.connect(
                language=self._language, token=self._token, vad_thold=self._vad_thold
            )
            self._health.mark_connected()
        except Exception as e:  # noqa: BLE001 — frontera: start() nunca lanza (contrato)
            self._last_error = describe_error(e)
            logger.error(
                "[%s] transcriber initial connect failed: %s", self._client_id, describe_error(e)
            )
            self._set_state(ConnectionState.RECONNECTING)
        if self._closed:
            # close() llegó mientras connect() estaba en vuelo: el socket que acaba de
            # abrirse no tiene dueño, se cierra aquí (close() es idempotente).
            await self._client.close()

    async def send_audio(self, audio_bytes: bytes) -> None:
        if self._client._end_sent or self._held:  # the latter keeps order while replaying
            self._hold(audio_bytes)
            return
        await self._send_now(audio_bytes)

    async def send_end(self) -> None:
        if self._client._end_sent:  # before the new `ready`: a whole phrase in the gap is not kept
            if self._held or self._held_cut:  # an `end` with no audio does nothing
                self._held.clear()
                self._held_cut = False
                await self._phrase_lost()
            return
        if self._held:  # `ready` came, replay under way: the `end` goes after its frames
            self._hold(None)
            return
        await self._end_now()

    def _hold(self, item: bytes | None) -> None:
        self._held_cut |= len(self._held) == HELD_MAX
        self._held.append(item)

    async def _send_now(self, audio_bytes: bytes) -> None:
        if self.state != ConnectionState.DEGRADED:  # E-r4-3: `unavailable` already told them
            self._phrase_open = True
        await self._client.send_audio(audio_bytes)
        if self._phrase_open and not self._client._is_ready:  # the inner client dropped it
            self._lost = True

    async def _end_now(self) -> None:
        await self._client.send_end()
        if self._phrase_open and not self._client._is_ready:
            await self._phrase_lost()  # this end was dropped: no final will ever come

    async def _replay_held(self) -> None:
        if self._held_cut:
            self._phrase_open = self._lost = True
        self._held_cut = False
        while self._held and not self._client._end_sent:  # a replayed `end` stops it
            if self._held[0] is None:
                self._held.popleft()
                await self._end_now()
            else:  # popped after sending: frames arriving meanwhile queue behind it
                await self._send_now(self._held[0])
                self._held.popleft()

    async def _phrase_lost(self) -> None:
        """The phrase in flight when the connection dropped is gone: tell the client to
        repeat it (a fixed code, never the text) instead of passing on a truncated final."""
        self._phrase_open = self._lost = False
        if self.state == ConnectionState.DEGRADED:  # E-r4-3: the client already has `unavailable`
            return
        logger.warning("[%s] transcriber: phrase cut by a reconnect, final withheld",
                       self._client_id)
        await self._callbacks.on_incomplete(PHRASE_INTERRUPTED)
        await self._callbacks.on_warning(PHRASE_INTERRUPTED, None)

    async def _on_transcription(self, text: str, is_final: bool) -> None:
        if self._lost:
            if is_final:
                await self._phrase_lost()
            return  # partials of a cut phrase could trigger a barge-in
        if is_final:
            self._phrase_open = False
        await self._callbacks.on_transcription(text, is_final)

    async def _on_incomplete(self, reason: str | None) -> None:
        if not self._lost:  # a withheld final reports phrase_interrupted instead: one per phrase
            await self._callbacks.on_incomplete(reason)

    async def close(self) -> None:
        self._closed = True
        # G-52 (#221): Teardown termina el cierre aunque cancelen a quien llama y relanza
        # esa cancelación al final. El listen loop suelta enseguida al cancelarse (2 s acota
        # uno colgado); el cierre del socket tiene 12 s, por encima del close_timeout de
        # websockets (10 s): un ws.close() cancelado deja de esperar sin cerrar el transporte.
        async with Teardown(f"[{self._client_id}] transcriber close") as td:
            await td.cancel(self._run_task, "bucle de escucha", timeout=2.0)
            await td.step(self._client.close(), "cierre del socket", timeout=12.0)

    async def run(self) -> None:
        """Supervises the listen loop for the session's lifetime: listens,
        and on an unexpected drop, reconnects with backoff and resumes
        listening — until close() is called or reconnection is exhausted
        (DEGRADED, in which case this returns and the session continues
        without a working transcriber for the rest of its lifetime)."""
        self._run_task = asyncio.current_task()
        while not self._closed:
            if self.state != ConnectionState.CONNECTED:
                recovered = await self._reconnect_loop()
                if not recovered:
                    return
                await self._replay_held()

            self._client._dropped_unexpectedly = False
            try:
                cb = self._callbacks
                await self._client.listen_loop(
                    self._on_transcription, cb.on_warning, cb.on_status, self._on_incomplete
                )
            except asyncio.CancelledError:
                raise
            except Exception as e:  # noqa: BLE001 — frontera: el fallo degrada, no cierra la sesión
                # Unexpected failure (e.g. a bug in the transcription callback), not a
                # connection drop: don't reconnect, don't close the session — degrade
                # so the client gets `status unavailable` via on_state_change (#131).
                self._last_error = describe_error(e)
                logger.error(
                    "[%s] transcriber listen loop crashed (%s) — DEGRADED.",
                    self._client_id,
                    self._last_error,
                )
                logger.debug("[%s] listen loop: %s", self._client_id, traceback_no_message(e))
                self._set_state(ConnectionState.DEGRADED)
                return

            if self._closed:
                return
            if not self._client._dropped_unexpectedly:
                # Our own close(): not a failure, don't reconnect. Any server-side
                # close, 1000/1001 included, sets _dropped_unexpectedly.
                return
            if self._client.closed_after_end:
                # Fin de frase: la cierra. El audio tras nuestro `end` está retenido, así que
                # _lost solo viene de un corte de esta frase; su final vacía no lo avisó (E-r4-2).
                if self._lost:
                    await self._phrase_lost()
                self._phrase_open = self._lost = False
                # Fin de frase (1000 tras nuestro `end`): se reabre sin avisar al cliente; si no
                # se puede, sí es una caída y sigue el camino normal con su backoff.
                try:
                    await self._client.connect(
                        language=self._language, token=self._token, vad_thold=self._vad_thold
                    )
                    self._health.mark_connected()  # mide la vida de esta conexión (E-r2-1)
                    # The server answered fine: a later early drop is a lone one, not a flap
                    # in a row (E-r3-1).
                    self._health.flap_backoff = 0.0
                    await self._replay_held()
                    continue
                except asyncio.CancelledError:
                    raise
                except Exception as e:  # noqa: BLE001 — frontera: el fallo pasa a reconexión con backoff
                    logger.info("[%s] transcriber: reabrir tras la frase falló (%s)",
                                self._client_id, describe_error(e))
            if self._phrase_open:
                if self._client._end_sent:  # cut after our end, before the final: all lost
                    await self._phrase_lost()
                else:  # the rest of the phrase goes to the new connection, without its head
                    self._lost = True
            self._set_state(ConnectionState.RECONNECTING)

    async def _reconnect_loop(self) -> bool:
        async def attempt() -> None:
            await self._client.connect(
                language=self._language, token=self._token, vad_thold=self._vad_thold
            )

        return await reconnect_with_backoff(
            attempt,
            self._health,
            initial_backoff=self._initial_backoff,
            max_backoff=self._max_backoff,
            max_duration=self._max_duration,
            label=f"{self._client_id} transcriber",
        )

    def status(self) -> ServiceStatus:
        return self._health.status()
