"""Fuzz permanente del borde de frase del transcriber `fogar` (auditoría 3.0 ronda 5).

Un servidor websockets real por semilla imita a fogar-transcriber: tras nuestro `end` deja de
leer, espera la inferencia, manda la final con los ids de las tramas que leyó y cierra 1000.
Mete cortes 1011/1001 en tramas al azar, 1011 entre el `end` y la final y rechazos antes de
`ready`; el cliente habla frases con huecos de 0–1 s, `end` sueltos, tramas grandes y, en
algunas semillas, el servidor deja de aceptar (DEGRADED). Todas las semillas van a la vez.

Invariantes: ninguna final truncada, fusionada ni duplicada; finales + `phrase_interrupted`
= frases habladas (≤ si degrada, y ningún aviso ya degradado); warning e incomplete a la par;
bytes retenidos ≤ HELD_MAX_BYTES.
"""

import asyncio
import json
import random

import websockets

from fogar_gateway.services.reconnection import ConnectionState
from fogar_gateway.services.speech.ports import SttCallbacks, SttParams
from fogar_gateway.services.transcriber_reconnecting import (
    HELD_MAX_BYTES,
    ReconnectingTranscriberClient,
)

SEEDS = range(48)


class _Server:
    """fogar-transcriber en pequeño (StreamingSession: handleConfig, audio, handleEnd)."""

    def __init__(self, rng: random.Random) -> None:
        self.rng = rng
        self.refuse_all = False

    async def handler(self, ws) -> None:
        r = self.rng
        await ws.recv()  # config
        if self.refuse_all or r.random() < 0.1:
            await ws.close(1011)
            return
        await ws.send(json.dumps({"type": "ready", "session_id": "s"}))
        cut_at = r.randrange(1, 25) if r.random() < 0.3 else None
        cut_code = r.choice([1011, 1001])
        ids: list[str] = []
        try:
            async for msg in ws:
                if isinstance(msg, bytes):
                    if cut_at is not None and len(ids) + 1 >= cut_at:
                        await ws.close(cut_code)
                        return
                    ids.append(msg.split(b"|")[0].decode())
                elif json.loads(msg).get("type") == "end":
                    break  # handleEnd: deja de leer
            else:
                return  # lo cerró el cliente
            await asyncio.sleep(r.uniform(0.02, 0.3))  # inferencia
            if r.random() < 0.15:
                await ws.close(1011)
                return
            await ws.send(json.dumps({"type": "transcription", "text": " ".join(ids), "is_final": True}))
            await ws.close(1000)
        except websockets.ConnectionClosed:
            pass


async def _run_seed(seed: int) -> list[str]:
    rng = random.Random(seed)
    server = _Server(random.Random(seed + 10_000))
    finals: list[str] = []
    warnings: list[ConnectionState] = []
    incompletes: list[str | None] = []
    errors: list[str] = []
    w: ReconnectingTranscriberClient

    async def on_transcription(text: str, is_final: bool) -> None:
        if is_final:
            finals.append(text)

    async def on_warning(code: str, _msg: str | None) -> None:
        if code == "phrase_interrupted":
            warnings.append(w.state)

    async def on_incomplete(reason: str | None) -> None:
        incompletes.append(reason)

    async def on_status(_s: str, _r: str | None) -> None:
        pass

    def check_held() -> None:
        held = sum(len(x) for x in w._held if isinstance(x, bytes))
        if held > HELD_MAX_BYTES:
            errors.append(f"held {held} B > {HELD_MAX_BYTES}")
        if w._held_bytes != held:  # a drifting counter drops audio early
            errors.append(f"_held_bytes {w._held_bytes} != {held}")

    async with websockets.serve(server.handler, "127.0.0.1", 0) as srv:
        w = ReconnectingTranscriberClient(
            url=f"127.0.0.1:{srv.sockets[0].getsockname()[1]}", client_id=f"fuzz{seed}",
            callbacks=SttCallbacks(on_transcription=on_transcription, on_warning=on_warning,
                                   on_status=on_status, on_incomplete=on_incomplete),
            params=SttParams(language="es", vad_threshold=0.0),
            initial_backoff=0.01, max_backoff=0.05, max_duration=1.0,
        )
        await w.start()
        run = asyncio.create_task(w.run())
        degrade = rng.random() < 0.15
        big = rng.random() < 0.3
        phrases: list[list[str]] = []
        n = rng.randint(4, 7)
        try:
            for p in range(n):
                if degrade and p == n // 2:
                    server.refuse_all = True
                if rng.random() < 0.15:
                    await w.send_end()  # `end` suelto, sin audio
                ids = [f"{p}.{j}" for j in range(rng.randint(1, 10))]
                for frame_id in ids:
                    size = rng.choice([64_000, 120_000]) if big else 640
                    await w.send_audio(f"{frame_id}|".encode().ljust(size))
                    check_held()
                    await asyncio.sleep(0.005)
                await w.send_end()
                check_held()
                phrases.append(ids)
                await asyncio.sleep(rng.random() ** 2)  # huecos de 0 a 1 s, más a menudo cortos
            try:
                async with asyncio.timeout(5.0):
                    while len(finals) + len(warnings) < n and w.state != ConnectionState.DEGRADED:
                        await asyncio.sleep(0.02)
            except TimeoutError:
                errors.append("phrase lost without a warning (timeout)")
            await asyncio.sleep(0.4)  # a late duplicate would show up here
        finally:
            await w.close()
            await asyncio.wait([run], timeout=2.0)

    owner = {i: k for k, ids in enumerate(phrases) for i in ids}
    delivered: set[int] = set()
    for text in finals:
        got = text.split()
        ks = {owner.get(i, -1) for i in got}
        if len(ks) != 1 or -1 in ks:
            errors.append(f"merged/foreign final {text!r}")
            continue
        (k,) = ks
        if got != phrases[k]:
            errors.append(f"truncated final of phrase {k}: {text!r}")
        if k in delivered:
            errors.append(f"duplicated final of phrase {k}")
        delivered.add(k)
    if len(warnings) != len(incompletes):
        errors.append(f"{len(warnings)} warnings vs {len(incompletes)} incompletes")
    total = len(finals) + len(warnings)
    if w.state == ConnectionState.DEGRADED:
        if total > n:
            errors.append(f"{total} outcomes > {n} phrases")
        if ConnectionState.DEGRADED in warnings:
            errors.append("warning while DEGRADED")
        if w._held:
            errors.append("audio still held while DEGRADED")
    elif total != n:
        errors.append(f"{len(finals)} finals + {len(warnings)} warnings != {n} phrases")
    return [f"seed {seed}: {e}" for e in errors]


async def test_phrase_boundary_fuzz():
    results = await asyncio.gather(*(_run_seed(s) for s in SEEDS))
    failures = [e for r in results for e in r]
    assert not failures, "\n".join(failures)
