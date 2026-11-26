"""
events_routes.py
~~~~~~~~~~~~~~~~
`GET /events` (5.0, #412): el stream SSE de un aparato (`conversations:read`), con todo lo que
pasa en las conversaciones que puede ver. Ver `services/events.py`.

- El primer evento es `ready`. Regla del cliente: espera `ready` antes de su primer `POST`.
- Cada evento lleva un `id:` que siempre sube; reconectar con `Last-Event-ID` reenvía lo perdido
  si sigue en el búfer del aparato y, si no, manda `resync` (el cliente vuelve a pedir el estado
  de lo que tenga abierto: `GET /conversations/{id}`).
- `?audio=1` (5c): los avisos de las conversaciones de las que este aparato es dispositivo de
  avisos suenan aquí (eventos `audio`, sin id: no se reenvían al reanudar).
- Cada stream dura como mucho `STREAM_MAX_S` (`retry: 1000`): el navegador reconecta solo y no
  pierde nada. Cortar la conexión nunca para un turno.
"""

import asyncio
import time

from fastapi import APIRouter, Depends, Header, HTTPException, Query, Request, status
from fastapi.responses import StreamingResponse

from fogar_gateway.api.deps import Caller, Principal, require_scope
from fogar_gateway.core.exceptions import ClientInactive, ClientNotFound
from fogar_gateway.models.schemas import PROTOCOL_VERSION
from fogar_gateway.services import events as ev
from fogar_gateway.services.db_client import db_client
from fogar_gateway.services.scopes import CONVERSATIONS_READ

router = APIRouter()


def _last_id(value: str | None) -> int | None:
    if value is None:
        return None
    try:
        return int(value.strip())
    except ValueError:
        return -1  # basura: no se puede reanudar (resync)


@router.get("/events")
async def get_events(
    request: Request,
    principal: Principal = Depends(require_scope(CONVERSATIONS_READ)),
    last_event_id: str | None = Header(default=None),
    audio: bool = Query(default=False),
) -> StreamingResponse:
    if not isinstance(principal, Caller):
        # Un stream es de un aparato (sus conversaciones visibles): la máquina no tiene ninguno.
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="device_required")
    try:
        client, config = await db_client.revalidate(principal.device.key_hash)
    except (ClientNotFound, ClientInactive):  # retirado entre medias
        raise HTTPException(status_code=401, detail="not_authenticated") from None
    hub: ev.EventHub = request.app.state.events
    resume = _last_id(last_event_id)
    ready = {"type": "ready", "protocol": PROTOCOL_VERSION, "device_id": client.id}

    async def stream():
        # Dentro del generador: si el cliente se va antes de que empiece, no queda un oyente
        # colgado (su `finally` no correría). Sin await hasta registrarlo: nada se pierde entre
        # la reanudación y la cola.
        device = hub.open(client, config, audio=audio)
        replay = device.replay(resume) if resume is not None else None
        listener = device.listen()
        head = ["retry: 1000\n\n"]
        if resume is not None and replay is not None:
            head.append(ev.sse(ready))  # sin id: el cliente ya sabe desde dónde va
            head.extend(replay)
        else:
            now = ev.current_id()
            head.append(ev.sse(ready, now))
            if resume is not None:
                head.append(ev.sse({"type": "resync"}, now))
        try:
            for line in head:
                yield line
            deadline = time.monotonic() + ev.STREAM_MAX_S
            while (left := deadline - time.monotonic()) > 0:
                try:
                    item = await asyncio.wait_for(
                        listener.queue.get(), timeout=min(left, ev.KEEPALIVE_S)
                    )
                except TimeoutError:
                    yield ": ping\n\n"
                    continue
                if item is None:  # cola desbordada o aparato cerrado: reconecta y reanuda
                    return
                yield item[1]
        finally:
            device.unlisten(listener)

    return StreamingResponse(
        stream(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )
