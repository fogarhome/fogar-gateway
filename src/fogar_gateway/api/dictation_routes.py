"""
dictation_routes.py
~~~~~~~~~~~~~~~~~~~
`/speech/dictation` (5.0 5c, #412): un WebSocket solo de dictado, abierto mientras se graba.
Transcribir no inicia ningún turno ni toca ninguna conversación: el cliente enseña el texto y, si
quiere, lo envía con `POST /conversations/{id}/messages`.

1. Handshake `{"protocol": 5, "token": "…"}` (`conversations:write`; usa el STT, que puede costar
   dinero). Mismos cierres que `/ws/stream`: 4000, 4001, 4002, 4003, 4010, 4011, 4900.
2. Audio en frames binarios (PCM float32 LE mono a 16 kHz, como `/ws/stream`) y
   `{"type": "end"}` al terminar la toma. Llegan `transcription_partial` y `transcription`.
3. Tras la final que sigue a `end`, cierre 1000. Sin audio `IDLE_S`, o pasados `MAX_S`, la toma
   se termina como con `end`. STT `off`, o sin final en `FINAL_S` tras `end`: 4901.

El texto dictado va solo al cliente, nunca a los logs (solo su longitud a DEBUG).
"""

import asyncio
import json
import logging
import time

from fastapi import APIRouter, WebSocket, WebSocketDisconnect

from fogar_gateway.core.config import settings
from fogar_gateway.core.exceptions import ClientInactive, ClientNotFound
from fogar_gateway.models.close_codes import Close, CloseFrame, send_close
from fogar_gateway.models.schemas import CREDENTIAL_MAX_LEN, PROTOCOL_VERSION
from fogar_gateway.services.db_client import db_client
from fogar_gateway.services.reconnection import describe_error
from fogar_gateway.services.scopes import CONVERSATIONS_WRITE
from fogar_gateway.services.speech.ports import SttCallbacks, SttParams

logger = logging.getLogger(__name__)

router = APIRouter()

IDLE_S = 10.0  # sin audio: la toma termina
MAX_S = 120.0  # una toma, como mucho
FINAL_S = 10.0  # tras `end`, lo que se espera a la final


async def _handshake(websocket: WebSocket) -> str | CloseFrame:
    """El token del handshake, o el cierre que toca."""
    try:
        raw = await asyncio.wait_for(websocket.receive_text(), timeout=settings.HANDSHAKE_TIMEOUT_S)
    except TimeoutError:
        return Close.HANDSHAKE_TIMEOUT
    except KeyError:  # un primer frame binario (#232)
        return Close.HANDSHAKE_NOT_TEXT
    if len(raw) > settings.HANDSHAKE_MAX_BYTES:
        return Close.HANDSHAKE_TOO_LARGE
    try:
        data = json.loads(raw)
    except (ValueError, RecursionError):
        return Close.HANDSHAKE_INVALID
    if not isinstance(data, dict):
        return Close.HANDSHAKE_INVALID
    protocol = data.get("protocol")
    if type(protocol) is not int or protocol != PROTOCOL_VERSION:
        return Close.PROTOCOL_UNSUPPORTED
    token = data.get("token")
    if not isinstance(token, str) or not token or len(token) > CREDENTIAL_MAX_LEN:
        return Close.HANDSHAKE_INVALID
    return token


@router.websocket("/speech/dictation")
async def dictation(websocket: WebSocket) -> None:
    await websocket.accept()
    try:
        token = await _handshake(websocket)
    except WebSocketDisconnect:
        return
    if isinstance(token, CloseFrame):
        logger.warning(f"Dictado rechazado en el handshake ({token.code}).")
        await send_close(websocket, token)
        return
    try:
        client, config = await db_client.get_session(token)
    except (ClientNotFound, ClientInactive):
        await send_close(websocket, Close.INVALID_CREDENTIAL)
        return
    except ValueError as e:  # configuración ilegible (C-r7-2)
        logger.error(f"Dictado: configuración del cliente ilegible: {describe_error(e)}")
        await send_close(websocket, Close.SETUP_FAILED)
        return
    if CONVERSATIONS_WRITE not in client.scopes:
        await send_close(websocket, Close.MISSING_SCOPE)
        return
    stt = websocket.scope["app"].state.speech.stt
    if stt.name == "off":
        await send_close(websocket, Close.SPEECH_UNAVAILABLE)
        return

    lock = asyncio.Lock()
    ended = False
    final = asyncio.Event()

    async def send(payload: dict) -> None:
        async with lock:
            try:
                await websocket.send_json(payload)
            except Exception as e:  # noqa: BLE001 — frontera: el cliente puede haberse ido
                logger.debug(f"[{client.id}] dictado: envío fallido ({describe_error(e)})")

    async def on_transcription(text: str, is_final: bool) -> None:
        await send({"type": "transcription" if is_final else "transcription_partial", "text": text})
        if is_final and ended:
            final.set()

    async def on_incomplete(reason: str | None) -> None:
        if ended:
            final.set()  # la final truncada también cierra la toma

    async def ignore(*_args) -> None:
        return None

    session = stt.open_session(
        client=client,
        params=SttParams(language=config.stt_language, vad_threshold=config.stt_vad_thold),
        callbacks=SttCallbacks(
            on_transcription=on_transcription, on_warning=ignore, on_status=ignore,
            on_incomplete=on_incomplete,
        ),
    )
    run: asyncio.Task | None = None
    close: CloseFrame = Close.NORMAL
    try:
        await session.start()
        run = asyncio.create_task(session.run(), name="dictation_stt")
        deadline = time.monotonic() + MAX_S
        while (left := deadline - time.monotonic()) > 0:
            try:
                message = await asyncio.wait_for(websocket.receive(), timeout=min(left, IDLE_S))
            except TimeoutError:
                break  # sin audio: la toma termina
            if message["type"] == "websocket.disconnect":
                return
            if message.get("bytes") is not None:
                await session.send_audio(message["bytes"])
            elif message.get("text") is not None:
                try:
                    kind = json.loads(message["text"]).get("type")
                except (ValueError, AttributeError):
                    kind = None
                if kind == "end":
                    break
        ended = True
        await session.send_end()
        try:
            await asyncio.wait_for(final.wait(), timeout=FINAL_S)
        except TimeoutError:
            close = Close.SPEECH_UNAVAILABLE  # el STT no contestó
    except WebSocketDisconnect:
        return
    except Exception as e:  # noqa: BLE001 — frontera: un fallo del STT cierra la toma con 1011
        logger.error(f"[{client.id}] Dictado: fallo del STT ({describe_error(e)})")
        close = Close.INTERNAL_ERROR
    finally:
        if run is not None:
            run.cancel()
            await asyncio.wait({run})
        await session.close()
    await send_close(websocket, close)
