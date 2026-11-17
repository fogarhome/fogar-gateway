"""HTTP request body cap, enforced before any route parses the body (auditoría 3.0, D-3)."""

from starlette.responses import JSONResponse
from starlette.types import ASGIApp, Message, Receive, Scope, Send

# No route takes more than a few KiB; 1 MiB matches the WebSocket message cap of `serve`.
MAX_BODY_BYTES = 1024 * 1024


class BodySizeLimitMiddleware:
    """Reads the whole HTTP body first, counting the bytes actually received (Content-Length
    may be absent or lie, e.g. chunked), and answers 413 without calling the app if it is over
    MAX_BODY_BYTES. WebSocket and lifespan scopes pass through untouched."""

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        chunks: list[bytes] = []
        size = 0
        too_large = _declared_length(scope) > MAX_BODY_BYTES
        while not too_large:
            message = await receive()
            if message["type"] != "http.request":  # http.disconnect: nobody to answer
                return
            body = message.get("body", b"")
            size += len(body)
            too_large = size > MAX_BODY_BYTES
            chunks.append(body)
            if not message.get("more_body", False):
                break
        if too_large:
            response = JSONResponse({"detail": "request body too large"}, status_code=413)
            await response(scope, receive, send)
            return

        replayed = False

        async def replay() -> Message:
            nonlocal replayed
            if not replayed:
                replayed = True
                return {"type": "http.request", "body": b"".join(chunks), "more_body": False}
            return await receive()  # http.disconnect for streaming responses

        await self.app(scope, replay, send)


def _declared_length(scope: Scope) -> int:
    for name, value in scope.get("headers", ()):
        if name == b"content-length":
            try:
                return int(value)
            except ValueError:
                return 0
    return 0
