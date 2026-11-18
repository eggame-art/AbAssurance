import asyncio
import math
import time
from threading import Lock

from starlette.responses import JSONResponse
from starlette.types import ASGIApp, Receive, Scope, Send


class RequestLimits:
    def __init__(self, max_keys: int = 4096):
        self.windows: dict[str, tuple[float, int]] = {}
        self.max_keys = max_keys
        self.lock = Lock()

    def retry_after(self, key: str, limit: int, window: int = 60) -> int:
        now = time.monotonic()

        with self.lock:
            expires, count = self.windows.get(key, (now + window, 0))

            if expires <= now:
                expires, count = now + window, 0

            if key not in self.windows and len(self.windows) >= self.max_keys:
                self.windows = {key: value for key, value in self.windows.items() if value[0] > now}

                if len(self.windows) >= self.max_keys:
                    return window

            if count >= limit:
                return max(1, math.ceil(expires - now))

            self.windows[key] = (expires, count + 1)

        return 0


class RequestBodyLimit:
    def __init__(self, app: ASGIApp, max_bytes: int = 65536, timeout: float = 15):
        self.app = app
        self.max_bytes = max_bytes
        self.timeout = timeout

    async def __call__(self, scope: Scope, receive: Receive, send: Send):
        if scope["type"] != "http" or scope["method"] not in {"POST", "PUT", "PATCH"}:
            return await self.app(scope, receive, send)

        headers = dict(scope["headers"])

        try:
            length = int(headers.get(b"content-length", b"0"))

            if length < 0:
                raise ValueError
        except ValueError:
            return await JSONResponse({"detail": "Invalid Content-Length."}, status_code=400)(scope, receive, send)

        if length > self.max_bytes:
            return await JSONResponse({"detail": "Request body is too large."}, status_code=413)(scope, receive, send)

        async def read_body():
            body = bytearray()

            while True:
                message = await receive()

                if message["type"] == "http.disconnect":
                    return None

                chunk = message.get("body", b"")

                if len(body) + len(chunk) > self.max_bytes:
                    raise OverflowError

                body.extend(chunk)

                if not message.get("more_body", False):
                    return bytes(body)

        try:
            body = await asyncio.wait_for(read_body(), timeout=self.timeout)
        except OverflowError:
            return await JSONResponse({"detail": "Request body is too large."}, status_code=413)(scope, receive, send)
        except TimeoutError:
            return await JSONResponse({"detail": "Request body timed out."}, status_code=408)(scope, receive, send)

        if body is None:
            return

        consumed = False

        async def replay():
            nonlocal consumed

            if not consumed:
                consumed = True
                return {"type": "http.request", "body": body, "more_body": False}

            return await receive()

        await self.app(scope, replay, send)
