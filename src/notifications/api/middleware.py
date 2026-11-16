"""Middleware ASGI de correlación: ``X-Correlation-ID`` entrante (validado) o uno nuevo."""

from __future__ import annotations

import re
from typing import Final

from starlette.datastructures import MutableHeaders
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from notifications.observability import correlation_context, new_correlation_id

CORRELATION_HEADER: Final = "X-Correlation-ID"
_VALID = re.compile(r"^[A-Za-z0-9._:-]{1,64}$")


class CorrelationIdMiddleware:
    """ASGI puro (no ``BaseHTTPMiddleware``): el contextvar llega intacto al endpoint."""

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        incoming = _header(scope, CORRELATION_HEADER.lower())
        cid = incoming if incoming and _VALID.match(incoming) else new_correlation_id()

        async def send_with_header(message: Message) -> None:
            if message["type"] == "http.response.start":
                MutableHeaders(scope=message)[CORRELATION_HEADER] = cid
            await send(message)

        with correlation_context(cid):
            await self.app(scope, receive, send_with_header)


def _header(scope: Scope, name: str) -> str | None:
    for key, value in scope.get("headers", []):
        if key.decode("latin-1") == name:
            return str(value.decode("latin-1"))
    return None
