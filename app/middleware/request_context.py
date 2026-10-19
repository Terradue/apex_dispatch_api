"""Keep the incoming request available to authentication and platform services."""
from contextvars import ContextVar

from starlette.requests import Request
from starlette.types import ASGIApp, Receive, Scope, Send


request_context: ContextVar[Request] = ContextVar("request_context")


class RequestContextMiddleware:
    def __init__(self, app: ASGIApp):
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send):
        if scope["type"] not in {"http", "websocket"}:
            await self.app(scope, receive, send)
            return

        # The registry only reads request.url. Preserve the WebSocket URL while
        # adapting the scope to the Request interface required by its constructors.
        request = Request(scope if scope["type"] == "http" else {**scope, "type": "http"})
        token = request_context.set(request)
        try:
            await self.app(scope, receive, send)
        finally:
            request_context.reset(token)
