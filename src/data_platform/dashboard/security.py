"""Bound public requests without another service or shared cache.

The limiter is per process: run the dashboard with one worker. Only the tunnel
connector may supply Cloudflare's visitor address; Uvicorn proxy parsing is off.
"""

import asyncio
import math
import os
from ipaddress import ip_address
from time import monotonic

from starlette.responses import JSONResponse


class RequestBounds:
    """Replace the proxy's body limit and accept visitor IPs from our tunnel only."""

    def __init__(self, app, *, trusted_ip=None):
        self.app = app
        self.trusted_ip = trusted_ip or os.getenv("TRUSTED_TUNNEL_IP")

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            return await self.app(scope, receive, send)
        headers = dict(scope["headers"])
        peer = (scope.get("client") or (None, 0))[0]
        if self.trusted_ip and peer == self.trusted_ip:
            try:
                visitor = str(ip_address(headers.get(b"cf-connecting-ip", b"").decode("ascii")))
            except (ValueError, UnicodeDecodeError):
                response = JSONResponse({"detail": "Invalid visitor address."}, 400)
                return await response(scope, receive, send)
            scope = dict(scope, client=(visitor, 0))
            if headers.get(b"x-forwarded-proto") == b"https":
                scope["scheme"] = "https"

        # Count actual bytes, including chunked requests, before invoking routes.
        body = bytearray()
        try:
            async with asyncio.timeout(10):
                while True:
                    message = await receive()
                    if message["type"] == "http.disconnect":
                        return
                    chunk = message.get("body", b"")
                    if len(body) + len(chunk) > 1_024:
                        response = JSONResponse({"detail": "Request body too large."}, 413)
                        return await response(scope, receive, send)
                    body.extend(chunk)
                    if not message.get("more_body"):
                        break
        except TimeoutError:
            response = JSONResponse({"detail": "Request body timed out."}, 408)
            return await response(scope, receive, send)

        replayed = False

        async def bounded_receive():
            nonlocal replayed
            if not replayed:
                replayed = True
                return {"type": "http.request", "body": bytes(body), "more_body": False}
            return await receive()

        await self.app(scope, bounded_receive, send)


class ApiLimits:
    def __init__(self, app, *, clock=monotonic):
        self.app = app
        self.clock = clock
        self.clients = {}

    async def __call__(self, scope, receive, send):
        if scope["type"] == "lifespan":
            self.clients.clear()
        if scope["type"] != "http" or not scope["path"].startswith("/api/"):
            return await self.app(scope, receive, send)
        if len(scope.get("query_string", b"")) > 16_384:
            response = JSONResponse(
                {"detail": "Query too large. Select fewer filter values."}, 414
            )
            return await response(scope, receive, send)
        if scope["path"] != "/api/health":
            now = self.clock()
            # Capacity is bounded even when requests use many distinct addresses.
            self.clients = {
                ip: bucket
                for ip, bucket in self.clients.items()
                if now - bucket[1] < 600
            }
            ip = (scope.get("client") or ("unknown", 0))[0]
            if ip not in self.clients and len(self.clients) >= 1_024:
                retry = 60
            else:
                tokens, last = self.clients.get(ip, (30, now))
                tokens = min(30, tokens + (now - last) * 2)
                retry = math.ceil((1 - tokens) / 2) if tokens < 1 else 0
                self.clients[ip] = (tokens if retry else tokens - 1, now)
            if retry:
                response = JSONResponse(
                    {"detail": "Too many requests. Please retry shortly."},
                    429,
                    headers={"Retry-After": str(retry)},
                )
                return await response(scope, receive, send)
        await self.app(scope, receive, send)
