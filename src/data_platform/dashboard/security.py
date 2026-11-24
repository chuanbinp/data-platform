"""Bound public requests without another service or shared cache.

The limiter is per process: run the dashboard with one worker. Uvicorn resolves
client addresses only from the explicitly trusted reverse-proxy network.
"""

import math
from time import monotonic

from starlette.responses import JSONResponse


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
