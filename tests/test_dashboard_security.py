"""Public request bounds, including client-address spoofing attempts."""

import asyncio
from unittest.mock import AsyncMock

import pytest
from fastapi import FastAPI, Request
from fastapi.testclient import TestClient

from data_platform.dashboard.security import ApiLimits, RequestBounds


def client_at(clock):
    app = FastAPI()

    @app.get("/api/example")
    @app.get("/api/health")
    @app.get("/static/example")
    def example():
        return {"ok": True}

    return TestClient(ApiLimits(app, clock=lambda: clock[0]))


def test_burst_refill_and_forwarded_header_spoofing():
    clock = [0]
    client = client_at(clock)
    for index in range(30):
        assert (
            client.get(
                "/api/example", headers={"X-Forwarded-For": f"192.0.2.{index}"}
            ).status_code
            == 200
        )
    response = client.get("/api/example")
    assert response.status_code == 429
    assert response.headers["retry-after"] == "1"
    clock[0] = 0.5
    assert client.get("/api/example").status_code == 200
    assert client.get("/api/example").status_code == 429
    clock[0] = 601
    assert client.get("/api/example").status_code == 200


def test_health_and_static_requests_do_not_consume_api_tokens():
    client = client_at([0])
    for _ in range(35):
        assert client.get("/api/health").status_code == 200
        assert client.get("/static/example").status_code == 200
    assert client.get("/api/example").status_code == 200


def test_query_size_is_bounded_before_parsing():
    client = client_at([0])
    assert client.get("/api/example?x=" + "a" * 16_383).status_code == 414
    assert client.get("/api/example?x=valid").status_code == 200


def test_client_cache_rejects_new_addresses_until_idle_entries_expire():
    clock = [0]
    client = client_at(clock)
    active = client.app
    active.clients = {f"192.0.2.{index}": (30, 0) for index in range(1_024)}
    assert client.get("/api/example").status_code == 429
    assert len(active.clients) == 1_024
    clock[0] = 601
    assert client.get("/api/example").status_code == 200
    assert len(active.clients) == 1


def tunnel_client(peer="172.30.10.2"):
    app = FastAPI()

    @app.get("/api/example")
    def visitor(request: Request):
        return {"ip": request.client.host, "scheme": request.url.scheme}

    return TestClient(
        RequestBounds(ApiLimits(app), trusted_ip="172.30.10.2"), client=(peer, 1234)
    )


def test_tunnel_visitor_address_and_independent_rate_limits():
    client = tunnel_client()
    headers = {"CF-Connecting-IP": "2001:db8::1", "X-Forwarded-Proto": "https"}
    for _ in range(30):
        response = client.get("/api/example", headers=headers)
        assert response.json() == {"ip": "2001:db8::1", "scheme": "https"}
    assert client.get("/api/example", headers=headers).status_code == 429
    headers["CF-Connecting-IP"] = "192.0.2.2"
    assert client.get("/api/example", headers=headers).status_code == 200


def test_untrusted_peer_cannot_supply_visitor_headers():
    client = tunnel_client(peer="192.0.2.10")
    for index in range(30):
        response = client.get("/api/example", headers={
            "CF-Connecting-IP": f"192.0.2.{index}",
            "X-Forwarded-For": f"192.0.2.{index}",
            "X-Forwarded-Proto": "https",
        })
        assert response.json() == {"ip": "192.0.2.10", "scheme": "http"}
    assert client.get("/api/example").status_code == 429


def test_tunnel_rejects_missing_or_invalid_visitor_addresses():
    client = tunnel_client()
    for headers in (
        {}, {"CF-Connecting-IP": "invalid"},
        {"CF-Connecting-IP": "192.0.2.1, 192.0.2.2"},
    ):
        assert client.get("/api/example", headers=headers).status_code == 400


def test_body_size_boundary():
    client = tunnel_client(peer="192.0.2.10")
    assert client.request("GET", "/api/example", content=b"x" * 1_024).status_code == 200
    assert client.request("GET", "/api/example", content=b"x" * 1_025).status_code == 413


@pytest.mark.parametrize("chunks,accepted", [([512, 512], True), ([700, 400], False)])
def test_body_limit_counts_separate_asgi_chunks(chunks, accepted):
    app, send = AsyncMock(), AsyncMock()
    receive = AsyncMock(side_effect=[
        {"type": "http.request", "body": b"x" * size, "more_body": index == 0}
        for index, size in enumerate(chunks)
    ])
    scope = {"type": "http", "headers": [], "client": ("192.0.2.10", 1234)}
    asyncio.run(RequestBounds(app)(scope, receive, send))
    if accepted:
        app.assert_awaited_once()
    else:
        app.assert_not_awaited()
        assert send.await_args_list[0].args[0]["status"] == 413


def test_slow_body_times_out_before_reaching_application(monkeypatch):
    timeout = asyncio.timeout
    monkeypatch.setattr(asyncio, "timeout", lambda seconds: timeout(0))
    app, send = AsyncMock(), AsyncMock()

    async def receive():
        await asyncio.sleep(60)

    scope = {"type": "http", "headers": [], "client": ("192.0.2.10", 1234)}
    asyncio.run(RequestBounds(app)(scope, receive, send))
    app.assert_not_awaited()
    assert send.await_args_list[0].args[0]["status"] == 408
