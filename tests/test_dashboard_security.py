"""Public request bounds, including client-address spoofing attempts."""

from fastapi import FastAPI
from fastapi.testclient import TestClient

from data_platform.dashboard.security import ApiLimits


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
