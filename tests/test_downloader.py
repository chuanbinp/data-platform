from datetime import datetime, timezone

import httpx
import pytest

from data_platform.downloader import Downloader, RetryPolicy


def test_success_preserves_params_headers_and_timeout():
    def handler(request):
        assert request.url.params.get_list("fuel[]") == ["SUN", "WND"]
        assert request.headers["Authorization"] == "Bearer secret"
        assert request.extensions["timeout"]["read"] == 12.0
        return httpx.Response(200, json={"data": [1, 2]})

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        with Downloader(client=client, timeout=12) as downloader:
            response = downloader.get(
                "https://example.test/data",
                params={"fuel[]": ["SUN", "WND"]},
                headers={"Authorization": "Bearer secret"},
            )
            assert response.json() == {"data": [1, 2]}
        assert not client.is_closed  # Caller retains ownership.


def test_backoff_jitter_and_cap(caplog):
    caplog.set_level("DEBUG")
    calls = []
    ceilings = []
    sleeps = []

    def handler(request):
        calls.append(request)
        return httpx.Response(503 if len(calls) < 5 else 200)

    def uniform(low, high):
        assert low == 0
        ceilings.append(high)
        return high / 2

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        downloader = Downloader(
            client=client,
            retry_policy=RetryPolicy(base_delay=1, max_delay=3),
            sleep=sleeps.append,
            uniform=uniform,
        )
        assert downloader.get(
            "https://example.test/data?api_key=top-secret",
            headers={"Authorization": "Bearer header-secret"},
        ).status_code == 200

    assert ceilings == [1, 2, 3, 3]
    assert sleeps == [0.5, 1, 1.5, 1.5]
    assert "top-secret" not in caplog.text
    assert "header-secret" not in caplog.text
    assert "example.test" not in caplog.text


@pytest.mark.parametrize("status", [408, 429, 500, 502, 503, 504])
def test_transient_http_status_retries(status):
    calls = []

    def handler(request):
        calls.append(request)
        return httpx.Response(status if len(calls) == 1 else 200)

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        downloader = Downloader(client=client, sleep=lambda _: None)
        assert downloader.get("https://example.test").status_code == 200
    assert len(calls) == 2


@pytest.mark.parametrize("status", [301, 400, 401, 403, 404, 422])
def test_non_retryable_http_status_fails_immediately(status):
    calls = []
    sleeps = []

    def handler(request):
        calls.append(request)
        return httpx.Response(status)

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        downloader = Downloader(client=client, sleep=sleeps.append)
        with pytest.raises(httpx.HTTPStatusError) as error:
            downloader.get("https://example.test")
        assert error.value.response.status_code == status
    assert len(calls) == 1
    assert sleeps == []


@pytest.mark.parametrize(
    "error_type", [httpx.ConnectError, httpx.ReadTimeout, httpx.RemoteProtocolError]
)
def test_network_error_can_recover(error_type):
    calls = []

    def handler(request):
        calls.append(request)
        if len(calls) == 1:
            raise error_type("temporary failure", request=request)
        return httpx.Response(200)

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        downloader = Downloader(client=client, sleep=lambda _: None)
        assert downloader.get("https://example.test").status_code == 200
    assert len(calls) == 2


@pytest.mark.parametrize("network_error", [False, True])
def test_exhaustion_raises_last_error_without_final_sleep(network_error):
    calls = []
    sleeps = []

    def handler(request):
        calls.append(request)
        if network_error:
            raise httpx.ConnectError("unavailable", request=request)
        return httpx.Response(503)

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        downloader = Downloader(
            client=client,
            retry_policy=RetryPolicy(max_attempts=3),
            sleep=sleeps.append,
        )
        expected = httpx.ConnectError if network_error else httpx.HTTPStatusError
        with pytest.raises(expected):
            downloader.get("https://example.test")
    assert len(calls) == 3
    assert len(sleeps) == 2


@pytest.mark.parametrize(
    "header,expected",
    [
        ("120", 120),  # Server minimum can exceed the backoff cap.
        ("Wed, 01 Jan 2025 00:01:00 GMT", 60),
        ("Tue, 31 Dec 2024 23:59:00 GMT", 0.5),
        ("invalid", 0.5),
        ("inf", 0.5),
        ("-10", 0.5),
    ],
)
def test_retry_after(header, expected):
    calls = []
    sleeps = []

    def handler(request):
        calls.append(request)
        if len(calls) == 1:
            return httpx.Response(429, headers={"Retry-After": header})
        return httpx.Response(200)

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        downloader = Downloader(
            client=client,
            sleep=sleeps.append,
            uniform=lambda low, high: high / 2,
            now=lambda: datetime(2025, 1, 1, tzinfo=timezone.utc),
        )
        downloader.get("https://example.test")
    assert sleeps == [expected]


def test_invalid_json_is_not_retried():
    calls = []

    def handler(request):
        calls.append(request)
        return httpx.Response(200, text="not JSON")

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        response = Downloader(client=client).get("https://example.test")
        with pytest.raises(ValueError):
            response.json()
    assert len(calls) == 1


def test_single_attempt_never_sleeps():
    sleeps = []
    with httpx.Client(
        transport=httpx.MockTransport(lambda request: httpx.Response(503))
    ) as client:
        downloader = Downloader(
            client=client,
            retry_policy=RetryPolicy(max_attempts=1),
            sleep=sleeps.append,
        )
        with pytest.raises(httpx.HTTPStatusError):
            downloader.get("https://example.test")
    assert sleeps == []


@pytest.mark.parametrize(
    "kwargs",
    [
        {"max_attempts": 0},
        {"max_attempts": 1.5},
        {"base_delay": 0},
        {"max_delay": 0.5},
        {"max_delay": float("inf")},
        {"retry_statuses": (200,)},
    ],
)
def test_invalid_policy(kwargs):
    with pytest.raises(ValueError):
        RetryPolicy(**kwargs)


@pytest.mark.parametrize("timeout", [0, -1, float("nan")])
def test_invalid_timeout(timeout):
    with pytest.raises(ValueError):
        Downloader(timeout=timeout)


def test_owned_client_closes_on_exit():
    with Downloader() as downloader:
        assert not downloader._client.is_closed
    assert downloader._client.is_closed
