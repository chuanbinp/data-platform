from contextlib import contextmanager
from datetime import datetime, timedelta, timezone

import httpx
import pytest

from data_platform.downloader import USER_AGENT, Downloader, RetryPolicy
from data_platform.sources.eia930 import EIA930Client, EIA930Error

START = datetime(2026, 11, 1, tzinfo=timezone.utc)
END = START + timedelta(hours=1)


def row(period="2026-11-01T00", metric="D", *, fuel=False, value="123"):
    return {
        "period": period,
        "respondent": "NYIS",
        "fueltype" if fuel else "type": metric,
        "value": value,
        "value-units": "megawatthours",
    }


def page(rows, total=None):
    return {
        "response": {
            "frequency": "hourly",
            "total": str(len(rows) if total is None else total),
            "data": rows,
        }
    }


@contextmanager
def api(handler, **kwargs):
    with httpx.Client(transport=httpx.MockTransport(handler)) as http:
        downloader = Downloader(
            client=http, sleep=lambda _: None, retry_policy=RetryPolicy(max_attempts=2)
        )
        with EIA930Client("test-secret", downloader=downloader, **kwargs) as client:
            yield client
        assert not http.is_closed


def test_region_query_filters_sorting_and_pagination():
    calls = []
    rows = [row(metric=metric) for metric in ("D", "DF", "NG")]

    def handler(request):
        calls.append(request)
        assert request.headers["User-Agent"] == USER_AGENT
        params = request.url.params
        assert request.url.path == "/v2/electricity/rto/region-data/data/"
        assert params["api_key"] == "test-secret"
        assert params["frequency"] == "hourly"
        assert params["data[]"] == "value"
        assert params["start"] == params["end"] == "2026-11-01T00"
        assert params.get_list("facets[respondent][]") == ["NYIS", "PJM"]
        assert params.get_list("facets[type][]") == ["D", "DF", "NG"]
        for index, column in enumerate(("period", "respondent", "type")):
            assert params[f"sort[{index}][column]"] == column
            assert params[f"sort[{index}][direction]"] == "asc"
        offset = int(params["offset"])
        return httpx.Response(200, json=page(rows[offset:offset + 2], total=3))

    with api(handler, page_size=2) as client:
        result = list(client.iter_records(
            "region-data", start=START, end=END,
            respondents=["NYIS", "PJM"], metrics=["D", "DF", "NG"],
        ))
    assert result == rows
    assert [request.url.params["offset"] for request in calls] == ["0", "2"]


def test_fuel_dataset_and_null_values():
    def handler(request):
        assert request.url.path.endswith("fuel-type-data/data/")
        assert request.url.params.get_list("facets[fueltype][]") == ["SUN"]
        assert request.url.params["sort[2][column]"] == "fueltype"
        return httpx.Response(200, json=page([row(metric="SUN", fuel=True, value=None)]))

    with api(handler) as client:
        result = list(client.iter_records(
            "fuel-type-data", start=START, end=END, fuels=["SUN"]
        ))
    assert result[0]["value"] is None
    assert result[0]["value-units"] == "megawatthours"


def test_daily_chunks_are_nonoverlapping_and_convert_to_utc():
    windows = []

    def handler(request):
        params = request.url.params
        windows.append((params["start"], params["end"], params["offset"]))
        return httpx.Response(200, json=page([]))

    eastern_standard = timezone(timedelta(hours=-5))
    start = START.astimezone(eastern_standard)
    with api(handler) as client:
        assert list(client.iter_records(
            "region-data", start=start, end=START + timedelta(hours=25)
        )) == []
    assert windows == [
        ("2026-11-01T00", "2026-11-01T23", "0"),
        ("2026-11-02T00", "2026-11-02T00", "0"),
    ]


def test_short_page_does_not_terminate_early():
    calls = []

    def handler(request):
        offset = int(request.url.params["offset"])
        calls.append(offset)
        return httpx.Response(200, json=page([row(metric=("D", "DF")[offset])], 2))

    with api(handler) as client:
        assert len(list(client.iter_records("region-data", start=START, end=END))) == 2
    assert calls == [0, 1]


def test_pagination_restarts_for_each_daily_window():
    calls = []
    records = [row(metric="D"), row(metric="DF"), row(period="2026-11-02T00")]

    def handler(request):
        params = request.url.params
        offset = int(params["offset"])
        calls.append((params["start"], offset))
        window = records[:2] if params["start"] == "2026-11-01T00" else records[2:]
        return httpx.Response(200, json=page(window[offset:offset + 1], len(window)))

    with api(handler, page_size=1) as client:
        assert list(client.iter_records(
            "region-data", start=START, end=START + timedelta(hours=25),
        )) == records
    assert calls == [
        ("2026-11-01T00", 0), ("2026-11-01T00", 1), ("2026-11-02T00", 0),
    ]


@pytest.mark.parametrize("failure", ["empty", "changed_total"])
def test_inconsistent_pagination_fails(failure):
    def handler(request):
        if request.url.params["offset"] == "0":
            return httpx.Response(200, json=page([row(metric="DF")], 2))
        responses = {
            "empty": page([], 2),
            "changed_total": page([row(metric="NG")], 3),
        }
        return httpx.Response(200, json=responses[failure])

    with api(handler, page_size=1) as client:
        with pytest.raises(EIA930Error):
            list(client.iter_records("region-data", start=START, end=END))


@pytest.mark.parametrize("body", [
    [], {}, {"error": "bad request"},
    {"response": {"error": "bad request"}},
    {"response": {"frequency": "hourly", "total": "bad", "data": []}},
    {"response": {"frequency": "hourly", "total": -1, "data": []}},
    {"response": {"frequency": "hourly", "total": True, "data": []}},
    {"response": {"frequency": "daily", "total": 0, "data": []}},
    {"response": {"frequency": "hourly", "total": 1, "data": {}}},
    page([row()], 0),
    page([row(), row(metric="DF")], 2),  # Exceeds requested page size.
])
def test_invalid_response_fails(body):
    with api(lambda request: httpx.Response(200, json=body), page_size=1) as client:
        with pytest.raises(EIA930Error):
            list(client.iter_records("region-data", start=START, end=END))


def test_raw_records_are_preserved_without_row_validation():
    source_row = row(value="NA")
    source_row["extra-source-field"] = "preserved"
    del source_row["value-units"]
    with api(lambda request: httpx.Response(200, json=page([source_row]))) as client:
        result = list(client.iter_records("region-data", start=START, end=END))
    assert result == [source_row]


@pytest.mark.parametrize("kwargs", [
    {"dataset": "unknown"},
    {"start": START.replace(tzinfo=None)},
    {"end": START},
    {"end": START - timedelta(hours=1)},
    {"start": START + timedelta(minutes=1)},
    {"respondents": []},
    {"respondents": "NYIS"},
    {"respondents": [""]},
    {"fuels": ["SUN"]},
    {"dataset": "fuel-type-data", "metrics": ["D"]},
])
def test_invalid_query_fails_before_request(kwargs):
    def handler(request):
        pytest.fail("Invalid query should not perform an HTTP request")

    query = {"dataset": "region-data", "start": START, "end": END, **kwargs}
    with api(handler) as client:
        with pytest.raises(ValueError):
            list(client.iter_records(**query))


def test_api_key_configuration(monkeypatch):
    monkeypatch.delenv("EIA_API_KEY", raising=False)
    with pytest.raises(ValueError, match="API key"):
        EIA930Client()
    monkeypatch.setenv("EIA_API_KEY", "environment-key")
    with EIA930Client() as client:
        assert client._api_key == "environment-key"
        assert client._downloader.min_request_interval == 1.0
    with EIA930Client("explicit-key") as client:
        assert client._api_key == "explicit-key"


@pytest.mark.parametrize("size", [0, 5001, 1.5])
def test_invalid_page_size(size):
    with pytest.raises(ValueError, match="page_size"):
        EIA930Client("key", page_size=size)


def test_shared_downloader_retries_transient_api_failure():
    calls = []

    def handler(request):
        calls.append(request)
        return httpx.Response(503) if len(calls) == 1 else httpx.Response(200, json=page([row()]))

    with api(handler) as client:
        assert list(client.iter_records("region-data", start=START, end=END)) == [row()]
    assert len(calls) == 2


@pytest.mark.parametrize("kind", ["http", "network", "json", "api"])
def test_errors_do_not_expose_api_key(kind):
    def handler(request):
        if kind == "network":
            raise httpx.ConnectError(f"Failure at {request.url}", request=request)
        if kind == "http":
            return httpx.Response(403)
        if kind == "json":
            return httpx.Response(200, text="invalid-json-test-secret")
        return httpx.Response(200, json={"error": "invalid key: test-secret"})

    with api(handler) as client:
        with pytest.raises(EIA930Error) as error:
            list(client.iter_records("region-data", start=START, end=END))
    assert "test-secret" not in str(error.value)
    assert error.value.__context__ is None or error.value.__suppress_context__


