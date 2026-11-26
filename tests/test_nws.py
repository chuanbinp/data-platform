"""NWS contract checks run offline; real availability is not a CI dependency."""

import os
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from unittest.mock import Mock
from uuid import uuid4

import httpx
import pytest
from psycopg import sql

from data_platform.downloader import Downloader, RetryPolicy
from data_platform.ingestor import PostgresIngestor
from data_platform.sources import nws


def quantity(value, unit="wmoUnit:degC"):
    return {"value": value, "unitCode": unit, "qualityControl": "V"}


@pytest.fixture
def api():
    state = {
        "requests": [],
        "forecast": {
            "updateTime": "2026-11-01T04:00:00Z",
            "generatedAt": "2026-11-01T04:10:00Z",
            "periods": [{
                "number": 1,
                "startTime": "2026-11-01T01:00:00-04:00",
                "endTime": "2026-11-01T01:00:00-05:00",
                "temperature": quantity(10),
                "windSpeed": quantity(12, "wmoUnit:km_h-1"),
                "icon": "https://example.com/icon",
            }],
        },
    }

    def handle(request):
        state["requests"].append(request)
        assert "data-platform" in request.headers["User-Agent"]
        path = request.url.path
        if state.get("unavailable"):
            return httpx.Response(503)
        if path == "/stations/KJFK":
            return httpx.Response(200, json={"geometry": {"coordinates": [-73.76393, 40.63915]}})
        if path.startswith("/points/"):
            assert path == "/points/40.6392,-73.7639"
            forecast_path = state.get("forecast_path", "/gridpoints/OKX/1,2/forecast/hourly")
            return httpx.Response(200, json={
                "properties": {"forecastHourly": nws.BASE_URL + forecast_path},
            })
        if path.endswith("/forecast/hourly"):
            assert request.url.params["units"] == "si"
            assert "forecast_wind_speed_qv" in request.headers["Feature-Flags"]
            return httpx.Response(200, json={"properties": state["forecast"]})
        if path.endswith("/observations"):
            cursor = request.url.params.get("cursor")
            properties = {
                "timestamp": "2026-11-01T01:30:00-05:00" if cursor else "2026-11-01T01:30:00-04:00",
                "temperature": quantity(None if cursor else state.get("temperature", 10)),
            }
            page = {"features": [{"properties": properties}]}
            if not cursor or state.get("cycle"):
                page["pagination"] = {"next": nws.BASE_URL + "/stations/KJFK/observations?cursor=next"}
            return httpx.Response(200, json=page)
        raise AssertionError(path)

    transport = httpx.Client(transport=httpx.MockTransport(handle))
    downloader = Downloader(client=transport, retry_policy=RetryPolicy(max_attempts=2), sleep=lambda _: None)
    collected = datetime(2026, 11, 1, 7, tzinfo=timezone.utc)
    client = nws.NWSClient(downloader=downloader, now=lambda: collected)
    yield client, state, collected
    transport.close()


def test_observations_paginate_preserve_nulls_and_distinguish_dst_hours(api):
    client, state, collected = api
    rows = list(client.iter_records("observations", location="nyc", observation_lookback_days=7))
    assert [row["period"].hour for row in rows] == [5, 6]
    assert all(row["period"].minute == 30 for row in rows)
    assert all(row["observed_at"] == collected for row in rows)
    assert rows[1]["temperature_value"] is None
    assert rows[0]["temperature_value"] == "10"
    assert rows[0]["temperature_units"] == "wmoUnit:degC"
    assert rows[0]["temperature_quality_code"] == "V"
    assert "payload" not in rows[0] and "source_updated_at" not in rows[0]
    assert state["requests"][2].url.params["start"] == "2026-10-25T07:00:00+00:00"
    assert dict(state["requests"][3].url.params) == {"cursor": "next"}


def test_forecasts_separate_source_target_and_collection_time(api):
    client, state, collected = api
    row, = client.iter_records("forecasts", location="nyc")
    assert row["period"].hour == 5
    assert row["source_updated_at"] == "2026-11-01T04:00:00Z"
    assert row["observed_at"] == collected
    assert row["period_end"] == "2026-11-01T01:00:00-05:00"
    assert row["wind_speed_value"] == "12"
    assert "payload" not in row and "number" not in row and "icon" not in row
    state["forecast"]["generatedAt"] = "2026-11-01T06:59:00Z"
    state["forecast"]["periods"][0]["number"] = 3
    second, = client.iter_records("forecasts", location="nyc")
    assert row == second


@pytest.mark.parametrize("days", [0, 8, True, 1.5])
def test_invalid_observation_window(api, days):
    with pytest.raises(ValueError, match="integer from 1 to 7"):
        list(api[0].iter_records("observations", location="nyc", observation_lookback_days=days))


def test_pagination_cycle_fails_instead_of_loading_incomplete_data(api):
    client, state, _ = api
    state["cycle"] = True
    with pytest.raises(ValueError, match="Repeated"):
        list(client.iter_records("observations", location="nyc"))


def test_forecast_shape_and_intervals_are_validated(api):
    client, state, _ = api
    period = state["forecast"]["periods"][0]
    period["temperature"] = 10
    with pytest.raises(ValueError, match="structured temperature"):
        list(client.iter_records("forecasts", location="nyc"))
    period["temperature"] = quantity(10)
    period["endTime"] = period["startTime"]
    with pytest.raises(ValueError, match="interval"):
        list(client.iter_records("forecasts", location="nyc"))


def test_raw_quantities_preserve_zero_and_reject_invalid_shapes():
    columns = nws.quantity_columns({"temperature": quantity(0), "windGust": None}, "observations")
    assert columns["temperature_value"] == "0"
    assert columns["wind_gust_value"] is None
    with pytest.raises(ValueError, match="structured dewpoint"):
        nws.quantity_columns({"dewpoint": False}, "forecasts")


def test_network_errors_retry_and_fail(api):
    client, state, _ = api
    state["unavailable"] = True
    with pytest.raises(httpx.HTTPStatusError):
        list(client.iter_records("forecasts", location="nyc"))
    assert len(state["requests"]) == 2


def test_location_failure_does_not_skip_other_locations(api, monkeypatch):
    client, _, _ = api
    monkeypatch.setattr(nws, "LOCATIONS", {"nyc": ("NYC", "KJFK"), "albany": ("Albany", "KALB")})
    loader = Mock()
    loader.ingest.side_effect = [ValueError("station failed"), 3]
    monkeypatch.setattr(client, "_location_metadata", Mock(return_value={}))
    with pytest.raises(RuntimeError, match="nyc"):
        client.ingest("forecasts", ingestor=loader)
    assert loader.ingest.call_count == 2


def test_naive_source_timestamp_is_rejected():
    with pytest.raises(ValueError, match="timezone"):
        nws.timestamp("2026-11-01T01:00:00")


def test_close_preserves_injected_downloader(api):
    client, _, _ = api
    client.close()
    assert not client._downloader._client.is_closed


@pytest.fixture
def warehouse():
    dsn = os.getenv("TEST_DATABASE_URL")
    if not dsn:
        pytest.skip("Set TEST_DATABASE_URL for PostgreSQL integration tests")
    loader = PostgresIngestor(dsn)
    schema = "test_nws_" + uuid4().hex
    try:
        yield loader, schema
    finally:
        with loader.connect() as connection:
            connection.execute(sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(sql.Identifier(schema)))


@pytest.mark.integration
@pytest.mark.parametrize("dataset,expected", [("observations", 2), ("forecasts", 1)])
def test_ingestion_retries_preserve_revisions(api, monkeypatch, dataset, expected, warehouse):
    loader, schema = warehouse
    client, state, _ = api
    monkeypatch.setattr(nws, "LOCATIONS", {"nyc": ("New York City", "KJFK")})
    assert client.ingest(dataset, schema=schema, ingestor=loader, ingestion_run_id="attempt-1") == expected
    assert client.ingest(dataset, schema=schema, ingestor=loader, ingestion_run_id="attempt-2") == 0
    state["temperature"] = 20
    state["forecast"]["periods"][0]["temperature"] = quantity(20)
    assert client.ingest(dataset, schema=schema, ingestor=loader, ingestion_run_id="attempt-3") == 1
    with loader.connect() as connection:
        assert connection.execute(sql.SQL("SELECT count(*) FROM {}").format(
            sql.Identifier(schema, nws.DATASETS[dataset]["table"]),
        )).fetchone() == (expected + 1,)


@pytest.mark.integration
def test_location_metadata_is_shared_refreshed_daily_and_preserved_on_failure(api, monkeypatch, warehouse):
    loader, schema = warehouse
    client, state, collected = api
    monkeypatch.setattr(nws, "LOCATIONS", {"nyc": ("New York City", "KJFK")})
    table = sql.Identifier(schema, "nws_locations")

    def cached():
        with loader.connect() as connection:
            return connection.execute(sql.SQL("SELECT forecast_url, refreshed_at FROM {}").format(table)).fetchone()

    def lookups():
        return [
            request for request in state["requests"]
            if request.url.path == "/stations/KJFK" or request.url.path.startswith("/points/")
        ]

    with ThreadPoolExecutor(max_workers=2) as workers:
        metadata = list(workers.map(
            lambda _: client._location_metadata("nyc", loader, schema), range(2),
        ))
    assert metadata[0] == metadata[1]
    assert len(lookups()) == 2
    client.ingest("observations", schema=schema, ingestor=loader)
    assert len(lookups()) == 2
    # A new client represents another task/process, with no in-memory cache.
    with nws.NWSClient(downloader=client._downloader, now=lambda: collected) as other:
        other.ingest("forecasts", schema=schema, ingestor=loader)
    assert len(lookups()) == 2

    before = cached()
    monkeypatch.setattr(client, "_now", lambda: collected + timedelta(hours=23))
    client.ingest("forecasts", schema=schema, ingestor=loader)
    assert len(lookups()) == 2

    monkeypatch.setattr(client, "_now", lambda: collected + timedelta(days=1))
    state["forecast_path"] = "/gridpoints/OKX/3,4/forecast/hourly"
    client.ingest("forecasts", schema=schema, ingestor=loader)
    after = cached()
    assert len(lookups()) == 4
    assert after[0].endswith("/OKX/3,4/forecast/hourly") and after[1] > before[1]
    assert state["requests"][-1].url.path == state["forecast_path"]

    monkeypatch.setattr(client, "_now", lambda: collected + timedelta(days=2))
    state["unavailable"] = True
    with pytest.raises(nws.NWSError):
        client.ingest("forecasts", schema=schema, ingestor=loader)
    assert cached() == after
