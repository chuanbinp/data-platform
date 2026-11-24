from datetime import datetime, timezone
import os
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier
from unittest.mock import MagicMock
from uuid import uuid4

import httpx
import psycopg
import pytest
from psycopg import sql

from data_platform.sources import eia930
from data_platform.downloader import Downloader
from data_platform.ingestor import PostgresIngestor
from data_platform.sources.eia930 import EIA930Client, EIA930Error

START = datetime(2026, 11, 1, tzinfo=timezone.utc)
END = datetime(2026, 11, 1, 1, tzinfo=timezone.utc)


@pytest.fixture(autouse=True)
def isolated_working_directory(monkeypatch, tmp_path):
    # CLI tests must never load the developer's real .env file.
    monkeypatch.chdir(tmp_path)


@pytest.mark.parametrize("exported_key", [None, "exported-key"])
def test_cli_loads_dotenv_without_overriding_environment(monkeypatch, tmp_path, exported_key):
    (tmp_path / ".env").write_text("EIA_API_KEY=file-key\nPOSTGRES_DB=test_database\n")
    monkeypatch.delenv("POSTGRES_DB", raising=False)
    monkeypatch.delenv("EIA_API_KEY", raising=False)
    if exported_key:
        monkeypatch.setenv("EIA_API_KEY", exported_key)
    captured = {}

    def create_client():
        captured["key"] = os.getenv("EIA_API_KEY")
        captured["database"] = os.getenv("POSTGRES_DB")
        client = MagicMock()
        client.__enter__.return_value = client
        client.ingest.return_value = 0
        return client

    monkeypatch.setattr(eia930, "EIA930Client", create_client)
    assert eia930.main([
        "--dataset", "region-data", "--start", START.isoformat(), "--end", END.isoformat(),
    ]) == 0
    assert captured == {"key": exported_key or "file-key", "database": "test_database"}


def observation(category="type", value="123"):
    return {
        "period": "2026-11-01T00", "respondent": "NYIS",
        category: "D" if category == "type" else "SUN",
        "value": value, "value-units": "megawatthours",
    }


def response(rows, total=None):
    return httpx.Response(200, json={
        "response": {
            "frequency": "hourly", "total": str(len(rows) if total is None else total),
            "data": rows,
        }
    })


@pytest.mark.parametrize("dataset,source_key,table,column", [
    ("region-data", "type", "eia930_region_data", "type"),
    ("fuel-type-data", "fueltype", "eia930_fuel_type_data", "fueltype"),
])
def test_cli_ingests_correct_dataset(monkeypatch, capsys, dataset, source_key, table, column):
    record = observation(source_key)
    captured = {}

    class RecordingIngestor:
        def ingest(self, records, **options):
            captured["rows"] = list(records)
            captured["options"] = options
            return len(captured["rows"])

    with httpx.Client(transport=httpx.MockTransport(lambda request: response([record]))) as http:
        with EIA930Client("test-key", downloader=Downloader(client=http)) as client:
            monkeypatch.setattr(eia930, "EIA930Client", lambda: client)
            monkeypatch.setattr(eia930, "PostgresIngestor", RecordingIngestor)
            code = eia930.main([
                "--dataset", dataset, "--start", START.isoformat(), "--end", END.isoformat(),
                "--run-id", "test-cli-run",
            ])
    assert code == 0
    setup_sql = captured["options"].pop("setup_sql")
    assert captured["options"] == {
        "schema": "bronze", "table": table,
        "key_columns": ["period", "respondent", column],
        "ingestion_run_id": "test-cli-run",
    }
    assert f'CREATE TABLE IF NOT EXISTS "bronze"."{table}"' in (
        setup_sql.as_string()
    )
    assert captured["rows"][0][column] == record[source_key]
    assert "payload" not in captured["rows"][0]
    assert "inserted_at" not in captured["rows"][0]
    assert captured["rows"][0]["observed_at"].tzinfo is not None
    assert f"bronze.{table}" in capsys.readouterr().out


def test_cli_reports_database_failure_without_credentials(monkeypatch, caplog):
    class FailingIngestor:
        def ingest(self, records, **options):
            raise psycopg.OperationalError("password=secret")

    monkeypatch.setenv("EIA_API_KEY", "test-key")
    monkeypatch.setattr(eia930, "PostgresIngestor", FailingIngestor)
    assert eia930.main([
        "--dataset", "region-data", "--start", START.isoformat(), "--end", END.isoformat(),
    ]) == 1
    error = caplog.text
    assert "OperationalError" in error
    assert "secret" not in error


@pytest.fixture
def raw_database():
    dsn = os.getenv("TEST_DATABASE_URL")
    if not dsn:
        pytest.skip("Set TEST_DATABASE_URL to run PostgreSQL integration tests")
    schema = f"test_eia930_{uuid4().hex}"
    loader = PostgresIngestor(dsn)
    try:
        with httpx.Client(transport=httpx.MockTransport(lambda request: response([]))) as http:
            with EIA930Client("test-key", downloader=Downloader(client=http)) as client:
                for dataset in eia930.DATASETS:
                    # Use the same automatic setup as production, including reruns.
                    for _ in range(2):
                        client.ingest(
                            dataset, start=START, end=END, ingestor=loader, schema=schema,
                        )
        yield loader, schema, dsn
    finally:
        with psycopg.connect(dsn) as connection:
            connection.execute(sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(
                sql.Identifier(schema)
            ))


@pytest.mark.integration
@pytest.mark.parametrize("dataset,source_key,table", [
    ("region-data", "type", "eia930_region_data"),
    ("fuel-type-data", "fueltype", "eia930_fuel_type_data"),
])
def test_changed_versions_and_reversions(raw_database, dataset, source_key, table):
    loader, schema, dsn = raw_database
    record = observation(source_key)
    with httpx.Client(transport=httpx.MockTransport(lambda request: response([record]))) as http:
        with EIA930Client("test-key", downloader=Downloader(client=http)) as client:
            options = {"start": START, "end": END, "ingestor": loader, "schema": schema}
            assert client.ingest(dataset, **options) == 1
            assert client.ingest(dataset, **options) == 0  # Unchanged poll.
            record["value"] = "456"
            assert client.ingest(dataset, **options) == 1
            record["value"] = "123"  # A -> B -> A must retain all three.
            assert client.ingest(dataset, **options) == 1
    with psycopg.connect(dsn) as connection:
        result = connection.execute(sql.SQL(
            "SELECT value, observed_at, inserted_at, ingestion_run_id FROM {} ORDER BY observed_at"
        ).format(sql.Identifier(schema, table))).fetchall()
        as_of = connection.execute(sql.SQL("""
            SELECT value FROM {} WHERE observed_at <= %s
            ORDER BY observed_at DESC LIMIT 1
        """).format(sql.Identifier(schema, table)), (result[1][1],)).fetchone()
    assert [row[0] for row in result] == ["123", "456", "123"]
    assert all(row[1].tzinfo is not None and row[2].tzinfo is not None for row in result)
    assert len({row[3] for row in result}) == 3
    assert as_of == ("456",)


@pytest.mark.integration
@pytest.mark.parametrize("dataset,category", [
    ("region-data", "type"),
    ("fuel-type-data", "fueltype"),
])
def test_wide_columns_preserve_versions(raw_database, dataset, category):
    loader, schema, dsn = raw_database
    table = sql.Identifier(schema, eia930.DATASETS[dataset]["table"])
    record = {
        **observation(category),
        "respondent-name": "New York ISO",
        "type-name": "Demand" if category == "type" else "Solar",
        "value-units": "megawatthours",
    }
    with httpx.Client(transport=httpx.MockTransport(lambda request: response([record]))) as http:
        with EIA930Client("test-key", downloader=Downloader(client=http)) as client:
            options = {"start": START, "end": END, "ingestor": loader, "schema": schema}
            assert client.ingest(dataset, **options) == 1
            assert client.ingest(dataset, **options) == 0
            record["value"] = None
            assert client.ingest(dataset, **options) == 1
            assert client.ingest(dataset, **options) == 0
    with psycopg.connect(dsn) as connection:
        rows = connection.execute(sql.SQL("""
            SELECT period, respondent, {}, respondent_name, type_name, value,
                   value_units
            FROM {} ORDER BY observed_at
        """).format(sql.Identifier(category), table)).fetchall()
    for row, value in zip(rows, ["123", None], strict=True):
        assert row == (
            record["period"], record["respondent"], record[category],
            "New York ISO", record["type-name"], value, "megawatthours",
        )


@pytest.mark.integration
@pytest.mark.parametrize("field", ["respondent-name", "type-name", "value-units"])
def test_label_changes_and_missing_fields_create_versions(raw_database, field):
    loader, schema, _ = raw_database
    record = {**observation(), field: "original"}
    with httpx.Client(transport=httpx.MockTransport(lambda request: response([record]))) as http:
        with EIA930Client("test-key", downloader=Downloader(client=http)) as client:
            options = {"start": START, "end": END, "ingestor": loader, "schema": schema}
            assert client.ingest("region-data", **options) == 1
            assert client.ingest("region-data", **options) == 0
            record[field] = "revised"
            assert client.ingest("region-data", **options) == 1
            del record[field]
            assert client.ingest("region-data", **options) == 1
            assert client.ingest("region-data", **options) == 0


@pytest.mark.integration
@pytest.mark.parametrize("failure", ["pagination", "missing_key"])
def test_eia_failure_rolls_back_earlier_versions(raw_database, failure):
    loader, schema, dsn = raw_database

    def handler(request):
        if request.url.params["offset"] == "0":
            return response([observation()], total=2)
        return response([], total=2) if failure == "pagination" else response([{}], total=2)

    with httpx.Client(transport=httpx.MockTransport(handler)) as http:
        with EIA930Client("test-key", downloader=Downloader(client=http), page_size=1) as client:
            expected_error = EIA930Error if failure == "pagination" else psycopg.errors.NotNullViolation
            with pytest.raises(expected_error):
                client.ingest(
                    "region-data", start=START, end=END, ingestor=loader,
                    schema=schema, ingestion_run_id=f"failed-{failure}",
                )
    with psycopg.connect(dsn) as connection:
        assert connection.execute(sql.SQL("SELECT count(*) FROM {}").format(
            sql.Identifier(schema, "eia930_region_data")
        )).fetchone() == (0,)

    with httpx.Client(transport=httpx.MockTransport(lambda request: response([observation()]))) as http:
        with EIA930Client("test-key", downloader=Downloader(client=http)) as client:
            assert client.ingest(
                "region-data", start=START, end=END, ingestor=loader, schema=schema,
                ingestion_run_id=f"failed-{failure}",
            ) == 1


@pytest.mark.integration
@pytest.mark.parametrize("empty", [False, True])
def test_ingestion_creates_missing_schema_and_table(raw_database, empty):
    loader, schema, dsn = raw_database
    with psycopg.connect(dsn) as connection:
        connection.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema)))

    rows = [] if empty else [observation()]
    with httpx.Client(transport=httpx.MockTransport(lambda request: response(rows))) as http:
        with EIA930Client("test-key", downloader=Downloader(client=http)) as client:
            assert client.ingest(
                "region-data", start=START, end=END, ingestor=loader, schema=schema,
            ) == len(rows)
    with psycopg.connect(dsn) as connection:
        assert connection.execute(sql.SQL("SELECT count(*) FROM {}").format(
            sql.Identifier(schema, "eia930_region_data")
        )).fetchone() == (len(rows),)


@pytest.mark.integration
def test_new_schema_and_table_roll_back_on_download_failure(raw_database):
    loader, schema, dsn = raw_database
    with psycopg.connect(dsn) as connection:
        connection.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema)))

    def handler(request):
        rows = [observation()] if request.url.params["offset"] == "0" else []
        return response(rows, total=2)

    with httpx.Client(transport=httpx.MockTransport(handler)) as http:
        with EIA930Client("test-key", downloader=Downloader(client=http), page_size=1) as client:
            with pytest.raises(EIA930Error):
                client.ingest(
                    "region-data", start=START, end=END, ingestor=loader,
                    schema=schema,
                )
    with psycopg.connect(dsn) as connection:
        assert connection.execute(
            "SELECT count(*) FROM pg_namespace WHERE nspname = %s", (schema,)
        ).fetchone() == (0,)


@pytest.mark.integration
def test_retry_rechecks_api_without_duplicating_versions(raw_database):
    loader, schema, dsn = raw_database
    record = observation()
    calls = []

    def handler(request):
        calls.append(request)
        return response([record])

    with httpx.Client(transport=httpx.MockTransport(handler)) as http:
        with EIA930Client("test-key", downloader=Downloader(client=http)) as client:
            options = {"start": START, "end": END, "ingestor": loader, "schema": schema}
            assert client.ingest("region-data", ingestion_run_id="initial", **options) == 1
            assert client.ingest("region-data", ingestion_run_id="initial", **options) == 0
            record["value"] = "456"
            assert client.ingest("region-data", ingestion_run_id="revision", **options) == 1
            assert client.ingest("region-data", ingestion_run_id="revision", **options) == 0
            record["value"] = "789"
            # A run keeps its first captured version of a key on retry.
            assert client.ingest("region-data", ingestion_run_id="initial", **options) == 0
            assert len(calls) == 5
    with psycopg.connect(dsn) as connection:
        rows = connection.execute(sql.SQL(
            "SELECT value FROM {} ORDER BY observed_at"
        ).format(sql.Identifier(schema, "eia930_region_data"))).fetchall()
    assert rows == [("123",), ("456",)]


@pytest.mark.integration
def test_concurrent_unchanged_polls_store_one_version(raw_database):
    loader, schema, dsn = raw_database
    barrier = Barrier(2)

    def poll(run_id):
        barrier.wait(timeout=10)
        with httpx.Client(transport=httpx.MockTransport(lambda request: response([observation()]))) as http:
            with EIA930Client("test-key", downloader=Downloader(client=http)) as client:
                return client.ingest(
                    "region-data", start=START, end=END, ingestor=loader,
                    schema=schema, ingestion_run_id=run_id,
                )

    with ThreadPoolExecutor(max_workers=2) as executor:
        counts = list(executor.map(poll, ["concurrent-a", "concurrent-b"]))
    assert sorted(counts) == [0, 1]
    with psycopg.connect(dsn) as connection:
        assert connection.execute(sql.SQL("SELECT count(*) FROM {}").format(
            sql.Identifier(schema, "eia930_region_data")
        )).fetchone() == (1,)
