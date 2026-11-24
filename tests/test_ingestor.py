import os
from datetime import datetime, timezone
from unittest.mock import MagicMock
from uuid import uuid4

import psycopg
import pytest
from psycopg import sql

from data_platform.ingestor import PostgresIngestor


@pytest.mark.parametrize("keys", [[], "id"])
def test_invalid_key_columns(keys):
    with pytest.raises(ValueError, match="key_columns"):
        PostgresIngestor().ingest([], table="records", key_columns=keys, ingestion_run_id="run")


@pytest.mark.parametrize("run_id", ["", "   "])
def test_invalid_run_id(run_id):
    with pytest.raises(ValueError, match="run ID"):
        PostgresIngestor().ingest([], table="records", key_columns=["id"], ingestion_run_id=run_id)


def test_empty_input_runs_setup(monkeypatch):
    connection = MagicMock()
    connection.__enter__.return_value = connection
    monkeypatch.setattr(psycopg, "connect", lambda **kwargs: connection)
    assert PostgresIngestor().ingest(
        [], table="records", key_columns=["id"], ingestion_run_id="run",
        setup_sql="CREATE SCHEMA IF NOT EXISTS bronze",
    ) == 0
    connection.execute.assert_any_call("CREATE SCHEMA IF NOT EXISTS bronze")
    connection.cursor.return_value.__enter__.return_value.execute.assert_not_called()


def test_empty_input_detects_connection_failure(monkeypatch):
    def failed_connection(**kwargs):
        raise psycopg.OperationalError("Database unavailable")

    monkeypatch.setattr(psycopg, "connect", failed_connection)
    with pytest.raises(psycopg.OperationalError):
        PostgresIngestor().ingest([], table="records", key_columns=["id"], ingestion_run_id="run")


def test_commit_failure_does_not_log_success(monkeypatch, caplog):
    caplog.set_level("INFO", logger="data_platform.ingestor")
    connection = MagicMock()
    connection.__enter__.return_value = connection
    connection.__exit__.side_effect = psycopg.OperationalError("password=secret-test-key")
    monkeypatch.setattr(psycopg, "connect", lambda **kwargs: connection)
    with pytest.raises(psycopg.OperationalError):
        PostgresIngestor().ingest([], table="records", key_columns=["id"], ingestion_run_id="run")
    assert "Load failed:" in caplog.text
    assert "Load committed:" not in caplog.text
    assert "secret-test-key" not in caplog.text


def test_environment_configuration(monkeypatch):
    monkeypatch.delenv('WAREHOUSE_USER', raising=False)
    monkeypatch.delenv('WAREHOUSE_PASSWORD', raising=False)
    for name, value in {
        "HOST": "postgres", "PORT": "5432", "DB": "warehouse",
        "USER": "loader", "PASSWORD": "test-password",
    }.items():
        monkeypatch.setenv(f"POSTGRES_{name}", value)
    assert PostgresIngestor()._connection_options == {
        "host": "postgres", "port": "5432", "dbname": "warehouse",
        "user": "loader", "password": "test-password",
    }
    assert PostgresIngestor("dbname=another")._connection_options == {"conninfo": "dbname=another"}


def test_local_ingestion_prefers_runtime_credentials(monkeypatch):
    monkeypatch.setenv('POSTGRES_USER', 'administrator')
    monkeypatch.setenv('POSTGRES_PASSWORD', 'admin-example')
    monkeypatch.setenv('WAREHOUSE_USER', 'warehouse_writer')
    monkeypatch.setenv('WAREHOUSE_PASSWORD', 'writer-example')
    options = PostgresIngestor()._connection_options
    assert options['user'] == 'warehouse_writer'
    assert options['password'] == 'writer-example'


@pytest.fixture
def database():
    dsn = os.getenv("TEST_DATABASE_URL")
    if not dsn:
        pytest.skip("Set TEST_DATABASE_URL to run PostgreSQL integration tests")
    schema = f"test_ingestor_{uuid4().hex}"
    with psycopg.connect(dsn) as connection:
        connection.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
        connection.execute(sql.SQL("""
            CREATE TABLE {} (
                id integer NOT NULL CHECK (id > 0),
                value text,
                observed_at timestamptz NOT NULL,
                ingestion_run_id text NOT NULL,
                inserted_at timestamptz NOT NULL DEFAULT clock_timestamp(),
                note text,
                PRIMARY KEY (id, ingestion_run_id)
            )
        """).format(sql.Identifier(schema, "records")))
    try:
        yield PostgresIngestor(dsn), schema, dsn
    finally:
        with psycopg.connect(dsn) as connection:
            connection.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema)))


def record(identifier=1, value="123", note=None):
    return {
        "id": identifier,
        "value": value,
        "note": note,
        "observed_at": datetime.now(timezone.utc),
    }


def stored_rows(database):
    _, schema, dsn = database
    with psycopg.connect(dsn) as connection:
        return connection.execute(sql.SQL("SELECT id, value, note FROM {} ORDER BY id").format(
            sql.Identifier(schema, "records")
        )).fetchall()


@pytest.mark.integration
class TestPostgresIngestor:
    def test_nulls_and_record_column_order(self, database, caplog):
        caplog.set_level("INFO", logger="data_platform.ingestor")
        loader, schema, _ = database
        first = record(1, None)
        second = dict(reversed(record(2, "123", note="quoted ' text").items()))
        assert loader.ingest(
            iter([first, second]), table="records", schema=schema,
            key_columns=["id"], ingestion_run_id="run",
        ) == 2
        assert stored_rows(database) == [
            (1, None, None),
            (2, "123", "quoted ' text"),
        ]
        assert "Load committed:" in caplog.text
        assert "processed=2 inserted=2 skipped=0" in caplog.text

    @pytest.mark.parametrize("before,after,inserted", [
        (None, None, 0),
        (None, "123", 1),
        ("123", None, 1),
        ("123", "123", 0),
    ])
    def test_comparison_handles_nulls(self, database, before, after, inserted):
        loader, schema, _ = database
        options = {"table": "records", "schema": schema, "key_columns": ["id"]}
        assert loader.ingest([record(value=before)], ingestion_run_id="first", **options) == 1
        assert loader.ingest([record(value=after)], ingestion_run_id="next", **options) == inserted

    def test_database_failure_rolls_back_earlier_records(self, database):
        loader, schema, _ = database
        with pytest.raises(psycopg.errors.CheckViolation):
            loader.ingest(
                [record(1), record(-1)], table="records", schema=schema,
                key_columns=["id"], ingestion_run_id="run",
            )
        assert stored_rows(database) == []

    def test_failed_download_rolls_back_earlier_records(self, database, caplog):
        caplog.set_level("INFO", logger="data_platform.ingestor")
        loader, schema, _ = database

        def failed_download():
            yield record()
            raise RuntimeError("Download failed on the next page: api_key=secret-test-key")

        with pytest.raises(RuntimeError, match="Download failed"):
            loader.ingest(
                failed_download(), table="records", schema=schema,
                key_columns=["id"], ingestion_run_id="run",
            )
        assert stored_rows(database) == []
        assert "Load failed:" in caplog.text
        assert "processed=1" in caplog.text
        assert "Load committed:" not in caplog.text
        assert "secret-test-key" not in caplog.text

    def test_missing_key_rolls_back(self, database):
        loader, schema, _ = database
        invalid = record(2)
        del invalid["id"]
        with pytest.raises(KeyError, match="id"):
            loader.ingest(
                [record(1), invalid], table="records", schema=schema,
                key_columns=["id"], ingestion_run_id="run",
            )
        assert stored_rows(database) == []

    def test_sql_identifiers_are_quoted(self, database):
        loader, schema, dsn = database
        table, key = 'odd-name; "records"', 'metric-code'
        with psycopg.connect(dsn) as connection:
            connection.execute(sql.SQL("""
                CREATE TABLE {} (
                    {} text NOT NULL, value text, observed_at timestamptz,
                    inserted_at timestamptz DEFAULT clock_timestamp(), ingestion_run_id text,
                    PRIMARY KEY ({}, ingestion_run_id)
                )
            """).format(sql.Identifier(schema, table), sql.Identifier(key), sql.Identifier(key)))
        assert loader.ingest([{
            key: "D", "value": "123",
            "observed_at": datetime.now(timezone.utc),
        }], table=table, schema=schema, key_columns=[key], ingestion_run_id="run") == 1
        with psycopg.connect(dsn) as connection:
            assert connection.execute(sql.SQL("SELECT value FROM {}").format(
                sql.Identifier(schema, table)
            )).fetchone() == ("123",)

