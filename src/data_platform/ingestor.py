"""Load records from any source into PostgreSQL tables."""

from __future__ import annotations

import logging
import os
from collections.abc import Iterable, Mapping, Sequence
from time import monotonic
from typing import Any

import psycopg
from psycopg import sql

logger = logging.getLogger(__name__)


def vintage_table_sql(
    schema: str,
    table: str,
    columns: Mapping[str, str],
    key_columns: Sequence[str],
) -> sql.Composable:
    """Build a source table with the platform's shared revision metadata and index.

    Column types are SQL definitions supplied by source modules, never user input.
    Source modules own their fields and observation identity.
    """
    definitions = sql.SQL(",\n").join(
        sql.SQL("{} {}").format(sql.Identifier(name), sql.SQL(definition))
        for name, definition in columns.items()
    )
    keys = sql.SQL(", ").join(map(sql.Identifier, key_columns))
    return sql.SQL("""
        CREATE SCHEMA IF NOT EXISTS {schema};
        CREATE TABLE IF NOT EXISTS {table} (
            {columns},
            observed_at timestamptz NOT NULL,
            ingestion_run_id text NOT NULL,
            inserted_at timestamptz NOT NULL DEFAULT clock_timestamp(),
            PRIMARY KEY ({keys}, ingestion_run_id)
        );
        CREATE INDEX IF NOT EXISTS {index} ON {table}
            ({keys}, observed_at DESC, inserted_at DESC);
    """).format(
        schema=sql.Identifier(schema),
        table=sql.Identifier(schema, table),
        columns=definitions,
        keys=keys,
        index=sql.Identifier(table + "_lookup_idx"),
    )


class PostgresIngestor:
    """Each ingest call opens a connection and commits or rolls back one load."""

    def __init__(self, dsn: str | None = None) -> None:
        if dsn is not None:
            self._connection_options = {"conninfo": dsn}
            return
        self._connection_options = {
            "host": os.getenv("POSTGRES_HOST", "127.0.0.1"),
            "port": os.getenv("POSTGRES_PORT", "5000"),
            "dbname": os.getenv("POSTGRES_DB", "warehouse"),
            "user": os.getenv(
                "WAREHOUSE_USER", os.getenv("POSTGRES_USER", "warehouse_writer")
            ),
            "password": os.getenv(
                "WAREHOUSE_PASSWORD", os.getenv("POSTGRES_PASSWORD")
            ),
        }

    def connect(self):
        """Open a warehouse connection; callers own its transaction and lifetime."""
        return psycopg.connect(**self._connection_options, connect_timeout=10)

    def ingest(
        self,
        records: Iterable[Mapping[str, Any]],
        *,
        table: str,
        key_columns: Sequence[str],
        ingestion_run_id: str,
        schema: str = "public",
        setup_sql: str | sql.Composable | None = None,
    ) -> int:
        """Compare source columns with the latest version and insert only changes.

        Records contain source columns and observed_at.
        Setup and all inserts share one transaction. Return new versions stored.
        """
        if not key_columns or isinstance(key_columns, str):
            raise ValueError("key_columns must be a nonempty sequence")
        if not ingestion_run_id.strip():
            raise ValueError("Supply a nonempty run ID")

        destination = sql.Identifier(schema, table)
        matches = sql.SQL(" AND ").join(
            sql.SQL("{} = %s").format(sql.Identifier(key)) for key in key_columns
        )
        count = 0
        processed = 0
        started = monotonic()
        logger.info("Starting load: table=%s run_id=%s", destination.as_string(), ingestion_run_id)
        try:
            with self.connect() as connection:
                # One lock per table, before setup and downloading, prevents two
                # overlapping polls from both deciding the same observation is new.
                connection.execute(
                    "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))",
                    (destination.as_string(),),
                )
                logger.info("Acquired ingestion lock: table=%s", destination.as_string())
                if setup_sql is not None:
                    connection.execute(setup_sql)
                    logger.debug("Table setup completed: table=%s", destination.as_string())
                with connection.cursor() as cursor:
                    for processed, record in enumerate(records, start=1):
                        key_values = tuple(record[key] for key in key_columns)
                        source = {
                            key: value for key, value in record.items()
                            if key not in {"observed_at", "inserted_at", "ingestion_run_id"}
                        }
                        comparison = sql.SQL(" AND ").join(
                            sql.SQL("{} IS NOT DISTINCT FROM %s").format(sql.Identifier(key))
                            for key in source
                        )
                        lookup = sql.SQL("""
                            SELECT {comparison} FROM {table} WHERE {matches}
                            ORDER BY observed_at DESC, inserted_at DESC LIMIT 1
                        """).format(comparison=comparison, table=destination, matches=matches)
                        latest = cursor.execute(lookup, (*source.values(), *key_values)).fetchone()
                        if latest and latest[0]:
                            continue
                        columns = (*record, "ingestion_run_id")
                        insert = _insert_query(schema, table, columns)
                        cursor.execute(insert, (*record.values(), ingestion_run_id))
                        count += cursor.rowcount
        except Exception as error:
            logger.error(
                "Load failed: table=%s run_id=%s processed=%d elapsed=%.2fs error_type=%s",
                destination.as_string(), ingestion_run_id, processed,
                monotonic() - started, type(error).__name__,
            )
            raise
        logger.info(
            "Load committed: table=%s run_id=%s processed=%d inserted=%d skipped=%d elapsed=%.2fs",
            destination.as_string(), ingestion_run_id, processed, count,
            processed - count, monotonic() - started,
        )
        return count


def _insert_query(schema: str, table: str, columns: Sequence[str]) -> sql.Composable:
    return sql.SQL("INSERT INTO {} ({}) VALUES ({}) ON CONFLICT DO NOTHING").format(
        sql.Identifier(schema, table),
        sql.SQL(", ").join(map(sql.Identifier, columns)),
        sql.SQL(", ").join(sql.Placeholder() for _ in columns),
    )
