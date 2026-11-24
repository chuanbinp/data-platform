"""Download hourly EIA-930 data and ingest it into PostgreSQL."""

from __future__ import annotations

import argparse
import logging
import os
import sys
from collections.abc import Iterator, Sequence
from datetime import datetime, timedelta, timezone
from typing import Any
from uuid import uuid4

import httpx
import psycopg
from dotenv import load_dotenv
from psycopg import sql

from data_platform.downloader import Downloader, QueryParams
from data_platform.ingestor import PostgresIngestor

logger = logging.getLogger(__name__)

BASE_URL = "https://api.eia.gov/v2/electricity/rto"
DATASETS = {
    "region-data": {
        "table": "eia930_region_data",
        "category_column": "type",
    },
    "fuel-type-data": {
        "table": "eia930_fuel_type_data",
        "category_column": "fueltype",
    },
}


class EIA930Error(RuntimeError):
    """An EIA request failed or returned incomplete data."""


def _utc_hour(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("start and end must be timezone-aware datetimes")
    value = value.astimezone(timezone.utc)
    if value.minute or value.second or value.microsecond:
        raise ValueError("start and end must align to whole UTC hours")
    return value


class EIA930Client:
    """Fetch raw records using the shared downloader's retries and backoff."""

    def __init__(
        self,
        api_key: str | None = None,
        *,
        downloader: Downloader | None = None,
        page_size: int = 5000,
    ) -> None:
        key = api_key if api_key is not None else os.getenv("EIA_API_KEY", "")
        self._api_key = key.strip()
        if not self._api_key:
            raise ValueError("Supply an API key or set EIA_API_KEY")
        if not isinstance(page_size, int) or not 1 <= page_size <= 5000:
            raise ValueError("page_size must be an integer between 1 and 5000")
        self.page_size = page_size
        self._owns_downloader = downloader is None
        self._downloader = downloader if downloader is not None else Downloader()

    def iter_records(
        self,
        dataset: str,
        *,
        start: datetime,
        end: datetime,
        respondents: Sequence[str] | None = None,
        metrics: Sequence[str] | None = None,
        fuels: Sequence[str] | None = None,
    ) -> Iterator[dict[str, Any]]:
        """Yield raw records for [start, end), fetching one UTC day at a time."""
        if dataset not in DATASETS:
            raise ValueError(f"Unknown dataset: {dataset}")
        start, end = _utc_hour(start), _utc_hour(end)
        if start >= end:
            raise ValueError("start must be earlier than end")
        if fuels is not None and dataset != "fuel-type-data":
            raise ValueError("fuels is only valid for fuel-type-data")
        if metrics is not None and dataset != "region-data":
            raise ValueError("metrics is only valid for region-data")

        params: QueryParams = {
            "api_key": self._api_key,
            "frequency": "hourly",
            "data[]": "value",
            "length": self.page_size,
        }
        filters = {"respondent": respondents, "type": metrics, "fueltype": fuels}
        for name, codes in filters.items():
            if codes is None:
                continue
            if isinstance(codes, str) or not codes:
                raise ValueError(f"{name} must be a nonempty list of codes")
            if any(not isinstance(code, str) or not code.strip() for code in codes):
                raise ValueError(f"{name} must contain nonempty strings")
            params[f"facets[{name}][]"] = list(codes)
        source_column = DATASETS[dataset]["category_column"]
        for index, column in enumerate(("period", "respondent", source_column)):
            params[f"sort[{index}][column]"] = column
            params[f"sort[{index}][direction]"] = "asc"

        while start < end:
            window_end = min(start + timedelta(days=1), end)
            params["start"] = start.strftime("%Y-%m-%dT%H")
            # EIA's end is inclusive; our public end is exclusive.
            params["end"] = (window_end - timedelta(hours=1)).strftime("%Y-%m-%dT%H")
            yield from self._iter_pages(dataset, params)
            start = window_end

    def ingest(
        self,
        dataset: str,
        *,
        start: datetime,
        end: datetime,
        respondents: Sequence[str] | None = None,
        metrics: Sequence[str] | None = None,
        fuels: Sequence[str] | None = None,
        ingestor: PostgresIngestor | None = None,
        schema: str = "bronze",
        ingestion_run_id: str | None = None,
    ) -> int:
        """Append changed observations; preserve every earlier vintage."""
        if dataset not in DATASETS:
            raise ValueError(f"Unknown dataset: {dataset}")
        settings = DATASETS[dataset]
        category_column = settings["category_column"]
        records = self.iter_records(
            dataset, start=start, end=end,
            respondents=respondents, metrics=metrics, fuels=fuels,
        )
        rows = (
            {
                "period": record.get("period"),
                "respondent": record.get("respondent"),
                "respondent_name": record.get("respondent-name"),
                category_column: record.get(category_column),
                "type_name": record.get("type-name"),
                "value": None if record.get("value") is None else str(record.get("value")),
                "value_units": record.get("value-units"),
                "observed_at": datetime.now(timezone.utc),
            }
            for record in records
        )
        loader = ingestor if ingestor is not None else PostgresIngestor()
        run_id = str(uuid4()) if ingestion_run_id is None else ingestion_run_id
        logger.info(
            "Starting EIA-930 load: dataset=%s table=%s.%s run_id=%s start=%s end=%s "
            "respondents=%s metrics=%s fuels=%s",
            dataset, schema, settings["table"], run_id, start, end,
            respondents, metrics, fuels,
        )
        count = loader.ingest(
            rows,
            schema=schema,
            table=settings["table"],
            key_columns=["period", "respondent", category_column],
            ingestion_run_id=run_id,
            setup_sql=_vintage_table_sql(schema, settings["table"], category_column),
        )
        logger.info("EIA-930 load completed: dataset=%s inserted=%d run_id=%s", dataset, count, run_id)
        return count

    def _iter_pages(self, dataset: str, params: QueryParams) -> Iterator[dict[str, Any]]:
        """Fetch pages until the reported total is reached."""
        offset = 0
        expected_total = None
        while True:
            params["offset"] = offset
            logger.debug("Requesting EIA-930 page: dataset=%s offset=%d", dataset, offset)
            try:
                response = self._downloader.get(f"{BASE_URL}/{dataset}/data/", params=params)
                body = response.json()
                page = body["response"]
                if body.get("error") or page.get("error") or page["frequency"] != "hourly":
                    raise ValueError("Invalid response")
                if type(page["total"]) not in (int, str):
                    raise ValueError("Invalid total")
                total, rows = int(page["total"]), page["data"]
                if total < 0 or not isinstance(rows, list) or len(rows) > self.page_size:
                    raise ValueError("Invalid page")
            except (httpx.HTTPError, ValueError, KeyError, TypeError, AttributeError) as error:
                # Raw HTTP errors and API responses can contain the API key.
                logger.error(
                    "EIA-930 page failed: dataset=%s offset=%d error_type=%s",
                    dataset, offset, type(error).__name__,
                )
                raise EIA930Error(f"{dataset}: request failed or returned invalid data") from None

            if expected_total is not None and total != expected_total:
                raise EIA930Error("Total changed during pagination; rerun the window")
            expected_total = total
            if offset + len(rows) > total or (not rows and offset < total):
                raise EIA930Error("Incomplete or inconsistent pagination")
            logger.info(
                "Fetched EIA-930 page: dataset=%s start=%s end=%s (inclusive) "
                "offset=%d rows=%d total=%d",
                dataset, params["start"], params["end"], offset, len(rows), total,
            )
            yield from rows
            offset += len(rows)
            if offset == total:
                return

    def close(self) -> None:
        if self._owns_downloader:
            self._downloader.close()

    def __enter__(self) -> EIA930Client:
        return self

    def __exit__(self, *args) -> None:
        self.close()


def _vintage_table_sql(schema: str, table: str, category: str) -> sql.Composable:
    """Create the schema, vintage table, and lookup index if missing."""
    return sql.SQL("""
        CREATE SCHEMA IF NOT EXISTS {schema};
        CREATE TABLE IF NOT EXISTS {table} (
            period text NOT NULL,
            respondent text NOT NULL,
            respondent_name text,
            {category} text NOT NULL,
            type_name text,
            value text,
            value_units text,
            observed_at timestamptz NOT NULL,
            ingestion_run_id text NOT NULL,
            inserted_at timestamptz NOT NULL DEFAULT clock_timestamp(),
            PRIMARY KEY (period, respondent, {category}, ingestion_run_id)
        );
        CREATE INDEX IF NOT EXISTS {index} ON {table}
            (period, respondent, {category}, observed_at DESC, inserted_at DESC);
    """).format(
        schema=sql.Identifier(schema),
        table=sql.Identifier(schema, table),
        category=sql.Identifier(category),
        index=sql.Identifier(f"{table}_vintages_idx"),
    )


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dataset", choices=DATASETS, required=True)
    parser.add_argument("--start", type=datetime.fromisoformat, required=True)
    parser.add_argument("--end", type=datetime.fromisoformat, required=True)
    parser.add_argument("--respondents", nargs="+")
    parser.add_argument("--metrics", nargs="+")
    parser.add_argument("--fuels", nargs="+")
    parser.add_argument("--run-id", dest="ingestion_run_id", help="Stable ID reused for retries")
    args = parser.parse_args(argv)
    args.ingestion_run_id = args.ingestion_run_id or str(uuid4())
    load_dotenv(".env", override=False)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    logging.getLogger("httpx").setLevel(logging.WARNING)

    try:
        with EIA930Client() as client:
            count = client.ingest(**vars(args))
    except (ValueError, KeyError, EIA930Error) as error:
        print(f"Ingestion failed: {error}", file=sys.stderr)
        return 1
    except psycopg.Error as error:
        print(
            f"Database load failed ({type(error).__name__}); check connection and tables",
            file=sys.stderr,
        )
        return 1

    table = DATASETS[args.dataset]["table"]
    print(f"Inserted {count} versions into bronze.{table} (run {args.ingestion_run_id})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
