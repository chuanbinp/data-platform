"""Collect NWS station observations and hourly forecast revisions."""

from __future__ import annotations

import logging
from collections.abc import Callable, Iterator
from datetime import datetime, timedelta, timezone
from typing import Any
from uuid import uuid4

from psycopg import sql
from psycopg.rows import dict_row

from data_platform.downloader import Downloader
from data_platform.ingestor import PostgresIngestor, vintage_table_sql

logger = logging.getLogger(__name__)
BASE_URL = "https://api.weather.gov"
COMMON_QUANTITIES = {
    "temperature": "temperature",
    "dewpoint": "dewpoint",
    "relativeHumidity": "relative_humidity",
    "windSpeed": "wind_speed",
    "windGust": "wind_gust",
}
LOCATIONS = {
    "nyc": ("New York City", "KJFK"),
    "albany": ("Albany", "KALB"),
    "buffalo": ("Buffalo", "KBUF"),
    "rochester": ("Rochester", "KROC"),
    "syracuse": ("Syracuse", "KSYR"),
}
DATASETS = {
    "locations": {
        "table": "nws_locations",
        "key_columns": ["location"],
    },
    "observations": {
        "table": "nws_observation_vintages",
        "key_columns": ["location", "station_id", "period"],
        "quantities": {
            **COMMON_QUANTITIES,
            "windDirection": "wind_direction",
            "precipitationLastHour": "precipitation_last_hour",
        },
    },
    "forecasts": {
        "table": "nws_forecast_hourly_vintages",
        "key_columns": ["location", "station_id", "period"],
        "quantities": {
            **COMMON_QUANTITIES,
            "probabilityOfPrecipitation": "precipitation_probability",
        },
    },
}
HEADERS = {
    "Accept": "application/geo+json",
    "Feature-Flags": "forecast_temperature_qv,forecast_wind_speed_qv",
}


class NWSError(RuntimeError):
    """One or more NWS locations could not be collected."""


def timestamp(value: str) -> datetime:
    """Require an explicit offset; preserve observation precision in UTC."""
    result = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if result.utcoffset() is None:
        raise ValueError("NWS timestamp must include a timezone")
    return result.astimezone(timezone.utc)


def source_table_sql(schema: str, dataset: str) -> sql.Composable:
    """Declare the location lookup and standard weather revision tables."""
    settings = DATASETS[dataset]
    if dataset == "locations":
        return sql.SQL("""
            CREATE SCHEMA IF NOT EXISTS {schema};
            CREATE TABLE IF NOT EXISTS {table} (
                location text PRIMARY KEY,
                location_name text NOT NULL,
                station_id text NOT NULL,
                latitude double precision NOT NULL,
                longitude double precision NOT NULL,
                forecast_url text NOT NULL,
                refreshed_at timestamptz NOT NULL
            );
        """).format(
            schema=sql.Identifier(schema),
            table=sql.Identifier(schema, settings["table"]),
        )
    columns = {
        "location": "text NOT NULL",
        "location_name": "text NOT NULL",
        "station_id": "text NOT NULL",
        "latitude": "double precision NOT NULL",
        "longitude": "double precision NOT NULL",
        "period": "timestamptz NOT NULL",
    }
    if dataset == "forecasts":
        columns.update({
            "source_updated_at": "text NOT NULL",
            "period_end": "text NOT NULL",
            "wind_direction": "text",
        })
    for name in settings["quantities"].values():
        columns.update({
            f"{name}_value": "text",
            f"{name}_units": "text",
            f"{name}_quality_code": "text",
        })
    return vintage_table_sql(schema, settings["table"], columns, settings["key_columns"])


def quantity_columns(properties, dataset):
    """Map selected source fields without casting values or converting units."""
    columns = {}
    for field, name in DATASETS[dataset]["quantities"].items():
        quantity = properties.get(field)
        if quantity is None:
            quantity = {}
        if not isinstance(quantity, dict):
            raise ValueError(f"NWS did not return structured {field}")
        value = quantity.get("value")
        columns.update({
            f"{name}_value": None if value is None else str(value),
            f"{name}_units": quantity.get("unitCode"),
            f"{name}_quality_code": quantity.get("qualityControl"),
        })
    return columns


class NWSClient:
    """Fetch NWS records using the shared downloader and revision ingestor."""

    def __init__(
        self, *, downloader: Downloader | None = None,
        now: Callable[[], datetime] | None = None,
    ) -> None:
        self._downloader = downloader if downloader is not None else Downloader(timeout=60)
        self._owns_downloader = downloader is None
        self._now = now or (lambda: datetime.now(timezone.utc))

    def iter_records(
        self, dataset: str, *, location: str, observation_lookback_days: int = 2,
        location_metadata: dict[str, Any] | None = None,
    ) -> Iterator[dict[str, Any]]:
        """Yield a location's source values with collection timestamps."""
        if dataset not in ("observations", "forecasts"):
            raise ValueError("Unknown NWS dataset")
        if type(observation_lookback_days) is not int or not 1 <= observation_lookback_days <= 7:
            raise ValueError("observation_lookback_days must be an integer from 1 to 7")
        metadata = location_metadata if location_metadata is not None else self._discover_location(location)
        common = {
            key: metadata[key]
            for key in ("location", "location_name", "station_id", "latitude", "longitude")
        }
        records = (
            self._forecasts(metadata["forecast_url"])
            if dataset == "forecasts"
            else self._observations(metadata["station_id"], observation_lookback_days)
        )
        for record in records:
            yield common | record

    def ingest(
        self, dataset: str, *, schema: str = "bronze",
        ingestor: PostgresIngestor | None = None,
        observation_lookback_days: int = 2, ingestion_run_id: str | None = None,
    ) -> int:
        """Append changed observations; preserve every earlier vintage."""
        if dataset not in ("observations", "forecasts"):
            raise ValueError("Unknown NWS dataset")
        loader = ingestor if ingestor is not None else PostgresIngestor()
        settings = DATASETS[dataset]
        table = settings["table"]
        count = 0
        failed = []
        run_id = str(uuid4()) if ingestion_run_id is None else ingestion_run_id
        logger.info(
            "Starting NWS load: dataset=%s table=%s.%s run_id=%s",
            dataset, schema, table, run_id,
        )
        for location in LOCATIONS:
            try:
                metadata = self._location_metadata(location, loader, schema)
                count += loader.ingest(
                    self.iter_records(
                        dataset, location=location, location_metadata=metadata,
                        observation_lookback_days=observation_lookback_days,
                    ),
                    schema=schema,
                    table=table,
                    key_columns=settings["key_columns"],
                    ingestion_run_id=run_id,
                    setup_sql=source_table_sql(schema, dataset),
                )
            except Exception as error:
                logger.error(
                    "NWS load failed: dataset=%s location=%s run_id=%s error_type=%s",
                    dataset, location, run_id, type(error).__name__,
                )
                failed.append(location)
        if failed:
            raise NWSError(f"NWS {dataset} failed for: {', '.join(failed)}")
        logger.info("NWS load completed: dataset=%s inserted=%d run_id=%s", dataset, count, run_id)
        return count

    def close(self) -> None:
        if self._owns_downloader:
            self._downloader.close()

    def __enter__(self) -> NWSClient:
        return self

    def __exit__(self, *args) -> None:
        self.close()

    def _get(self, url, params=None):
        # Discovery and pagination URLs come from the API; keep requests on NWS.
        if not url.startswith(BASE_URL + "/"):
            raise ValueError("Unexpected NWS URL")
        response = self._downloader.get(url, params=params, headers=HEADERS)
        collected_at = self._now()
        return response.json(), collected_at

    def _discover_location(self, location):
        name, station = LOCATIONS[location]
        metadata, _ = self._get(f"{BASE_URL}/stations/{station}")
        longitude, latitude = metadata["geometry"]["coordinates"][:2]
        coordinates = f"{round(latitude, 4)},{round(longitude, 4)}"
        point, collected_at = self._get(f"{BASE_URL}/points/{coordinates}")
        forecast_url = point["properties"]["forecastHourly"]
        if not forecast_url.startswith(BASE_URL + "/"):
            raise ValueError("Unexpected NWS forecast URL")
        return {
            "location": location,
            "location_name": name,
            "station_id": station,
            "latitude": latitude,
            "longitude": longitude,
            "forecast_url": forecast_url,
            "refreshed_at": collected_at,
        }

    def _location_metadata(self, location, loader, schema):
        """Reuse discovery for 24 hours, including across tasks and deployments."""
        table = sql.Identifier(schema, DATASETS["locations"]["table"])
        with loader.connect() as connection:
            # Both dataset tasks share the cache; only one refreshes a stale row.
            connection.execute(
                "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))", (table.as_string(),),
            )
            connection.execute(source_table_sql(schema, "locations"))
            with connection.cursor(row_factory=dict_row) as cursor:
                cached = cursor.execute(
                    sql.SQL("SELECT * FROM {} WHERE location = %s").format(table), (location,),
                ).fetchone()
            name, station = LOCATIONS[location]
            if (
                cached
                and cached["station_id"] == station
                and cached["location_name"] == name
                and self._now() - cached["refreshed_at"] < timedelta(days=1)
            ):
                return cached
            metadata = self._discover_location(location)
            connection.execute(sql.SQL("""
                INSERT INTO {} (location, location_name, station_id, latitude, longitude, forecast_url, refreshed_at)
                VALUES (%(location)s, %(location_name)s, %(station_id)s, %(latitude)s, %(longitude)s,
                        %(forecast_url)s, %(refreshed_at)s)
                ON CONFLICT (location) DO UPDATE SET
                    location_name = EXCLUDED.location_name, station_id = EXCLUDED.station_id,
                    latitude = EXCLUDED.latitude, longitude = EXCLUDED.longitude,
                    forecast_url = EXCLUDED.forecast_url, refreshed_at = EXCLUDED.refreshed_at
            """).format(table), metadata)
            return metadata

    def _observations(self, station, lookback_days):
        end = self._now()
        params = {
            "start": (end - timedelta(days=lookback_days)).isoformat(),
            "end": end.isoformat(),
            "limit": 500,
        }
        url = f"{BASE_URL}/stations/{station}/observations"
        visited = set()
        while url:
            if url in visited:
                raise ValueError("Repeated NWS pagination URL")
            visited.add(url)
            page, collected_at = self._get(url, params)
            for feature in page["features"]:
                properties = feature["properties"]
                yield {
                    "period": timestamp(properties["timestamp"]),
                    **quantity_columns(properties, "observations"),
                    "observed_at": collected_at,
                }
            url = page.get("pagination", {}).get("next")
            params = None

    def _forecasts(self, forecast_url):
        forecast, collected_at = self._get(forecast_url, {"units": "si"})
        properties = forecast["properties"]
        if not properties["periods"]:
            raise ValueError("NWS returned an empty forecast")
        for period in properties["periods"]:
            start = timestamp(period["startTime"])
            if timestamp(period["endTime"]) <= start:
                raise ValueError("Invalid NWS forecast interval")
            for field in ("temperature", "windSpeed"):
                if not isinstance(period.get(field), dict):
                    raise ValueError(f"NWS did not return structured {field}")
            yield {
                "period": start,
                "period_end": period["endTime"],
                "source_updated_at": properties["updateTime"],
                "wind_direction": period.get("windDirection"),
                **quantity_columns(period, "forecasts"),
                "observed_at": collected_at,
            }
