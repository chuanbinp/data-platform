"""Integration checks for dbt; requires TEST_DATABASE_URL and the dbt group."""

import os
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from uuid import uuid4

import psycopg
import pytest
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict

from data_platform.ingestor import PostgresIngestor
from data_platform.sources import eia930, nws, prepare_sources
from data_platform.sources.eia930 import DATASETS

PROJECT = Path(__file__).resolve().parents[1] / "dbt"
DBT = Path(sys.executable).with_name("dbt")
pytestmark = pytest.mark.integration


@pytest.fixture
def warehouse(tmp_path):
    dsn = os.getenv("TEST_DATABASE_URL")
    if not dsn or not DBT.exists():
        pytest.skip("Set TEST_DATABASE_URL and install the dbt dependency group")
    source, target = f"test_bronze_{uuid4().hex}", f"test_silver_{uuid4().hex}"
    options = conninfo_to_dict(dsn)
    env = {
        **os.environ,
        "POSTGRES_HOST": options.get("host", "127.0.0.1"),
        "POSTGRES_PORT": options.get("port", "5432"),
        "POSTGRES_DB": options["dbname"],
        "POSTGRES_USER": options["user"],
        "DBT_ENV_SECRET_POSTGRES_PASSWORD": options.get("password", ""),
        "DBT_BRONZE_SCHEMA": source,
        "DBT_SILVER_SCHEMA": target,
        "DBT_GOLD_SCHEMA": f"{target}_gold",
        "DBT_TARGET_PATH": str(tmp_path / "target"),
        "DBT_LOG_PATH": str(tmp_path / "logs"),
        "PGTZ": "America/New_York",
    }

    def run(command="build", select=None, exclude=None):
        args = [str(DBT), command, "--project-dir", str(PROJECT), "--profiles-dir", str(PROJECT)]
        if select:
            args += ["--select", select]
        if exclude:
            args += ["--exclude", exclude]
        return subprocess.run(
            args,
            env=env, capture_output=True, text=True, timeout=120,
        )

    try:
        loader = PostgresIngestor(dsn)
        values = ["10.1234567890123456789", "20", "10.1234567890123456789", None, " "]
        for dataset, settings in DATASETS.items():
            category, table = settings["category_column"], settings["table"]
            for index, value in enumerate(values):
                record = {
                    "period": "2026-11-01T00",
                    "respondent": "NYIS",
                    category: "D" if category == "type" else "SUN",
                    "respondent_name": "New York ISO", "type_name": "Example",
                    "value": value, "value_units": "megawatthours",
                    "observed_at": datetime(2026, 11, 2, index, tzinfo=timezone.utc),
                }
                loader.ingest(
                    [record], schema=source, table=table,
                    key_columns=["period", "respondent", category],
                    ingestion_run_id=f"run-{index}",
                    setup_sql=eia930.source_table_sql(source, dataset),
                )
        with psycopg.connect(dsn) as connection:
            prepare_sources(connection, source)
        yield dsn, source, target, run
    finally:
        with psycopg.connect(dsn) as connection:
            for schema in (f"{target}_gold", target, source):
                connection.execute(sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(
                    sql.Identifier(schema),
                ))


def test_staging_preserves_vintages_and_types(warehouse):
    dsn, source, target, run = warehouse
    for command in ("debug", "build", "build"):
        result = run(command)
        assert result.returncode == 0, result.stdout + result.stderr
    for source_table, view in (
        ("eia930_region_hourly_vintages", "electricity_metrics_hourly_vintages"),
        ("eia930_fuel_type_hourly_vintages", "generation_by_fuel_hourly_vintages"),
    ):
        with psycopg.connect(dsn) as connection:
            rows = connection.execute(sql.SQL("""
                SELECT period, value, observed_at, inserted_at, ingestion_run_id
                FROM {} ORDER BY observed_at
            """).format(sql.Identifier(target, view))).fetchall()
            original = connection.execute(sql.SQL("""
                SELECT observed_at, inserted_at, ingestion_run_id FROM {} ORDER BY observed_at
            """).format(sql.Identifier(source, source_table))).fetchall()
        assert len(rows) == 5
        assert [row[0] for row in rows] == [datetime(2026, 11, 1, tzinfo=timezone.utc)] * 5
        assert [row[1] for row in rows] == [
            Decimal("10.1234567890123456789"), Decimal("20"),
            Decimal("10.1234567890123456789"), None, None,
        ]
        assert [row[2:] for row in rows] == original
    with psycopg.connect(dsn) as connection:
        # Current NULLs remain NULL rather than falling back to an older value.
        for view, value_column in (
            ("electricity_demand_hourly_history", "value"),
            ("generation_by_fuel_hourly_history", "generation"),
        ):
            assert connection.execute(sql.SQL("SELECT {} FROM {} WHERE known_until IS NULL").format(
                sql.Identifier(value_column), sql.Identifier(f"{target}_gold", view),
            )).fetchall() == [(None,)]


@pytest.mark.parametrize("dataset,mart", [
    ("region-data", "electricity_demand_hourly_history"),
    ("region-data", "electricity_demand_forecast_hourly_history"),
    ("region-data", "electricity_net_generation_hourly_history"),
    ("region-data", "electricity_total_interchange_hourly_history"),
    ("fuel-type-data", "generation_by_fuel_hourly_history"),
])
def test_silver_and_gold_build_separately(warehouse, dataset, mart):
    dsn, _, target, run = warehouse
    model = f"stg_{DATASETS[dataset]['table']}"
    relation = f"{target}_gold.{mart}"
    result = run(select=f"+{model}")
    assert result.returncode == 0, result.stdout + result.stderr
    with psycopg.connect(dsn) as connection:
        assert connection.execute("SELECT to_regclass(%s)", (relation,)).fetchone() == (None,)
    result = run(select=f"{model}+", exclude=model)
    assert result.returncode == 0, result.stdout + result.stderr
    with psycopg.connect(dsn) as connection:
        assert connection.execute("SELECT to_regclass(%s) IS NOT NULL", (relation,)).fetchone() == (True,)


@pytest.mark.parametrize("column,value", [("value", "invalid"), ("period", "invalid")])
@pytest.mark.parametrize("dataset", ["region-data", "fuel-type-data"])
def test_each_build_validates_only_its_source(warehouse, column, value, dataset):
    dsn, source, _, run = warehouse
    table = DATASETS[dataset]["table"]
    other = "eia930_fuel_type_hourly_vintages" if dataset == "region-data" else "eia930_region_hourly_vintages"
    with psycopg.connect(dsn) as connection:
        connection.execute(sql.SQL("UPDATE {} SET {} = %s WHERE ingestion_run_id = 'run-0'").format(
            sql.Identifier(source, table), sql.Identifier(column),
        ), (value,))
    result = run(select=f"+stg_{other}+")
    assert result.returncode == 0, result.stdout + result.stderr
    result = run(select=f"+stg_{table}+")
    assert result.returncode != 0
    test_name = "region_valid_inputs" if dataset == "region-data" else "fuel_valid_inputs"
    assert test_name in result.stdout


def test_gold_as_of_history_and_metric_split(warehouse):
    dsn, source, target, run = warehouse
    gold = f"{target}_gold"
    with psycopg.connect(dsn) as connection:
        for settings in DATASETS.values():
            # Keep A -> B -> A, removing the later NULL/blank fixture versions.
            connection.execute(sql.SQL("DELETE FROM {} WHERE ingestion_run_id IN ('run-3', 'run-4')").format(
                sql.Identifier(source, settings["table"]),
            ))
        with connection.cursor() as cursor:
            cursor.executemany(sql.SQL("""
                INSERT INTO {} (period, respondent, type, value, value_units, observed_at, ingestion_run_id)
                VALUES ('2026-11-01T00', 'NYIS', %s, %s, 'megawatthours', %s, %s)
            """).format(sql.Identifier(source, "eia930_region_hourly_vintages")), [
                ("DF", "12", datetime(2026, 11, 2, tzinfo=timezone.utc), "forecast-first"),
                ("DF", "13", datetime(2026, 11, 2, 3, tzinfo=timezone.utc), "forecast-revised"),
                ("NG", "8", datetime(2026, 11, 2, tzinfo=timezone.utc), "generation"),
                ("TI", "-2", datetime(2026, 11, 2, tzinfo=timezone.utc), "interchange"),
            ])
        connection.execute(sql.SQL("""
            INSERT INTO {} (period, respondent, fueltype, value, value_units, observed_at, ingestion_run_id)
            VALUES ('2026-11-01T00', 'NYIS', 'WND', '80', 'megawatthours', %s, 'wind')
        """).format(sql.Identifier(source, "eia930_fuel_type_hourly_vintages")), (
            datetime(2026, 11, 2, tzinfo=timezone.utc),
        ))
        connection.execute(sql.SQL("""
            INSERT INTO {} (period, respondent, fueltype, value, value_units, observed_at, ingestion_run_id)
            VALUES ('2026-11-01T00', 'NYIS', 'BAT', '-7', 'megawatthours', %s, 'battery')
        """).format(sql.Identifier(source, "eia930_fuel_type_hourly_vintages")), (
            datetime(2026, 11, 2, tzinfo=timezone.utc),
        ))

    result = run()
    assert result.returncode == 0, result.stdout + result.stderr
    expected = Decimal("10.1234567890123456789")
    with psycopg.connect(dsn) as connection:
        for history, category, value_column in (
            ("electricity_demand_hourly_history", "true", "value"),
            ("generation_by_fuel_hourly_history", "fueltype = 'SUN'", "generation"),
        ):
            relation = sql.Identifier(gold, history)
            versions = connection.execute(sql.SQL("""
                SELECT {}, known_from, known_until, ingestion_run_id, observed_at
                FROM {} WHERE {} ORDER BY known_from
            """).format(sql.Identifier(value_column), relation, sql.SQL(category))).fetchall()
            assert [row[0] for row in versions] == [expected, Decimal("20"), expected]
            assert [row[3] for row in versions] == ["run-0", "run-1", "run-2"]
            assert all(row[1] == row[4] for row in versions)
            assert [row[2] for row in versions] == [versions[1][1], versions[2][1], None]
            for cutoff, value in (
                (datetime(2026, 11, 1, tzinfo=timezone.utc), None),
                (versions[0][1], expected),
                (versions[1][1], Decimal("20")),
                (versions[2][1], expected),
            ):
                rows = connection.execute(sql.SQL("""
                    SELECT {} FROM {} WHERE {} AND known_from <= %s
                    AND (known_until > %s OR known_until IS NULL)
                """).format(sql.Identifier(value_column), relation, sql.SQL(category)), (cutoff, cutoff)).fetchall()
                assert rows == ([] if value is None else [(value,)])

        for metric, expected_rows in (
            ("demand", [(expected, "megawatthours")]),
            ("demand_forecast", [(Decimal("13"), "megawatthours")]),
            ("net_generation", [(Decimal("8"), "megawatthours")]),
            ("total_interchange", [(Decimal("-2"), "megawatthours")]),
        ):
            assert connection.execute(sql.SQL("""
                SELECT value, value_units FROM {} WHERE known_until IS NULL
            """).format(sql.Identifier(gold, f"electricity_{metric}_hourly_history"))).fetchall() == expected_rows
        assert connection.execute(sql.SQL("""
            SELECT fueltype, generation FROM {} WHERE known_until IS NULL ORDER BY fueltype
        """).format(sql.Identifier(gold, "generation_by_fuel_hourly_history"))).fetchall() == [
            ("BAT", Decimal("-7")), ("SUN", expected), ("WND", Decimal("80")),
        ]



def test_unmapped_fuel_code_fails_build(warehouse):
    dsn, source, _, run = warehouse
    with psycopg.connect(dsn) as connection:
        connection.execute(sql.SQL("""
            UPDATE {} SET fueltype = 'NEW' WHERE ingestion_run_id = 'run-0'
        """).format(sql.Identifier(source, "eia930_fuel_type_hourly_vintages")))
    result = run(select="+stg_eia930_fuel_type_hourly_vintages+")
    assert result.returncode != 0
    assert "FAIL" in result.stdout
    assert "accepted_values" in result.stdout


def test_metric_histories_preserve_provenance_and_independent_intervals(warehouse):
    dsn, source, target, run = warehouse
    expected = {}
    start = datetime(2026, 11, 2, tzinfo=timezone.utc)
    with psycopg.connect(dsn) as connection:
        table = sql.Identifier(source, "eia930_region_hourly_vintages")
        connection.execute(sql.SQL("DELETE FROM {}").format(table))
        for metric, code in (
            ("demand", "D"), ("demand_forecast", "DF"),
            ("net_generation", "NG"), ("total_interchange", "TI"),
        ):
            expected[metric] = []
            for respondent in ("NYIS", "CISO"):
                for hour in (0, 1):
                    period = datetime(2026, 11, 1, hour, tzinfo=timezone.utc)
                    for version, value in enumerate(("10.1234567890123456789", "20", None)):
                        observed = start + timedelta(hours=version)
                        inserted = observed + timedelta(seconds=1)
                        run_id = f"{code}-{respondent}-{hour}-{version}"
                        connection.execute(sql.SQL("""
                            INSERT INTO {} (period, respondent, respondent_name, type,
                                value, value_units, observed_at, inserted_at, ingestion_run_id)
                            VALUES (%s, %s, %s, %s, %s, 'megawatthours', %s, %s, %s)
                        """).format(table), (
                            f"2026-11-01T{hour:02}", respondent, respondent, code,
                            value, observed, inserted, run_id,
                        ))
                        expected[metric].append((
                            period, respondent, respondent, Decimal(value) if value else None,
                            "megawatthours", observed, inserted, run_id, observed,
                            observed + timedelta(hours=1) if version < 2 else None,
                        ))
    result = run()
    assert result.returncode == 0, result.stdout + result.stderr
    with psycopg.connect(dsn) as connection:
        for metric, rows in expected.items():
            result = connection.execute(sql.SQL("""
                SELECT * FROM {} ORDER BY respondent, period, observed_at
            """).format(sql.Identifier(f"{target}_gold", f"electricity_{metric}_hourly_history")))
            assert [column.name for column in result.description] == [
                "period", "respondent", "respondent_name", "value", "value_units",
                "observed_at", "inserted_at", "ingestion_run_id", "known_from", "known_until",
            ]
            assert result.fetchall() == sorted(rows, key=lambda row: (row[1], row[0], row[5]))


def test_gold_tied_timestamps_have_one_current_version(warehouse):
    dsn, source, target, run = warehouse
    with psycopg.connect(dsn) as connection:
        for settings in DATASETS.values():
            connection.execute(sql.SQL("""
                INSERT INTO {table} (period, respondent, {category}, value, observed_at, ingestion_run_id)
                SELECT period, respondent, {category}, '77', observed_at, 'tied'
                FROM {table} WHERE ingestion_run_id = 'run-4'
            """).format(
                table=sql.Identifier(source, settings["table"]),
                category=sql.Identifier(settings["category_column"]),
            ))
    result = run()
    assert result.returncode == 0, result.stdout + result.stderr
    with psycopg.connect(dsn) as connection:
        for history, value_column in (
            ("electricity_demand_hourly_history", "value"),
            ("generation_by_fuel_hourly_history", "generation"),
        ):
            assert connection.execute(sql.SQL("""
                SELECT {}, ingestion_run_id FROM {} WHERE known_from <= %s
                AND (known_until > %s OR known_until IS NULL)
            """).format(sql.Identifier(value_column), sql.Identifier(f"{target}_gold", history)), (
                datetime(2026, 11, 2, 4, tzinfo=timezone.utc),
                datetime(2026, 11, 2, 4, tzinfo=timezone.utc),
            )).fetchall() == [(Decimal("77"), "tied")]


def test_first_deployment_builds_empty_history_views(warehouse):
    dsn, source, target, run = warehouse
    with psycopg.connect(dsn) as connection:
        connection.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(source)))
        prepare_sources(connection, source)
        prepare_sources(connection, source)
    result = run()
    assert result.returncode == 0, result.stdout + result.stderr
    with psycopg.connect(dsn) as connection:
        names = connection.execute(
            "SELECT viewname FROM pg_views WHERE schemaname = %s", (f"{target}_gold",),
        ).fetchall()
        assert len(names) == 7
        for (name,) in names:
            assert connection.execute(sql.SQL("SELECT count(*) FROM {}").format(
                sql.Identifier(f"{target}_gold", name),
            )).fetchone() == (0,)


def test_nws_revisions_units_and_prediction_cutoff(warehouse):
    dsn, source, target, run = warehouse
    loader = PostgresIngestor(dsn)
    period = datetime(2026, 11, 1, 6, 30, tzinfo=timezone.utc)
    first = datetime(2026, 11, 1, 5, tzinfo=timezone.utc)
    common = dict(location="nyc", location_name="New York City", station_id="KJFK",
                  latitude=40.64, longitude=-73.78, period=period)
    for dataset in ("observations", "forecasts"):
        settings = nws.DATASETS[dataset]
        table = settings["table"]
        # Bronze stores source values as text, like EIA-930; dbt owns conversion.
        row = dict(common,
                   temperature_value="50", temperature_units="wmoUnit:degF", temperature_quality_code="V",
                   wind_speed_value="36", wind_speed_units="wmoUnit:km_h-1")
        if dataset == "observations":
            row.update(precipitation_last_hour_value="0.001", precipitation_last_hour_units="wmoUnit:m",
                       wind_direction_value="315", wind_direction_units="wmoUnit:degree_(angle)")
        else:
            row.update(period_end="2026-11-01T07:30:00Z", wind_direction="NW",
                       source_updated_at="2026-11-01T04:00:00Z")
        received = first + (timedelta(hours=2) if dataset == "observations" else timedelta())
        row["observed_at"] = received

        def load(record, run_id):
            return loader.ingest(
                [record], schema=source, table=table,
                key_columns=settings["key_columns"], ingestion_run_id=run_id,
                setup_sql=nws.source_table_sql(source, dataset),
            )

        assert load(row, "first") == 1
        assert load(dict(row, observed_at=received+timedelta(minutes=5)), "unchanged") == 0
        corrected = dict(row, temperature_value="68", observed_at=received+timedelta(minutes=15))
        assert load(corrected, "correction") == 1
        assert load(corrected, "correction") == 0
        assert load(dict(row, temperature_value=None, observed_at=received+timedelta(minutes=30)), "missing") == 1
    result = run()
    assert result.returncode == 0, result.stdout + result.stderr
    with psycopg.connect(dsn) as connection:
        for name in ("weather_observation_history", "weather_forecast_hourly_history"):
            relation = sql.Identifier(f"{target}_gold", name)
            rows = connection.execute(sql.SQL("""
                SELECT temperature_c, wind_speed_ms, temperature_quality_code,
                       known_from, known_until FROM {} ORDER BY known_from
            """).format(relation)).fetchall()
            assert [row[0] for row in rows] == [Decimal(10), Decimal(20), None]
            assert all(row[1] == Decimal(10) for row in rows)
            assert all(row[2] == "V" for row in rows)
            cutoff = rows[0][3] + timedelta(minutes=10)
            assert rows[0][4] == rows[1][3] and rows[-1][4] is None
            assert connection.execute(sql.SQL("""
                SELECT temperature_c FROM {} WHERE known_from <= %s
                  AND (known_until IS NULL OR %s < known_until)
            """).format(relation), (cutoff, cutoff)).fetchall() == [(Decimal(10),)]
            assert connection.execute(sql.SQL("SELECT count(*) FROM {} WHERE known_from <= %s").format(relation),
                                      (rows[0][3]-timedelta(seconds=1),)).fetchone() == (0,)
        for dataset in ("observations", "forecasts"):
            bronze = nws.DATASETS[dataset]["table"]
            silver = "weather_observation_vintages" if dataset == "observations" else "weather_forecast_hourly_vintages"
            gold = "weather_observation_history" if dataset == "observations" else "weather_forecast_hourly_history"

            def columns(schema, table):
                return dict(connection.execute("""
                    SELECT column_name, data_type FROM information_schema.columns
                    WHERE table_schema = %s AND table_name = %s
                """, (schema, table)).fetchall())

            raw_columns = columns(source, bronze)
            typed_columns = columns(target, silver)
            history_columns = columns(f"{target}_gold", gold)
            assert raw_columns["temperature_value"] == "text"
            assert typed_columns["temperature_c"] == "numeric"
            assert set(history_columns) - set(typed_columns) == {"known_from", "known_until"}
            all_columns = set(raw_columns) | set(typed_columns) | set(history_columns)
            assert not {"payload", "source_data"}.intersection(all_columns)
            if dataset == "forecasts":
                assert raw_columns["source_updated_at"] == "text"
                assert typed_columns["source_updated_at"] == "timestamp with time zone"
            else:
                assert "source_updated_at" not in raw_columns
        assert connection.execute(sql.SQL("SELECT source_updated_at, period_end FROM {} LIMIT 1").format(
            sql.Identifier(f"{target}_gold", "weather_forecast_hourly_history"))).fetchone() == (
                datetime(2026, 11, 1, 4, tzinfo=timezone.utc),
                datetime(2026, 11, 1, 7, 30, tzinfo=timezone.utc),
            )
        assert connection.execute(sql.SQL("SELECT precipitation_last_hour_mm FROM {} LIMIT 1").format(
            sql.Identifier(f"{target}_gold", "weather_observation_history"))).fetchone() == (Decimal(1),)
        connection.execute(sql.SQL("UPDATE {} SET temperature_units = 'unknown'").format(
            sql.Identifier(source, "nws_observation_vintages")))
    result = run(select="+stg_nws_observation_vintages+")
    assert result.returncode != 0
    assert "nws_observations_valid_inputs" in result.stdout
    with psycopg.connect(dsn) as connection:
        connection.execute(sql.SQL("UPDATE {} SET temperature_units = 'wmoUnit:degF'").format(
            sql.Identifier(source, "nws_observation_vintages")))
    for table, column, bad_value, restore_value in (
        ("nws_observation_vintages", "temperature_value", "not-a-number", None),
        ("nws_forecast_hourly_vintages", "source_updated_at", "not-a-time", "2026-11-01T04:00:00Z"),
        ("nws_forecast_hourly_vintages", "period_end", "not-a-time", "2026-11-01T07:30:00Z"),
    ):
        query = sql.SQL("UPDATE {} SET {} = %s").format(sql.Identifier(source, table), sql.Identifier(column))
        with psycopg.connect(dsn) as connection:
            connection.execute(query, (bad_value,))
        result = run(select="source:nws")
        assert result.returncode != 0, result.stdout + result.stderr
        assert "valid_inputs" in result.stdout
        with psycopg.connect(dsn) as connection:
            connection.execute(query, (restore_value,))
