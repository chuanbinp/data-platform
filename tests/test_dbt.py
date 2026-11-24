"""Integration checks for dbt; requires TEST_DATABASE_URL and the dbt group."""

import os
import subprocess
import sys
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from uuid import uuid4

import psycopg
import pytest
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict

from data_platform.ingestor import PostgresIngestor
from data_platform.sources.eia930 import DATASETS, _vintage_table_sql

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
        for settings in DATASETS.values():
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
                    setup_sql=_vintage_table_sql(source, table, category),
                )
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
        ("eia930_region_data", "electricity_region_hourly_vintages"),
        ("eia930_fuel_type_data", "electricity_fuel_hourly_vintages"),
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
        # A missing latest value must not fall back to an older non-null version.
        assert connection.execute(sql.SQL("SELECT demand, demand_forecast FROM {}").format(
            sql.Identifier(f"{target}_gold", "electricity_hourly_latest"),
        )).fetchall() == [(None, None)]
        assert connection.execute(sql.SQL("SELECT solar_generation, wind_generation, solar_units FROM {}").format(
            sql.Identifier(f"{target}_gold", "generation_by_fuel_hourly_latest"),
        )).fetchall() == [(None, None, "megawatthours")]


@pytest.mark.parametrize("dataset,mart", [
    ("region-data", "electricity_hourly_latest"),
    ("fuel-type-data", "generation_by_fuel_hourly_latest"),
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
    other = "eia930_fuel_type_data" if dataset == "region-data" else "eia930_region_data"
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


def test_gold_as_of_history_and_latest_pivot(warehouse):
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
            """).format(sql.Identifier(source, "eia930_region_data")), [
                ("DF", "12", datetime(2026, 11, 2, tzinfo=timezone.utc), "forecast-first"),
                ("DF", "13", datetime(2026, 11, 2, 3, tzinfo=timezone.utc), "forecast-revised"),
                ("NG", "8", datetime(2026, 11, 2, tzinfo=timezone.utc), "generation"),
                ("TI", "-2", datetime(2026, 11, 2, tzinfo=timezone.utc), "interchange"),
            ])
        connection.execute(sql.SQL("""
            INSERT INTO {} (period, respondent, fueltype, value, value_units, observed_at, ingestion_run_id)
            VALUES ('2026-11-01T00', 'NYIS', 'WND', '80', 'megawatthours', %s, 'wind')
        """).format(sql.Identifier(source, "eia930_fuel_type_data")), (
            datetime(2026, 11, 2, tzinfo=timezone.utc),
        ))
        connection.execute(sql.SQL("""
            INSERT INTO {} (period, respondent, fueltype, value, value_units, observed_at, ingestion_run_id)
            VALUES ('2026-11-01T00', 'NYIS', 'BAT', '-7', 'megawatthours', %s, 'battery')
        """).format(sql.Identifier(source, "eia930_fuel_type_data")), (
            datetime(2026, 11, 2, tzinfo=timezone.utc),
        ))

    result = run()
    assert result.returncode == 0, result.stdout + result.stderr
    expected = Decimal("10.1234567890123456789")
    with psycopg.connect(dsn) as connection:
        for history, category, value_column in (
            ("electricity_metrics_history", "metric = 'D'", "value"),
            ("generation_by_fuel_history", "fueltype = 'SUN'", "generation"),
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

        assert connection.execute(sql.SQL("""
            SELECT demand, demand_forecast, net_generation, total_interchange, demand_units
            FROM {}
        """).format(sql.Identifier(gold, "electricity_hourly_latest"))).fetchall() == [
            (expected, Decimal("13"), Decimal("8"), Decimal("-2"), "megawatthours"),
        ]
        assert connection.execute(sql.SQL("""
            SELECT solar_generation, solar_units, wind_generation, wind_units,
                   battery_generation, coal_generation
            FROM {}
        """).format(sql.Identifier(gold, "generation_by_fuel_hourly_latest"))).fetchall() == [
            (expected, "megawatthours", Decimal("80"), "megawatthours", Decimal("-7"), None),
        ]


def test_unmapped_fuel_code_fails_build(warehouse):
    dsn, source, _, run = warehouse
    with psycopg.connect(dsn) as connection:
        connection.execute(sql.SQL("""
            UPDATE {} SET fueltype = 'NEW' WHERE ingestion_run_id = 'run-0'
        """).format(sql.Identifier(source, "eia930_fuel_type_data")))
    result = run(select="+stg_eia930_fuel_type_data+")
    assert result.returncode != 0
    assert "FAIL" in result.stdout
    assert "accepted_values" in result.stdout


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
            ("electricity_metrics_history", "value"),
            ("generation_by_fuel_history", "generation"),
        ):
            assert connection.execute(sql.SQL("""
                SELECT {}, ingestion_run_id FROM {} WHERE known_from <= %s
                AND (known_until > %s OR known_until IS NULL)
            """).format(sql.Identifier(value_column), sql.Identifier(f"{target}_gold", history)), (
                datetime(2026, 11, 2, 4, tzinfo=timezone.utc),
                datetime(2026, 11, 2, 4, tzinfo=timezone.utc),
            )).fetchall() == [(Decimal("77"), "tied")]
