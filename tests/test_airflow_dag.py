"""Run with unittest inside the Airflow image, or pytest when Airflow is installed."""

import importlib.util
import subprocess
import unittest
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch


@unittest.skipUnless(importlib.util.find_spec("airflow"), "Airflow is installed in Docker")
class TestNWSDag(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        path = Path(__file__).resolve().parents[1] / "dags/nws_ingestion.py"
        spec = importlib.util.spec_from_file_location("nws_ingestion", path)
        cls.module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(cls.module)
        cls.dag = cls.module.nws_dag

    def test_independent_branches_and_no_catchup(self):
        self.assertFalse(self.dag.catchup)
        self.assertEqual(self.dag.max_active_runs, 1)
        self.assertEqual(len(self.dag.tasks), 6)
        for dataset in ("observations", "forecasts"):
            bronze = self.dag.get_task(f"bronze_{dataset}")
            silver = self.dag.get_task(f"silver_{dataset}")
            gold = self.dag.get_task(f"gold_{dataset}")
            self.assertEqual(bronze.upstream_task_ids, set())
            self.assertEqual(bronze.downstream_task_ids, {silver.task_id})
            self.assertEqual(silver.upstream_task_ids, {bronze.task_id})
            self.assertEqual(silver.downstream_task_ids, {gold.task_id})
            self.assertEqual(gold.upstream_task_ids, {silver.task_id})
            self.assertEqual(gold.downstream_task_ids, set())
            model = {
                "observations": "stg_nws_observation_vintages",
                "forecasts": "stg_nws_forecast_hourly_vintages",
            }[dataset]
            self.assertIn(f"--select +{model}", silver.bash_command)
            self.assertNotIn(f"--select +{model}+", silver.bash_command)
            self.assertIn(f"--select {model}+ --exclude {model}", gold.bash_command)
            for task in (silver, gold):
                self.assertEqual(task.trigger_rule, "all_success")
                self.assertIn("{{ task.task_id }}", task.env["DBT_TARGET_PATH"])

    def test_task_passes_observation_window(self):
        with patch("data_platform.sources.nws.NWSClient") as factory:
            self.module.ingest_nws(
                "observations", {"observation_lookback_days": 7},
                dag_run=SimpleNamespace(dag_id="nws_ingestion"),
                run_id="manual__test", ti=SimpleNamespace(try_number=2),
            )
            factory.return_value.__enter__.return_value.ingest.assert_called_once_with(
                "observations", schema="bronze", observation_lookback_days=7,
                ingestion_run_id="nws_ingestion/manual__test/attempt-2",
            )


@unittest.skipUnless(importlib.util.find_spec("airflow"), "Airflow is installed in Docker")
class TestEIA930Dag(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        path = Path(__file__).resolve().parents[1] / "dags/eia930_ingestion.py"
        spec = importlib.util.spec_from_file_location("eia930_ingestion", path)
        cls.module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(cls.module)
        cls.dag = cls.module.eia930_dag

    def call_task(self, start=None, end=None, respondents=None):
        with patch("data_platform.sources.eia930.EIA930Client") as factory:
            client = factory.return_value.__enter__.return_value
            client.ingest.return_value = 3
            count = self.module.ingest_eia930(
                "region-data",
                params={"start": start, "end": end, "respondents": respondents or []},
                dag_run=SimpleNamespace(
                    dag_id="eia930_ingestion",
                    run_after=datetime(2026, 11, 23, 15, 37, tzinfo=timezone.utc),
                ),
                run_id="manual__test", ti=SimpleNamespace(try_number=1),
            )
            self.assertEqual(count, 3)
            return client.ingest.call_args.kwargs

    def test_independent_bronze_silver_gold_branches(self):
        from airflow.providers.standard.operators.bash import BashOperator
        from airflow.providers.standard.operators.python import PythonOperator

        self.assertEqual(len(self.dag.tasks), 6)
        for name in ("region_data", "fuel_type_data"):
            bronze = self.dag.get_task(f"bronze_{name}")
            silver = self.dag.get_task(f"silver_{name}")
            gold = self.dag.get_task(f"gold_{name}")
            self.assertIsInstance(bronze, PythonOperator)
            self.assertEqual(bronze.upstream_task_ids, set())
            self.assertEqual(bronze.downstream_task_ids, {silver.task_id})
            self.assertEqual(silver.upstream_task_ids, {bronze.task_id})
            self.assertEqual(silver.downstream_task_ids, {gold.task_id})
            self.assertEqual(gold.upstream_task_ids, {silver.task_id})
            self.assertEqual(gold.downstream_task_ids, set())
            for task in (silver, gold):
                self.assertIsInstance(task, BashOperator)
                self.assertEqual(task.trigger_rule, "all_success")
                self.assertIn("{{ task.task_id }}", task.env["DBT_TARGET_PATH"])
            model = f"stg_eia930_{name.removesuffix('_data')}_hourly_vintages"
            self.assertIn(f"--select +{model}", silver.bash_command)
            self.assertNotIn(f"--select +{model}+", silver.bash_command)
            self.assertIn(f"--select {model}+ --exclude {model}", gold.bash_command)
        self.assertFalse(self.dag.catchup)
        self.assertEqual(self.dag.max_active_runs, 1)

    def test_default_window_is_hour_aligned_and_stable(self):
        first, retry = self.call_task(), self.call_task()
        self.assertEqual(first, retry)
        self.assertEqual(first["start"], datetime(2026, 11, 20, 15, tzinfo=timezone.utc))
        self.assertEqual(first["end"], datetime(2026, 11, 25, 15, tzinfo=timezone.utc))
        self.assertIsNone(first["respondents"])

    def test_build_completion_is_logged_only_on_success(self):
        for name in ("region_data", "fuel_type_data"):
            for layer in ("silver", "gold"):
                task = self.dag.get_task(f"{layer}_{name}")
                for command, expected_code in (("true", 0), ("false", 1)):
                    script = task.bash_command.replace("/opt/data-platform/.venv/bin/dbt", command)
                    result = subprocess.run(["bash", "-c", script], capture_output=True, text=True)
                    self.assertEqual(result.returncode, expected_code)
                    self.assertIn("Starting dbt build:", result.stdout)
                    self.assertEqual("Completed dbt build:" in result.stdout, expected_code == 0)

    def test_manual_window_and_stable_run_id(self):
        kwargs = self.call_task("2026-11-01T00:00:00Z", "2026-11-02T00:00:00Z", ["NYIS"])
        self.assertEqual(kwargs, {
            "schema": "bronze",
            "start": datetime(2026, 11, 1, tzinfo=timezone.utc),
            "end": datetime(2026, 11, 2, tzinfo=timezone.utc),
            "respondents": ["NYIS"],
            "ingestion_run_id": "eia930_ingestion/manual__test/attempt-1",
        })

    def test_partial_manual_window_fails(self):
        with self.assertRaises(ValueError):
            self.call_task(start="2026-11-01T00:00:00Z")
