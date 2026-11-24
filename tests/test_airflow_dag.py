"""Run with unittest inside the Airflow image, or pytest when Airflow is installed."""

import importlib.util
import unittest
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch


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
                run_id="manual__test",
            )
            self.assertEqual(count, 3)
            return client.ingest.call_args.kwargs

    def test_python_operators_can_run_in_parallel(self):
        from airflow.providers.standard.operators.python import PythonOperator

        self.assertEqual(set(self.dag.task_ids), {"ingest_region_data", "ingest_fuel_type_data"})
        self.assertTrue(all(isinstance(task, PythonOperator) for task in self.dag.tasks))
        for task in self.dag.tasks:
            self.assertEqual(task.upstream_task_ids, set())
            self.assertEqual(task.downstream_task_ids, set())
        self.assertFalse(self.dag.catchup)
        self.assertEqual(self.dag.max_active_runs, 1)

    def test_default_window_is_hour_aligned_and_stable(self):
        first, retry = self.call_task(), self.call_task()
        self.assertEqual(first, retry)
        self.assertEqual(first["start"], datetime(2026, 11, 20, 15, tzinfo=timezone.utc))
        self.assertEqual(first["end"], datetime(2026, 11, 25, 15, tzinfo=timezone.utc))
        self.assertIsNone(first["respondents"])

    def test_manual_window_and_stable_run_id(self):
        kwargs = self.call_task("2026-11-01T00:00:00Z", "2026-11-02T00:00:00Z", ["NYIS"])
        self.assertEqual(kwargs, {
            "start": datetime(2026, 11, 1, tzinfo=timezone.utc),
            "end": datetime(2026, 11, 2, tzinfo=timezone.utc),
            "respondents": ["NYIS"],
            "ingestion_run_id": "eia930_ingestion/manual__test",
        })

    def test_partial_manual_window_fails(self):
        with self.assertRaises(ValueError):
            self.call_task(start="2026-11-01T00:00:00Z")
