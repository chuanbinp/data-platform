"""Collect NWS revisions and build independent observation/forecast histories."""

import os
from datetime import datetime, timedelta, timezone

from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG, Param

from data_platform.orchestration import dbt_build


def ingest_nws(dataset, params, dag_run, run_id, ti):
    from data_platform.sources.nws import NWSClient

    with NWSClient() as client:
        return client.ingest(
            dataset,
            schema=os.getenv("DBT_BRONZE_SCHEMA", "bronze"),
            observation_lookback_days=params["observation_lookback_days"],
            ingestion_run_id=f"{dag_run.dag_id}/{run_id}/attempt-{ti.try_number}",
        )


with DAG(
    dag_id="nws_ingestion",
    schedule="*/15 * * * *",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=2),
        "execution_timeout": timedelta(minutes=20),
    },
    params={"observation_lookback_days": Param(2, type="integer", minimum=1, maximum=7)},
    tags=["nws", "bronze", "silver", "gold"],
) as nws_dag:
    for dataset, model in {
        "observations": "stg_nws_observation_vintages",
        "forecasts": "stg_nws_forecast_hourly_vintages",
    }.items():
        bronze = PythonOperator(
            task_id=f"bronze_{dataset}",
            python_callable=ingest_nws,
            op_kwargs={"dataset": dataset},
        )
        silver = dbt_build(f"silver_{dataset}", select=f"+{model}")
        gold = dbt_build(f"gold_{dataset}", select=f"{model}+", exclude=model)
        bronze >> silver >> gold
