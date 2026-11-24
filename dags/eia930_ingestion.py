"""Ingest EIA-930 vintages and build each dataset's Silver and Gold models."""

from datetime import datetime, timedelta, timezone

from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG, Param


def ingest_eia930(dataset, params, dag_run, run_id):
    from data_platform.sources.eia930 import EIA930Client

    start, end = params["start"], params["end"]
    if (start is None) != (end is None):
        raise ValueError("Provide both start and end, or leave both empty")
    if start is not None:
        start, end = datetime.fromisoformat(start), datetime.fromisoformat(end)
    else:
        anchor = dag_run.run_after.astimezone(timezone.utc)
        anchor = anchor.replace(minute=0, second=0, microsecond=0)
        start, end = anchor - timedelta(hours=72), anchor + timedelta(hours=48)

    with EIA930Client() as client:
        return client.ingest(
            dataset,
            start=start,
            end=end,
            respondents=params["respondents"] or None,
            ingestion_run_id=f"{dag_run.dag_id}/{run_id}",
        )


def dbt_build(task_id, select, exclude=None):
    command = f"/opt/data-platform/.venv/bin/dbt --no-use-colors build --select {select}"
    if exclude:
        command += f" --exclude {exclude}"
    return BashOperator(
        task_id=task_id,
        bash_command=f"""
            set -e
            echo "Starting dbt build: {task_id}"
            {command}
            echo "Completed dbt build: {task_id}"
        """,
        env={
            "DBT_TARGET_PATH": "/tmp/dbt/{{ task.task_id }}/target",
            "DBT_LOG_PATH": "/tmp/dbt/{{ task.task_id }}/logs",
        },
        append_env=True,
        do_xcom_push=False,
    )


with DAG(
    dag_id="eia930_ingestion",
    schedule="*/15 * * * *",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=2),
        "execution_timeout": timedelta(minutes=20),
    },
    params={
        "start": Param(
            None, type=["null", "string"], format="date-time",
            title="Start time (inclusive)",
            description="Whole UTC hour. Leave both dates empty for the rolling window.",
        ),
        "end": Param(
            None, type=["null", "string"], format="date-time",
            title="End time (exclusive)",
            description="Whole UTC hour, later than start. This hour is excluded.",
        ),
        "respondents": Param([], type="array", items={"type": "string"}),
    },
    tags=["eia930", "bronze", "silver", "gold"],
) as eia930_dag:
    for dataset in ("region-data", "fuel-type-data"):
        name = dataset.replace("-", "_")
        model = f"stg_eia930_{name}"
        bronze = PythonOperator(
            task_id=f"bronze_{name}",
            python_callable=ingest_eia930,
            op_kwargs={"dataset": dataset},
        )
        silver = dbt_build(f"silver_{name}", select=f"+{model}")
        gold = dbt_build(f"gold_{name}", select=f"{model}+", exclude=model)
        bronze >> silver >> gold
