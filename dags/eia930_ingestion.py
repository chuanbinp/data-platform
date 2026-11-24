"""Poll EIA-930 and retain changed Bronze observations."""

import logging
from datetime import datetime, timedelta, timezone

from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG, Param

logger = logging.getLogger(__name__)


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

    # HTTPX's INFO messages include EIA's query-string API key.
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logger.info(
        "Ingesting %s: start=%s (inclusive), end=%s (exclusive)", dataset, start, end,
    )
    with EIA930Client() as client:
        return client.ingest(
            dataset,
            start=start,
            end=end,
            respondents=params["respondents"] or None,
            ingestion_run_id=f"{dag_run.dag_id}/{run_id}",
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
            description="Use a whole UTC hour, e.g. 2026-11-01T00:00:00Z. Leave both dates empty for the rolling window.",
        ),
        "end": Param(
            None, type=["null", "string"], format="date-time",
            title="End time (exclusive)",
            description="Use a whole UTC hour, e.g. 2026-11-02T00:00:00Z. Provide both dates or leave both empty.",
        ),
        "respondents": Param([], type="array", items={"type": "string"}),
    },
    tags=["eia930", "bronze"],
) as eia930_dag:
    PythonOperator(
        task_id="ingest_region_data",
        python_callable=ingest_eia930,
        op_kwargs={"dataset": "region-data"},
    )
    PythonOperator(
        task_id="ingest_fuel_type_data",
        python_callable=ingest_eia930,
        op_kwargs={"dataset": "fuel-type-data"},
    )
