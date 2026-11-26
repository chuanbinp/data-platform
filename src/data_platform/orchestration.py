"""Airflow helpers shared by source pipelines; imported only by DAGs."""

from airflow.providers.standard.operators.bash import BashOperator


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
