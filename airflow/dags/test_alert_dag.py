"""
Standalone DAG used only to verify the alert_on_failure callback writes
to /opt/airflow/logs/ALERTS.log when a task exhausts its retries and fails.
Not part of the production pipeline.
"""
from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator


def alert_on_failure(context):
    """Same logic as sales_pipeline_dag.alert_on_failure, duplicated here
    so this test file never imports (and re-executes) the production DAG module."""
    with open("/opt/airflow/logs/ALERTS.log", "a") as f:
        f.write(
            f"[ALERT] DAG {context['dag'].dag_id} task "
            f"{context['task_instance'].task_id} failed at {context['ts']}\n"
        )


with DAG(
    dag_id="test_alert_callback",
    schedule_interval=None,
    start_date=datetime(2026, 9, 1),
    catchup=False,
    default_args={
        "retries": 0,
        "on_failure_callback": alert_on_failure,
    },
    tags=["test"],
) as dag:
    fail_task = BashOperator(
        task_id="always_fails",
        bash_command="exit 1",
    )
