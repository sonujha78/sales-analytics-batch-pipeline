# Alert-on-Failure Mechanism — Test & Verification

## Objective
Verify that the `on_failure_callback` (`alert_on_failure`) configured on the
production DAG's tasks actually fires when a task exhausts its retries and
fails, and that it writes a usable alert record to
`/opt/airflow/logs/ALERTS.log`.

## Setup
A standalone test DAG (`test_alert_callback`) was created with:
- `retries: 0` so the task fails immediately on first attempt
- `on_failure_callback: alert_on_failure` — same callback logic used in the
  production DAG (`sales_pipeline_dag.py`), duplicated locally in the test
  file instead of imported, to avoid re-executing the production DAG module
  and colliding with its `dag_id` at parse time
- A single `BashOperator` task (`always_fails`) running `exit 1`, guaranteed
  to fail every run

This isolates the alert mechanism from the rest of the pipeline so it can be
tested independently and repeatably.

## Issue Found During Testing
Initial attempt imported `alert_on_failure` directly from
`sales_pipeline_dag.py`:

```python
from sales_pipeline_dag import alert_on_failure
```

This caused Airflow's DagBag parser to execute the entire production DAG
file as a side effect of the import, which redefined the same `dag_id`
(`sales_analytics_batch_pipeline`) in two files. Airflow raised
`AirflowDagDuplicatedIdException` and silently dropped the production DAG
from being loaded — a serious risk if left unnoticed.

**Fix:** duplicated the callback function locally inside the test DAG file
instead of importing it, so the production module is never re-executed.

## Test Execution

```bash
docker exec -u airflow -it airflow-scheduler airflow dags reserialize
docker exec -u airflow -it airflow-scheduler airflow dags list-import-errors   # confirmed: no errors
docker exec -u airflow -it airflow-scheduler airflow dags unpause test_alert_callback
docker exec -u airflow -it airflow-scheduler airflow dags trigger test_alert_callback
docker exec -u airflow -it airflow-scheduler cat /opt/airflow/logs/ALERTS.log
```

## Result — Alert Fired Successfully
[ALERT] DAG test_alert_callback task always_fails failed at 2026-09-03T17:11:08+00:00


The task run confirmed as `failed`, and the callback correctly appended a
timestamped alert line to `ALERTS.log`, proving the alert mechanism works
end-to-end: task failure → callback triggered → alert persisted.

## Conclusion
`on_failure_callback` on the production DAG is verified working. When a
task in `sales_pipeline_dag.py` exhausts its configured retries and fails,
the same mechanism will log an alert to `/opt/airflow/logs/ALERTS.log`
rather than failing silently.

*Cleanup: the standalone `test_alert_dag.py` was removed from `airflow/dags/`
after verification to keep the production DAG folder clean; this document
serves as the evidence record.*
