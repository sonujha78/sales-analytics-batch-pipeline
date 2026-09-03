# Final Consolidated Report — Sales Analytics Batch Pipeline

**Stack:** Apache Hadoop (HDFS) + Apache Spark (on YARN) + Apache Hive + Apache Sqoop + Apache Airflow
**DAG:** `sales_analytics_batch_pipeline` — `Ingest → Validate → Transform → Aggregate → Load`, scheduled `@daily`

This document summarizes every requirement from the task brief, what was
built, and where the supporting evidence lives in this `docs/` folder.

---

## 1. Environment

A multi-node Hadoop cluster (NameNode + 2 DataNodes), YARN (ResourceManager +
NodeManager), Hive (server + metastore on Postgres), MySQL (simulated
operational source), Sqoop, Spark, and Airflow (webserver + scheduler on
Postgres) all run as separate Docker containers — no Kubernetes, no
microservices, as specified.

## 2. Pipeline Stages — Implemented & Verified

| Stage | What it does | Evidence |
|---|---|---|
| **Ingest** | Sqoop imports `raw_sales` from MySQL into `/data/raw/mysql/`; daily CSV drop moved into `/data/raw/csv/` | [`dag_graph_screenshot.png`](dag_graph_screenshot.png) — `ingest_mysql` + `ingest_csv` run in parallel |
| **Validate** | Spark job checks schema, nulls, duplicates; quarantines bad rows to `/data/rejected/` with a reason instead of dropping/crashing | [`data_quality_gate_test.md`](data_quality_gate_test.md) |
| **Transform** | Spark job standardizes dates, fixes negative quantities, dedupes, joins MySQL + CSV into one schema | [`failure_recovery_test.md`](failure_recovery_test.md) |
| **Aggregate** | Spark computes daily rollups: revenue by region, top products | [`analyst_queries.md`](analyst_queries.md) |
| **Load** | Cleaned, aggregated data written to Hive tables partitioned by `txn_date` | [`idempotency_test.md`](idempotency_test.md), [`partitioning_design.md`](partitioning_design.md) |

DAG dependency graph (Airflow UI, Graph view) confirms the correct order and
the parallel dual-ingest fan-in before `validate`:
`ingest_mysql` + `ingest_csv` → `validate` → `transform` → `aggregate` → `load_to_hive`.

## 3. Requirements That Make This Hard — How Each Was Met

### 3.1 Idempotency
Re-running the DAG for an already-processed date must not double-count data.
**Verified:** re-ran `2026-09-03` end-to-end; row counts and total revenue
were byte-identical before and after (`5` / `50` rows, `141,274,526.22`
revenue, unchanged). Achieved via `INSERT OVERWRITE TABLE ... PARTITION
(txn_date='<date>')`, which replaces the partition rather than appending.
→ [`idempotency_test.md`](idempotency_test.md)

### 3.2 Partitioning Strategy
Hive tables (`sales_by_region`, `sales_by_region_product`) are partitioned by
`txn_date` only. `region` was deliberately **not** used as a second
partition column — its low cardinality (5 fixed values) combined with daily
date partitioning would cause the Hadoop small-file problem. Region-level
speed is instead achieved via pre-aggregation; bucketing on `region` is
recommended if per-region file pruning is ever needed at larger scale.
→ [`partitioning_design.md`](partitioning_design.md)

Query performance was measured partitioned vs. non-partitioned: the
partitioned table's `EXPLAIN` plan shows Hive pruning to the single matching
`txn_date=.../` HDFS directory (no post-scan filter needed), while the flat
table does a full scan followed by a `Filter Operator`. Wall-clock time was
~17% faster on this small test dataset (14.29s vs 17.28s); the gap is
expected to widen substantially at production scale where pruning skips
entire files/directories rather than a handful of rows.
→ [`partition_performance_test.md`](partition_performance_test.md)

### 3.3 Failure Recovery
The production `transform` Spark job was killed mid-run on YARN
(`yarn application -kill`) to simulate a real crash. Airflow correctly
marked the task `up_for_retry`, downstream tasks (`aggregate`,
`load_to_hive`) stayed in `None` state (no half-updated warehouse), and the
automatic retry (per `retries=2`, exponential backoff) succeeded. Final
aggregated `total_transactions` (55,479) was **less than** raw input
(59,032) — consistent with normal validation rejection, not the ~2x
inflation that would indicate double-processing from the retry.
→ [`failure_recovery_test.md`](failure_recovery_test.md)

### 3.4 Data Quality Gate
5,000 rows with `region = NULL` were injected into MySQL, pushing the
rejection rate to 11.80% (threshold: 5%). The `validate` task correctly
exited non-zero, Airflow marked it `FAILED`, and `transform`, `aggregate`,
`load_to_hive` never ran — the warehouse was never loaded with bad data.
→ [`data_quality_gate_test.md`](data_quality_gate_test.md)

## 4. Orchestration (Airflow)

- DAG scheduled `@daily` (not manual-only); task dependencies correctly
  modeled as shown in the graph.
- Retries configured via `default_args`: `retries=2`,
  `retry_delay=timedelta(minutes=2)`, `retry_exponential_backoff=True`,
  `max_retry_delay=timedelta(minutes=10)` — exercised live in the failure
  recovery test above.
- **Alert on failure:** `on_failure_callback=alert_on_failure` fires when a
  task exhausts retries, writing a timestamped line to
  `/opt/airflow/logs/ALERTS.log`. Verified with an isolated test DAG
  (`test_alert_callback`) whose only task always fails; the alert log
  confirmed the callback fired correctly. A duplicate-`dag_id` pitfall
  (importing the callback re-executed the whole production DAG module) was
  found and fixed by duplicating the callback locally in the test file
  instead of importing it.
  → [`alert_on_failure_test.md`](alert_on_failure_test.md)

## 5. Querying / Output

Two representative analyst queries run successfully against the partitioned
Hive tables:
- **Region-wise revenue, rolling 7-day window** — top region: North
  (₹57.0M), spread across all 5 regions is tight (~₹1.2M range), consistent
  with the synthetic data generator.
- **Top 10 products this month** — led by Widget B (₹29.08M revenue,
  114,360 units).
→ [`analyst_queries.md`](analyst_queries.md)

## 6. Documentation Checklist (from task brief)

| Requirement | Status | Location |
|---|---|---|
| DAG graph screenshot (Airflow UI) | ✅ | [`dag_graph_screenshot.png`](dag_graph_screenshot.png) |
| Evidence of data quality gate triggering and blocking a bad run | ✅ | [`data_quality_gate_test.md`](data_quality_gate_test.md) |
| Failure-recovery test (killed job → retry → correct final state) | ✅ | [`failure_recovery_test.md`](failure_recovery_test.md) |
| Query performance comparison (partitioned vs. not) | ✅ | [`partition_performance_test.md`](partition_performance_test.md) |
| Idempotent rerun evidence | ✅ | [`idempotency_test.md`](idempotency_test.md) |
| Partitioning design rationale | ✅ | [`partitioning_design.md`](partitioning_design.md) |
| Analyst SQL queries with results | ✅ | [`analyst_queries.md`](analyst_queries.md) |
| Alert-on-failure mechanism, tested | ✅ | [`alert_on_failure_test.md`](alert_on_failure_test.md) |

## 7. Summary

Every hard requirement in the task brief — idempotent reruns, partition
design with a measured performance justification, mid-pipeline failure
recovery without data corruption, a data quality gate that blocks bad loads,
daily orchestration with retries, and failure alerting — was implemented in
the DAG and independently exercised with a real test, not just asserted.
All evidence is captured in this `docs/` folder alongside this report.
