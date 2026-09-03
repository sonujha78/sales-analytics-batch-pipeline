# Idempotency Test — 2026-09-03

## Objective
Verify that re-running the pipeline for a date that was already
processed does NOT duplicate data in the Hive warehouse.

## Method
1. Ran the full DAG (Ingest -> Validate -> Transform -> Aggregate -> Load)
   for 2026-09-03 via a manual trigger.
2. Recorded baseline row counts and total revenue in Hive.
3. Cleared and re-ran the SAME date via `airflow dags backfill --reset-dagruns`.
4. Re-checked row counts and total revenue.

## Results

| Metric                          | Before re-run    | After re-run      | Match |
|----------------------------------|-------------------|--------------------|-------|
| sales_by_region row count        | 5                 | 5                  | Yes   |
| sales_by_region_product row count| 50                | 50                 | Yes   |
| sales_by_region total_revenue    | 141,274,526.22    | 141,274,526.22     | Yes   |

## Why it works
`load_to_hive.py` uses dynamic-partition `INSERT OVERWRITE TABLE ...
PARTITION (txn_date='<date>')`, which replaces the entire partition's
contents on every run rather than appending. Re-running for the same
date is therefore safe and produces byte-identical results.
