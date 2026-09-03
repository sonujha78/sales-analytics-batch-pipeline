# Data Quality Gate Trigger Test

## Objective
Prove the pipeline halts before Load when more than 5% of a day's
rows fail validation.

## Method
1. Injected 5,000 additional rows into `raw_sales` in MySQL, all with
   `region = NULL` (on top of the naturally-occurring messy data).
2. Total MySQL rows: 51,032 -> 56,032.
3. Triggered the Airflow DAG (`manual__2026-09-03T11:16:57+00:00`).

## Result
VALIDATION SUMMARY for 2026-09-03
MySQL: total=56032, valid=47479, rejected=7558
CSV: total=8000, valid=8000, rejected=0
TOTAL: total=64032, rejected=7558, reject_pct=11.80%
DATA QUALITY GATE FAILED: 11.80% rejected (threshold: 5%)


- `ingest_mysql`, `ingest_csv`: SUCCESS
- `validate`: FAILED (exit code 1, application_1788424443883_0019)
- `transform`, `aggregate`, `load_to_hive`: never executed (state: None)
  -- correctly blocked by the upstream validate failure.

## Conclusion
The data quality gate in `validate.py` (`sys.exit(1)` when
reject_pct > 5.0) successfully propagates as an Airflow task failure,
which prevents all downstream tasks in the DAG from running. The
warehouse is never loaded with a day whose data quality is below
threshold.
