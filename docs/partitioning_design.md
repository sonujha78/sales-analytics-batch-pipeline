# Partitioning Design Decision

## What's implemented
Both warehouse tables (`sales_by_region`, `sales_by_region_product`) are
partitioned by `txn_date` only.

## Why region was not used as a second partition column
The task brief suggests considering a second partition dimension such as
region. `region` was deliberately **not** added as a partition column,
for the following reason:

- `region` has very low cardinality (5 fixed values: North, South, East,
  West, Central).
- Partitioning on a low-cardinality column alongside a high-cardinality
  one (date) multiplies the number of partition directories
  (`dates × 5`), each holding a small slice of data. In Hadoop this leads
  to the well-known **small-file problem**: many tiny files per partition,
  which increases NameNode metadata overhead and can *hurt* query planning
  and MapReduce/Spark task-scheduling overhead rather than help it,
  especially as the table grows over months of daily runs.
- `txn_date` is the dimension analysts filter on most often ("last 7 days",
  "this month"), and it has natural, ever-growing cardinality that suits
  partitioning well — each partition stays a reasonably sized, self-
  contained unit of daily data.

## What was done instead for the region dimension
Region-level filtering and aggregation is already fast without a second
partition because:
1. Regional aggregates are pre-computed by the `aggregate` stage into
   `sales_by_region` (grouped by region + date) and
   `sales_by_region_product` (grouped by region + product + date) — so
   analyst queries filtering/grouping by region scan an already-small,
   pre-aggregated table rather than the raw fact-level data.
2. If per-region file-level pruning became necessary at larger scale, the
   recommended approach is **bucketing** on `region`
   (`CLUSTERED BY (region) INTO 5 BUCKETS`) rather than partitioning —
   bucketing gives join/aggregation benefits on a fixed, known set of
   values without the small-file explosion that partitioning would cause.

## Conclusion
Partitioning by `txn_date` alone, combined with pre-aggregation by region,
satisfies the query patterns analysts need (date-range and region-wise
queries) while avoiding the small-file anti-pattern that a
`(txn_date, region)` composite partition scheme would introduce.
