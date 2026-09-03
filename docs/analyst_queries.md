# Analyst Queries

Demonstrates the kind of ad-hoc SQL business analysts can run against the
warehouse once the pipeline has loaded data into Hive.

## 1. Region-wise revenue (last 7 days)
```sql
SELECT region, SUM(total_revenue) as revenue
FROM sales_by_region
WHERE txn_date >= date_sub('2026-09-03', 7)
GROUP BY region
ORDER BY revenue DESC;
```

| region  | revenue        |
|---------|----------------|
| North   | 57,026,010.75  |
| South   | 56,875,391.40  |
| West    | 56,613,992.92  |
| East    | 56,565,174.56  |
| Central | 55,804,463.87  |

(Result set currently spans the 3 days of data loaded so far; the query is
written against a rolling 7-day window so it will automatically reflect a
full week once the DAG has run daily for 7+ days.)

## 2. Top 10 products this month
```sql
SELECT product_name, SUM(total_revenue) as revenue, SUM(total_units) as units
FROM sales_by_region_product
WHERE txn_date >= '2026-09-01'
GROUP BY product_name
ORDER BY revenue DESC
LIMIT 10;
```

| product_name | revenue       | units   |
|--------------|---------------|---------|
| Widget B     | 29,082,751.35 | 114,360 |
| Gizmo Lite   | 28,881,500.74 | 115,608 |
| Gizmo Pro    | 28,698,293.27 | 112,847 |
| MegaKit      | 28,576,546.85 | 113,864 |
| Widget A     | 28,255,883.39 | 112,389 |
| Gadget X     | 28,239,610.08 | 112,845 |
| Gadget Y     | 27,988,323.10 | 109,387 |
| Doohickey    | 27,968,939.41 | 111,410 |
| SuperTool    | 27,895,851.17 | 111,024 |
| Thingamajig  | 27,297,333.88 | 108,833 |

Both queries execute successfully against the partitioned Hive tables
(`sales_by_region`, `sales_by_region_product`) populated by the
`load_to_hive` stage of the DAG.
