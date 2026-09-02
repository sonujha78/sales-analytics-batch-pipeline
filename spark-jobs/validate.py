"""
Validate stage: reads raw MySQL + CSV data, checks for nulls in required
fields and duplicates, quarantines bad rows to /data/rejected/ with a
reason, and enforces a data quality gate (fail if >5% rows rejected).

Usage: spark-submit validate.py <date>   e.g. validate.py 2026-09-01
"""
import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def main():
    if len(sys.argv) < 2:
        print("Usage: validate.py <date YYYY-MM-DD>")
        sys.exit(1)

    run_date = sys.argv[1]
    spark = SparkSession.builder.appName("Validate-{}".format(run_date)).getOrCreate()

    mysql_path = "/data/raw/mysql/{}".format(run_date)
    csv_path = "/data/raw/csv/{}".format(run_date)
    rejected_path = "/data/rejected/{}".format(run_date)
    valid_mysql_path = "/data/validated/mysql/{}".format(run_date)
    valid_csv_path = "/data/validated/csv/{}".format(run_date)

    # ---------- MySQL source ----------
    mysql_schema_cols = ["order_id", "customer_id", "product_name", "region",
                         "quantity", "unit_price", "order_date", "created_at"]
    mysql_df = spark.read.csv(mysql_path, sep=",", inferSchema=True) \
        .toDF(*mysql_schema_cols)

    mysql_total = mysql_df.count()

    # null checks on required fields
    mysql_null_mask = F.col("customer_id").isNull() | F.col("region").isNull() \
        | F.col("order_id").isNull() | F.col("product_name").isNull() \
        | (F.trim(F.lower(F.col("region"))).isin("null", "\\n")) \
        | (F.trim(F.lower(F.col("customer_id"))).isin("null", "\\n"))
    mysql_bad_nulls = mysql_df.filter(mysql_null_mask) \
        .withColumn("reject_reason", F.lit("null_required_field"))

    mysql_ok_nulls = mysql_df.filter(~mysql_null_mask)

    # duplicate check (same order_id appearing more than once)
    dup_ids = mysql_ok_nulls.groupBy("order_id").count().filter(F.col("count") > 1) \
        .select("order_id")
    mysql_dupes = mysql_ok_nulls.join(dup_ids, "order_id", "inner") \
        .dropDuplicates(["order_id"]) \
        .withColumn("reject_reason", F.lit("duplicate_order_id"))
    mysql_valid = mysql_ok_nulls.join(dup_ids, "order_id", "left_anti")

    mysql_rejected = mysql_bad_nulls.select(*(mysql_schema_cols + ["reject_reason"])) \
        .unionByName(
            mysql_dupes.select(*(mysql_schema_cols + ["reject_reason"]))
        )

    mysql_rejected_count = mysql_rejected.count()
    mysql_valid_count = mysql_valid.count()

    # ---------- CSV source ----------
    csv_schema_cols = ["txn_id", "cust_id", "item", "region", "qty", "price", "txn_date"]
    csv_df = spark.read.option("header", "true").csv(csv_path) \
        .toDF(*csv_schema_cols)

    csv_total = csv_df.count()

    csv_null_mask = F.col("cust_id").isNull() | F.col("region").isNull() \
        | F.col("txn_id").isNull() | F.col("item").isNull()
    csv_bad_nulls = csv_df.filter(csv_null_mask) \
        .withColumn("reject_reason", F.lit("null_required_field"))
    csv_ok_nulls = csv_df.filter(~csv_null_mask)

    csv_dup_ids = csv_ok_nulls.groupBy("txn_id").count().filter(F.col("count") > 1) \
        .select("txn_id")
    csv_dupes = csv_ok_nulls.join(csv_dup_ids, "txn_id", "inner") \
        .dropDuplicates(["txn_id"]) \
        .withColumn("reject_reason", F.lit("duplicate_txn_id"))
    csv_valid = csv_ok_nulls.join(csv_dup_ids, "txn_id", "left_anti")

    csv_rejected = csv_bad_nulls.select(*(csv_schema_cols + ["reject_reason"])) \
        .unionByName(csv_dupes.select(*(csv_schema_cols + ["reject_reason"])))

    csv_rejected_count = csv_rejected.count()
    csv_valid_count = csv_valid.count()

    # ---------- Data quality gate ----------
    total_rows = mysql_total + csv_total
    total_rejected = mysql_rejected_count + csv_rejected_count
    reject_pct = (float(total_rejected) / total_rows * 100) if total_rows > 0 else 0

    print("=" * 60)
    print("VALIDATION SUMMARY for {}".format(run_date))
    print("MySQL:  total={}, valid={}, rejected={}".format(mysql_total, mysql_valid_count, mysql_rejected_count))
    print("CSV:    total={}, valid={}, rejected={}".format(csv_total, csv_valid_count, csv_rejected_count))
    print("TOTAL:  total={}, rejected={}, reject_pct={:.2f}%".format(total_rows, total_rejected, reject_pct))
    print("=" * 60)

    # write rejected rows regardless of gate outcome (for inspection/audit)
    mysql_rejected.write.mode("overwrite").option("header", "true").csv("{}/mysql".format(rejected_path))
    csv_rejected.write.mode("overwrite").option("header", "true").csv("{}/csv".format(rejected_path))

    if reject_pct > 5.0:
        print("DATA QUALITY GATE FAILED: {:.2f}% rejected (threshold: 5%)".format(reject_pct))
        spark.stop()
        sys.exit(1)  # non-zero exit -> Airflow task fails, DAG must not proceed to Load

    # gate passed -> write valid data for the Transform stage
    mysql_valid.write.mode("overwrite").option("header", "true").csv(valid_mysql_path)
    csv_valid.write.mode("overwrite").option("header", "true").csv(valid_csv_path)

    print("DATA QUALITY GATE PASSED: {:.2f}% rejected (threshold: 5%)".format(reject_pct))
    spark.stop()

if __name__ == "__main__":
    main()
