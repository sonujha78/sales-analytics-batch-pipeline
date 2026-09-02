"""
Transform stage: reads validated MySQL + CSV data, standardizes date
formats, handles negative quantities (take absolute value, flag them),
deduplicates, and joins both sources into one unified schema.

Usage: spark-submit transform.py <date>   e.g. transform.py 2026-09-01
"""
import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

def main():
    if len(sys.argv) < 2:
        print("Usage: transform.py <date YYYY-MM-DD>")
        sys.exit(1)

    run_date = sys.argv[1]
    spark = SparkSession.builder.appName("Transform-{}".format(run_date)).getOrCreate()

    mysql_path = "/data/validated/mysql/{}".format(run_date)
    csv_path = "/data/validated/csv/{}".format(run_date)
    output_path = "/data/transformed/{}".format(run_date)

    # ---------- Load validated sources ----------
    mysql_df = spark.read.option("header", "true").csv(mysql_path)
    csv_df = spark.read.option("header", "true").csv(csv_path)

    # ---------- Standardize MySQL dates (4 mixed formats) ----------
    # formats seen: %Y-%m-%d, %d/%m/%Y, %m-%d-%Y, %d-%b-%Y
    mysql_std = mysql_df.withColumn(
        "std_date",
        F.coalesce(
            F.to_date("order_date", "yyyy-MM-dd"),
            F.to_date("order_date", "dd/MM/yyyy"),
            F.to_date("order_date", "MM-dd-yyyy"),
            F.to_date("order_date", "dd-MMM-yyyy")
        )
    )

    # ---------- Handle negative quantities: take absolute value, flag ----------
    mysql_std = mysql_std.withColumn("quantity", F.col("quantity").cast("int")) \
        .withColumn("unit_price", F.col("unit_price").cast("double")) \
        .withColumn("was_negative_qty", F.col("quantity") < 0) \
        .withColumn("quantity", F.abs(F.col("quantity")))

    # ---------- Unify schema: MySQL side ----------
    mysql_unified = mysql_std.select(
        F.col("order_id").alias("txn_id"),
        F.col("customer_id").cast("int").alias("customer_id"),
        F.col("product_name").alias("product_name"),
        F.col("region").alias("region"),
        F.col("quantity").alias("quantity"),
        F.col("unit_price").alias("unit_price"),
        F.col("std_date").alias("txn_date"),
        F.col("was_negative_qty").alias("was_negative_qty"),
        F.lit("mysql").alias("source_channel")
    )

    # ---------- Unify schema: CSV side ----------
    csv_std = csv_df.withColumn("std_date", F.to_date("txn_date", "yyyy-MM-dd")) \
        .withColumn("qty", F.col("qty").cast("int")) \
        .withColumn("price", F.col("price").cast("double")) \
        .withColumn("was_negative_qty", F.col("qty") < 0) \
        .withColumn("qty", F.abs(F.col("qty")))

    csv_unified = csv_std.select(
        F.col("txn_id").alias("txn_id"),
        F.col("cust_id").cast("int").alias("customer_id"),
        F.col("item").alias("product_name"),
        F.col("region").alias("region"),
        F.col("qty").alias("quantity"),
        F.col("price").alias("unit_price"),
        F.col("std_date").alias("txn_date"),
        F.col("was_negative_qty").alias("was_negative_qty"),
        F.lit("csv").alias("source_channel")
    )

    # ---------- Union both sources ----------
    unified = mysql_unified.unionByName(csv_unified)

    # ---------- Deduplicate (idempotency: same txn_id + source_channel = same record) ----------
    unified_dedup = unified.dropDuplicates(["txn_id", "source_channel"])

    total_before = unified.count()
    total_after = unified_dedup.count()
    negative_fixed = unified_dedup.filter(F.col("was_negative_qty") == True).count()

    print("=" * 60)
    print("TRANSFORM SUMMARY for {}".format(run_date))
    print("Rows before dedup: {}".format(total_before))
    print("Rows after dedup:  {}".format(total_after))
    print("Negative quantities fixed (abs'd): {}".format(negative_fixed))
    print("=" * 60)

    # ---------- Write unified, transformed output (overwrite = idempotent) ----------
    unified_dedup.write.mode("overwrite").option("header", "true").csv(output_path)

    print("Transform complete. Output written to {}".format(output_path))
    spark.stop()

if __name__ == "__main__":
    main()
