"""
Aggregate stage: reads transformed data, computes daily rollups --
total sales by region, top products, revenue trends. Output is
partitioned by date and region for Hive query performance.

Usage: spark-submit aggregate.py <date>   e.g. aggregate.py 2026-09-01
"""
import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def main():
    if len(sys.argv) < 2:
        print("Usage: aggregate.py <date YYYY-MM-DD>")
        sys.exit(1)

    run_date = sys.argv[1]
    spark = SparkSession.builder.appName("Aggregate-{}".format(run_date)).getOrCreate()

    input_path = "/data/transformed/{}".format(run_date)
    output_base = "/data/aggregated/{}".format(run_date)

    df = spark.read.option("header", "true").csv(input_path)
    df = df.withColumn("quantity", F.col("quantity").cast("int")) \
           .withColumn("unit_price", F.col("unit_price").cast("double")) \
           .withColumn("revenue", F.col("quantity") * F.col("unit_price"))

    # ---------- Region-level daily rollup ----------
    region_rollup = df.groupBy("region").agg(
        F.sum("revenue").alias("total_revenue"),
        F.sum("quantity").alias("total_units"),
        F.count("*").alias("total_transactions")
    ).withColumn("txn_date", F.lit(run_date))

    # ---------- Top products ----------
    product_rollup = df.groupBy("product_name").agg(
        F.sum("revenue").alias("total_revenue"),
        F.sum("quantity").alias("total_units_sold")
    ).withColumn("txn_date", F.lit(run_date)) \
     .orderBy(F.col("total_revenue").desc())

    # ---------- Region + product breakdown (for partition-pruning demo later) ----------
    region_product_rollup = df.groupBy("region", "product_name").agg(
        F.sum("revenue").alias("total_revenue"),
        F.sum("quantity").alias("total_units")
    ).withColumn("txn_date", F.lit(run_date))

    total_revenue_all = df.agg(F.sum("revenue")).collect()[0][0]
    total_txns_all = df.count()

    print("=" * 60)
    print("AGGREGATE SUMMARY for {}".format(run_date))
    print("Total transactions: {}".format(total_txns_all))
    print("Total revenue: {:.2f}".format(total_revenue_all if total_revenue_all else 0))
    print("=" * 60)

    region_rollup.coalesce(1).write.mode("overwrite").option("header", "true") \
        .csv("{}/by_region".format(output_base))
    product_rollup.coalesce(1).write.mode("overwrite").option("header", "true") \
        .csv("{}/top_products".format(output_base))
    region_product_rollup.coalesce(1).write.mode("overwrite").option("header", "true") \
        .csv("{}/by_region_product".format(output_base))

    print("Aggregate complete. Output written to {}".format(output_base))
    spark.stop()

if __name__ == "__main__":
    main()
