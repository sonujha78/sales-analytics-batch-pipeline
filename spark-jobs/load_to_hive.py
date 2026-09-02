"""
Load stage: reads aggregated region rollup data and loads it into a
Hive table, partitioned by date (and region for the region_product
breakdown), so analysts can query with Hive SQL.

Usage: spark-submit load_to_hive.py <date>   e.g. load_to_hive.py 2026-09-01
"""
import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def main():
    if len(sys.argv) < 2:
        print("Usage: load_to_hive.py <date YYYY-MM-DD>")
        sys.exit(1)

    run_date = sys.argv[1]
    spark = SparkSession.builder \
        .appName("LoadToHive-{}".format(run_date)) \
        .config("hive.metastore.uris", "thrift://hive-metastore:9083") \
        .enableHiveSupport() \
        .getOrCreate()

    agg_base = "/data/aggregated/{}".format(run_date)

    # ---------- Create Hive tables if they don't exist (partitioned by date) ----------
    spark.sql("""
        CREATE TABLE IF NOT EXISTS sales_by_region (
            region STRING,
            total_revenue DOUBLE,
            total_units INT,
            total_transactions INT
        )
        PARTITIONED BY (txn_date STRING)
        STORED AS PARQUET
    """)

    spark.sql("""
        CREATE TABLE IF NOT EXISTS sales_by_region_product (
            region STRING,
            product_name STRING,
            total_revenue DOUBLE,
            total_units INT
        )
        PARTITIONED BY (txn_date STRING)
        STORED AS PARQUET
    """)

    # ---------- Read aggregated CSVs ----------
    by_region = spark.read.option("header", "true").csv("{}/by_region".format(agg_base)) \
        .withColumn("total_revenue", F.col("total_revenue").cast("double")) \
        .withColumn("total_units", F.col("total_units").cast("int")) \
        .withColumn("total_transactions", F.col("total_transactions").cast("int"))

    by_region_product = spark.read.option("header", "true").csv("{}/by_region_product".format(agg_base)) \
        .withColumn("total_revenue", F.col("total_revenue").cast("double")) \
        .withColumn("total_units", F.col("total_units").cast("int"))

    # ---------- Idempotent load: dynamic partition overwrite (re-running for
    # the same date replaces only that date's partition, never duplicates) ----------
    spark.conf.set("hive.exec.dynamic.partition", "true")
    spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")

    by_region.createOrReplaceTempView("tmp_by_region")
    spark.sql("""
        INSERT OVERWRITE TABLE sales_by_region PARTITION (txn_date='{}')
        SELECT region, total_revenue, total_units, total_transactions FROM tmp_by_region
    """.format(run_date))

    by_region_product.createOrReplaceTempView("tmp_by_region_product")
    spark.sql("""
        INSERT OVERWRITE TABLE sales_by_region_product PARTITION (txn_date='{}')
        SELECT region, product_name, total_revenue, total_units FROM tmp_by_region_product
    """.format(run_date))

    print("=" * 60)
    print("LOAD SUMMARY for {}".format(run_date))
    count1 = spark.sql("SELECT COUNT(*) FROM sales_by_region WHERE txn_date='{}'".format(run_date)).collect()[0][0]
    count2 = spark.sql("SELECT COUNT(*) FROM sales_by_region_product WHERE txn_date='{}'".format(run_date)).collect()[0][0]
    print("sales_by_region rows loaded: {}".format(count1))
    print("sales_by_region_product rows loaded: {}".format(count2))
    print("=" * 60)

    spark.stop()

if __name__ == "__main__":
    main()
