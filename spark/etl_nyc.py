"""
TaaSim — Spark ETL: NYC TLC Trip Records
==========================================
Reads 3 months of NYC TLC Yellow Taxi Parquet from MinIO,
computes per-zone-per-hour demand aggregates, and writes
results to s3a://curated/nyc-demand/.

This job demonstrates Spark batch processing at scale
(~10M rows/month) and produces demand-curve reference data.

Usage (inside Spark container):
  /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --deploy-mode client \
    /opt/spark-jobs/etl_nyc.py
"""

import logging
from functools import reduce
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DoubleType

logging.basicConfig(level=logging.INFO, format="%(asctime)s [ETL-NYC] %(levelname)s %(message)s")
log = logging.getLogger("ETL-NYC")

INPUT_PATH = "s3a://raw/nyc-tlc/"
OUTPUT_PATH = "s3a://curated/nyc-demand/"

# NYC TLC Parquet files to process
NYC_FILES = [
    "yellow_tripdata_2023-01.parquet",
    "yellow_tripdata_2023-02.parquet",
    "yellow_tripdata_2023-03.parquet",
]

# Common column names — cast to widest compatible types
# Handles: VendorID (INT vs BIGINT), passenger_count (DOUBLE vs INT64),
#          RatecodeID (DOUBLE vs BIGINT), PULocationID (INT vs BIGINT),
#          Airport_fee vs airport_fee (case drift)
COMMON_COLS = [
    ("VendorID", "long"),
    ("tpep_pickup_datetime", None),
    ("tpep_dropoff_datetime", None),
    ("passenger_count", "long"),
    ("trip_distance", "double"),
    ("RatecodeID", "long"),
    ("store_and_fwd_flag", None),
    ("PULocationID", "long"),
    ("DOLocationID", "long"),
    ("payment_type", "long"),
    ("fare_amount", "double"),
    ("extra", "double"),
    ("mta_tax", "double"),
    ("tip_amount", "double"),
    ("tolls_amount", "double"),
    ("improvement_surcharge", "double"),
    ("total_amount", "double"),
    ("congestion_surcharge", "double"),
    ("airport_fee", "double"),
]


def build_spark():
    return (
        SparkSession.builder
        .appName("TaaSim-ETL-NYC")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )


def read_and_normalize(spark, path):
    """Read a single Parquet file and normalize column names/types."""
    df = spark.read.parquet(path)
    # Handle Airport_fee vs airport_fee column name drift
    if "Airport_fee" in df.columns:
        df = df.withColumnRenamed("Airport_fee", "airport_fee")
    cols = []
    for name, cast_type in COMMON_COLS:
        c = F.col(name)
        if cast_type:
            c = c.cast(cast_type)
        cols.append(c.alias(name))
    return df.select(cols)


def main():
    log.info("=== TaaSim NYC TLC ETL Starting ===")
    spark = build_spark()
    log.info("SparkSession created: %s", spark.sparkContext.applicationId)

    # ── 1. Read NYC TLC Parquet (per-file to handle schema drift) ────
    log.info("Reading NYC TLC Parquet files from %s", INPUT_PATH)
    dfs = []
    for fname in NYC_FILES:
        fpath = INPUT_PATH + fname
        log.info("  Reading %s", fname)
        dfs.append(read_and_normalize(spark, fpath))

    raw_df = reduce(DataFrame.unionByName, dfs)
    raw_count = raw_df.count()
    log.info("Raw rows (all months): %d", raw_count)

    # Print schema for reference
    raw_df.printSchema()

    # ── 2. Clean and filter ──────────────────────────────────────────
    cleaned = (
        raw_df
        .filter(F.col("tpep_pickup_datetime").isNotNull())
        .filter(F.col("tpep_dropoff_datetime").isNotNull())
        .filter(F.col("trip_distance") > 0)
        .filter(F.col("trip_distance") < 100)  # filter outliers >100mi
        .filter(F.col("fare_amount") > 0)
        .filter(F.col("fare_amount") < 500)    # filter outlier fares
        .filter(F.col("passenger_count") > 0)
        .filter(F.col("PULocationID").isNotNull())
        .filter(F.col("DOLocationID").isNotNull())
    )
    clean_count = cleaned.count()
    log.info("After cleaning: %d (removed %d)", clean_count, raw_count - clean_count)

    # ── 3. Extract temporal features ─────────────────────────────────
    with_features = (
        cleaned
        .withColumn("pickup_hour", F.hour("tpep_pickup_datetime"))
        .withColumn("pickup_date", F.to_date("tpep_pickup_datetime"))
        .withColumn("day_of_week", F.dayofweek("tpep_pickup_datetime"))  # 1=Sunday
        .withColumn("is_weekend",
                     F.when(F.dayofweek("tpep_pickup_datetime").isin(1, 7), 1).otherwise(0))
        .withColumn("year_month",
                     F.date_format("tpep_pickup_datetime", "yyyy-MM"))
        .withColumn("trip_duration_sec",
                     (F.unix_timestamp("tpep_dropoff_datetime") -
                      F.unix_timestamp("tpep_pickup_datetime")).cast(IntegerType()))
    )

    # ── 4. Compute per-zone per-hour demand aggregates ───────────────
    log.info("Computing per-zone per-hour demand aggregates...")

    demand_by_zone_hour = (
        with_features
        .groupBy("PULocationID", "pickup_hour", "pickup_date", "day_of_week", "is_weekend")
        .agg(
            F.count("*").alias("trip_count"),
            F.avg("trip_distance").alias("avg_distance"),
            F.avg("fare_amount").alias("avg_fare"),
            F.avg("trip_duration_sec").alias("avg_duration_sec"),
            F.sum("passenger_count").alias("total_passengers"),
        )
        .withColumnRenamed("PULocationID", "pickup_zone_id")
    )

    demand_count = demand_by_zone_hour.count()
    log.info("Demand aggregation rows: %d", demand_count)

    # ── 5. Compute overall KPIs ──────────────────────────────────────
    log.info("Computing overall KPIs...")

    # Hourly demand curve (average trips per hour across all zones)
    hourly_curve = (
        demand_by_zone_hour
        .groupBy("pickup_hour")
        .agg(
            F.avg("trip_count").alias("avg_trips_per_zone"),
            F.sum("trip_count").alias("total_trips"),
        )
        .orderBy("pickup_hour")
    )

    log.info("=== Hourly Demand Curve ===")
    hourly_curve.show(24, False)

    # Top zones by total demand
    top_zones = (
        demand_by_zone_hour
        .groupBy("pickup_zone_id")
        .agg(F.sum("trip_count").alias("total_trips"))
        .orderBy(F.desc("total_trips"))
    )

    log.info("=== Top 20 Pickup Zones ===")
    top_zones.show(20, False)

    # ── 6. Write demand aggregates to curated ────────────────────────
    log.info("Writing demand aggregates to %s", OUTPUT_PATH)
    (
        demand_by_zone_hour
        .repartition(4)
        .write
        .mode("overwrite")
        .parquet(OUTPUT_PATH)
    )

    # ── 7. Summary ───────────────────────────────────────────────────
    log.info("=== NYC ETL Summary ===")
    log.info("  Raw rows:         %d", raw_count)
    log.info("  Cleaned rows:     %d", clean_count)
    log.info("  Demand agg rows:  %d", demand_count)
    log.info("  Output:           %s", OUTPUT_PATH)
    log.info("  Avg trip distance: %.2f mi",
             with_features.agg(F.avg("trip_distance")).collect()[0][0])
    log.info("  Avg fare:          $%.2f",
             with_features.agg(F.avg("fare_amount")).collect()[0][0])
    log.info("  Avg duration:      %.0f sec",
             with_features.agg(F.avg("trip_duration_sec")).collect()[0][0])

    log.info("=== NYC ETL Complete ===")
    spark.stop()


if __name__ == "__main__":
    main()
