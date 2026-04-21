"""
TaaSim — Spark ML: Feature Engineering
========================================
Reads curated Porto trips from s3a://curated/trips/ and builds a
feature matrix for demand forecasting.

Target: trip requests per zone per 30-min slot
Features: hour, day_of_week, is_weekend, zone_id, slot_of_day,
          demand_lag_1d, demand_lag_7d, rolling_7d_mean

Output: s3a://mldata/features/ (Parquet)

Usage (inside Spark container):
  /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --deploy-mode client \
    /opt/spark-jobs/feature_engineering.py
"""

import logging
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DoubleType

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [FEAT] %(levelname)s  %(message)s",
)
log = logging.getLogger("FEAT")

INPUT_PATH = "s3a://curated/trips/"
OUTPUT_PATH = "s3a://mldata/features/"
ZONE_MAPPING_PATH = "/opt/data/zone_mapping.csv"


def build_spark():
    return (
        SparkSession.builder
        .appName("TaaSim-Feature-Engineering")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )


def main():
    log.info("=== Feature Engineering Starting ===")
    spark = build_spark()

    # ── 1. Read curated trips ────────────────────────────────────────
    log.info("Reading curated trips from %s", INPUT_PATH)
    trips = spark.read.parquet(INPUT_PATH)
    total = trips.count()
    log.info("Total curated trips: %d", total)

    # ── 2. Compute 30-min slot and date ──────────────────────────────
    # Each trip has: event_time (timestamp), origin_zone, hour_of_day, day_of_week, is_weekend
    log.info("Computing 30-min demand slots...")
    trips_slotted = (
        trips
        .withColumn("trip_date", F.to_date("event_time"))
        .withColumn("slot_of_day",
                     (F.col("hour_of_day") * 2 +
                      F.when(F.minute("event_time") >= 30, 1).otherwise(0))
                     .cast(IntegerType()))
    )

    # ── 3. Aggregate: demand per zone per date per slot ──────────────
    log.info("Aggregating demand per zone/date/slot...")
    demand = (
        trips_slotted
        .groupBy("origin_zone", "trip_date", "slot_of_day")
        .agg(
            F.count("*").alias("demand"),
            F.countDistinct("taxi_id").alias("supply"),
            F.first("day_of_week").alias("day_of_week"),
            F.first("is_weekend").alias("is_weekend"),
        )
    )

    # Compute hour from slot
    demand = demand.withColumn("hour_of_day",
                               (F.col("slot_of_day") / 2).cast(IntegerType()))

    demand_count = demand.count()
    log.info("Demand rows (zone × date × slot): %d", demand_count)

    # ── 4. Lag features ──────────────────────────────────────────────
    log.info("Computing lag features (1-day, 7-day) and rolling 7d mean...")

    # Window: per zone per slot, ordered by date
    w_zone_slot = (
        Window
        .partitionBy("origin_zone", "slot_of_day")
        .orderBy("trip_date")
    )

    # lag_1d: same zone, same slot, 1 day ago
    # lag_7d: same zone, same slot, 7 days ago
    demand_lagged = (
        demand
        .withColumn("demand_lag_1d", F.lag("demand", 1).over(w_zone_slot))
        .withColumn("demand_lag_7d", F.lag("demand", 7).over(w_zone_slot))
    )

    # Rolling 7-day mean: average of the last 7 days for same zone+slot
    w_rolling = (
        Window
        .partitionBy("origin_zone", "slot_of_day")
        .orderBy("trip_date")
        .rowsBetween(-7, -1)
    )
    demand_lagged = demand_lagged.withColumn(
        "rolling_7d_mean", F.avg("demand").over(w_rolling)
    )

    # ── 5. Supply/demand ratio ───────────────────────────────────────
    demand_lagged = demand_lagged.withColumn(
        "supply_demand_ratio",
        F.when(F.col("demand") > 0,
               F.col("supply").cast(DoubleType()) / F.col("demand"))
        .otherwise(F.lit(0.0))
    )

    # ── 6. Is peak hour feature ──────────────────────────────────────
    demand_lagged = demand_lagged.withColumn(
        "is_peak",
        F.when(F.col("hour_of_day").isin([8, 9, 13, 14, 17, 18]), 1).otherwise(0)
    )

    # ── 7. Drop rows with null lag features (first 7 days) ──────────
    features = demand_lagged.filter(
        F.col("demand_lag_1d").isNotNull() &
        F.col("demand_lag_7d").isNotNull() &
        F.col("rolling_7d_mean").isNotNull()
    )
    feature_count = features.count()
    log.info("Feature rows after lag filter: %d (dropped %d with null lags)",
             feature_count, demand_count - feature_count)

    # ── 7b. Join zone reference attributes ───────────────────────────
    log.info("Joining zone attributes (population_density, zone_type)...")
    zones_ref = (
        spark.read
        .option("header", "true")
        .csv(ZONE_MAPPING_PATH)
        .select(
            F.col("zone_id").cast(IntegerType()).alias("zm_zone_id"),
            F.col("population_density").cast(DoubleType()).alias("population_density"),
            F.col("zone_type"),
        )
    )
    features = (
        features
        .join(F.broadcast(zones_ref), F.col("origin_zone") == F.col("zm_zone_id"), "left")
        .drop("zm_zone_id")
    )

    # ── 8. Select final feature columns ──────────────────────────────
    feature_matrix = features.select(
        "origin_zone",
        "trip_date",
        "slot_of_day",
        "hour_of_day",
        "day_of_week",
        "is_weekend",
        "is_peak",
        "demand",           # target variable
        "supply",
        "supply_demand_ratio",
        "demand_lag_1d",
        "demand_lag_7d",
        "rolling_7d_mean",
        "population_density",
        "zone_type",
    )

    # ── 9. Write feature matrix ──────────────────────────────────────
    log.info("Writing feature matrix to %s", OUTPUT_PATH)
    (
        feature_matrix
        .repartition(4)
        .write
        .mode("overwrite")
        .parquet(OUTPUT_PATH)
    )

    # ── 10. Summary ─────────────────────────────────────────────────
    log.info("=== Feature Engineering Summary ===")
    log.info("  Input trips:     %d", total)
    log.info("  Demand rows:     %d", demand_count)
    log.info("  Feature rows:    %d", feature_count)
    try:
        written = spark.read.parquet(OUTPUT_PATH)
        log.info("  Zones:           %d", written.select("origin_zone").distinct().count())
        date_row = written.agg(F.min("trip_date"), F.max("trip_date")).collect()[0]
        log.info("  Date range:      %s to %s", date_row[0], date_row[1])
        avg_row = written.agg(F.avg("demand")).collect()[0]
        log.info("  Avg demand/slot: %.1f", avg_row[0])
        written.describe("demand", "demand_lag_1d", "demand_lag_7d",
                         "rolling_7d_mean", "supply_demand_ratio").show()
    except Exception as e:  # noqa: BLE001
        log.warning("  Post-write stats skipped: %s", e)

    log.info("  Output: %s", OUTPUT_PATH)
    log.info("=== Feature Engineering Complete ===")
    spark.stop()


if __name__ == "__main__":
    main()
