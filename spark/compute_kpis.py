"""
TaaSim — Spark Analytics: Weekly KPIs
======================================
Reads curated Porto trips from s3a://curated/trips/ and computes
analytics KPIs: trips per zone, avg duration, peak hours, coverage gaps.
Writes KPI results to s3a://curated/kpis/.

Usage (inside Spark container):
  /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --deploy-mode client \
    /opt/spark-jobs/compute_kpis.py
"""

import logging
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType

logging.basicConfig(level=logging.INFO, format="%(asctime)s [KPI] %(levelname)s %(message)s")
log = logging.getLogger("KPI")

INPUT_PATH = "s3a://curated/trips/"
OUTPUT_PATH = "s3a://curated/kpis/"


def build_spark():
    return (
        SparkSession.builder
        .appName("TaaSim-KPI-Analytics")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )


def main():
    log.info("=== TaaSim KPI Analytics Starting ===")
    spark = build_spark()

    # ── 1. Read curated trips ────────────────────────────────────────
    log.info("Reading curated trips from %s", INPUT_PATH)
    trips = spark.read.parquet(INPUT_PATH)
    total = trips.count()
    log.info("Total curated trips: %d", total)

    # ── 2. KPI: Trips per zone ───────────────────────────────────────
    log.info("Computing trips per zone...")
    trips_per_zone = (
        trips
        .groupBy("origin_zone", "origin_zone_name")
        .agg(
            F.count("*").alias("trip_count"),
            F.avg("trip_duration_sec").alias("avg_duration_sec"),
            F.avg("trip_distance_km").alias("avg_distance_km"),
            F.countDistinct("taxi_id").alias("unique_taxis"),
        )
        .orderBy(F.desc("trip_count"))
    )

    log.info("=== Trips per Zone ===")
    trips_per_zone.show(20, False)

    # Write trips per zone
    trips_per_zone.coalesce(1).write.mode("overwrite").parquet(OUTPUT_PATH + "trips_per_zone/")

    # ── 3. KPI: Hourly demand curve ──────────────────────────────────
    log.info("Computing hourly demand curve...")
    hourly_demand = (
        trips
        .groupBy("hour_of_day")
        .agg(
            F.count("*").alias("trip_count"),
            F.avg("trip_duration_sec").alias("avg_duration_sec"),
        )
        .orderBy("hour_of_day")
    )

    log.info("=== Hourly Demand Curve ===")
    hourly_demand.show(24, False)
    hourly_demand.coalesce(1).write.mode("overwrite").parquet(OUTPUT_PATH + "hourly_demand/")

    # ── 4. KPI: Peak demand hours (top 5) ────────────────────────────
    peak_hours = hourly_demand.orderBy(F.desc("trip_count")).limit(5)
    log.info("=== Peak Hours ===")
    peak_hours.show(5, False)

    # ── 5. KPI: Day-of-week patterns ─────────────────────────────────
    log.info("Computing day-of-week patterns...")
    daily_pattern = (
        trips
        .groupBy("day_of_week", "is_weekend")
        .agg(
            F.count("*").alias("trip_count"),
            F.avg("trip_duration_sec").alias("avg_duration_sec"),
        )
        .orderBy("day_of_week")
    )

    log.info("=== Day-of-Week Pattern ===")
    daily_pattern.show(7, False)
    daily_pattern.coalesce(1).write.mode("overwrite").parquet(OUTPUT_PATH + "daily_pattern/")

    # ── 6. KPI: Zone-hour heatmap data ───────────────────────────────
    log.info("Computing zone-hour heatmap...")
    zone_hour = (
        trips
        .groupBy("origin_zone", "origin_zone_name", "hour_of_day")
        .agg(F.count("*").alias("trip_count"))
        .orderBy("origin_zone", "hour_of_day")
    )
    zone_hour.repartition(1).write.mode("overwrite").parquet(OUTPUT_PATH + "zone_hour_heatmap/")

    # ── 7. KPI: Coverage gaps ────────────────────────────────────────
    log.info("Computing coverage gaps (zones with demand but few taxis)...")
    coverage = (
        trips
        .groupBy("origin_zone", "origin_zone_name", "hour_of_day")
        .agg(
            F.count("*").alias("demand"),
            F.countDistinct("taxi_id").alias("supply"),
        )
        .withColumn("coverage_ratio", F.col("supply") / F.col("demand"))
        .filter(F.col("coverage_ratio") < 0.1)  # less than 10% coverage
        .orderBy("coverage_ratio")
    )

    gap_count = coverage.count()
    log.info("Coverage gaps found: %d zone-hour combos with < 10%% supply/demand ratio", gap_count)
    if gap_count > 0:
        coverage.show(20, False)
        coverage.coalesce(1).write.mode("overwrite").parquet(OUTPUT_PATH + "coverage_gaps/")

    # ── 8. KPI: Call type breakdown ──────────────────────────────────
    call_types = (
        trips
        .groupBy("call_type")
        .agg(
            F.count("*").alias("trip_count"),
            F.avg("trip_duration_sec").alias("avg_duration_sec"),
            F.avg("trip_distance_km").alias("avg_distance_km"),
        )
        .orderBy(F.desc("trip_count"))
    )
    log.info("=== Call Type Breakdown ===")
    call_types.show(5, False)
    call_types.coalesce(1).write.mode("overwrite").parquet(OUTPUT_PATH + "call_type_breakdown/")

    # ── Summary ──────────────────────────────────────────────────────
    log.info("=== KPI Analytics Summary ===")
    log.info("  Total trips analyzed: %d", total)
    log.info("  Zones: %d", trips_per_zone.count())
    log.info("  Peak hour: %d:00 (%d trips)",
             peak_hours.collect()[0]["hour_of_day"],
             peak_hours.collect()[0]["trip_count"])
    log.info("  Coverage gaps: %d", gap_count)
    log.info("  Output: %s", OUTPUT_PATH)
    log.info("=== KPI Analytics Complete ===")
    spark.stop()


if __name__ == "__main__":
    main()
