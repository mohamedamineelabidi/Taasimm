"""
TaaSim — Spark ETL: Porto Taxi Trajectories
=============================================
Reads raw Porto CSV from MinIO (s3a://raw/porto-trips/train.csv),
parses POLYLINE JSON, applies zone remapping (Porto → Casablanca),
deduplicates, computes trip-level aggregates, and writes cleaned
Parquet to s3a://curated/trips/ partitioned by year_month.

Usage (inside Spark container):
  /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --deploy-mode client \
    /opt/spark-jobs/etl_porto.py
"""

import json
import logging
from datetime import datetime

from pyspark.sql import SparkSession, Row
from pyspark.sql import functions as F
from pyspark.sql.types import (

    StructType, StructField, StringType, LongType, BooleanType,
    IntegerType, DoubleType, ArrayType, TimestampType
)

try:
    import h3 as _h3_check  # noqa: F401
    HAS_H3 = True
except ImportError:
    HAS_H3 = False

logging.basicConfig(level=logging.INFO, format="%(asctime)s [ETL-Porto] %(levelname)s %(message)s")
log = logging.getLogger("ETL-Porto")

# ── Bounding boxes (must match producers/config.py) ─────────────────
PORTO_LAT_MIN, PORTO_LAT_MAX = 41.135, 41.174
PORTO_LON_MIN, PORTO_LON_MAX = -8.650, -8.585
CASA_LAT_MIN, CASA_LAT_MAX = 33.450, 33.680
CASA_LON_MIN, CASA_LON_MAX = -7.720, -7.480

# Porto metro-area bounds (for filtering outliers)
PORTO_METRO_LAT = (41.10, 41.25)
PORTO_METRO_LON = (-8.72, -8.55)

# ── I/O paths ────────────────────────────────────────────────────────
INPUT_PATH = "s3a://raw/porto-trips/train.csv"
OUTPUT_PATH = "s3a://curated/trips/"
ZONE_MAPPING_PATH = "/opt/data/zone_mapping.csv"


def build_spark():
    """Create SparkSession with S3A config (defaults loaded from spark-defaults.conf)."""
    return (
        SparkSession.builder
        .appName("TaaSim-ETL-Porto")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.shuffle.partitions", "8")
        # S3A committer: algorithm v2 writes tasks directly to final path,
        # avoiding the copy+delete "rename" that silently fails on MinIO
        # with partitionBy() writes (FileOutputCommitter v1 default).
        .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
        .config("spark.hadoop.fs.s3a.fast.upload", "true")
        .config("spark.hadoop.fs.s3a.fast.upload.buffer", "bytebuffer")
        .getOrCreate()
    )


def transform_coord(val, src_min, src_max, dst_min, dst_max):
    """Linear bounding-box transform (scalar)."""
    ratio = (val - src_min) / (src_max - src_min)
    ratio = max(0.0, min(1.0, ratio))
    return ratio * (dst_max - dst_min) + dst_min


def parse_and_explode_polyline(row):
    """Parse POLYLINE JSON, transform each point to Casablanca coords.
    Yields one Row per GPS point with trip metadata."""
    trip_id = row.TRIP_ID
    taxi_id = row.TAXI_ID
    call_type = row.CALL_TYPE
    origin_call = row.ORIGIN_CALL
    origin_stand = row.ORIGIN_STAND
    timestamp = int(row.TIMESTAMP)
    day_type = row.DAY_TYPE
    missing = row.MISSING_DATA

    # Skip trips with missing data flag
    if missing and str(missing).strip().upper() == "TRUE":
        return

    polyline_str = row.POLYLINE
    if not polyline_str or polyline_str in ("[]", ""):
        return

    try:
        coords = json.loads(polyline_str)
    except (json.JSONDecodeError, TypeError):
        return

    if not coords or len(coords) < 2:
        return

    dt = datetime.utcfromtimestamp(timestamp)
    year_month = dt.strftime("%Y-%m")

    # Trip duration = (num_points - 1) * 15 seconds
    trip_duration_sec = (len(coords) - 1) * 15

    # Filter unrealistic trips (< 30s or > 3 hours)
    if trip_duration_sec < 30 or trip_duration_sec > 10800:
        return

    # Origin and destination (first and last GPS point)
    orig_lon, orig_lat = coords[0]
    dest_lon, dest_lat = coords[-1]

    # Filter: origin must be within Porto metro area
    if not (PORTO_METRO_LAT[0] <= orig_lat <= PORTO_METRO_LAT[1] and
            PORTO_METRO_LON[0] <= orig_lon <= PORTO_METRO_LON[1]):
        return

    # Transform origin/destination to Casablanca
    casa_orig_lat = transform_coord(orig_lat, PORTO_LAT_MIN, PORTO_LAT_MAX,
                                    CASA_LAT_MIN, CASA_LAT_MAX)
    casa_orig_lon = transform_coord(orig_lon, PORTO_LON_MIN, PORTO_LON_MAX,
                                    CASA_LON_MIN, CASA_LON_MAX)
    casa_dest_lat = transform_coord(dest_lat, PORTO_LAT_MIN, PORTO_LAT_MAX,
                                    CASA_LAT_MIN, CASA_LAT_MAX)
    casa_dest_lon = transform_coord(dest_lon, PORTO_LON_MIN, PORTO_LON_MAX,
                                    CASA_LON_MIN, CASA_LON_MAX)

    yield Row(
        trip_id=trip_id,
        taxi_id=str(taxi_id),
        call_type=call_type,
        origin_call=origin_call if origin_call else None,
        origin_stand=origin_stand if origin_stand else None,
        timestamp=timestamp,
        event_time=dt,
        year_month=year_month,
        day_type=day_type,
        num_gps_points=len(coords),
        trip_duration_sec=trip_duration_sec,
        porto_orig_lat=orig_lat,
        porto_orig_lon=orig_lon,
        porto_dest_lat=dest_lat,
        porto_dest_lon=dest_lon,
        casa_orig_lat=casa_orig_lat,
        casa_orig_lon=casa_orig_lon,
        casa_dest_lat=casa_dest_lat,
        casa_dest_lon=casa_dest_lon,
        hour_of_day=dt.hour,
        day_of_week=dt.weekday(),  # 0=Monday
        is_weekend=1 if dt.weekday() >= 5 else 0,
        is_friday=1 if dt.weekday() == 4 else 0,
    )


def main():
    log.info("=== TaaSim Porto ETL Starting ===")
    spark = build_spark()
    log.info("SparkSession created: %s", spark.sparkContext.applicationId)

    # ── 1. Read raw Porto CSV ────────────────────────────────────────
    log.info("Reading Porto CSV from %s", INPUT_PATH)
    raw_df = (
        spark.read
        .option("header", "true")
        .option("quote", '"')
        .option("escape", '"')
        .csv(INPUT_PATH)
    )
    raw_count = raw_df.count()
    log.info("Raw rows: %d", raw_count)

    # ── 2. Deduplicate on TRIP_ID ────────────────────────────────────
    deduped_df = raw_df.dropDuplicates(["TRIP_ID"])
    dedup_count = deduped_df.count()
    log.info("After dedup: %d (removed %d duplicates)", dedup_count, raw_count - dedup_count)

    # ── 3. Parse POLYLINE, transform coords, compute trip features ───
    log.info("Parsing POLYLINE and transforming coordinates...")
    trip_rows = deduped_df.rdd.flatMap(parse_and_explode_polyline)
    trips_df = spark.createDataFrame(trip_rows)
    trip_count = trips_df.count()
    log.info("Valid trips after parse + filter: %d", trip_count)

    # ── 4. Load zone mapping and assign zones ────────────────────────
    log.info("Loading zone mapping from %s", ZONE_MAPPING_PATH)
    zones_df = (
        spark.read
        .option("header", "true")
        .csv(ZONE_MAPPING_PATH)
        .select(
            F.col("zone_id").cast(IntegerType()).alias("zone_id"),
            F.col("arrondissement_name").alias("zone_name"),
            F.col("casa_lat_min").cast(DoubleType()),
            F.col("casa_lat_max").cast(DoubleType()),
            F.col("casa_lon_min").cast(DoubleType()),
            F.col("casa_lon_max").cast(DoubleType()),
            F.col("casa_centroid_lat").cast(DoubleType()).alias("centroid_lat"),
            F.col("casa_centroid_lon").cast(DoubleType()).alias("centroid_lon"),
        )
    )

    # Broadcast the small zone table for efficient join
    zones_bc = F.broadcast(zones_df)

    # Assign origin zone via range join
    with_origin_zone = (
        trips_df
        .join(
            zones_bc.alias("oz"),
            (F.col("casa_orig_lat") >= F.col("oz.casa_lat_min")) &
            (F.col("casa_orig_lat") <= F.col("oz.casa_lat_max")) &
            (F.col("casa_orig_lon") >= F.col("oz.casa_lon_min")) &
            (F.col("casa_orig_lon") <= F.col("oz.casa_lon_max")),
            "left"
        )
        .select(
            trips_df["*"],
            F.col("oz.zone_id").alias("origin_zone"),
            F.col("oz.zone_name").alias("origin_zone_name"),
        )
    )

    # Assign destination zone via range join
    enriched = (
        with_origin_zone
        .join(
            zones_bc.alias("dz"),
            (F.col("casa_dest_lat") >= F.col("dz.casa_lat_min")) &
            (F.col("casa_dest_lat") <= F.col("dz.casa_lat_max")) &
            (F.col("casa_dest_lon") >= F.col("dz.casa_lon_min")) &
            (F.col("casa_dest_lon") <= F.col("dz.casa_lon_max")),
            "left"
        )
        .select(
            with_origin_zone["*"],
            F.col("dz.zone_id").alias("dest_zone"),
            F.col("dz.zone_name").alias("dest_zone_name"),
        )
    )

    # Drop trips where zone assignment failed (outside Casablanca bbox)
    zoned = enriched.filter(
        F.col("origin_zone").isNotNull() & F.col("dest_zone").isNotNull()
    )
    zoned_count = zoned.count()
    log.info("Trips with valid zones: %d (dropped %d unzoned)",
             zoned_count, trip_count - zoned_count)

    # ── 5. Compute trip distance (approximate, Haversine) ────────────
    final = zoned.withColumn(
        "trip_distance_km",
        F.lit(6371.0) * F.acos(
            F.least(F.lit(1.0),
                    F.cos(F.radians(F.col("casa_orig_lat"))) *
                    F.cos(F.radians(F.col("casa_dest_lat"))) *
                    F.cos(F.radians(F.col("casa_dest_lon")) - F.radians(F.col("casa_orig_lon"))) +
                    F.sin(F.radians(F.col("casa_orig_lat"))) *
                    F.sin(F.radians(F.col("casa_dest_lat")))
                    )
        )
    )

    # ── 5b. H3 zone index (resolution 9) ──────────────────────────────
    if HAS_H3:
        import pandas as pd

        @F.pandas_udf("string")
        def _geo_to_h3(lat: pd.Series, lon: pd.Series) -> pd.Series:
            import h3
            return pd.Series(
                [h3.geo_to_h3(float(la), float(lo), 9) for la, lo in zip(lat, lon)]
            )

        final = (
            final
            .withColumn("h3_origin", _geo_to_h3(F.col("casa_orig_lat"), F.col("casa_orig_lon")))
            .withColumn("h3_dest",   _geo_to_h3(F.col("casa_dest_lat"), F.col("casa_dest_lon")))
        )
        log.info("H3 columns added (resolution 9)")
    else:
        log.warning("h3 not available — skipping H3 indexing (pip install h3==3.7.7)")

    # ── 6. Write Parquet partitioned by year_month ───────────────────
    log.info("Writing curated trips to %s (partitioned by year_month)", OUTPUT_PATH)
    (
        final
        .repartition(4, "year_month")
        .write
        .mode("overwrite")
        .partitionBy("year_month")
        .parquet(OUTPUT_PATH)
    )

    # Verify the write actually landed in S3
    written_count = spark.read.parquet(OUTPUT_PATH).count()
    log.info("Write verified: %d rows confirmed at %s", written_count, OUTPUT_PATH)
    if written_count == 0:
        raise RuntimeError("S3A write produced 0 rows — data was not persisted to MinIO")

    # ── 7. Summary stats ────────────────────────────────────────────
    log.info("=== ETL Summary ===")
    log.info("  Raw rows:          %d", raw_count)
    log.info("  After dedup:       %d", dedup_count)
    log.info("  Valid parsed:      %d", trip_count)
    log.info("  With zones:        %d", zoned_count)
    log.info("  Output:            %s", OUTPUT_PATH)

    # Print some sample stats
    final.groupBy("year_month").count().orderBy("year_month").show(20, False)
    final.groupBy("origin_zone", "origin_zone_name").count().orderBy("origin_zone").show(20, False)
    log.info("  Avg trip duration: %.1f sec",
             final.agg(F.avg("trip_duration_sec")).collect()[0][0])
    log.info("  Avg trip distance: %.2f km",
             final.agg(F.avg("trip_distance_km")).collect()[0][0])

    log.info("=== Porto ETL Complete ===")
    spark.stop()


if __name__ == "__main__":
    main()
