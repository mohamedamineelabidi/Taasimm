"""
TaaSim — Spark: Batch Demand Forecast
=======================================
Loads the trained GBT PipelineModel from MinIO, generates forecasts
for all 16 Casablanca zones × 48 thirty-minute slots (24 hours),
and writes the predictions into Cassandra demand_zones.forecast_demand
with a 24-hour TTL so they auto-expire if not refreshed.

Run daily (e.g. via cron or Spark submit):
  /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --deploy-mode client \
    /opt/spark-jobs/batch_forecast.py
"""

import os
import logging
from datetime import datetime, timedelta

from pyspark.sql import SparkSession, Row
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DoubleType, StringType
from pyspark.ml import PipelineModel

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [FORECAST] %(levelname)s  %(message)s",
)
log = logging.getLogger("FORECAST")

MODEL_PATH = "s3a://mldata/models/demand_v1/"
FEATURES_PATH = "s3a://mldata/features/"
ZONE_MAPPING_PATH = "/opt/data/zone_mapping.csv"

# Zone metadata (population_density, zone_type) mirrored from zone_mapping.csv
ZONE_META = {
    1:  {"population_density": 12000.0, "zone_type": "residential"},
    2:  {"population_density": 25000.0, "zone_type": "residential"},
    3:  {"population_density":  8000.0, "zone_type": "residential"},
    4:  {"population_density": 15000.0, "zone_type": "residential"},
    5:  {"population_density": 18000.0, "zone_type": "residential"},
    6:  {"population_density": 22000.0, "zone_type": "residential"},
    7:  {"population_density": 20000.0, "zone_type": "residential"},
    8:  {"population_density":  5000.0, "zone_type": "commercial"},
    9:  {"population_density": 30000.0, "zone_type": "residential"},
    10: {"population_density":  8000.0, "zone_type": "commercial"},
    11: {"population_density": 12000.0, "zone_type": "mixed"},
    12: {"population_density": 20000.0, "zone_type": "mixed"},
    13: {"population_density":  3000.0, "zone_type": "commercial"},
    14: {"population_density": 25000.0, "zone_type": "transit_hub"},
    15: {"population_density":  6000.0, "zone_type": "mixed"},
    16: {"population_density": 15000.0, "zone_type": "mixed"},
}


def build_spark():
    return (
        SparkSession.builder
        .appName("TaaSim-Batch-Forecast")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
        .config("spark.hadoop.fs.s3a.fast.upload", "true")
        .getOrCreate()
    )


def main():
    log.info("=== Batch Demand Forecast Starting ===")
    spark = build_spark()

    # ── 1. Load trained model ────────────────────────────────────────
    log.info("Loading PipelineModel from %s", MODEL_PATH)
    model = PipelineModel.load(MODEL_PATH)
    log.info("Model loaded.")

    # ── 2. Compute per-zone average lag values from feature matrix ───
    log.info("Computing per-zone average lag values from %s", FEATURES_PATH)
    features_df = spark.read.parquet(FEATURES_PATH)
    zone_avgs = (
        features_df
        .groupBy("origin_zone")
        .agg(
            F.avg("demand_lag_1d").alias("avg_lag_1d"),
            F.avg("demand_lag_7d").alias("avg_lag_7d"),
            F.avg("rolling_7d_mean").alias("avg_rolling_7d"),
            F.avg("supply_demand_ratio").alias("avg_sdr"),
        )
        .collect()
    )
    # Build lookup dict: zone_id -> avg values
    zone_lag_map = {row["origin_zone"]: row for row in zone_avgs}
    log.info("Lag averages computed for %d zones.", len(zone_lag_map))

    # ── 3. Generate input rows: 16 zones × 48 slots ──────────────────
    now = datetime.utcnow()
    today_dow = now.weekday()     # 0=Monday
    is_weekend = 1 if today_dow >= 5 else 0

    rows = []
    for zone_id in range(1, 17):
        meta = ZONE_META.get(zone_id, {"population_density": 10000.0, "zone_type": "mixed"})
        lags = zone_lag_map.get(zone_id)
        lag_1d   = float(lags["avg_lag_1d"])   if lags else 30.0
        lag_7d   = float(lags["avg_lag_7d"])   if lags else 30.0
        rolling  = float(lags["avg_rolling_7d"]) if lags else 30.0
        sdr      = float(lags["avg_sdr"])      if lags else 0.5

        for slot in range(48):
            hour = slot // 2
            is_peak = 1 if hour in [8, 9, 13, 14, 17, 18] else 0
            rows.append(Row(
                origin_zone=zone_id,
                slot_of_day=slot,
                hour_of_day=hour,
                day_of_week=today_dow,
                is_weekend=is_weekend,
                is_peak=is_peak,
                supply_demand_ratio=sdr,
                demand_lag_1d=lag_1d,
                demand_lag_7d=lag_7d,
                rolling_7d_mean=rolling,
                population_density=meta["population_density"],
                zone_type=meta["zone_type"],
            ))

    log.info("Generated %d forecast input rows (16 zones × 48 slots)", len(rows))
    input_df = spark.createDataFrame(rows)

    # ── 4. Run model transform ───────────────────────────────────────
    log.info("Running model.transform()...")
    predictions = model.transform(input_df)
    pred_count = predictions.count()
    log.info("Predictions generated: %d rows", pred_count)

    # ── 5. Write forecasts to Cassandra demand_zones ─────────────────
    log.info("Writing forecasts to Cassandra demand_zones (TTL 86400s)...")
    try:
        from cassandra.cluster import Cluster as _Cluster

        _cass_host = os.getenv("CASSANDRA_HOST", "cassandra")
        _cluster = _Cluster([_cass_host])
        _cass = _cluster.connect("taasim")

        # Prepare INSERT with TTL 86400 (24h)
        _stmt = _cass.prepare(
            "INSERT INTO demand_zones "
            "(city, zone_id, window_start, forecast_demand) "
            "VALUES ('casablanca', ?, ?, ?) USING TTL 86400"
        )

        preds = predictions.select("origin_zone", "slot_of_day", "prediction").collect()
        written = 0
        for row in preds:
            zone_id = int(row["origin_zone"])
            slot = int(row["slot_of_day"])
            pred_val = float(row["prediction"])
            # window_start = start of this slot today (UTC)
            window_start = now.replace(hour=slot // 2, minute=(slot % 2) * 30, second=0, microsecond=0)
            _cass.execute(_stmt, (zone_id, window_start, pred_val))
            written += 1

        _cluster.shutdown()
        log.info("Written %d forecast rows to Cassandra", written)

    except Exception as e:
        log.error("Cassandra forecast write failed: %s", e)
        raise

    # ── 6. Summary ──────────────────────────────────────────────────
    log.info("=== Batch Forecast Summary ===")
    log.info("  Zones:      16")
    log.info("  Slots:      48 (30-min intervals)")
    log.info("  Total rows: %d", written)
    log.info("  TTL:        86400s (24h)")
    log.info("=== Batch Forecast Complete ===")
    spark.stop()


if __name__ == "__main__":
    main()
