"""
TaaSim — Spark ML: GBT Demand Forecasting
===========================================
Reads feature matrix from s3a://mldata/features/, trains a
GBTRegressor (Spark MLlib) to predict 30-min demand per zone,
compares with a naive 7-day-lag baseline, and saves the model.

- Algorithm: GBTRegressor, maxDepth=5, maxIter=50
- Train/test: temporal split (10 months train, 2 months test)
- Baseline: naive 7-day-lag (must beat this)
- Artifact: s3a://mldata/models/demand_v1/

Usage (inside Spark container):
  /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --deploy-mode client \
    /opt/spark-jobs/train_demand_model.py
"""

import logging
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.ml.regression import GBTRegressor
from pyspark.ml.evaluation import RegressionEvaluator

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [ML] %(levelname)s  %(message)s",
)
log = logging.getLogger("ML")

FEATURES_PATH = "s3a://mldata/features/"
MODEL_PATH = "s3a://mldata/models/demand_v1/"
METRICS_PATH = "s3a://mldata/metrics/demand_v1/"

# Feature columns for the model
FEATURE_COLS = [
    "hour_of_day",
    "day_of_week",
    "is_weekend",
    "is_peak",
    "zone_idx",          # StringIndexer output
    "slot_of_day",
    "supply_demand_ratio",
    "demand_lag_1d",
    "demand_lag_7d",
    "rolling_7d_mean",
]

TARGET_COL = "demand"


def build_spark():
    return (
        SparkSession.builder
        .appName("TaaSim-GBT-Training")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )


def main():
    log.info("=== GBT Demand Model Training Starting ===")
    spark = build_spark()

    # ── 1. Read feature matrix ───────────────────────────────────────
    log.info("Reading features from %s", FEATURES_PATH)
    features = spark.read.parquet(FEATURES_PATH)
    total = features.count()
    log.info("Total feature rows: %d", total)

    # ── 2. Temporal train/test split ─────────────────────────────────
    # Porto data: 2013-07 to 2014-06 (12 months)
    # Train: first 10 months (2013-07 to 2014-04)
    # Test:  last 2 months  (2014-05 to 2014-06)
    split_date = "2014-05-01"
    train_df = features.filter(F.col("trip_date") < split_date)
    test_df = features.filter(F.col("trip_date") >= split_date)

    train_count = train_df.count()
    test_count = test_df.count()
    log.info("Train: %d rows (before %s)", train_count, split_date)
    log.info("Test:  %d rows (>= %s)", test_count, split_date)

    # ── 3. Build ML pipeline ─────────────────────────────────────────
    log.info("Building ML pipeline...")

    # Index zone_id as categorical
    zone_indexer = StringIndexer(
        inputCol="origin_zone",
        outputCol="zone_idx",
        handleInvalid="keep",
    )

    # Assemble feature vector
    assembler = VectorAssembler(
        inputCols=FEATURE_COLS,
        outputCol="features",
        handleInvalid="skip",
    )

    # GBT Regressor
    gbt = GBTRegressor(
        featuresCol="features",
        labelCol=TARGET_COL,
        predictionCol="prediction",
        maxDepth=5,
        maxIter=50,
        stepSize=0.1,
        seed=42,
    )

    pipeline = Pipeline(stages=[zone_indexer, assembler, gbt])

    # ── 4. Train ─────────────────────────────────────────────────────
    log.info("Training GBT model (maxDepth=5, maxIter=50)...")
    model = pipeline.fit(train_df)
    log.info("Training complete.")

    # ── 5. Evaluate on test set ──────────────────────────────────────
    log.info("Evaluating on test set...")
    predictions = model.transform(test_df)

    # Evaluators
    rmse_eval = RegressionEvaluator(
        labelCol=TARGET_COL, predictionCol="prediction", metricName="rmse"
    )
    mae_eval = RegressionEvaluator(
        labelCol=TARGET_COL, predictionCol="prediction", metricName="mae"
    )
    r2_eval = RegressionEvaluator(
        labelCol=TARGET_COL, predictionCol="prediction", metricName="r2"
    )

    gbt_rmse = rmse_eval.evaluate(predictions)
    gbt_mae = mae_eval.evaluate(predictions)
    gbt_r2 = r2_eval.evaluate(predictions)

    log.info("=== GBT Model Metrics ===")
    log.info("  RMSE: %.4f", gbt_rmse)
    log.info("  MAE:  %.4f", gbt_mae)
    log.info("  R²:   %.4f", gbt_r2)

    # ── 6. Naive baseline (7-day lag) ────────────────────────────────
    log.info("Computing naive 7-day-lag baseline...")
    baseline_pred = test_df.withColumn("prediction", F.col("demand_lag_7d").cast("double"))
    # Drop nulls in baseline (edge case: no lag available)
    baseline_valid = baseline_pred.filter(F.col("prediction").isNotNull())

    baseline_rmse = rmse_eval.evaluate(baseline_valid)
    baseline_mae = mae_eval.evaluate(baseline_valid)
    baseline_r2 = r2_eval.evaluate(baseline_valid)

    log.info("=== Naive 7-day-lag Baseline ===")
    log.info("  RMSE: %.4f", baseline_rmse)
    log.info("  MAE:  %.4f", baseline_mae)
    log.info("  R²:   %.4f", baseline_r2)

    # ── 7. Comparison ────────────────────────────────────────────────
    rmse_improvement = ((baseline_rmse - gbt_rmse) / baseline_rmse) * 100
    mae_improvement = ((baseline_mae - gbt_mae) / baseline_mae) * 100

    log.info("=== Model vs Baseline ===")
    log.info("  RMSE improvement: %.1f%%", rmse_improvement)
    log.info("  MAE  improvement: %.1f%%", mae_improvement)
    if gbt_rmse < baseline_rmse:
        log.info("  RESULT: GBT BEATS naive baseline!")
    else:
        log.info("  RESULT: GBT does NOT beat baseline — needs tuning")

    # ── 8. Feature importance ────────────────────────────────────────
    gbt_model = model.stages[-1]
    importances = gbt_model.featureImportances
    log.info("=== Feature Importance ===")
    for i, col in enumerate(FEATURE_COLS):
        log.info("  %-22s %.4f", col, importances[i])

    # ── 9. Save model ────────────────────────────────────────────────
    log.info("Saving model to %s", MODEL_PATH)
    model.write().overwrite().save(MODEL_PATH)
    log.info("Model saved.")

    # ── 10. Save metrics as Parquet ──────────────────────────────────
    metrics_data = [
        ("gbt", "rmse", float(gbt_rmse)),
        ("gbt", "mae", float(gbt_mae)),
        ("gbt", "r2", float(gbt_r2)),
        ("baseline_7d_lag", "rmse", float(baseline_rmse)),
        ("baseline_7d_lag", "mae", float(baseline_mae)),
        ("baseline_7d_lag", "r2", float(baseline_r2)),
        ("improvement", "rmse_pct", float(rmse_improvement)),
        ("improvement", "mae_pct", float(mae_improvement)),
    ]
    metrics_df = spark.createDataFrame(metrics_data, ["model", "metric", "value"])
    metrics_df.write.mode("overwrite").parquet(METRICS_PATH)
    log.info("Metrics saved to %s", METRICS_PATH)

    # ── 11. Show sample predictions ──────────────────────────────────
    log.info("Sample predictions (test set):")
    (
        predictions
        .select("origin_zone", "trip_date", "slot_of_day", "demand", "prediction")
        .orderBy("trip_date", "origin_zone", "slot_of_day")
        .show(20, truncate=False)
    )

    log.info("=== GBT Training Complete ===")
    log.info("  Train rows:   %d", train_count)
    log.info("  Test rows:    %d", test_count)
    log.info("  GBT RMSE:     %.4f", gbt_rmse)
    log.info("  Baseline RMSE:%.4f", baseline_rmse)
    log.info("  Improvement:  %.1f%%", rmse_improvement)
    log.info("  Model path:   %s", MODEL_PATH)

    spark.stop()


if __name__ == "__main__":
    main()
