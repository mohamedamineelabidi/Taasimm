"""
TaaSim — Quick model verification script.
Reads saved metrics + model, runs a few sample predictions.
"""
from pyspark.sql import SparkSession, Row
from pyspark.ml import PipelineModel

spark = (
    SparkSession.builder
    .appName("TaaSim-ModelVerify")
    .config("spark.sql.adaptive.enabled", "true")
    .getOrCreate()
)

# ── 1. Read saved metrics ────────────────────────────────────────
print("\n" + "="*60)
print("  SAVED MODEL METRICS")
print("="*60)
metrics = spark.read.parquet("s3a://mldata/metrics/demand_v1/")
metrics.show(truncate=False)

# ── 2. Load model ────────────────────────────────────────────────
print("Loading model from s3a://mldata/models/demand_v1/ ...")
model = PipelineModel.load("s3a://mldata/models/demand_v1/")
print(f"Model loaded: {len(model.stages)} stages")
for i, stage in enumerate(model.stages):
    print(f"  Stage {i}: {type(stage).__name__}")

# ── 3. Feature importances ───────────────────────────────────────
FEATURE_COLS = [
    "hour_of_day", "day_of_week", "is_weekend", "is_peak",
    "zone_idx", "slot_of_day", "supply_demand_ratio",
    "demand_lag_1d", "demand_lag_7d", "rolling_7d_mean",
]
gbt = model.stages[-1]
print("\n" + "="*60)
print("  FEATURE IMPORTANCES")
print("="*60)
for name, imp in sorted(zip(FEATURE_COLS, gbt.featureImportances.toArray()), key=lambda x: -x[1]):
    bar = "#" * int(imp * 50)
    print(f"  {name:25s} {imp:.4f}  {bar}")

# ── 4. Sample predictions ────────────────────────────────────────
print("\n" + "="*60)
print("  SAMPLE PREDICTIONS (what would the model predict?)")
print("="*60)
# Simulate: zone 1 (Ain Chock), Wednesday, 8:00 AM rush hour
# vs same zone, Sunday 3:00 AM
samples = [
    Row(origin_zone="1", slot_of_day=16, hour_of_day=8,  day_of_week=2, is_weekend=0, is_peak=1, supply_demand_ratio=0.5, demand_lag_1d=60.0, demand_lag_7d=55.0, rolling_7d_mean=58.0),
    Row(origin_zone="1", slot_of_day=6,  hour_of_day=3,  day_of_week=6, is_weekend=1, is_peak=0, supply_demand_ratio=0.8, demand_lag_1d=8.0,  demand_lag_7d=7.0,  rolling_7d_mean=7.5),
    Row(origin_zone="8", slot_of_day=36, hour_of_day=18, day_of_week=4, is_weekend=0, is_peak=1, supply_demand_ratio=0.3, demand_lag_1d=70.0, demand_lag_7d=65.0, rolling_7d_mean=68.0),
    Row(origin_zone="16",slot_of_day=24, hour_of_day=12, day_of_week=0, is_weekend=0, is_peak=0, supply_demand_ratio=0.6, demand_lag_1d=40.0, demand_lag_7d=38.0, rolling_7d_mean=39.0),
]
test_df = spark.createDataFrame(samples)
preds = model.transform(test_df)

ZONES = {
    "1": "Ain Chock", "8": "Maarif", "16": "Sidi Bernoussi",
}
print(f"\n{'Zone':20s} {'Day':10s} {'Time':6s} {'Peak':5s} {'Predicted':>10s}")
print("-" * 55)
for row in preds.select("origin_zone","day_of_week","hour_of_day","is_peak","prediction").collect():
    days = ["Mon","Tue","Wed","Thu","Fri","Sat","Sun"]
    zone_name = ZONES.get(row.origin_zone, f"Zone {row.origin_zone}")
    peak = "YES" if row.is_peak else "no"
    print(f"{zone_name:20s} {days[row.day_of_week]:10s} {row.hour_of_day:02d}:00  {peak:5s} {row.prediction:10.1f} trips")

# ── 5. Actual vs predicted on test set ───────────────────────────
print("\n" + "="*60)
print("  ACTUAL vs PREDICTED (first 15 test rows)")
print("="*60)
features = spark.read.parquet("s3a://mldata/features/")
test_data = features.filter("trip_date >= '2014-05-01'").orderBy("trip_date","origin_zone","slot_of_day")
preds_test = model.transform(test_data)
preds_test.select("origin_zone","trip_date","slot_of_day","demand","prediction").show(15, truncate=False)

print("\nDone!")
spark.stop()
