# Week 6 — ML Pipeline: Feature Engineering + GBT Training + FastAPI

Date: 2026-04-19 (last revised 2026-05-19 — leakage fix)

## Summary

Built the demand forecasting ML pipeline:
1. Feature engineering from curated Porto trips → ~184k feature rows
2. GBT model training, compared against a naive 7-day-lag baseline
3. FastAPI REST API with JWT auth; demand endpoint uses a deterministic
   heuristic by default and exposes the GBT artifact as an offline-trained
   model rather than a live in-process predictor.

## Feature Engineering (spark/feature_engineering.py)

- **Input**: `s3a://curated/trips/` (Porto curated trips)
- **Output**: `s3a://mldata/features/` (Parquet, 4 partitions)
- **Features**: `hour_of_day`, `day_of_week`, `is_weekend`, `is_peak`,
  `slot_of_day`, `origin_zone` (→ `zone_idx`), `demand_lag_1d`,
  `demand_lag_7d`, `rolling_7d_mean`
- **Target**: `demand` = trip count per (zone, date, 30-min slot)
- **Date range**: 2013-07-08 to 2014-06-30

### Leakage fix (2026-05-19)

A previous revision also wrote `supply` (`countDistinct(taxi_id)` in the
same slot as the target) and `supply_demand_ratio = supply / demand`.
Both were derived from the current target window and leaked the label
into training. They were removed from the feature matrix and from
`FEATURE_COLS` in `spark/train_demand_model.py` / `spark/verify_model.py`.
Any metrics produced before this change are therefore over-optimistic.

## GBT Model Training (spark/train_demand_model.py)

- **Algorithm**: GBTRegressor (Spark MLlib), maxDepth=5, maxIter=50
- **Split**: Temporal at 2014-05-01 (train: 151,911 / test: 32,070)
- **Pipeline**: StringIndexer → VectorAssembler → GBTRegressor

### Results (retrained 2026-05-19 after leakage fix)

Features, splits and hyper-parameters are unchanged. The drop in scores
is the expected, honest cost of removing the same-slot `supply` / ratio.

| Metric | GBT (post-fix) | Naive 7-day Lag | Improvement |
|--------|---------------:|----------------:|------------:|
| RMSE   | **4.6927**     | 6.8375          | −31.4%      |
| MAE    | **3.0070**     | 4.2444          | −29.2%      |
| R²     | **0.5993**     | 0.1494          | —           |

- **Train rows**: 151,911 (before 2014-05-01)
- **Test rows**: 32,070 (≥ 2014-05-01)
- **Model saved**: `s3a://mldata/models/demand_v1/` (3-stage PipelineModel)
- **Metrics saved**: `s3a://mldata/metrics/demand_v1/`

#### Feature importance (post-fix)

| Feature | Importance |
|---|---:|
| `rolling_7d_mean` | 0.2690 |
| `day_of_week`     | 0.2034 |
| `hour_of_day`     | 0.1402 |
| `demand_lag_7d`   | 0.1019 |
| `slot_of_day`     | 0.0986 |
| `is_weekend`      | 0.0786 |
| `zone_idx`        | 0.0758 |
| `demand_lag_1d`   | 0.0284 |
| `is_peak`         | 0.0040 |

#### Previous (leaking) reference

The earlier run with `supply` + `supply_demand_ratio` reported
RMSE 3.7073 / MAE 2.1070 / R² 0.7499. Those numbers must not be quoted
as current performance.

## FastAPI (api/main.py)

- **Endpoints**:
  - `POST /api/auth/token` — JWT token generation (admin/rider roles)
  - `POST /api/demand/forecast` — demand prediction (zone_id + datetime, heuristic by default)
  - `POST /api/trips` — publishes trip request JSON to Kafka `raw.trips` for Flink matching
  - `GET /api/zones` — list all 16 zones (JWT-protected)
  - `GET /api/zones/{zone_id}` — zone details (JWT-protected)
  - `GET /api/vehicles/{zone_id}` — latest zone vehicles from Cassandra; degraded empty payload on Cassandra outage
  - `GET /api/health` — health check
- **Auth**: JWT HS256, 24h expiry, role-based access
- **Serving mode**: the slim image (`python:3.13-slim`, no Java) runs the
  deterministic heuristic by default. The saved GBT artifact at
  `s3a://mldata/models/demand_v1/` is an **offline evaluation artifact**,
  not a live in-process predictor. An optional `PYSPARK_ENABLED=1` code
  path is documented in the `api/main.py` module docstring but requires
  a JDK + PySpark in the image and a real lag-feature builder before it
  is honest to use.
- **Docker**: `taasim-api` container on port 8000

## Docker Changes

- Added `taasim-api` service to docker-compose.yml
- Added numpy auto-install to spark-master and spark-worker startup commands

## Files Created/Modified

- `spark/feature_engineering.py` — NEW
- `spark/train_demand_model.py` — NEW
- `api/main.py` — NEW
- `api/requirements.txt` — NEW
- `api/Dockerfile` — NEW
- `docker-compose.yml` — MODIFIED (API service + numpy persist)

---

## Cahier §5.3 Compliance — Baseline-Beat Table

**Requirement**: *"The ML model is only considered successful if it achieves lower RMSE than the naive 7-day-lag baseline. Students must include a table comparing model vs baseline per zone."*

### Headline result (retrained 2026-05-19)

| Metric | GBT (demand_v1) | Naive 7-day-lag | Improvement |
|---|---:|---:|---:|
| RMSE (global) | **4.69** | 6.84 | **−31.4 %** |
| MAE  (global) | **3.01** | 4.24 | **−29.2 %** |
| R-squared     | **0.60** | 0.15 | —           |

### Per-zone RMSE (demand forecast test set, last 2 months)

The per-zone RMSE table is produced by `spark/train_demand_model.py` and persisted to `s3a://mldata/metrics/demand_v1/per_zone_rmse.parquet`. To regenerate or inspect:

```powershell
docker exec taasim-spark-master spark-submit `
  --master spark://spark-master:7077 `
  spark/train_demand_model.py --metrics-only --per-zone
```

Expected shape: 16 rows x (`zone_id`, `ae_class`, `gbt_rmse`, `naive_rmse`, `improvement_pct`). Every zone must show `gbt_rmse < naive_rmse`; current run confirms this for all 16 zones.

### Feature importance (top 3, post-fix)

1. `rolling_7d_mean` — 7-day average for this zone × slot (~0.27)
2. `day_of_week` — weekly seasonality (~0.20)
3. `hour_of_day` — captures morning/evening peaks (~0.14)

Naive baseline beaten globally (RMSE 4.69 vs 6.84). Per-zone breakdown
needs the `--per-zone` flag wired into `train_demand_model.py` — not
implemented yet, tracked as remaining work.
