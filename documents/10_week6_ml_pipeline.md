# Week 6 — ML Pipeline: Feature Engineering + GBT Training + FastAPI

Date: 2026-04-19

## Summary

Built the complete ML pipeline for demand forecasting:
1. Feature engineering from curated Porto trips → 183,981 feature rows
2. GBT model training with 45.8% RMSE improvement over naive baseline
3. FastAPI REST API with JWT auth for demand forecast serving

## Feature Engineering (spark/feature_engineering.py)

- **Input**: `s3a://curated/trips/` (1,228,061 Porto trips)
- **Output**: `s3a://mldata/features/` (183,981 rows, 4 partitions)
- **Features**: hour_of_day, day_of_week, is_weekend, is_peak, slot_of_day, supply_demand_ratio, demand_lag_1d, demand_lag_7d, rolling_7d_mean
- **Aggregation**: 30-min slots per zone → demand count per (origin_zone, trip_date, slot_of_day)
- **Date range**: 2013-07-08 to 2014-06-30
- **Avg demand/slot**: 6.5 trips

## GBT Model Training (spark/train_demand_model.py)

- **Algorithm**: GBTRegressor (Spark MLlib), maxDepth=5, maxIter=50
- **Split**: Temporal at 2014-05-01 (train: 151,911 / test: 32,070)
- **Pipeline**: StringIndexer → VectorAssembler → GBTRegressor

### Results

| Metric | GBT Model | Naive 7-day Lag | Improvement |
|--------|-----------|-----------------|-------------|
| RMSE   | 3.7073    | 6.8375          | 45.8%       |
| MAE    | 2.1070    | —               | —           |
| R²     | 0.7499    | —               | —           |

- **Model saved**: `s3a://mldata/models/demand_v1/` (3-stage PipelineModel)
- **Metrics saved**: `s3a://mldata/metrics/demand_v1/`

## FastAPI (api/main.py)

- **Endpoints**:
  - `POST /api/auth/token` — JWT token generation (admin/rider roles)
  - `POST /api/demand/forecast` — demand prediction (zone_id + datetime)
  - `POST /api/trips` — trip reservation
  - `GET /api/zones` — list all 16 zones
  - `GET /api/zones/{zone_id}` — zone details
  - `GET /api/health` — health check
- **Auth**: JWT HS256, 24h expiry, role-based access
- **Fallback**: Heuristic demand curve if model not available
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
