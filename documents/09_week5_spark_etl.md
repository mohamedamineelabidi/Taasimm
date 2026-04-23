# Week 5 — Spark ETL: Porto + NYC Batch Processing + KPI Analytics

**Status**: COMPLETED  
**Date**: 2026-04-19  

## Task 1: Porto ETL (etl_porto.py)

**Input**: `s3a://raw/porto-trips/train.csv` (1.8 GiB, 1,710,670 rows)  
**Output**: `s3a://curated/trips/` (43 MiB, 12 monthly Parquet partitions)  

### Pipeline
1. Read CSV from MinIO (Porto taxi GPS trajectories)
2. Deduplicate by TRIP_ID → 1,710,589 unique trips
3. Parse POLYLINE JSON → extract origin/destination coordinates
4. Bounding-box transform: Porto → Casablanca (linear mapping + Gaussian noise σ=0.0002°)
5. Zone assignment: 16 irregular Casablanca arrondissements via zone_mapping.csv
6. Temporal feature extraction: hour_of_day, day_of_week, is_weekend, year_month
7. Validation filters: duration 30s–7200s, distance 0.1–50 km
8. Write Parquet partitioned by year_month

### Results
| Metric | Value |
|--------|-------|
| Raw rows | 1,710,670 |
| After dedup | 1,710,589 |
| Valid trips | 1,660,794 |
| Avg duration | 577.5 sec |
| Avg distance | 7.27 km |
| Output size | 43 MiB |
| Partitions | 12 (2013-07 to 2014-06) |

---

## Task 2: NYC TLC ETL (etl_nyc.py)

**Input**: `s3a://raw/nyc-tlc/` (3 Parquet files, ~146 MiB, Jan–Mar 2024)  
**Output**: `s3a://curated/nyc-demand/` (4.3 MiB, 4 Parquet partitions)  

### Schema Drift Resolution
NYC TLC files have schema inconsistencies across months:
- `passenger_count`: DOUBLE (Jan/Feb) vs INT64 (Mar)
- `VendorID`: BIGINT (Jan/Feb) vs INT (Mar)
- `RatecodeID`: DOUBLE (Jan/Feb) vs BIGINT (Mar)
- `PULocationID`: BIGINT (Jan/Feb) vs INT (Mar)
- `Airport_fee` (Mar) vs `airport_fee` (Jan/Feb) — case drift

**Solution**: Per-file reading with `read_and_normalize()` — COMMON_COLS as `(name, cast_type)` tuples, 
lazy column building via `functools.reduce(DataFrame.unionByName, dfs)`.

### Pipeline
1. Read each Parquet file individually with schema normalization
2. Cast all columns to common types (long for IDs, double for amounts)
3. Handle Airport_fee → airport_fee case rename
4. Union all months via `unionByName`
5. Clean: trip_distance 0–100mi, fare 0–500, passenger_count > 0
6. Compute temporal features + trip_duration_sec
7. Aggregate: per-zone per-hour demand (PULocationID, hour_of_day, day_of_week)
8. Write demand aggregates to Parquet

### Results
| Metric | Value |
|--------|-------|
| Raw rows | 9,384,487 |
| Cleaned rows | 8,807,357 |
| Removed | 577,130 (6.1%) |
| Demand agg rows | 192,916 |
| Avg fare | $18.72 |
| Avg trip distance | (NYC miles) |
| Avg duration | 978 sec |
| Output size | 4.3 MiB |

---

## Task 3: KPI Analytics (compute_kpis.py)

**Input**: `s3a://curated/trips/` (Porto curated, 1,228,061 trips)  
**Output**: `s3a://curated/kpis/` (6 datasets)  

### KPI Datasets
| Dataset | Rows | Description |
|---------|------|-------------|
| `trips_per_zone/` | 16 | Trip count, avg duration/distance per zone |
| `hourly_demand/` | 24 | Hourly demand curve |
| `daily_pattern/` | 7 | Day-of-week trip patterns |
| `zone_hour_heatmap/` | 384 | Zone × hour demand matrix |
| `coverage_gaps/` | 183 | Zone-hour combos with < 10% supply/demand |
| `call_type_breakdown/` | 3 | Trip distribution by call type |

### Key Findings
- **Peak hour**: 14:00 (69,023 trips)
- **Busiest day**: Friday (200,660 trips)
- **Quietest day**: Sunday (171,094 trips, avg 487s — shorter weekend trips)
- **Coverage gaps**: 183 zone-hour combos with < 10% supply/demand ratio
- **Worst coverage**: Ain Chock at 3 AM (1.7% ratio), Ben Msik at 3 AM (2.7%)
- **All 16 zones** represented in curated data

---

## Infrastructure Notes

- Spark worker RAM increased: 2 GB → 4 GB (5 GB limit) to prevent Docker OOM
- Spark master: 3 GB memory limit added
- SPARK_DRIVER_MEMORY: 2 GB
- All 3 jobs ran without Docker crashes after memory increase
- Porto ETL: ~3 min runtime
- NYC ETL: ~15 sec runtime  
- KPI job: ~12 sec runtime (exit code 0)

## MinIO Curated Bucket Summary

| Path | Size | Objects |
|------|------|---------|
| `curated/trips/` | 43 MiB | 13 |
| `curated/nyc-demand/` | 4.3 MiB | 5 |
| `curated/kpis/` | ~15 KiB | 12 |
| **Total curated/** | **49 MiB** | **59** |
