# Architecture Decision Record — TaaSim v1

**Date**: 2025-04-12  
**Status**: Accepted  
**Authors**: TaaSim Team — ENSAH Capstone 2025–2026

---

## 1. Kappa Architecture (over Lambda)

**Decision**: All real-time processing goes through Kafka → Flink. Spark is used exclusively for offline batch ETL and ML training — not for serving queries.

**Context**: TaaSim requires sub-5s trip matching and 30s demand updates. Lambda would duplicate logic across batch and speed layers.

**Rationale**:
- Kafka is the single source of truth (7-day retention, 4 partitions per topic)
- Flink handles all three streaming jobs: GPS normalization, demand aggregation, trip matching
- Spark runs offline only: historical ETL on Porto/NYC datasets and GBT model training
- No dual-write or reconciliation complexity — one processing path per event
- Kafka Connect S3 Sink archives raw events to MinIO for ML replay without affecting the live pipeline

**Trade-off**: We lose the ability to reprocess unbounded history in real-time. Acceptable because Kafka's 7-day retention covers our replay needs, and Spark handles anything older via MinIO archives.

---

## 2. Cassandra Partition Key Choices

**Decision**: Partition keys are designed by API query pattern, not by entity normalization.

| Table | Partition Key | Query Pattern | Justification |
|-------|-------------|---------------|---------------|
| `vehicle_positions` | `(city, zone_id)` | "All vehicles in zone X" | Grafana map + Flink trip matcher need zone-level lookups. Partitioning by `taxi_id` would scatter zone queries across all partitions. |
| `trips` | `(city, date_bucket)` | "Trip history for a given day" | `date_bucket` (TEXT, e.g. '2025-04-12') bounds partition growth. Without it, a single city partition grows unboundedly. |
| `demand_zones` | `(city, zone_id)` | "Demand heatmap per zone" | Each zone's demand time-series is self-contained. Grafana queries one zone at a time for heatmap rendering. |

**Clustering keys**: All tables use `DESC` ordering on timestamp columns so `LIMIT N` returns the most recent records without a full scan.

**TTLs**: `vehicle_positions` = 24h (stale GPS is useless), `demand_zones` = 7 days (weekly trend analysis), `trips` = no TTL (permanent record).

---

## 3. MinIO Bucket Structure

**Decision**: Four top-level buckets organized by data lifecycle stage.

| Bucket | Purpose | Writer | Reader |
|--------|---------|--------|--------|
| `raw/` | Immutable source data (Porto CSV, NYC Parquet) | Manual upload | Spark ETL |
| `curated/` | Cleaned, geo-enriched Parquet + Flink checkpoints | Spark ETL, Flink | Spark ML, Flink |
| `ml/` | Feature matrices and trained models | Spark ML | FastAPI |
| `kafka-archive/` | Kafka Connect S3 Sink mirror of raw topics | Kafka Connect | Spark ETL |

**Rationale**: Separating by lifecycle stage (raw → curated → ml) enables independent access control, retention policies, and clear data lineage. `kafka-archive/` bridges streaming and batch worlds — raw events are archived hourly in JSON format, partitioned by `year/month/day/hour` for efficient Spark reads.

---

## 4. Kafka Topic Retention

**Decision**: 7-day retention (168 hours) on all topics. No compaction.

**Rationale**:
- 7 days covers a full business week for replay/debugging
- Kafka Connect S3 Sink provides permanent archival to MinIO — no data loss after retention expiry
- No log compaction because GPS and trip events are immutable time-series, not key-value state
- 4 partitions per topic: sufficient for 1-broker dev setup while allowing parallel Flink consumption

**Risk**: If Kafka Connect is down for >7 days, events are lost. Mitigation: Grafana alert on connector status + MinIO archive verification script.
