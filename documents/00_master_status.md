# TaaSim Project Master Status

Last updated: 2026-04-19
Project: TaaSim (Transport as a Service) - Casablanca

## Global Progress

- Week 1 / Sprint 1: COMPLETED
- Week 2 / Sprint 2: COMPLETED
- Week 3 / Sprint 3: COMPLETED
- Week 5 / Spark ETL: COMPLETED
- Week 6 / ML Pipeline: COMPLETED
- Currently targeting: Week 7 (Security, integration tests, SLA)

## Task Board — Week 1

- [x] Task 1: Docker Compose stack provisioning
- [x] Task 2: Dataset download and upload to MinIO
- [x] Task 3: Porto EDA notebook
- [x] Task 4: Zone remapping Porto -> Casablanca (v5 geographic redesign, Gini 0.275)
- [x] Task 5: Kafka producers (GPS + trip requests)

## Task Board — Week 2

- [x] Task 1: Kafka Connect S3 Sink (raw.gps + raw.trips → kafka-archive/)
- [x] Task 2: Cassandra schema verified (3 tables, INSERT + SELECT tested)
- [x] Task 3: ADR v1 written (documents/07_adr_v1.md)

## Task Board — Week 3

- [x] Task 1: Flink checkpointing configured (RocksDB + S3/MinIO, 60s interval, EXACTLY_ONCE)
- [x] Task 2: Flink Job 1 — GPS Normalizer (PyFlink DataStream, validates, assigns zone, anonymizes to centroid)
- [x] Task 3: Watermark test (late event test script, event-time watermarks with 3-min lateness)
- [x] Task 4: Grafana vehicle map dashboard (Geomap + bar chart + table panels)

## Task Board — Week 5 (Spark ETL)

- [x] Task 1: Porto ETL — 1,710,670 raw → 1,660,794 valid trips, 12 Parquet partitions (43 MiB)
- [x] Task 2: NYC TLC ETL — 9.38M raw → 8.81M cleaned → 192K demand agg rows (4.3 MiB)
- [x] Task 3: KPI Analytics — 6 datasets: trips_per_zone, hourly_demand, daily_pattern, zone_hour_heatmap, coverage_gaps, call_type_breakdown

## Task Board — Week 6 (ML Pipeline)

- [x] Task 1: Feature Engineering — 183,981 feature rows (30-min slots, lag features, rolling means)
- [x] Task 2: GBT Model Training — RMSE 3.71, R² 0.75, 45.8% improvement over naive baseline
- [x] Task 3: FastAPI REST API — JWT auth, demand forecast, trip reservation, zone lookup

## Evidence Files

- [01_task_docker_stack.md](01_task_docker_stack.md)
- [02_task_datasets_minio.md](02_task_datasets_minio.md)
- [03_task_porto_eda.md](03_task_porto_eda.md)
- [04_task_zone_remapping_v3.md](04_task_zone_remapping_v3.md)
- [05_task_kafka_producers.md](05_task_kafka_producers.md)
- [06_next_steps.md](06_next_steps.md)
- [07_adr_v1.md](07_adr_v1.md)
- [08_week3_completion.md](08_week3_completion.md) ← Week 3 Verification Report
- [10_week6_ml_pipeline.md](10_week6_ml_pipeline.md) ← Week 6 ML Pipeline Report
- [09_week5_spark_etl.md](09_week5_spark_etl.md) ← Week 5 Spark ETL Report

## Cahier-des-charges Compliance Matrix

One row per grading-relevant cahier section. Status reflects current state on `test-workflow-dev`.

| Cahier § | Requirement | Implementation | Doc | Status |
|---|---|---|---|---|
| §2.1 | Porto → Casa linear transform + 22→16 zone remap | Phase 2 step 4 affine warp; 16 zones in `zone_mapping_v4.csv` | [13_remapping_and_synthesis_deep_dive.md](13_remapping_and_synthesis_deep_dive.md) §0, §4 | ✅ |
| §2.2 | NYC = batch only, never streamed | NYC Parquet read by Spark ETL (Week 5) and as offline fingerprint (Phase 3); never reaches Kafka | [13](13_remapping_and_synthesis_deep_dive.md) §5.1 box | ✅ |
| §2.3 | Lightweight simulator, ±20 m drift, 5% blackout, out-of-order up to 3 min | Producer emits Gaussian jitter σ≈20 m + 5% blackout (60–180 s late) | [05_task_kafka_producers.md](05_task_kafka_producers.md) | ✅ |
| §3.3 | Three Flink jobs, event-time watermarks, RocksDB, MinIO checkpoints | Jobs 1/2/3 running, 60 s checkpoint cadence, 3-min allowed lateness | [08_week3_completion.md](08_week3_completion.md) | ✅ |
| §4.1 | Cassandra: 3 tables with partition-key justification | `cassandra-init.cql` + ADR | [07_adr_v1.md](07_adr_v1.md) | ✅ |
| §4.2 | MinIO zones raw/curated/mldata/kafka-archive | All four buckets active | [06_task_week2_storage.md](06_task_week2_storage.md) | ✅ |
| §5.3 | ML GBT must beat naive 7-day-lag baseline; baseline-vs-model table | RMSE 3.71 vs 6.84 (45.8% improvement) | [10_week6_ml_pipeline.md](10_week6_ml_pipeline.md) | ✅ |
| §6.1 | SLA targets (< 5 s match P95, < 15 s GPS freshness, …) | Measured live: match P95 ≈ 1.2 s, GPS freshness ≈ 4 s | [10_week6_ml_pipeline.md](10_week6_ml_pipeline.md) + Grafana | ✅ |
| §6.3 | GPS anonymization — centroid snap before Cassandra write | Flink Job 1 snaps to `zone.centroid_lat/lon`; raw lat/lon never persisted | [08_week3_completion.md](08_week3_completion.md) anonymization audit | ✅ |
| §6.3 | FastAPI JWT auth, 2 roles | `api/main.py` implements `rider` + `admin` roles, HS256, 24 h | [10_week6_ml_pipeline.md](10_week6_ml_pipeline.md) | ✅ |
| §6.3 | Kafka topic ACLs (raw.* producers, processed.* admin) | ⚠ Week 7 pending | — | 🟡 Pending |
| Week 7 | Checkpoint-recovery demo (kill TM, job resumes) | ⚠ Recording pending | — | 🟡 Pending |
| Week 8 | Live demo + pitch deck + technical report | Draft presentation [12_presentation_week6.md] exists | — | 🟡 Pending |

**Beyond-cahier extensions** (none cross the Kappa boundary): OSRM routing, A–E tier system, HCP/Glovo spatial weights, trajectory index, Phase 3 NYC fingerprint synthesis. Documented in [13 §11](13_remapping_and_synthesis_deep_dive.md#11-beyond-the-cahier--what-we-added-and-why-it-is-still-in-scope).

---

## Current State Summary

- 16 Docker containers running (Kafka, Kafka Connect, Kafka UI, MinIO, Cassandra, Flink JM + 3 TMs scaled, Spark, Grafana, Jupyter + init containers)
- Kafka Connect S3 Sink archiving raw.gps and raw.trips to MinIO kafka-archive/ bucket
- Cassandra schema deployed with 3 tables (vehicle_positions, trips, demand_zones)
- GPS and trip producers tested end-to-end through Kafka → S3 Sink → MinIO
- **Flink Job 1 (GPS Normalizer)** running on cluster: raw.gps → validate → zone assign → centroid snap → Cassandra + processed.gps
- **Flink Job 2 (Demand Aggregator)** running: processed.gps + raw.trips → 30s windowed aggregation → demand_zones Cassandra + processed.demand
- **Flink Job 3 (Trip Matcher)** running: raw.trips + processed.gps → nearest vehicle match → trip assignment Cassandra + processed.matches
- Flink TaskManagers scaled to 3 instances (12 total slots, 9 slots occupied by 3 jobs)
- Flink checkpointing to MinIO (s3://curated/flink-checkpoints/) verified — 10+ checkpoints completed
- Grafana vehicle tracking dashboard deployed with Geomap, zone bar chart, and event table
- Kafka UI added to stack for topic monitoring
- GPS producer generating 10× replay speed from Porto dataset with zone remapping + noise

## Week 3 Verification Summary (2026-04-17)

**Pipeline Status**: ✅ OPERATIONAL END-TO-END
- All 3 Flink jobs deployed and running successfully
- Flink Web UI: http://localhost:8081 (showing 12 slots, 3 jobs)
- TaskManagers: 3 running (scaled deployment)
- Kafka topics: raw.gps, raw.trips, processed.gps, processed.demand, processed.matches all active
- Cassandra: schema verified, ready for data ingestion
- MinIO: kafka-archive bucket receiving S3 Sink data
- Grafana: dashboards configured, ready for visualization

**Data Flow Confirmation**:
- GPS events: raw.gps topic active → Flink Job 1 consuming → processed.gps producing
- Trip requests: raw.trips topic active → Flink Jobs 2, 3 consuming
- Producers: vehicle_gps_producer.py and trip_request_producer.py tested (50 trips successful)

**Infrastructure Health**:
- Flink JobManager: HEALTHY
- All TaskManagers: RUNNING
- Kafka: HEALTHY
- Cassandra: HEALTHY
- MinIO: HEALTHY
- Grafana: RUNNING (health check starting)
- Kafka UI: RUNNING (health check starting)
