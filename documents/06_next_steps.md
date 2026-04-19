# Next Steps and Update Log

Last updated: 2026-04-19

## Immediate Next Steps (Week 6 — ML)

1. Feature engineering: build feature matrix from curated Porto trips (lag features, rolling averages, temporal features)
2. GBT model training: GBTRegressor (MLlib) — demand per zone per 30-min slot
3. Temporal train/test split: 10 months training, 2 months test
4. Naive baseline: 7-day-lag comparison (must beat this)
5. FastAPI forecast endpoint: POST /api/demand/forecast
6. Model artifact saved to s3a://mldata/models/demand_v1/

## Pending Verifications

- Verify trip matching accuracy (nearest vehicle assignment)
- Verify adjacent zone fallback works when no vehicle in request zone
- Verify demand zone window triggers every 30s
- Verify S3 Sink archival over extended producer runs (8+ hours)

## Update Log

- 2026-04-19: Week 5 Spark ETL complete — Porto ETL (43 MiB, 12 partitions), NYC ETL (4.3 MiB, 192K demand rows), KPI Analytics (6 datasets). All outputs verified in MinIO curated/. Docker RAM upgraded (worker 4g, master 3g limit). Evidence doc: 09_week5_spark_etl.md.
- 2026-04-17: Week 3 complete — All 3 Flink jobs deployed and running. TaskManagers scaled to 3 (12 slots). GPS Normalizer + Demand Aggregator + Trip Matcher operational. Kafka UI added. Data flow verified end-to-end. Created verification evidence doc (08_week3_completion.md).
- 2026-04-17: Flink Job 2 (Demand Aggregator) deployed: processes.gps + raw.trips → 30s tumbling windows → demand_zones Cassandra + processed.demand topic.
- 2026-04-17: Flink Job 3 (Trip Matcher) deployed: raw.trips + processed.gps → nearest vehicle matching → trips Cassandra + processed.matches topic.
- 2026-04-12: Week 3 Sprint 3 completed — all 4 deliverables done.
- 2026-04-12: Custom Flink Docker image built (PyFlink 1.18.1 + cassandra-driver + kafka-python).
- 2026-04-12: Flink Job 1 GPS Normalizer deployed and running (2/2 tasks, job f1100660).
- 2026-04-12: Centroid anonymization verified — raw GPS snapped to zone centroids in Cassandra.
- 2026-04-12: Flink checkpointing to MinIO verified — 10+ checkpoints in s3://curated/flink-checkpoints/.
- 2026-04-12: Late event watermark test created (scripts/test_late_events.py).
- 2026-04-12: Grafana vehicle tracking dashboard deployed (Geomap + zone bar chart + table).
- 2026-04-12: Week 2 Sprint 2 completed — all 3 deliverables done.
- 2026-04-12: Kafka Connect S3 Sink deployed and verified (raw.gps + raw.trips → kafka-archive/).
- 2026-04-12: Cassandra schema INSERT + SELECT tested on all 3 tables.
- 2026-04-12: ADR v1 created (documents/07_adr_v1.md) — Kappa, partition keys, MinIO, retention.
- 2026-04-12: Updated copilot-instructions.md with Week 2 progress (12 containers, S3 Sink configs, ADR).
- 2026-04-03: Rewrote copilot-instructions.md with full 11-section architecture reference (343 lines).
- 2026-04-03: Full infrastructure verification passed — all 11 containers healthy (Kafka, MinIO, Cassandra, Flink, Spark, Grafana, Jupyter).
- 2026-04-03: Added change tracking guidelines to copilot-instructions.md.
- 2026-04-03: Added full status documentation package under documents/.
- 2026-04-03: Completed v3 geographic remapping and validated notebook outputs.
- 2026-04-03: Identified Docker daemon runtime blocker for producer smoke test.
