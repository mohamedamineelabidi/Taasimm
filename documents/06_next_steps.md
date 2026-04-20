# Next Steps and Update Log

Last updated: 2026-04-19

## Immediate Next Steps (Week 7 — Security & Integration)

1. JWT auth hardening: rotate secrets, HTTPS/TLS for API
2. GPS anonymization audit: verify centroid snapping in Flink Job 1
3. Integration test: end-to-end pipeline (producer → Flink → Cassandra → API)
4. SLA measurement: trip match <5s P95, GPS freshness <15s, API <500ms at 20 req/s
5. Checkpoint recovery test: kill Flink TM, verify resume from checkpoint

## Pending Verifications

- Verify trip matching accuracy (nearest vehicle assignment)
- Verify adjacent zone fallback works when no vehicle in request zone
- Verify demand zone window triggers every 30s
- Verify S3 Sink archival over extended producer runs (8+ hours)

## Update Log

- 2026-04-20: Fixed S3A partitioned write silently failing on MinIO — root cause: FileOutputCommitter v1 uses server-side copy+delete rename which fails on MinIO with partitionBy(). Fix: algorithm.version=2 + fast.upload=true in spark-defaults.conf and SparkSession. Porto ETL re-verified: 1,481,827 rows in curated/trips/. KPI Analytics: 1.48M trips, busiest zone=5 (Sidi Bernoussi), peak hour=8:00. Feature engineering: 236,444 zone-window rows in mldata/features/. GBT model: RMSE=2.4491, R²=0.7943, 26% improvement over 7-day-lag baseline. Model saved to mldata/models/demand_v1/. Commit: 739aa42. API /api/vehicles/{zone_id} endpoint added + cassandra-driver==3.29.1 fix. Commit: 4a6bea9.

- 2026-04-19: Week 6 ML Pipeline complete — Feature engineering (183,981 rows), GBT model (RMSE 3.71, R² 0.75, 45.8% improvement), FastAPI API with JWT auth. Model saved to s3a://mldata/models/demand_v1/. Docker: added taasim-api service + numpy persistence. Evidence: 10_week6_ml_pipeline.md.
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
