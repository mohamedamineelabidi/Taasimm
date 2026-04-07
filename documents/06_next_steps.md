# Next Steps and Update Log

Last updated: 2026-04-12

## Immediate Next Steps

<<<<<<< HEAD
1. Begin Week 4: Flink Job 2 — Demand Aggregator (30s tumbling windows per zone)
2. Flink Job 3 — Trip Matcher (nearest vehicle + adjacent zone fallback)
3. Grafana demand heatmap panel
=======
1. Start Docker Desktop / Docker daemon.
2. Run `docker compose up -d` (stack now 13 containers; first start pulls `cp-kafka-connect-base:7.7.0` and downloads S3 Sink plugin).
3. Wait for all healthchecks — especially Kafka Connect (~3 min) and Cassandra (~1 min).
4. Run producer smoke tests:
   - `.venv/Scripts/python.exe producers/vehicle_gps_producer.py --max-trips 5`
   - `.venv/Scripts/python.exe producers/trip_request_producer.py --max-trips 5`
5. Verify S3 Sink connector is running: `curl -s http://localhost:8083/connectors/taasim-s3-sink/status`
6. Verify archived files appear: `docker exec taasim-minio mc ls -r local/kafka-archive/`
7. Begin Week 3: Flink Job 1 (GPS Normalizer) + Grafana vehicle positions panel.
>>>>>>> c9685c6 ([task-6] Week 2: Kafka Connect S3 Sink archival, ADR-001, fix mldata bucket name)

## Pending Verifications

- Verify S3 Sink archival over longer producer runs
- Verify Spark can read from s3a://kafka-archive/ (end-to-end test)

## Update Log

<<<<<<< HEAD
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
=======
- 2026-04-07: Week 2 complete — Kafka Connect S3 Sink added to stack, connector config created, ADR-001 written, helper scripts added.
>>>>>>> c9685c6 ([task-6] Week 2: Kafka Connect S3 Sink archival, ADR-001, fix mldata bucket name)
- 2026-04-03: Rewrote copilot-instructions.md with full 11-section architecture reference (343 lines).
- 2026-04-03: Full infrastructure verification passed — all 11 containers healthy (Kafka, MinIO, Cassandra, Flink, Spark, Grafana, Jupyter).
- 2026-04-03: Added change tracking guidelines to copilot-instructions.md.
- 2026-04-03: Added full status documentation package under documents/.
- 2026-04-03: Completed v3 geographic remapping and validated notebook outputs.
- 2026-04-03: Identified Docker daemon runtime blocker for producer smoke test.
