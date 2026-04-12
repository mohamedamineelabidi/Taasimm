# Next Steps and Update Log

Last updated: 2026-04-12

## Immediate Next Steps

1. Begin Week 3: Flink Job 1 — GPS Normalizer
   - Read from `raw.gps` topic
   - Validate coordinates, deduplicate, apply event-time watermarks (3-min lateness)
   - Map GPS to zone, snap to centroid
   - Write to Cassandra `vehicle_positions` + Kafka `processed.gps`
2. Set up Grafana vehicle map panel (live updating from Cassandra)
3. Continue with Flink Jobs 2 and 3 in Week 4

## Pending Verifications

- Verify S3 Sink archival over longer producer runs
- Verify Spark can read from s3a://kafka-archive/ (end-to-end test)

## Update Log

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
