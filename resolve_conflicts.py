import os

f = 'documents/00_master_status.md'
content = open(f, encoding='utf-8').read()

part1_head = """<<<<<<< HEAD
- [06_next_steps.md](06_next_steps.md)
- [07_adr_v1.md](07_adr_v1.md)

## Current State Summary

- 12 Docker containers running (Kafka, Kafka Connect, MinIO, Cassandra, Flink, Spark, Grafana, Jupyter + init containers)
- Kafka Connect S3 Sink archiving raw.gps and raw.trips to MinIO kafka-archive/ bucket
- Cassandra schema deployed with 3 tables (vehicle_positions, trips, demand_zones)
- GPS and trip producers tested end-to-end through Kafka â†’ S3 Sink â†’ MinIO
- Flink Job 1 (GPS Normalizer) running on cluster: raw.gps â†’ validate â†’ zone assign â†’ centroid snap â†’ Cassandra + processed.gps
- Flink checkpointing to MinIO (s3://curated/flink-checkpoints/) verified â€” 10+ checkpoints completed
- Grafana vehicle tracking dashboard deployed with Geomap, zone bar chart, and event table
=======
- [06_task_week2_storage.md](06_task_week2_storage.md)
- [07_adr_storage.md](07_adr_storage.md)
- [next_steps.md](next_steps.md)

## Current State Summary

- Geographic remapping was upgraded from a uniform grid to an irregular Casablanca-aware zone design.
- New remapping notebook was created, executed, validated, and cleaned from duplicate legacy cells.
- Key quality outputs generated:
  - data/zone_mapping.csv
  - data/zone_centroids.csv
  - data/remapped_trips_sample.csv
  - notebooks/02_zone_remapping.ipynb
  - notebooks/casablanca_zone_map.html

## Stack Size

13 containers total: Kafka, MinIO, minio-init, Cassandra, cassandra-init, Flink JM, Flink TM, Spark master, Spark worker, Grafana, Jupyter, Kafka Connect, connect-init.

## Current Risk

- Kafka Connect first startup requires internet (downloads S3 Sink plugin from Confluent Hub). Subsequent restarts use the cached plugin.
- Producers must be running to generate data in `kafka-archive/`; an empty `raw.gps` topic produces no archived files.
>>>>>>> c9685c6 ([task-6] Week 2: Kafka Connect S3 Sink archival, ADR-001, fix mldata bucket name)"""

part1_fix = """- [06_task_week2_storage.md](06_task_week2_storage.md)
- [07_adr_storage.md](07_adr_storage.md)
- [06_next_steps.md](06_next_steps.md)

## Current State Summary

- Geographic remapping was upgraded from a uniform grid to an irregular Casablanca-aware zone design.
- New remapping notebook was created, executed, validated, and cleaned from duplicate legacy cells.
- Key quality outputs generated:
  - data/zone_mapping.csv
  - data/zone_centroids.csv
  - data/remapped_trips_sample.csv
  - notebooks/02_zone_remapping.ipynb
  - notebooks/casablanca_zone_map.html
- 13 Docker containers running (Kafka, MinIO, minio-init, Cassandra, cassandra-init, Flink JM, Flink TM, Spark master, Spark worker, Grafana, Jupyter, Kafka Connect, connect-init)
- Kafka Connect S3 Sink archiving raw.gps and raw.trips to MinIO kafka-archive/ bucket
- Cassandra schema deployed with 3 tables (vehicle_positions, trips, demand_zones)
- GPS and trip producers tested end-to-end through Kafka â†’ S3 Sink â†’ MinIO
- Flink Job 1 (GPS Normalizer) running on cluster: raw.gps â†’ validate â†’ zone assign â†’ centroid snap â†’ Cassandra + processed.gps
- Flink checkpointing to MinIO (s3://curated/flink-checkpoints/) verified â€” 10+ checkpoints completed
- Grafana vehicle tracking dashboard deployed with Geomap, zone bar chart, and event table

## Current Risk

- Kafka Connect first startup requires internet (downloads S3 Sink plugin from Confluent Hub). Subsequent restarts use the cached plugin.
- Producers must be running to generate data in `kafka-archive/`; an empty `raw.gps` topic produces no archived files."""

new_content = content.replace(part1_head, part1_fix)
open(f, 'w', encoding='utf-8').write(new_content)


f2 = 'documents/06_next_steps.md'
content2 = open(f2, encoding='utf-8').read()

part2_head = """<<<<<<< HEAD
1. Begin Week 4: Flink Job 2 â€” Demand Aggregator (30s tumbling windows per zone)
2. Flink Job 3 â€” Trip Matcher (nearest vehicle + adjacent zone fallback)
3. Grafana demand heatmap panel
=======
1. Start Docker Desktop / Docker daemon.
2. Run `docker compose up -d` (stack now 13 containers; first start pulls `cp-kafka-connect-base:7.7.0` and downloads S3 Sink plugin).
3. Wait for all healthchecks â€” especially Kafka Connect (~3 min) and Cassandra (~1 min).
4. Run producer smoke tests:
   - `.venv/Scripts/python.exe producers/vehicle_gps_producer.py --max-trips 5`
   - `.venv/Scripts/python.exe producers/trip_request_producer.py --max-trips 5`
5. Verify S3 Sink connector is running: `curl -s http://localhost:8083/connectors/taasim-s3-sink/status`
6. Verify archived files appear: `docker exec taasim-minio mc ls -r local/kafka-archive/`
7. Begin Week 3: Flink Job 1 (GPS Normalizer) + Grafana vehicle positions panel.
>>>>>>> c9685c6 ([task-6] Week 2: Kafka Connect S3 Sink archival, ADR-001, fix mldata bucket name)"""

part2_fix = """1. Begin Week 4: Flink Job 2 â€” Demand Aggregator (30s tumbling windows per zone)
2. Flink Job 3 â€” Trip Matcher (nearest vehicle + adjacent zone fallback)
3. Grafana demand heatmap panel
4. Run `docker compose up -d` (13 containers total).
5. Verify S3 Sink connector is running: `curl -s http://localhost:8083/connectors/taasim-s3-sink/status`"""

part3_head = """<<<<<<< HEAD
- 2026-04-12: Week 3 Sprint 3 completed â€” all 4 deliverables done.
- 2026-04-12: Custom Flink Docker image built (PyFlink 1.18.1 + cassandra-driver + kafka-python).
- 2026-04-12: Flink Job 1 GPS Normalizer deployed and running (2/2 tasks, job f1100660).
- 2026-04-12: Centroid anonymization verified â€” raw GPS snapped to zone centroids in Cassandra.
- 2026-04-12: Flink checkpointing to MinIO verified â€” 10+ checkpoints in s3://curated/flink-checkpoints/.
- 2026-04-12: Late event watermark test created (scripts/test_late_events.py).
- 2026-04-12: Grafana vehicle tracking dashboard deployed (Geomap + zone bar chart + table).
- 2026-04-12: Week 2 Sprint 2 completed â€” all 3 deliverables done.
- 2026-04-12: Kafka Connect S3 Sink deployed and verified (raw.gps + raw.trips â†’ kafka-archive/).
- 2026-04-12: Cassandra schema INSERT + SELECT tested on all 3 tables.
- 2026-04-12: ADR v1 created (documents/07_adr_v1.md) â€” Kappa, partition keys, MinIO, retention.
- 2026-04-12: Updated copilot-instructions.md with Week 2 progress (12 containers, S3 Sink configs, ADR).
=======
- 2026-04-07: Week 2 complete â€” Kafka Connect S3 Sink added to stack, connector config created, ADR-001 written, helper scripts added.
>>>>>>> c9685c6 ([task-6] Week 2: Kafka Connect S3 Sink archival, ADR-001, fix mldata bucket name)"""

part3_fix = """- 2026-04-12: Week 3 Sprint 3 completed â€” all 4 deliverables done.
- 2026-04-12: Custom Flink Docker image built (PyFlink 1.18.1 + cassandra-driver + kafka-python).
- 2026-04-12: Flink Job 1 GPS Normalizer deployed and running (2/2 tasks, job f1100660).
- 2026-04-12: Centroid anonymization verified â€” raw GPS snapped to zone centroids in Cassandra.
- 2026-04-12: Flink checkpointing to MinIO verified â€” 10+ checkpoints in s3://curated/flink-checkpoints/.
- 2026-04-12: Late event watermark test created (scripts/test_late_events.py).
- 2026-04-12: Grafana vehicle tracking dashboard deployed (Geomap + zone bar chart + table).
- 2026-04-12: Week 2 Sprint 2 completed â€” all 3 deliverables done.
- 2026-04-12: Kafka Connect S3 Sink deployed and verified (raw.gps + raw.trips â†’ kafka-archive/).
- 2026-04-12: Cassandra schema INSERT + SELECT tested on all 3 tables.
- 2026-04-12: ADR v1 created (documents/07_adr_v1.md) â€” Kappa, partition keys, MinIO, retention.
- 2026-04-12: Updated copilot-instructions.md with Week 2 progress (12 containers, S3 Sink configs, ADR).
- 2026-04-07: Week 2 complete â€” Kafka Connect S3 Sink added to stack, connector config created, ADR-001 written, helper scripts added."""

new_content2 = content2.replace(part2_head, part2_fix).replace(part3_head, part3_fix)
open(f2, 'w', encoding='utf-8').write(new_content2)
print("done")
