# TaaSim Project Master Status

Last updated: 2026-04-12
Project: TaaSim (Transport as a Service) - Casablanca

## Global Progress

- Week 1 / Sprint 1: COMPLETED
- Week 2 / Sprint 2: COMPLETED
- Week 3 / Sprint 3: COMPLETED
- Currently targeting: Week 4 (Flink Jobs 2 & 3)

## Task Board — Week 1

- [x] Task 1: Docker Compose stack provisioning
- [x] Task 2: Dataset download and upload to MinIO
- [x] Task 3: Porto EDA notebook
- [x] Task 4: Zone remapping Porto -> Casablanca (v5 geographic redesign, Gini 0.275)
- [x] Task 5: Kafka producers (GPS + trip requests)
- [x] Task 6 (Week 2): Storage Design — Kafka Connect S3 Sink + Cassandra schema + ADR-001

## Task Board — Week 2

- [x] Task 1: Kafka Connect S3 Sink (raw.gps + raw.trips → kafka-archive/)
- [x] Task 2: Cassandra schema verified (3 tables, INSERT + SELECT tested)
- [x] Task 3: ADR v1 written (documents/07_adr_v1.md)

## Task Board — Week 3

- [x] Task 1: Flink checkpointing configured (RocksDB + S3/MinIO, 60s interval, EXACTLY_ONCE)
- [x] Task 2: Flink Job 1 — GPS Normalizer (PyFlink DataStream, validates, assigns zone, anonymizes to centroid)
- [x] Task 3: Watermark test (late event test script, event-time watermarks with 3-min lateness)
- [x] Task 4: Grafana vehicle map dashboard (Geomap + bar chart + table panels)

## Evidence Files

- [01_task_docker_stack.md](01_task_docker_stack.md)
- [02_task_datasets_minio.md](02_task_datasets_minio.md)
- [03_task_porto_eda.md](03_task_porto_eda.md)
- [04_task_zone_remapping_v3.md](04_task_zone_remapping_v3.md)
- [05_task_kafka_producers.md](05_task_kafka_producers.md)
<<<<<<< HEAD
- [06_next_steps.md](06_next_steps.md)
- [07_adr_v1.md](07_adr_v1.md)

## Current State Summary

- 12 Docker containers running (Kafka, Kafka Connect, MinIO, Cassandra, Flink, Spark, Grafana, Jupyter + init containers)
- Kafka Connect S3 Sink archiving raw.gps and raw.trips to MinIO kafka-archive/ bucket
- Cassandra schema deployed with 3 tables (vehicle_positions, trips, demand_zones)
- GPS and trip producers tested end-to-end through Kafka → S3 Sink → MinIO
- Flink Job 1 (GPS Normalizer) running on cluster: raw.gps → validate → zone assign → centroid snap → Cassandra + processed.gps
- Flink checkpointing to MinIO (s3://curated/flink-checkpoints/) verified — 10+ checkpoints completed
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
>>>>>>> c9685c6 ([task-6] Week 2: Kafka Connect S3 Sink archival, ADR-001, fix mldata bucket name)
