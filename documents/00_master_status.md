# TaaSim Project Master Status

Last updated: 2026-04-12
Project: TaaSim (Transport as a Service) - Casablanca

## Global Progress

- Week 1 / Sprint 1: COMPLETED
- Week 2 / Sprint 2: COMPLETED
- Currently targeting: Week 3 (Flink Job 1: GPS normalizer)

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

## Evidence Files

- [01_task_docker_stack.md](01_task_docker_stack.md)
- [02_task_datasets_minio.md](02_task_datasets_minio.md)
- [03_task_porto_eda.md](03_task_porto_eda.md)
- [04_task_zone_remapping_v3.md](04_task_zone_remapping_v3.md)
- [05_task_kafka_producers.md](05_task_kafka_producers.md)
- [06_next_steps.md](06_next_steps.md)
- [07_adr_v1.md](07_adr_v1.md)

## Current State Summary

- 12 Docker containers running (Kafka, Kafka Connect, MinIO, Cassandra, Flink, Spark, Grafana, Jupyter + init containers)
- Kafka Connect S3 Sink archiving raw.gps and raw.trips to MinIO kafka-archive/ bucket
- Cassandra schema deployed with 3 tables (vehicle_positions, trips, demand_zones)
- GPS and trip producers tested end-to-end through Kafka → S3 Sink → MinIO
