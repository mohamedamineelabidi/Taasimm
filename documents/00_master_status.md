# TaaSim Project Master Status

Last updated: 2026-04-03
Project: TaaSim (Transport as a Service) - Casablanca

## Global Progress

- Week 1 / Sprint foundation: mostly completed
- Tasks completed: 1, 2, 3, 4, 5
- Main blocker currently: local Docker engine is not running, so Kafka smoke tests cannot run now

## Task Board

- [x] Task 1: Docker Compose stack provisioning
- [x] Task 2: Dataset download and upload to MinIO
- [x] Task 3: Porto EDA notebook
- [x] Task 4: Zone remapping Porto -> Casablanca (v3 geographic redesign)
- [x] Task 5: Kafka producers (GPS + trip requests)

## Evidence Files

- [01_task_docker_stack.md](01_task_docker_stack.md)
- [02_task_datasets_minio.md](02_task_datasets_minio.md)
- [03_task_porto_eda.md](03_task_porto_eda.md)
- [04_task_zone_remapping_v3.md](04_task_zone_remapping_v3.md)
- [05_task_kafka_producers.md](05_task_kafka_producers.md)
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

## Current Risk

- Kafka producer smoke tests currently fail with NoBrokersAvailable because Docker daemon is unavailable on this machine session.
