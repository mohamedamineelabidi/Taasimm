# Project Guidelines

## Architecture
- This project is a local Big Data platform for urban mobility simulation (TaaSim).
- Core service boundaries:
  - Kafka for event transport
  - Flink for realtime processing
  - Spark for batch ETL and ML
  - MinIO for S3-compatible storage
  - Cassandra for serving tables
  - Grafana for observability
- Use existing configuration files as source of truth:
  - docker-compose.yml
  - config/cassandra-init.cql
  - config/spark-defaults.conf

## Build and Run
- Preferred startup order for infrastructure work:
  1. Ensure Docker daemon is running.
  2. Run: docker compose up -d
  3. Validate service reachability before debugging application code.
- Producer smoke tests:
  - .venv/Scripts/python.exe producers/vehicle_gps_producer.py --max-trips 5
  - .venv/Scripts/python.exe producers/trip_request_producer.py --max-trips 5
- If S3 integration fails, verify extra JAR preparation scripts:
  - download-jars.ps1
  - download-jars.sh

## Data and Geography Conventions
- Treat Casablanca remapping as v3 baseline.
- Keep remapping constants aligned between:
  - producers/config.py
  - notebooks/02_zone_remapping.ipynb
  - data/zone_mapping.csv
- Do not replace irregular geographic tessellation with a uniform grid.
- Preserve adjacency metadata in data/zone_mapping.csv for downstream matching logic.

## Documentation Conventions
- Update status documentation after meaningful progress in task files under documents/.
- Use the master index as entry point: documents/00_master_status.md
- Add new operational updates to: documents/06_next_steps.md
- Link to existing docs instead of duplicating large narrative content:
  - Taasim_project.md
  - Sprint1.md
  - documents/00_master_status.md

## Practical Pitfalls
- If Kafka tests fail with NoBrokersAvailable, check Docker daemon and broker availability first.
- Prefer fixing runtime availability issues before changing producer logic.
- Keep changes small and focused; avoid unrelated refactors when implementing sprint tasks.
