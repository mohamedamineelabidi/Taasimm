# TaaSim Agent Index

Compact guide for choosing the right specialist agent.

## Quick Selector

| If the task is mainly about... | Use this agent | Typical triggers |
|---|---|---|
| FastAPI behavior, route mismatch, Cassandra/API wiring, ML fallback mode | TaaSim API Integrator | route mismatch, /api/trips decision, model fallback, API contract drift |
| Data and notebook artifact hygiene, raw-vs-runtime classification, repo bloat cleanup | TaaSim Data Steward | data hygiene, ignored dataset audit, parquet/csv cleanup, notebook output cleanup |
| Kafka -> Flink -> Cassandra correctness, event_time/watermarks, matching flow | TaaSim Streaming Verifier | stale stream, watermark issues, schema drift, matching failures |
| README/doc claim verification against code and runtime behavior | TaaSim Doc Auditor | claim verification, roadmap drift, outdated docs, inconsistent architecture notes |
| Secret exposure, weak defaults, auth/CORS gaps, demo-vs-production risk | TaaSim Security Reviewer | hardcoded secrets, unsafe defaults, auth review, deployment hardening |

## First Files To Check

- TaaSim API Integrator:
  - api/main.py
  - api/requirements.txt
  - api/Dockerfile
  - README.MD
  - config/cassandra-init.cql
  - docker-compose.yml

- TaaSim Data Steward:
  - data/README.md
  - .gitignore
  - data/zone_mapping.csv
  - data/zone_mapping_v4.csv
  - data/h3_zone_lookup.json
  - data/casa_synthesis/
  - data/nyc-tlc/
  - data/hcp-data-casa/
  - notebooks/
  - scripts/

- TaaSim Streaming Verifier:
  - docker-compose.yml
  - producers/config.py
  - producers/trip_request_producer.py
  - producers/vehicle_gps_producer.py
  - flink/jobs/gps_normalizer.py
  - flink/jobs/demand_aggregator.py
  - flink/jobs/trip_matcher.py
  - config/cassandra-init.cql

- TaaSim Doc Auditor:
  - README.MD
  - documents/00_master_status.md
  - documents/06_next_steps.md
  - documents/08_week3_completion.md
  - documents/09_week5_spark_etl.md
  - documents/10_week6_ml_pipeline.md
  - documents/11_data_pipeline_architecture.md
  - documents/13_remapping_and_synthesis_deep_dive.md
  - api/main.py
  - producers/
  - flink/jobs/
  - config/kafka-connect/

- TaaSim Security Reviewer:
  - api/main.py
  - api/requirements.txt
  - api/Dockerfile
  - docker-compose.yml
  - README.MD
  - config/connect-s3-sink-gps.json
  - config/connect-s3-sink-trips.json
  - config/grafana-datasource.yml
  - .gitignore

## Operating Rule

For all five specialists:
1. Report findings first.
2. Avoid broad edits until findings are clear.
3. Run lightweight validation commands after changes.
