# TaaSim — Copilot Project Instructions

> **Role**: Senior Data Engineer & Software Engineer building TaaSim (Transport as a Service), an urban mobility simulation platform for Casablanca, Morocco.
> **Course**: Advanced Big Data Capstone — ENSAH, 2025–2026.
> **Duration**: 8 weeks. Week 1 complete. Currently targeting Week 2+.

---

## 1. Architecture Overview

TaaSim follows a **Kappa Architecture**: Kafka is the system of record, Flink handles all real-time processing, and Spark operates exclusively offline for batch ETL and ML training.

| Layer | Technology | Purpose |
|-------|-----------|---------|
| Event bus | Kafka 3.7.0 (KRaft, 1 broker) | Streams GPS + trip events, 7-day retention, 4 partitions default |
| Object store | MinIO (S3-compatible) | Data lake: `raw/`, `curated/`, `ml/`, `kafka-archive/` |
| Stream processing | Flink 1.18 (1 JM + 1 TM, 4 slots) | 3 jobs: GPS normalizer, demand aggregator, trip matcher |
| Batch + ML | Spark 3.5.4 (PySpark) | ETL on Porto/NYC data, GBT demand forecasting (MLlib) |
| Serving DB | Cassandra 4.1 | 3 tables: `vehicle_positions`, `trips`, `demand_zones` |
| Dashboard | Grafana 10.4.0 | Vehicle map, demand heatmap, KPI panels, ML forecast overlay |
| API | FastAPI (Python) | REST: trip reservation, vehicle lookup, demand forecast. JWT auth. |
| Notebooks | Jupyter (pyspark-notebook) | EDA and zone remapping analysis |

### Source-of-Truth Files
- `docker-compose.yml` — all 11 container definitions (Kafka, MinIO, minio-init, Cassandra, cassandra-init, Flink JM, Flink TM, Spark master, Spark worker, Grafana, Jupyter)
- `config/cassandra-init.cql` — keyspace `taasim` + 3 tables with partition key justification
- `config/spark-defaults.conf` — S3A endpoint, hadoop-aws/aws-sdk JARs, Kryo serializer
- `producers/config.py` — shared constants, bounding-box transform, zone loader

### Kafka Topics
| Topic | Producer | Consumer |
|-------|---------|----------|
| `raw.gps` | vehicle_gps_producer.py | Flink Job 1 |
| `raw.trips` | trip_request_producer.py | Flink Job 2, Job 3 |
| `processed.gps` | Flink Job 1 | Flink Job 2 |
| `processed.demand` | Flink Job 2 | Grafana / API |
| `processed.matches` | Flink Job 3 | API |

### Cassandra Schema (keyspace: `taasim`)
| Table | Partition Key | Clustering | TTL |
|-------|-------------|-----------|-----|
| `vehicle_positions` | `(city, zone_id)` | `event_time DESC, taxi_id ASC` | 24h |
| `trips` | `(city, date_bucket)` | `created_at DESC, trip_id ASC` | none |
| `demand_zones` | `(city, zone_id)` | `window_start DESC` | 7 days |

### MinIO Bucket Layout
| Bucket/Prefix | Contents | Writer | Reader |
|--------------|---------|--------|--------|
| `raw/porto-trips/` | Porto CSV (train.csv, 1.8 GiB) | Manual upload | Spark ETL |
| `raw/nyc-tlc/` | NYC TLC Parquet (3 months, ~150 MiB) | Manual upload | Spark ETL |
| `raw/kafka-archive/` | Kafka S3 Sink mirror | Kafka Connect | Spark ETL |
| `curated/trips/` | Cleaned geo-enriched trips (Parquet) | Spark ETL | Spark ML |
| `curated/flink-checkpoints/` | Flink checkpoint data | Flink | Flink |
| `ml/features/` | Feature matrix for ML training | Spark | Spark MLlib |
| `ml/models/demand_v1/` | Trained GBT model (PipelineModel) | Spark ML | FastAPI |

---

## 2. Build, Run & Validate

### Prerequisites
- Docker Desktop running (Docker Engine 28.x+)
- Python 3.13+ virtual environment: `.venv/`
- JARs downloaded: run `download-jars.ps1` (Windows) or `download-jars.sh` (Linux/Mac) once before first `docker compose up`

### Startup Sequence
```
1. Ensure Docker daemon is running
2. docker compose up -d
3. Wait for healthchecks (Cassandra ~33s, Flink ~17s, Spark ~18s)
4. Verify services before debugging application code
```

### Service Endpoints (Host)
| Service | URL/Port | Credentials |
|---------|---------|-------------|
| Kafka (host) | `localhost:9092` | — |
| Kafka (containers) | `kafka:9092` | — |
| MinIO S3 API | `localhost:9000` | minioadmin / minioadmin |
| MinIO Console | `localhost:9001` | minioadmin / minioadmin |
| Cassandra CQL | `localhost:9042` | — |
| Flink Web UI | `localhost:8081` | — |
| Spark Master UI | `localhost:8080` | — |
| Spark App UI | `localhost:4040` | — |
| Grafana | `localhost:3000` | admin / admin |
| Jupyter Lab | `localhost:8888` | token from logs |

### Smoke Tests
```powershell
# GPS producer (5 trips)
.venv/Scripts/python.exe producers/vehicle_gps_producer.py --max-trips 5

# Trip request producer (5 trips)
.venv/Scripts/python.exe producers/trip_request_producer.py --max-trips 5
```

### Health Verification Commands
```bash
# Kafka
docker exec taasim-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list

# MinIO
docker exec taasim-minio mc alias set local http://localhost:9000 minioadmin minioadmin
docker exec taasim-minio mc ls local/

# Cassandra
docker exec taasim-cassandra cqlsh -e "USE taasim; DESCRIBE TABLES;"

# Flink
docker exec taasim-flink-jm curl -s http://localhost:8081/overview

# Spark
docker exec taasim-spark-master curl -s http://localhost:8080/json/

# Grafana
docker exec taasim-grafana curl -s http://localhost:3000/api/health
```

---

## 3. Data & Geography Conventions

### Zone Remapping (v3 — Current Baseline)
- 16 irregular Casablanca zones (arrondissements), NOT a uniform grid
- 4 latitude bands: South (3 zones), Mid-South (4), Center (5), North (4)
- Porto GPS coordinates linearly transformed to Casablanca bounding box + Gaussian noise (σ=0.0002°)

### Bounding Boxes
| City | Lat Min | Lat Max | Lon Min | Lon Max |
|------|---------|---------|---------|---------|
| Porto (v3 shifted) | 41.085 | 41.195 | -8.690 | -8.560 |
| Casablanca | 33.450 | 33.680 | -7.720 | -7.480 |

### Critical Alignment Rule
These three sources MUST stay in sync for any constant change:
1. `producers/config.py` — transform bounds + zone loader
2. `notebooks/02_zone_remapping.ipynb` — analysis notebook
3. `data/zone_mapping.csv` — 16 zones with lat/lon bounds, centroids, adjacency

### Data Rules
- Never replace irregular geographic tessellation with a uniform grid
- Preserve `adjacent_zones` column in zone_mapping.csv — required by Flink Job 3 trip matcher fallback
- GPS anonymization in Flink Job 1: snap raw lat/lon to zone centroid before writing to Cassandra
- Porto dataset replay at 10× speed, with ±20m GPS drift noise and 5% blackout probability

---

## 4. Project Directory Structure

```
Taasimm/
├── .github/copilot-instructions.md    ← this file
├── .gitignore
├── docker-compose.yml                 ← stack definition (source of truth)
├── download-jars.ps1                  ← Windows JAR downloader
├── download-jars.sh                   ← Linux/Mac JAR downloader
├── upload-datasets.ps1                ← dataset upload to MinIO
├── config/
│   ├── cassandra-init.cql             ← Cassandra schema
│   └── spark-defaults.conf            ← Spark S3A config
├── data/
│   ├── zone_mapping.csv               ← 16 Casablanca zones + adjacency
│   ├── zone_centroids.csv             ← zone centroid coordinates
│   ├── train.csv                      ← Porto dataset (gitignored, in MinIO)
│   ├── nyc-tlc/                       ← NYC Parquet (gitignored, in MinIO)
│   └── remapped_trips_sample.csv      ← sample output (gitignored)
├── producers/
│   ├── config.py                      ← shared constants & transforms
│   ├── vehicle_gps_producer.py        ← GPS event simulator
│   └── trip_request_producer.py       ← trip request simulator
├── notebooks/
│   ├── 01_porto_eda.ipynb             ← Porto EDA analysis
│   ├── 02_zone_remapping.ipynb        ← v3 zone remapping
│   └── casablanca_zone_map.html       ← interactive zone map
├── jars/                              ← downloaded JARs (gitignored)
│   ├── flink/                         ← flink-s3, kafka connector, cassandra connector
│   └── spark/                         ← hadoop-aws, aws-sdk-bundle
├── scripts/                           ← utility scripts
├── documents/                         ← task status documentation
│   ├── 00_master_status.md            ← master index
│   ├── 01–05_task_*.md                ← per-task evidence
│   └── 06_next_steps.md               ← roadmap + update log
├── Taasim_project_description_all.md  ← full project specification
└── Sprint1.md                         ← Week 1 task descriptions
```

### What NOT to Track in Git (.gitignore)
- `data/train.csv`, `data/nyc-tlc/`, `data/remapped_trips_sample.csv` — large files stored in MinIO
- `jars/` — downloaded via scripts, not tracked
- `.venv/`, `__pycache__/`, `.ipynb_checkpoints/`

---

## 5. Weekly Roadmap & Current Progress

| Week | Focus | Status |
|------|-------|--------|
| **1** | Docker stack, datasets, EDA, zone remapping, Kafka producers | **DONE** |
| **2** | Storage design: Kafka Connect S3 Sink, Cassandra schema ADR | Next |
| **3** | Flink Job 1: GPS normalizer + watermarks + Grafana vehicle map | Upcoming |
| **4** | Flink Job 2 (demand agg) + Job 3 (trip matcher) + adjacent zone fallback | Upcoming |
| **5** | Spark ETL: Porto + NYC batch processing, KPI computation | Upcoming |
| **6** | ML: feature engineering, GBT training, FastAPI forecast endpoint | Upcoming |
| **7** | Security: JWT auth, GPS anonymization, integration test, SLA measurement | Upcoming |
| **8** | Demo: Grafana polish, event injector, pitch deck, technical report | Upcoming |

### Week 1 Completed Tasks
- [x] Task 1: Docker Compose stack (11 containers, all healthy)
- [x] Task 2: Datasets uploaded to MinIO (Porto 1.8 GiB + NYC 150 MiB)
- [x] Task 3: Porto EDA notebook (schema, durations, call types, demand curves)
- [x] Task 4: Zone remapping v3 (irregular geographic tessellation, 99.6% coverage)
- [x] Task 5: Kafka producers (GPS + trip request simulators)

### Performance Targets (SLAs)
| Metric | Target |
|--------|--------|
| Trip match latency (request → Cassandra write) | < 5s P95 |
| GPS position freshness (ping → Cassandra) | < 15s |
| Demand zone update frequency | every 30s |
| ML forecast API response | < 500ms at 20 req/s |
| Spark ETL on full Porto (1.7M rows) | < 5 min |

---

## 6. Flink Processing Jobs Reference

| Job | Input | Logic | Output |
|-----|-------|-------|--------|
| **Job 1 — GPS Normalizer** | `raw.gps` | Validate coords → deduplicate → event-time watermark (3-min lateness) → map to zone → snap to centroid | Cassandra: `vehicle_positions` · Kafka: `processed.gps` |
| **Job 2 — Demand Aggregator** | `processed.gps` + `raw.trips` | 30s tumbling window per zone → count vehicles + requests → supply/demand ratio | Cassandra: `demand_zones` · Kafka: `processed.demand` |
| **Job 3 — Trip Matcher** | `raw.trips` + `processed.gps` | Find nearest available vehicle in zone → 5s fallback to adjacent zones → assign match → compute ETA | Cassandra: `trips` · Kafka: `processed.matches` |

### Flink Configuration
- State backend: RocksDB
- Checkpoint interval: 60 seconds to `s3://curated/flink-checkpoints`
- S3 endpoint: `http://minio:9000` (path-style access)
- Extra JARs mounted: `jars/flink/` → `/opt/flink/lib/extra`

---

## 7. Spark ML Pipeline Reference

| Element | Detail |
|---------|--------|
| Target | Trip requests per zone per 30-min slot |
| Horizon | 30 minutes ahead |
| Algorithm | GBTRegressor (Spark MLlib), maxDepth=5, maxIter=50 |
| Training data | 10 months Porto remapped, test: 2 months (temporal split) |
| Baseline | Naive 7-day-lag (must beat this) |
| Features | hour, day_of_week, is_weekend, zone_id, population_density, is_raining, demand_lag_1d/7d, rolling_7d_mean |
| API | `POST /api/demand/forecast` → `{zone_id, datetime}` → `{predicted_demand}` |
| Artifact path | `s3a://ml/models/demand_v1/` |

---

## 8. Coding Standards

### General
- Language: Python 3.13+ for producers, API, notebooks. Java/Scala for Flink jobs if needed.
- Keep changes small, focused, and atomic — one concern per commit
- Prefer fixing runtime/infrastructure issues before changing application logic
- Test locally before pushing: run the code, verify output, check logs

### Kafka Producers
- Always use `KAFKA_BOOTSTRAP` env var with `localhost:9092` default
- Topic names: `raw.gps`, `raw.trips` (prefixed `raw.` for unprocessed events)
- Messages serialized as JSON with `event_time` field (Unix timestamp)

### Cassandra
- Partition keys designed by query pattern, not normalization
- Use `date_bucket` for unbounded partition prevention
- Idempotent writes: upserts or `IF NOT EXISTS` for trip matching
- TTLs: 24h on vehicle_positions, 7 days on demand_zones

### Flink
- Event-time processing with watermarks — never processing-time for demand aggregation
- Stateful deduplication for GPS pings (vehicles ping every 4s)
- At-least-once guarantees with checkpointing to MinIO

### FastAPI
- JWT tokens on all endpoints. Two roles: `rider` (read + reserve), `admin` (full access)
- Self-signed TLS certificate acceptable for demo
- Model loaded at startup, not per-request

---

## 9. Documentation & Change Tracking

### Documentation Rules
- Update task documents under `documents/` after meaningful progress
- Master index: `documents/00_master_status.md`
- Update log: `documents/06_next_steps.md` (append one-line entries)
- Link to existing docs — do not duplicate content from `Taasim_project_description_all.md` or `Sprint1.md`

### Commit Convention
- Every edit — even small — must be committed and pushed
- Format: `[scope] short description`
  - `[task-N]` for sprint tasks, e.g. `[task-4] fix zone centroid lat for Mechouar`
  - `[infra]` for infrastructure, e.g. `[infra] add healthcheck retry to cassandra`
  - `[docs]` for documentation, e.g. `[docs] update master status with week 2 progress`
  - `[fix]` for bug fixes, e.g. `[fix] handle null POLYLINE in GPS producer`
- Before committing: update the relevant document under `documents/`
- After pushing: add a one-line entry to `documents/06_next_steps.md` under Update Log
- Never push without verifying the change works locally first

### Git Rules
- Large files (data, JARs) belong in MinIO, not Git — see `.gitignore`
- Porto CSV (1.8 GiB), NYC Parquet, JAR files are excluded from tracking
- If repo size grows unexpectedly, check for accidental large file inclusion

---

## 10. Troubleshooting

| Problem | Fix |
|---------|-----|
| `NoBrokersAvailable` | Check Docker daemon is running → `docker compose up -d` → verify Kafka container |
| MinIO `Access Denied` | Run `mc alias set local http://localhost:9000 minioadmin minioadmin` inside container first |
| Cassandra `NoHostAvailable` | Cassandra takes ~60s to start. Check healthcheck status. |
| Flink S3 errors | Verify JARs in `jars/flink/` exist. Re-run `download-jars.ps1` if missing. |
| Spark can't read MinIO | Check `spark-defaults.conf` has correct S3A endpoint. Verify JARs in `jars/spark/`. |
| Push fails / times out | Check `.gitignore` — large data files must NOT be tracked in git |
| Kafka listener confusion | Host connects via `localhost:9092` (mapped from container port 29092). Containers use `kafka:9092` (PLAINTEXT). |
| Grafana empty panels | Verify Cassandra datasource plugin is installed: `hadesarchitect-cassandra-datasource` |
| Producer GPS noise | Intentional: ±20m drift + 5% blackout. This is by design for Flink watermark testing. |

---

## 11. Evaluation Criteria (Week 8 Demo)

| Pillar | Weight | What Matters |
|--------|--------|-------------|
| Pipeline Completeness | 30% | All 3 Flink jobs running, Spark ETL done, Cassandra populated, API serving |
| Engineering Quality | 25% | Checkpointing, anonymization, JWT auth, partition key justification |
| Technical Report | 25% | Architecture docs, ML baseline comparison, honest post-mortem |
| Startup Pitch + Live Demo | 20% | Live pipeline demo, demand spike injection, convincing pitch |

### Demo Day Checklist
1. GPS events: Kafka → Flink Job 1 → Cassandra → Grafana (live updating)
2. Trip reservation: `POST /api/trips` → Flink Job 3 → match in < 5s
3. Demand heatmap: Grafana updating every 30s per zone
4. ML forecast: `/api/demand/forecast` responds < 500ms, beats naive baseline
5. Checkpoint recovery: kill Flink TM → job resumes from checkpoint
