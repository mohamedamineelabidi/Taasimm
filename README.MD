<div align="center">

# TaaSim — Transport as a Service

**Real-time urban mobility simulation platform for Casablanca, Morocco**

[![Kafka](https://img.shields.io/badge/Kafka-3.7.0-231F20?logo=apachekafka&logoColor=white)](https://kafka.apache.org/)
[![Flink](https://img.shields.io/badge/Flink-1.18-E6526F?logo=apacheflink&logoColor=white)](https://flink.apache.org/)
[![Spark](https://img.shields.io/badge/Spark-3.5.4-E25A1C?logo=apachespark&logoColor=white)](https://spark.apache.org/)
[![Cassandra](https://img.shields.io/badge/Cassandra-4.1-1287B1?logo=apachecassandra&logoColor=white)](https://cassandra.apache.org/)
[![Grafana](https://img.shields.io/badge/Grafana-10.4-F46800?logo=grafana&logoColor=white)](https://grafana.com/)
[![Python](https://img.shields.io/badge/Python-3.13+-3776AB?logo=python&logoColor=white)](https://python.org/)
[![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker&logoColor=white)](https://docs.docker.com/compose/)
[![License](https://img.shields.io/badge/License-MIT-green)](#license)

---

[Problem](#-the-problem) · [Solution](#-the-solution) · [Architecture](#-architecture) · [Quick Start](#-quick-start) · [Tech Stack](#-technology-stack) · [Data Pipeline](#-data-pipeline) · [ML Model](#-machine-learning) · [Project Structure](#-project-structure) · [Roadmap](#-roadmap)

</div>

---

## 🚨 The Problem

**Casablanca is a city of 4 million people with zero shared mobility data.**

Despite significant investment in formal transit — the BRT corridor along Boulevard Mohammed VI, ONCF suburban rail, and thousands of licensed taxis — urban mobility remains deeply fragmented:

| Pain Point | Reality |
|:---|:---|
| **No shared data layer** | Grand taxis, petits taxis, and informal minibuses operate with no GPS tracking, no digital booking, no shared schedule |
| **Demand blindness** | Drivers cruise looking for passengers; passengers wait with no visibility of vehicle availability |
| **No interoperability** | Formal transit (ONCF, BRT) and informal taxis share no data, no ticketing, no trip planner |
| **Cash-only economy** | Zero payment data means zero trip history, zero analytics, zero personalization |
| **Underserved periphery** | New districts (Bouskoura, Ain Sebaâ, Sidi Moumen) grow faster than routes are planned |

> **The core problem is not a shortage of vehicles — it is the total absence of a data layer connecting supply to demand.**

---

## 💡 The Solution

TaaSim treats urban mobility as a **data engineering problem**. By ingesting GPS vehicle streams, processing trip reservations in real time, and applying machine learning to historical patterns, TaaSim can:

- **Match riders to vehicles dynamically** — find the nearest available taxi in < 5 seconds
- **Forecast demand surges** — predict trip volume per zone 30 minutes ahead
- **Give city planners a unified view** — live heatmaps, KPI dashboards, coverage gap analysis
- **Enable data-driven route planning** — identify underserved zones where demand exceeds supply

### How It Works

```
  Rider requests a trip                    Driver receives match
         │                                         ▲
         ▼                                         │
  ┌─────────────┐    ┌──────────────┐    ┌─────────────────┐
  │  Kafka Bus   │───▶│  Flink Jobs  │───▶│   Cassandra DB  │
  │  (raw events)│    │  (real-time) │    │  (serving layer) │
  └──────┬───────┘    └──────┬───────┘    └────────┬────────┘
         │                   │                     │
         ▼                   ▼                     ▼
  ┌─────────────┐    ┌──────────────┐    ┌─────────────────┐
  │  MinIO Lake  │───▶│  Spark ML   │───▶│  FastAPI + Grafana│
  │  (archive)   │    │  (forecast) │    │  (dashboard + API)│
  └─────────────┘    └──────────────┘    └─────────────────┘
```

---

## 🏗️ Architecture

TaaSim follows a **Kappa Architecture** — Kafka is the system of record, Flink handles all real-time processing, and Spark operates exclusively offline for batch ETL and ML training.

### System Components

| Layer | Technology | Role |
|:------|:-----------|:-----|
| **Event Bus** | Apache Kafka 3.7.0 (KRaft) | Central nervous system: GPS pings + trip reservations stream through Kafka with 7-day retention |
| **Stream Processing** | Apache Flink 1.18 | 3 real-time jobs: GPS normalization, demand aggregation (30s windows), trip matching |
| **Object Store** | MinIO (S3-compatible) | Data lake with 4 zones: `raw/`, `curated/`, `ml/`, `kafka-archive/` |
| **Batch + ML** | Apache Spark 3.5.4 | ETL on 1.7M Porto trips + 30M NYC rides; GBT demand forecasting |
| **Serving DB** | Apache Cassandra 4.1 | Low-latency queries: vehicle positions, trip records, demand per zone |
| **Dashboard** | Grafana 10.4.0 | Live vehicle map, demand heatmap, KPI panels, ML forecast overlay |
| **API** | FastAPI (Python) | REST endpoints: trip reservation, vehicle lookup, demand forecast. JWT auth |
| **Notebooks** | Jupyter Lab | EDA, zone remapping analysis, model evaluation |

### Real-Time Processing Pipeline

| Flink Job | Input | Processing | Output |
|:----------|:------|:-----------|:-------|
| **GPS Normalizer** | `raw.gps` | Validate → deduplicate → watermark (3-min lateness) → zone mapping → centroid snap | `vehicle_positions` table + `processed.gps` |
| **Demand Aggregator** | `processed.gps` + `raw.trips` | 30s tumbling window per zone → vehicle count + request count → supply/demand ratio | `demand_zones` table + `processed.demand` |
| **Trip Matcher** | `raw.trips` + `processed.gps` | Find nearest vehicle in zone → 5s adjacent zone fallback → assign match → compute ETA | `trips` table + `processed.matches` |

---

## 🚀 Quick Start

### Prerequisites

- **Docker Desktop** (Engine 28.x+) — [Install](https://www.docker.com/products/docker-desktop/)
- **Python 3.13+** — [Install](https://www.python.org/downloads/)
- **Git** — [Install](https://git-scm.com/)
- **8 GB RAM minimum** for running the full stack

### 1. Clone & Setup

```bash
git clone https://github.com/mohamedamineelabidi/Taasimm.git
cd Taasimm
```

### 2. Download Required JARs

```powershell
# Windows
.\download-jars.ps1

# Linux / Mac
chmod +x download-jars.sh && ./download-jars.sh
```

### 3. Start the Platform

```bash
docker compose up -d
```

Wait for all healthchecks to pass (~60 seconds):

```
✔ taasim-kafka         Started
✔ taasim-minio         Healthy     (7s)
✔ taasim-cassandra     Healthy    (33s)
✔ taasim-flink-jm      Healthy    (17s)
✔ taasim-spark-master   Healthy    (18s)
✔ taasim-grafana       Started
✔ taasim-jupyter       Started
```

### 4. Upload Datasets to MinIO

Download the datasets manually, then upload:
```powershell
.\upload-datasets.ps1
```

| Dataset | Source | Size |
|:--------|:-------|:-----|
| Porto Taxi Trajectories | [Kaggle](https://www.kaggle.com/c/pkdd-15-predict-taxi-service-trajectory-i) | 1.8 GiB |
| NYC TLC Trip Records (3 months) | [NYC Open Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) | ~150 MiB |

### 5. Run Producers (Smoke Test)

```bash
# Create Python virtual environment
python -m venv .venv
.venv/Scripts/activate    # Windows
# source .venv/bin/activate  # Linux/Mac

pip install kafka-python numpy

# GPS event simulator (5 sample trips)
python producers/vehicle_gps_producer.py --max-trips 5

# Trip request simulator (5 sample trips)
python producers/trip_request_producer.py --max-trips 5
```

### 6. Access Services

| Service | URL | Credentials |
|:--------|:----|:------------|
| **Grafana Dashboard** | http://localhost:3000 | admin / admin |
| **Flink Web UI** | http://localhost:8081 | — |
| **Spark Master UI** | http://localhost:8080 | — |
| **MinIO Console** | http://localhost:9001 | minioadmin / minioadmin |
| **Jupyter Lab** | http://localhost:8888 | token from `docker logs taasim-jupyter` |

---

## 🔧 Technology Stack

### Infrastructure (11 Docker Containers)

```
┌─────────────────────────────────────────────────────────────┐
│                    Docker Compose Stack                       │
│                                                               │
│  ┌──────────┐  ┌──────────┐  ┌───────────┐  ┌────────────┐  │
│  │  Kafka   │  │  MinIO   │  │ Cassandra │  │  Grafana   │  │
│  │  KRaft   │  │  S3 API  │  │   4.1     │  │   10.4     │  │
│  │  3.7.0   │  │  Console │  │           │  │            │  │
│  └──────────┘  └──────────┘  └───────────┘  └────────────┘  │
│                                                               │
│  ┌──────────────────┐  ┌──────────────────┐  ┌────────────┐  │
│  │   Flink 1.18     │  │   Spark 3.5.4    │  │  Jupyter   │  │
│  │  JM + TM (4 slots)│  │  Master + Worker │  │   Lab      │  │
│  └──────────────────┘  └──────────────────┘  └────────────┘  │
│                                                               │
│  ┌──────────┐  ┌──────────┐  ┌───────────┐                   │
│  │ minio-   │  │cassandra-│  │  spark-   │                   │
│  │ init     │  │ init     │  │  worker   │                   │
│  └──────────┘  └──────────┘  └───────────┘                   │
└─────────────────────────────────────────────────────────────┘
```

### Key JAR Dependencies

| Component | JAR | Purpose |
|:----------|:----|:--------|
| Spark | `hadoop-aws-3.3.4.jar` | S3A filesystem for MinIO access |
| Spark | `aws-java-sdk-bundle-1.12.367.jar` | AWS SDK for S3 operations |
| Flink | `flink-s3-fs-hadoop-1.18.1.jar` | S3 checkpointing to MinIO |
| Flink | `flink-sql-connector-kafka-3.1.0-1.18.jar` | Kafka source/sink |
| Flink | `flink-connector-cassandra_2.12-3.1.0-1.17.jar` | Cassandra sink |

---

## 📊 Data Pipeline

### Datasets

**Porto Taxi Trajectories** (Primary — Streaming)
- 1.7 million completed taxi trips from 442 taxis in Porto, Portugal
- 12 months of GPS traces (July 2013 – June 2014)
- GPS coordinates every 15 seconds per trip
- 3 call types: A (dispatched), B (taxi stand), C (street hail) — mirrors Casablanca taxi behavior

**NYC TLC Trip Records** (Secondary — Batch)
- ~10 million rows per month, 3 months used (~30M total)
- Used for Spark ETL at scale and KPI computation

### Casablanca Zone Mapping

Porto GPS coordinates are linearly transformed to Casablanca's bounding box, mapped to **16 irregular zones** matching real arrondissements:

```
         North ─────────────────────────────────────
         │  13-Anfa  │ 14-Sidi   │ 15-Ain  │ 16-Sidi    │
         │           │  Belyout  │  Sebaa  │  Bernoussi │
         ├───────────┼──────┼────┼─────────┼────────────┤
Center   │ 8-Maarif │9-Mech│10-Al│11-Mers  │12-Hay      │
         │          │ouar  │Fida │ Sultan  │ Mohammadi  │
         ├──────────┼──────┴──┼──┴────┼────┴────────────┤
Mid-S    │4-Hay     │ 5-Ain   │6-Ben  │ 7-Moulay       │
         │ Hassani  │  Chock  │ Msik  │  Rachid        │
         ├──────────┼─────────┼───────┼────────────────┤
South    │ 1-Bouskoura       │2-Sbata│ 3-Sidi Moumen  │
         └───────────────────┴───────┴────────────────┘
                    West ←→ East
```

### Kafka Topic Flow

```
Producers                    Flink                      Cassandra
─────────                    ─────                      ─────────
vehicle_gps_producer ──▶ raw.gps ──▶ Job 1 ──▶ processed.gps ──▶ vehicle_positions
                                          │
trip_request_producer ──▶ raw.trips ──▶ Job 2 ──▶ processed.demand ──▶ demand_zones
                              │
                              └──▶ Job 3 ──▶ processed.matches ──▶ trips
```

### Cassandra Schema

| Table | Partition Key | Clustering | TTL | Query Pattern |
|:------|:-------------|:-----------|:----|:--------------|
| `vehicle_positions` | `(city, zone_id)` | `event_time DESC` | 24h | All vehicles in a zone |
| `trips` | `(city, date_bucket)` | `created_at DESC` | — | Trip history by day |
| `demand_zones` | `(city, zone_id)` | `window_start DESC` | 7 days | Demand heatmap per zone |

---

## 🤖 Machine Learning

### Demand Forecasting Model

| Element | Detail |
|:--------|:-------|
| **Goal** | Predict trip requests per zone for the next 30-minute slot |
| **Algorithm** | Gradient Boosted Trees (Spark MLlib `GBTRegressor`) |
| **Training** | 10 months Porto remapped data (1.7M trips) |
| **Evaluation** | 2 months held-out test set (temporal split — no data leakage) |
| **Baseline** | Naive 7-day-lag predictor (model must beat this) |

### Feature Engineering

| Group | Features |
|:------|:---------|
| **Temporal** | hour_of_day, day_of_week, is_weekend, is_friday |
| **Spatial** | zone_id, population_density, zone_type |
| **Weather** | is_raining, temperature_bucket |
| **Lag** | demand_lag_1d, demand_lag_7d, rolling_7d_mean |

### Serving

```
POST /api/demand/forecast
{
  "zone_id": 8,
  "datetime": "2026-04-04T08:30:00"
}
──▶
{
  "zone_id": 8,
  "zone_name": "Maarif",
  "predicted_demand": 142,
  "confidence": 0.87
}
```

---

## 📁 Project Structure

```
Taasimm/
├── docker-compose.yml              # Full stack (11 containers)
├── download-jars.ps1               # JAR downloader (Windows)
├── download-jars.sh                # JAR downloader (Linux/Mac)
├── upload-datasets.ps1             # Dataset upload to MinIO
│
├── config/
│   ├── cassandra-init.cql          # Keyspace + 3 tables
│   └── spark-defaults.conf         # S3A configuration
│
├── producers/
│   ├── config.py                   # Shared constants & transforms
│   ├── vehicle_gps_producer.py     # GPS event simulator
│   └── trip_request_producer.py    # Trip request simulator
│
├── notebooks/
│   ├── 01_porto_eda.ipynb          # Porto data exploration
│   ├── 02_zone_remapping.ipynb     # v3 zone transformation
│   └── casablanca_zone_map.html    # Interactive zone visualization
│
├── data/
│   ├── zone_mapping.csv            # 16 zones + adjacency graph
│   └── zone_centroids.csv          # Zone centroid coordinates
│
├── documents/                      # Task status documentation
│   ├── 00_master_status.md
│   ├── 01–05_task_*.md
│   └── 06_next_steps.md
│
├── jars/                           # Downloaded JARs (gitignored)
│   ├── flink/                      # Kafka, S3, Cassandra connectors
│   └── spark/                      # hadoop-aws, aws-sdk-bundle
│
└── .github/
    └── copilot-instructions.md     # AI coding guidelines
```

---

## 🎯 Performance Targets

| Metric | Target | How Measured |
|:-------|:-------|:-------------|
| Trip match latency | < 5s P95 | Kafka timestamp → Cassandra write |
| GPS position freshness | < 15s | Producer timestamp → Cassandra write |
| Demand zone updates | Every 30s | `WRITETIME()` in Cassandra |
| ML forecast API | < 500ms at 20 req/s | Locust load test |
| Spark ETL (1.7M rows) | < 5 min | Spark UI job duration |

---

## 🗓️ Roadmap

| Week | Focus | Status |
|:-----|:------|:-------|
| 1 | Docker stack, datasets, EDA, zone remapping, Kafka producers | ✅ Done |
| 2 | Kafka Connect S3 Sink, Cassandra ADR, storage design | 🔄 Next |
| 3 | Flink Job 1 (GPS normalizer), watermarks, Grafana vehicle map | ⬜ |
| 4 | Flink Job 2 (demand) + Job 3 (trip matcher), adjacent zone fallback | ⬜ |
| 5 | Spark ETL on Porto + NYC, KPI computation | ⬜ |
| 6 | ML feature engineering, GBT training, FastAPI forecast endpoint | ⬜ |
| 7 | JWT auth, GPS anonymization, integration testing, SLA measurement | ⬜ |
| 8 | Grafana polish, live demo, pitch deck, technical report | ⬜ |

---

## 🏆 Key Differentiators

| Capability | Why It Matters |
|:-----------|:---------------|
| **Real-time matching** | Riders matched to nearest vehicle in < 5 seconds — eliminates blind cruising |
| **Demand forecasting** | ML predicts demand 30 minutes ahead — enables proactive fleet positioning |
| **Zone-level analytics** | 16 Casablanca zones with live supply/demand ratio — exposes underserved areas |
| **Fault-tolerant streaming** | Flink checkpointing + at-least-once guarantees — zero data loss on failure |
| **Scalable architecture** | Kappa architecture handles 10K+ events/sec — ready for city-wide deployment |

### Production Readiness Checklist

1. GPS events: Kafka → Flink → Cassandra → Grafana vehicle map (updating live)
2. Trip reservation: `POST /api/trips` → match in < 5 seconds
3. Demand heatmap: Grafana updating every 30s per zone
4. ML forecast: API responds < 500ms, beats naive baseline
5. Checkpoint recovery: kill Flink TM → job resumes from checkpoint

---

## 📚 References

- **Porto Taxi Trajectories** — [ECML/PKDD 2015 Challenge](https://www.kaggle.com/c/pkdd-15-predict-taxi-service-trajectory-i) (CC BY 4.0)
- **NYC TLC Trip Records** — [NYC Open Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) (Public Domain)
- **Apache Kafka** — https://kafka.apache.org/documentation/
- **Apache Flink** — https://flink.apache.org/docs/stable/
- **Apache Spark MLlib** — https://spark.apache.org/docs/latest/ml-guide.html

---

## 📝 License

MIT License. Datasets used under their respective open licenses (CC BY 4.0, Public Domain).

---

<div align="center">

*Built with data engineering. Powered by open source.*

**TaaSim** — *The data platform that moves Casablanca.*

</div>