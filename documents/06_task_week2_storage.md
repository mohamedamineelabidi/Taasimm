# Task 6 — Week 2: Storage Design

**Status:** Complete  
**Date:** 2026-04-07  
**Sprint:** Week 2

---

## Objective

Configure persistent storage for the streaming pipeline:
- Kafka Connect S3 Sink archiving `raw.gps` and `raw.trips` to MinIO `kafka-archive/`
- Cassandra keyspace `taasim` with 3 tables deployed and justified
- Written Architecture Decision Record (ADR-001)

> Historical note: this Week 2 file is evidence-oriented. Current canonical
> connector wiring uses split configs (`connect-s3-sink-gps.json`,
> `connect-s3-sink-trips.json`) registered via `scripts/register-connectors.ps1`.

---

## Deliverables

### 1. MinIO Bucket Structure

Created by `minio-init` container at stack startup. Four buckets:

| Bucket | Purpose |
|--------|---------|
| `raw/` | Porto CSV, NYC Parquet, Kafka archive |
| `curated/` | Spark ETL output (Parquet), Flink checkpoints |
| `ml/` | Feature matrix, trained GBT model artifact |
| `kafka-archive/` | Continuous Kafka Connect S3 Sink mirror |

**Verification:**
```bash
docker exec taasim-minio mc ls local/
```

### 2. Kafka Connect S3 Sink

**Container:** `taasim-kafka-connect` (`confluentinc/cp-kafka-connect-base:7.7.0`)  
**Port:** `localhost:8083`  
**Plugin:** `confluentinc/kafka-connect-s3:10.5.16` (installed on first start)

**Connector configs:**
- `config/connect-s3-sink-gps.json`
- `config/connect-s3-sink-trips.json`

Topics mirrored:
- `raw.gps` → `s3://kafka-archive/raw/raw.gps/year=YYYY/month=MM/day=dd/hour=HH/`
- `raw.trips` → `s3://kafka-archive/raw/raw.trips/year=YYYY/month=MM/day=dd/hour=HH/`

Connectors are registered manually after Connect is healthy.

**Manual re-registration (Windows):**
```powershell
.\scripts\register-connectors.ps1
```
**Manual re-registration (Linux/Mac):**
```bash
bash scripts/register-s3-sink.sh
```

**Verification:**
```bash
# Check connector list and statuses
curl -s http://localhost:8083/connectors | python -m json.tool
curl -s http://localhost:8083/connectors/s3-sink-raw-gps/status | python -m json.tool
curl -s http://localhost:8083/connectors/s3-sink-raw-trips/status | python -m json.tool

# List archived files in MinIO
docker exec taasim-minio mc ls -r local/kafka-archive/
```

### 3. Cassandra Schema

**Keyspace:** `taasim` (SimpleStrategy, RF=1)  
**Schema file:** `config/cassandra-init.cql`  
**Deployed by:** `taasim-cassandra-init` container on first stack start

Tables deployed:

| Table | Partition Key | TTL |
|-------|-------------|-----|
| `vehicle_positions` | `(city, zone_id)` | 24h |
| `trips` | `(city, date_bucket)` | none |
| `demand_zones` | `(city, zone_id)` | 7 days |

**Verification:**
```bash
docker exec taasim-cassandra cqlsh -e "USE taasim; DESCRIBE TABLES;"
docker exec taasim-cassandra cqlsh -e "USE taasim; DESCRIBE vehicle_positions;"
```

### 4. Architecture Decision Record

**File:** `documents/07_adr_storage.md`

ADR-001 covers:
- Why 3-layer storage model (Kafka + MinIO + Cassandra)
- Kafka Connect S3 Sink configuration rationale
- Cassandra partition key justification for all 3 tables
- Alternatives considered (PostgreSQL, HDFS, Flink FileSystem Sink)
- SLA compliance analysis

---

## New Files

| File | Description |
|------|-----------|
| `config/connect-s3-sink-gps.json` | S3 Sink connector payload for `raw.gps` |
| `config/connect-s3-sink-trips.json` | S3 Sink connector payload for `raw.trips` |
| `scripts/register-connectors.ps1` | PowerShell split-connector registration helper |
| `scripts/register-s3-sink.sh` | Bash single-script registration helper |
| `documents/07_adr_storage.md` | ADR-001 — Storage Architecture |

## Modified Files

| File | Change |
|------|--------|
| `docker-compose.yml` | Added `kafka-connect` (port 8083); current stack registers connectors via script (no `connect-init` service) |
