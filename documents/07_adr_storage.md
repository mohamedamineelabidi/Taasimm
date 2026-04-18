# ADR-001 — TaaSim Storage Architecture

**Status:** Accepted  
**Date:** 2026-04-07  
**Authors:** TaaSim Engineering Team  
**Week:** 2 — Storage Design

---

## Context

TaaSim ingests two event streams in real time:

- **`raw.gps`** — vehicle GPS pings produced every 4 seconds per taxi (up to 442 vehicles simulated).
- **`raw.trips`** — rider trip reservation requests following Porto demand curves mapped to Casablanca time zones.

The platform must satisfy three storage concerns simultaneously:

1. **Real-time serving** — Grafana dashboards and the FastAPI need sub-second reads of vehicle positions and zone demand. Events must be queryable milliseconds after they arrive.
2. **Historical archival** — Kafka's 7-day retention is insufficient for ML training, audit, and backfill. Raw events must be durably archived outside Kafka.
3. **Batch analytics** — Spark ETL and MLlib operate on months of historical data. The storage layer must support large sequential scans without impacting the serving path.

No single storage technology satisfies all three concerns equally well. This ADR documents the decision to separate them into three layers.

---

## Decision

TaaSim adopts a **three-layer storage model**:

| Layer | Technology | Concern Addressed |
|-------|-----------|-------------------|
| Event bus | Apache Kafka (KRaft, 7-day retention) | Decoupled transport; short-term replay |
| Object lake | MinIO (S3-compatible) | Long-term archival; Spark input/output |
| Serving DB | Apache Cassandra | Low-latency API reads; dashboard queries |

### Layer 1 — Apache Kafka (Event Bus)

Kafka is the **system of record** for all real-time events. It acts as the source for both stream processing (Flink) and archival (Kafka Connect S3 Sink). Topics `raw.gps` and `raw.trips` are configured with 7-day retention and 4 partitions to allow parallel consumption.

Kafka is **not** used as a durable archive — its purpose is transport and short-term buffering, not petabyte-scale storage.

### Layer 2 — MinIO Object Store (Data Lake)

MinIO provides an S3-compatible object store organized into four logical zones:

| Bucket prefix | Contents | Writer | Reader |
|--------------|----------|--------|--------|
| `raw/porto-trips/` | Porto CSV (1.8 GiB) | Manual upload | Spark ETL |
| `raw/nyc-tlc/` | NYC TLC Parquet (3 months) | Manual upload | Spark ETL |
| `raw/kafka-archive/` | Continuous Kafka mirror (raw.gps + raw.trips) | Kafka Connect S3 Sink | Spark ETL, audit |
| `curated/trips/` | Cleaned, geo-enriched Parquet trips | Spark ETL | Spark ML |
| `curated/flink-checkpoints/` | Flink state snapshots | Flink | Flink recovery |
| `mldata/features/` | GBT feature matrix | Spark ML Prep | Spark MLlib |
| `mldata/models/demand_v1/` | Trained GBT PipelineModel | Spark ML Train | FastAPI |

**Why MinIO over HDFS?** HDFS requires a NameNode + DataNode cluster (minimum 3 nodes). MinIO runs as a single container with an S3-compatible API, which both Spark (`s3a://`) and Flink natively support. For a single-node dev + demo environment, this is the correct trade-off.

#### Kafka Connect S3 Sink

The bridge from Kafka to MinIO is implemented via **Confluent's Kafka Connect S3 Sink connector** (`confluentinc/kafka-connect-s3:10.5.16`). Configuration choices:

| Setting | Value | Rationale |
|---------|-------|-----------|
| `flush.size` | 100 records | Small enough to be near-real-time without excessive S3 API calls |
| `rotate.schedule.interval.ms` | 60 000 ms | Force a file rotation every 60s even if flush.size not reached |
| `format.class` | `JsonFormat` | Human-readable; consistent with JSON producers |
| `path.format` | `year=YYYY/month=MM/day=dd/hour=HH` | Hive-compatible partitioning for future Spark reads via `spark.read.parquet("s3a://kafka-archive/raw/raw.gps/")` |
| `timestamp.extractor` | `RecordField` | Uses `event_time` from the message body, not Kafka broker timestamps, to avoid clock drift artifacts |
| `errors.tolerance` | `all` | Malformed messages are routed to a dead-letter topic (`_s3-sink-dlq`) rather than stopping the connector |

### Layer 3 — Apache Cassandra (Serving Database)

Cassandra is the **read-optimized serving layer** for Flink's output. It handles three query patterns:

#### Table: `vehicle_positions`

```cql
PRIMARY KEY ((city, zone_id), event_time, taxi_id)
```

**Partition key rationale:** The API and Grafana always query *"all available vehicles in zone X"*, not *"all positions from taxi Y"*. Partitioning by `(city, zone_id)` collocates every vehicle in a zone on the same partition — O(1) zone lookup. TTL of 24 hours prevents unbounded growth since old positions are irrelevant.

If we had partitioned by `taxi_id`, a zone query would require a full scatter-gather across all 442 taxi partitions — catastrophic for latency.

#### Table: `trips`

```cql
PRIMARY KEY ((city, date_bucket), created_at, trip_id)
```

**`date_bucket` rationale:** Without date bucketing, all trips for Casablanca would accumulate in a single partition, growing by ~50 000 rows/day. After 30 days this exceeds Cassandra's recommended partition size (~100 MB). Bucketing by date (`2026-04-07`) bounds partitions to one day of trips each.

#### Table: `demand_zones`

```cql
PRIMARY KEY ((city, zone_id), window_start)
```

**Partition key rationale:** Grafana queries *"latest demand metrics for zone X"* every 30 seconds. One partition per zone in a city gives constant-time access. TTL of 7 days keeps only the recent sliding window relevant to dashboards.

---

## Alternatives Considered

### Alt 1 — PostgreSQL instead of Cassandra

PostgreSQL offers richer query capabilities (JOINs, window functions) and is easier to operate. However:
- A GPS ping every 4 seconds × 442 vehicles = ~110 writes/sec sustained. PostgreSQL's write amplification (WAL + B-tree indexes) creates I/O pressure under this load.
- Cassandra's log-structured storage handles high-throughput append workloads with predictable low latency.
- Cassandra's partition key design *forces* students to think about query-driven schema design — a key learning objective.

**Decision:** Cassandra retained.

### Alt 2 — HDFS instead of MinIO

HDFS is the traditional Hadoop-era object store and integrates natively with Spark. However:
- Requires a multi-node deployment (NameNode + DataNodes) — too heavy for a single-machine Docker stack.
- MinIO exposes an S3-compatible API. Both Spark (`hadoop-aws` + S3AFileSystem) and Flink have native S3 support.
- The `s3a://` path format in `spark-defaults.conf` and Flink's S3 plugin make MinIO transparent to application code.

**Decision:** MinIO retained.

### Alt 3 — Flink FileSystem Sink instead of Kafka Connect

Flink can write directly to S3/MinIO using its FileSystem connector. This eliminates the Kafka Connect container but tightly couples archival to the Flink job lifecycle — if a Flink job fails or is reconfigured, archival gaps appear. Kafka Connect operates as a separate, independently recoverable service.

**Decision:** Kafka Connect S3 Sink retained for operational independence.

---

## Consequences

**Positive:**
- Clear separation of concerns: Kafka (transport) → Flink (processing) → Cassandra (serving) + MinIO (archival + batch).
- Kafka Connect S3 Sink is crash-safe: it commits offsets to Kafka only after successful S3 writes.
- MinIO's Hive-partitioned layout (`year=YYYY/month=MM/day=dd`) lets Spark ETL jobs use predicate pushdown to scan only the needed time range.
- Cassandra's TTLs (`vehicle_positions`: 24h, `demand_zones`: 7d) enforce automatic data lifecycle management.

**Negative / Trade-offs:**
- Kafka Connect adds one more container (12 total) and requires an internet connection on first start to download the S3 Sink plugin from Confluent Hub.
- Cassandra's data model is rigid: schema changes require `ALTER TABLE` and re-backfilling.
- MinIO is single-node; no replication or erasure coding. Acceptable for academic demo, not for production.

---

## Compliance with SLAs

| SLA | Storage Impact |
|-----|---------------|
| GPS freshness < 15s | Flink Job 1 writes to Cassandra in ~2s; no bottleneck here |
| Demand update every 30s | Cassandra `demand_zones` partition = O(1) write + read |
| ML forecast < 500ms | Model loaded in FastAPI from MinIO at startup; serving from RAM |
| Spark ETL < 5 min | Hive-partitioned Parquet on MinIO enables partition pruning |
