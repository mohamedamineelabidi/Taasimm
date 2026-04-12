Finalize MinIO Bucket Structure & Kafka Connect S3 Sink

MinIO buckets: raw/, curated/, ml/, kafka-archive/ all exist. Kafka Connect S3 Sink connector running: raw.gps and raw.trips mirroring to kafka-archive/ confirmed. S3A connector verified accessible from Spark (test read of MinIO path in PySpark).



Finalize the MinIO data lake zones (raw/, curated/, ml/, kafka-archive/). Configure Kafka Connect S3 Sink to automatically mirror raw.gps and raw.trips topics to MinIO for replay and historical ML feature computation.
## 🎯 Objective

Establish the persistent data lake structure. Kafka Connect S3 Sink is the archival bridge that saves raw streaming events for ML replay.

## 📋 Sub-tasks

- [ ]  Create MinIO buckets: `raw/`, `curated/`, `ml/`, `kafka-archive/`
- [ ]  Deploy Kafka Connect in Docker Compose (use `confluentinc/cp-kafka-connect`)
- [ ]  Configure S3 Sink connector for `raw.gps` topic → `s3a://kafka-archive/raw.gps/`
- [ ]  Configure S3 Sink connector for `raw.trips` topic → `s3a://kafka-archive/raw.trips/`
- [ ]  Set Kafka topic retention to 7 days (raw.gps, raw.trips)
- [ ]  Verify Spark can read from `s3a://raw/porto-trips/` using S3A
- [ ]  Document bucket structure in ADR

## 🗄️ Bucket Reference

| Bucket/Prefix | Written By | Read By |
| --- | --- | --- |
| raw/porto-trips/ | Manual | Spark ETL |
| raw/nyc-tlc/ | Manual | Spark ETL |
| raw/kafka-archive/ | Kafka Connect | Spark ETL |
| curated/trips/ | Spark ETL | Spark ML |
| ml/features/ | Spark ML Prep | Spark MLlib |
| ml/models/demand_v1/ | Spark ML Train | FastAPI |





Design & Deploy Cassandra Schema
Keyspace 'taasim' created. Tables: vehicle_positions (partition: city+zone_id, clustering: event_time DESC), trips (partition: city+date_bucket, clustering: created_at DESC), demand_zones (partition: city+zone_id, clustering: window_start DESC). CQL DDL committed to repo. ADR documents why each partition key was chosen.

Design the Cassandra keyspace and create all three required tables: vehicle_positions, trips, and demand_zones. Schema must be designed around API query patterns, not relational normalization. Partition key choices must be justified in the Architecture Decision Record.

## 🎯 Objective

Deploy the serving layer Cassandra schema. **This is a NoSQL design exercise** — every partition key decision must be justified by the query pattern, not by normalization instincts.

## 📋 Sub-tasks

- [ ]  Create keyspace: `taasim` with replication factor 1
- [ ]  Create `vehicle_positions` table
- [ ]  Create `trips` table with `date_bucket` to prevent unbounded partition growth
- [ ]  Create `demand_zones` table
- [ ]  Test each table with sample CQL INSERT + SELECT
- [ ]  Benchmark query: `SELECT * FROM vehicle_positions WHERE city='casa' AND zone_id=5 ORDER BY event_time DESC LIMIT 10`
- [ ]  Document partition key rationale in ADR

## 🗂️ Schema Reference

```sql
-- vehicle_positions
CREATE TABLE taasim.vehicle_positions (
  city TEXT,
  zone_id INT,
  event_time TIMESTAMP,
  taxi_id TEXT,
  lat DOUBLE,
  lon DOUBLE,
  speed DOUBLE,
  status TEXT,
  PRIMARY KEY ((city, zone_id), event_time)
) WITH CLUSTERING ORDER BY (event_time DESC);

-- trips
CREATE TABLE taasim.trips (
  city TEXT,
  date_bucket DATE,
  created_at TIMESTAMP,
  trip_id UUID,
  rider_id TEXT,
  taxi_id TEXT,
  origin_zone INT,
  dest_zone INT,
  status TEXT,
  fare DECIMAL,
  PRIMARY KEY ((city, date_bucket), created_at)
) WITH CLUSTERING ORDER BY (created_at DESC);

-- demand_zones
CREATE TABLE taasim.demand_zones (
  city TEXT,
  zone_id INT,
  window_start TIMESTAMP,
  active_vehicles INT,
  pending_requests INT,
  ratio DOUBLE,
  forecast_demand DOUBLE,
  PRIMARY KEY ((city, zone_id), window_start)
) WITH CLUSTERING ORDER BY (window_start DESC);
```

## ⚠️ Exam Trap

Students who copy this schema without understanding it **will not survive Q&A in Week 8**.

- Why `(city, zone_id)` not `taxi_id` for vehicle_positions? → API queries 'all vehicles in zone X'
- Why `date_bucket`? → Prevents unbounded partition growth / hotspots



Write Architecture Decision Record (ADR v1)
ADR document (PDF or Markdown) submitted. Covers: (1) Kappa architecture justification, (2) Cassandra partition key choices with query-pattern reasoning, (3) MinIO zone rationale, (4) Kafka retention policy. Maximum 1 page.Produce a 1-page Architecture Decision Record documenting the key storage and architecture choices made in TaaSim: Kappa vs Lambda architecture choice, Cassandra partition key decisions, MinIO bucket structure rationale, and Kafka topic retention strategy.