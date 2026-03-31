Provision Docker Compose Stack
All 6 services running: Kafka (KRaft, no Zookeeper), MinIO, Apache Cassandra, Apache Flink (1 JM + 1 TM), Apache Spark (master + 1 worker), Grafana. Health check: [kafka-topics.sh](http://kafka-topics.sh) --list returns OK, mc ls connects to MinIO, cqlsh connects to Cassandra, flink list returns no error, spark-shell launches, Grafana UI accessible on port 3000. Screenshot of all containers UP submitted as deliverable.Set up the full local development environment using Docker Compose. All 6 core services must run on a single workstation with at least 8 GB RAM. This is the foundation for every subsequent task — nothing else can proceed without this.## 🎯 Objective

Provision the full TaaSim data platform stack locally using Docker Compose. This is the **critical path blocker** — every other task depends on this.

## 📋 Sub-tasks

- []  Write `docker-compose.yml` with all 6 services
- []  Configure Kafka in KRaft mode (no Zookeeper dependency)
- []  Configure MinIO with default credentials and expose S3 API
- []  Configure Cassandra with CQL port exposed (9042)
- []  Configure Flink: 1 JobManager + 1 TaskManager, expose UI on port 8081
- []  Configure Spark: 1 master + 1 worker, expose UI on port 8080
- []  Install Grafana Cassandra datasource plugin at startup
- []  Configure S3A connector JARs for both Flink and Spark (`hadoop-aws`)
- []  Run health check on all services
- []  Take screenshot for deliverable

## ⚠️ Key Notes

- Flink and Spark **both** need `s3a://` path access to MinIO — configure `hadoop-aws` JAR and S3A endpoint in both containers
- Cassandra takes ~60s to fully start — add a healthcheck with retry in compose
- Grafana plugin install must be in `GF_INSTALL_PLUGINS` env var

## 📚 Reference

- Flink S3A setup: https://flink.apache.org/docs/stable/deployment/filesystems/s3/
- MinIO Docker: https://min.io/docs/minio/container/index.html



Download & Upload Datasets to MinIO
Porto CSV at s3a://raw/porto-trips/ accessible. NYC TLC Parquet (3 months) at s3a://raw/nyc-tlc/ accessible. Both confirmed with 'mc ls' command. Bucket raw/ exists and is readable by Spark.Download Porto Taxi Trajectories dataset from Kaggle (~1.5 GB) and NYC TLC Trip Records (3 months Parquet, ~1.5 GB). Upload both to MinIO raw zone under the correct bucket structure.
## 🎯 Objective

Download and stage both source datasets in MinIO for all subsequent Spark and Flink jobs.

## 📋 Sub-tasks

- [ ]  Create Kaggle account and download Porto Taxi Trajectories CSV (~1.5 GB)
- [ ]  Download NYC TLC Parquet files for 3 months from https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
- [ ]  Create MinIO buckets: `raw/`, `curated/`, `ml/`, `kafka-archive/`
- [ ]  Upload Porto CSV → `s3a://raw/porto-trips/`
- [ ]  Upload NYC Parquet → `s3a://raw/nyc-tlc/`
- [ ]  Verify access with `mc ls minio/raw/`

## 📚 Data Sources

- Porto: https://www.kaggle.com/c/pkdd-15-predict-taxi-service-trajectory-i
    - NYC TLC: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page



    Porto Dataset Exploration & Profiling (Jupyter)
    Jupyter notebook produced covering: schema inspection, null/missing data count (MISSING_DATA field), trip duration distribution histogram, call type A/B/C breakdown by %, temporal demand curve (trips per hour across full dataset), top 5 busiest origin zones. Notebook committed to repo.
    Deep exploratory data analysis on the Porto Taxi Trajectories dataset using PySpark in Jupyter. Understand schema, data quality, trip duration distributions, call type breakdown (A/B/C), and temporal demand curves. This analysis drives every downstream design decision.## 🎯 Objective

Understand the Porto dataset deeply before building any pipeline. Decisions about partitioning, zone mapping, and ML features all depend on this EDA.

## 📋 Sub-tasks

- [ ]  Load Porto CSV from MinIO into Spark DataFrame
- [ ]  Inspect schema: column types, nulls, POLYLINE format (JSON array)
- [ ]  Count MISSING_DATA=True rows — document exclusion strategy
- [ ]  Parse POLYLINE: extract trip length in GPS points, compute duration (points × 15 sec)
- [ ]  Plot trip duration distribution
- [ ]  Compute CALL_TYPE breakdown: % type A (dispatch), B (stand), C (street hail)
- [ ]  Compute temporal demand curve: aggregate trips by hour-of-day and day-of-week
- [ ]  Identify peak hours and weekend vs weekday patterns
- [ ]  Document findings with inline markdown in notebook

## 🔑 Key Fields



Zone Remapping: Porto → Casablanca (PySpark)

PySpark transformation script producing Casablanca-mapped trip coordinates. Visual validation on OSM map showing trips fall within Casablanca bounding box (lon: 33.4–33.7°N, lat: 7.4–7.8°W). zone_mapping.csv applied correctly. At least one Casablanca map visualization in notebook showing remapped trip density per arrondissement.
Implement the coordinate transformation that maps Porto GPS data to Casablanca's bounding box. Porto's 22 city zones are remapped to Casablanca's 16 arrondissements using the provided zone_mapping.csv. GPS coordinates are linearly transformed. The remapped output must be visualized on an OpenStreetMap base map to validate correctness.## 🎯 Objective

Transform Porto GPS coordinates into Casablanca space so the streaming simulation is geographically meaningful for the demo.

## 📋 Sub-tasks

- [ ]  Load `zone_mapping.csv` (Porto zone → Casablanca arrondissement)
- [ ]  Implement linear bounding box transform: `(lon - porto_min) / (porto_max - porto_min) * (casa_max - casa_min) + casa_min`
- [ ]  Apply transform to POLYLINE coordinates using PySpark `explode` + `from_json`
- [ ]  Join with zone_mapping to assign `arrondissement_id` to each trip
- [ ]  Add Gaussian noise (σ ≈ 0.0002°) to transformed coordinates for realism
- [ ]  Visualize remapped trips on OSM map (use `folium` or `pydeck`)
- [ ]  Validate: all coordinates fall within Casablanca bounding box
- [ ]  Save zone centroids per arrondissement for use in Flink anonymization

## 🗺️ Bounding Box Reference

- **Casablanca**: lon 33.4–33.7°N, lat 7.4–7.8°W
- **Porto**: approx lon 41.1–41.2°N, lat 8.6–8.7°W

## 📚 OSM Extract

- https://download.geofabrik.de/africa/morocco.html



Build Kafka Producers (GPS + Trip Request Simulators)vehicle_gps_[producer.py](http://producer.py) publishes to raw.gps topic. Events contain: taxi_id, timestamp (event time), lat, lon, speed, status. GPS blackout: 5% chance of 60-180s delay. trip_request_[producer.py](http://producer.py) publishes to raw.trips topic with fields: trip_id (UUID), rider_id, origin_zone, destination_zone, requested_at, call_type. Peak hours (7-9h, 17-19h) produce 3-5x off-peak rate. Both verified with [kafka-console-consumer.sh](http://kafka-console-consumer.sh).
Implement the two Kafka producer scripts that simulate real-time TaaSim operations: vehicle_gps_[producer.py](http://producer.py) replays Porto GPS polylines at 10x speed with noise and blackouts; trip_request_[producer.py](http://producer.py) generates trip reservation events following Porto's demand curve. Both are required for all streaming tasks in Weeks 3-4.## 🎯 Objective

Create the streaming data simulation layer that powers all real-time demo scenarios.

## 📋 Sub-tasks

### vehicle_gps_[producer.py](http://producer.py)

- [ ]  Read Porto POLYLINE column and iterate GPS coordinates at configurable speed (10×)
- [ ]  Apply coordinate transform to Casablanca bounding box
- [ ]  Add Gaussian noise (σ ≈ 0.0002°)
- [ ]  Implement 5% blackout probability: delay Kafka send by 60–180s
- [ ]  Set Kafka message key = `taxi_id`
- [ ]  Emit out-of-order events (required for Flink watermark testing)

### trip_request_[producer.py](http://producer.py)

- [ ]  Compute hourly demand multiplier from Porto dataset
- [ ]  Apply demand curve to event emission rate
- [ ]  Generate: `trip_id` (UUID4), `rider_id`, `origin_zone`, `destination_zone`, `requested_at`, `call_type`
- [ ]  Peak hours (7–9h, 17–19h): 3–5× off-peak rate
- [ ]  Friday 12–14h: reduced rate

### event_[injector.py](http://injector.py) (bonus, needed for Week 8 demo)

- [ ]  Demand spike: multiply emission rate in a zone by configurable factor for 5 min
- [ ]  GPS blackout: suppress GPS events from a set of vehicles
- [ ]  Rain event: 1.4× global trip rate for configurable duration

## ⚠️ Critical Engineering Note

> The GPS producer **deliberately** emits out-of-order events with up to 3-minute delay. This is intentional — Flink Job 1 MUST handle this with watermarks. A processing-time approach will fail evaluation.
>