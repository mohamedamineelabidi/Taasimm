"""
Flink Job 1 — GPS Normalizer
Reads raw.gps from Kafka, validates coordinates, assigns zones,
anonymizes lat/lon to centroid, writes to Cassandra + processed.gps Kafka topic.
"""

import json
import logging
import time
from datetime import datetime

from pyflink.common import Types, WatermarkStrategy, Duration, Row
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.datastream.connectors.kafka import (
    KafkaSource,
    KafkaOffsetsInitializer,
    KafkaSink,
    KafkaRecordSerializationSchema,
)
from pyflink.datastream.functions import MapFunction, FilterFunction, RuntimeContext

# zone_data functions inlined to avoid module resolution issues in PyFlink Beam workers

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("GPSNormalizer")

# ─── Configuration ───────────────────────────────────────────

KAFKA_BOOTSTRAP = "kafka:9092"
INPUT_TOPIC = "raw.gps"
OUTPUT_TOPIC = "processed.gps"
CASSANDRA_HOST = "cassandra"
CASSANDRA_PORT = 9042
CASSANDRA_KEYSPACE = "taasim"
CASSANDRA_TABLE = "vehicle_positions"


# ─── Timestamp Assigner ──────────────────────────────────────

class GpsTimestampAssigner(TimestampAssigner):
    """Extract event time from the 'timestamp' field (Unix epoch seconds)."""

    def extract_timestamp(self, value, record_timestamp):
        try:
            data = json.loads(value)
            return int(data["timestamp"]) * 1000  # Convert to millis
        except (json.JSONDecodeError, KeyError, TypeError):
            return record_timestamp


# ─── GPS Processing Function ────────────────────────────────

class GpsProcessor(MapFunction):
    """Validate, assign zone, anonymize coordinates, prepare for sinks."""

    def open(self, runtime_context: RuntimeContext):
        import csv
        import h3 as _h3
        self._h3 = _h3
        CASA_LAT_MIN, CASA_LAT_MAX = 33.450, 33.680
        CASA_LON_MIN, CASA_LON_MAX = -7.720, -7.480
        self._bounds = (CASA_LAT_MIN, CASA_LAT_MAX, CASA_LON_MIN, CASA_LON_MAX)

        # Load H3→zone lookup for O(1) zone assignment
        self.h3_lookup = {}
        try:
            with open("/opt/flink/data/h3_zone_lookup.json", "r") as f:
                self.h3_lookup = json.load(f)
        except Exception as e:
            logger.error(f"H3 lookup load failed: {e}")

        # Load zone centroids for anonymization
        self.zone_centroids = {}
        try:
            with open("/opt/flink/data/zone_mapping.csv", "r") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    zid = int(row["zone_id"])
                    self.zone_centroids[zid] = (
                        float(row["casa_centroid_lat"]),
                        float(row["casa_centroid_lon"]),
                    )
        except Exception as e:
            logger.error(f"Zone centroid load failed: {e}")
        logger.info(f"Loaded {len(self.h3_lookup)} H3 cells, {len(self.zone_centroids)} zone centroids")

        # Initialize Cassandra connection
        from cassandra.cluster import Cluster
        self.cluster = Cluster([CASSANDRA_HOST], port=CASSANDRA_PORT)
        self.session = self.cluster.connect(CASSANDRA_KEYSPACE)
        self.insert_stmt = self.session.prepare(
            f"INSERT INTO {CASSANDRA_TABLE} "
            "(city, zone_id, event_time, taxi_id, lat, lon, speed, status, h3_index) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"
        )
        logger.info("Connected to Cassandra")

    def close(self):
        if hasattr(self, "cluster"):
            self.cluster.shutdown()

    def map(self, value):
        try:
            data = json.loads(value)
        except json.JSONDecodeError:
            return None

        lat = data.get("lat")
        lon = data.get("lon")
        speed = data.get("speed_kmh", 0.0)
        taxi_id = data.get("taxi_id", "unknown")
        status = data.get("status", "unknown")
        timestamp = data.get("timestamp")
        snapped_valid = data.get("snapped_valid", True)
        snap_dist_m = data.get("snap_dist_m")

        if lat is None or lon is None or timestamp is None:
            return None

        # Validate coordinates and speed (inlined is_valid_gps)
        b = self._bounds
        if not (b[0] <= lat <= b[1] and b[2] <= lon <= b[3]):
            return None
        if speed is not None and speed > 150.0:
            return None
        # Strict runtime guard aligned with notebook filtering
        if not snapped_valid:
            return None
        if snap_dist_m is not None and float(snap_dist_m) > 333.0:
            return None

        # Assign zone via H3 O(1) lookup (replaces bbox loop)
        cell = self._h3.latlng_to_cell(lat, lon, 9)
        zone_id = None
        if cell in self.h3_lookup:
            zone_id = self.h3_lookup[cell]["zone_id"]
        else:
            for ring in range(1, 6):
                for neighbor in self._h3.grid_ring(cell, ring):
                    if neighbor in self.h3_lookup:
                        zone_id = self.h3_lookup[neighbor]["zone_id"]
                        break
                if zone_id is not None:
                    break
        if zone_id is None:
            return None

        # Anonymize: snap to zone centroid
        centroid = self.zone_centroids.get(zone_id)
        if centroid is None:
            return None
        centroid_lat, centroid_lon = centroid

        # Convert timestamp to datetime for Cassandra
        event_time = datetime.utcfromtimestamp(int(timestamp))

        # Write to Cassandra (anonymized: centroid coordinates)
        try:
            self.session.execute(
                self.insert_stmt,
                ("casablanca", zone_id, event_time, taxi_id,
                 centroid_lat, centroid_lon, speed, status, cell)
            )
        except Exception as e:
            logger.error(f"Cassandra write error: {e}")

        # Build output record for processed.gps Kafka topic
        output = {
            "taxi_id": taxi_id,
            "zone_id": zone_id,
            "h3_index": cell,
            "event_time": event_time.isoformat() + "Z",
            "lat": centroid_lat,
            "lon": centroid_lon,
            "speed_kmh": speed,
            "status": status,
            "timestamp": int(timestamp),
        }
        return json.dumps(output)


class NullFilter(FilterFunction):
    """Filter out None values from the stream."""

    def filter(self, value):
        return value is not None


# ─── Main ────────────────────────────────────────────────────

def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
    env.set_parallelism(2)

    # Checkpointing is configured via FLINK_PROPERTIES in docker-compose
    # (execution.checkpointing.interval: 60000, state.backend: rocksdb)

    logger.info("=== GPS Normalizer Job Starting ===")
    logger.info(f"Kafka: {KAFKA_BOOTSTRAP}, Input: {INPUT_TOPIC}, Output: {OUTPUT_TOPIC}")

    # ─── Kafka Source ────────────────────────────────────────
    kafka_source = (
        KafkaSource.builder()
        .set_bootstrap_servers(KAFKA_BOOTSTRAP)
        .set_topics(INPUT_TOPIC)
        .set_group_id("flink-gps-normalizer")
        .set_starting_offsets(KafkaOffsetsInitializer.latest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    # Watermark strategy: 3-minute max out-of-orderness
    watermark_strategy = (
        WatermarkStrategy
        .for_bounded_out_of_orderness(Duration.of_minutes(3))
        .with_timestamp_assigner(GpsTimestampAssigner())
    )

    # ─── Build Pipeline ─────────────────────────────────────
    gps_stream = env.from_source(
        kafka_source, watermark_strategy, "KafkaGPSSource"
    )

    # Process: validate → assign zone → anonymize → write Cassandra
    processed_stream = (
        gps_stream
        .map(GpsProcessor(), output_type=Types.STRING())
        .filter(NullFilter())
    )

    # ─── Kafka Sink (processed.gps) ─────────────────────────
    kafka_sink = (
        KafkaSink.builder()
        .set_bootstrap_servers(KAFKA_BOOTSTRAP)
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic(OUTPUT_TOPIC)
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .build()
    )

    processed_stream.sink_to(kafka_sink)

    logger.info("=== Submitting GPS Normalizer Job ===")
    env.execute("GPS Normalizer")


if __name__ == "__main__":
    main()
