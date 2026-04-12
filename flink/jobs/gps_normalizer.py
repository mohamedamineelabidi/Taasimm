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

from zone_data import load_zones, assign_zone, is_valid_gps

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
        self.zones = load_zones()
        logger.info(f"Loaded {len(self.zones)} zones")

        # Initialize Cassandra connection
        from cassandra.cluster import Cluster
        self.cluster = Cluster([CASSANDRA_HOST], port=CASSANDRA_PORT)
        self.session = self.cluster.connect(CASSANDRA_KEYSPACE)
        self.insert_stmt = self.session.prepare(
            f"INSERT INTO {CASSANDRA_TABLE} "
            "(city, zone_id, event_time, taxi_id, lat, lon, speed, status) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
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

        if lat is None or lon is None or timestamp is None:
            return None

        # Validate coordinates and speed
        if not is_valid_gps(lat, lon, speed):
            return None

        # Assign zone
        zone_result = assign_zone(lat, lon, self.zones)
        if zone_result is None:
            return None

        zone_id, centroid_lat, centroid_lon = zone_result

        # Convert timestamp to datetime for Cassandra
        event_time = datetime.utcfromtimestamp(int(timestamp))

        # Write to Cassandra (anonymized: centroid coordinates)
        try:
            self.session.execute(
                self.insert_stmt,
                ("casablanca", zone_id, event_time, taxi_id,
                 centroid_lat, centroid_lon, speed, status)
            )
        except Exception as e:
            logger.error(f"Cassandra write error: {e}")

        # Build output record for processed.gps Kafka topic
        output = {
            "taxi_id": taxi_id,
            "zone_id": zone_id,
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
