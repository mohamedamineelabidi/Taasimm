"""
Flink Job 1 — GPS Normalizer
Reads raw.gps from Kafka, validates coordinates, assigns zones,
anonymizes lat/lon to H3 cell center (~87m precision),
writes to Cassandra + processed.gps Kafka topic.
"""

import json
import logging
import time
from datetime import datetime

from pyflink.common import Types, WatermarkStrategy, Duration, Row, Time
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.datastream.connectors.kafka import (
    KafkaSource,
    KafkaOffsetsInitializer,
    KafkaSink,
    KafkaRecordSerializationSchema,
)
from pyflink.datastream.functions import KeyedProcessFunction, FilterFunction, RuntimeContext
from pyflink.datastream.state import ValueStateDescriptor, StateTtlConfig

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
            # event_time is ISO-8601 UTC string (canonical field)
            return int(datetime.fromisoformat(
                data["event_time"].replace("Z", "+00:00")
            ).timestamp() * 1000)
        except (json.JSONDecodeError, KeyError, TypeError, ValueError):
            return record_timestamp


# ─── GPS Deduplication + Processing ─────────────────────────

class GpsDeduplicator(KeyedProcessFunction):
    """Keyed by taxi_id. Deduplicates GPS pings via ValueState, then
    validates, assigns zone, anonymizes to centroid, writes Cassandra + Kafka."""

    def open(self, runtime_context: RuntimeContext):
        

        # ValueState for per-taxi deduplication (last seen event_time in ms)
        ttl_config = (
            StateTtlConfig
            .new_builder(Time.minutes(5))
            .set_update_type(StateTtlConfig.UpdateType.OnCreateAndWrite)
            .set_state_visibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
            .build()
        )
        ts_desc = ValueStateDescriptor("last_ts", Types.LONG())
        ts_desc.enable_time_to_live(ttl_config)
        self._last_ts = runtime_context.get_state(ts_desc)

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
        logger.info(f"Loaded {len(self.h3_lookup)} H3 cells")

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

    def process_element(self, value, ctx: KeyedProcessFunction.Context):
        try:
            raw_json = value[1] if isinstance(value, (tuple, list)) else value
            data = json.loads(raw_json)
        except (json.JSONDecodeError, IndexError):
            return

        event_time_str = data.get("event_time")
        if event_time_str is None:
            return

        # Parse event_time to millis for dedup check
        try:
            event_ts_ms = int(datetime.fromisoformat(
                event_time_str.replace("Z", "+00:00")
            ).timestamp() * 1000)
        except (ValueError, TypeError):
            return

        # Deduplicate: skip if event_time <= last seen for this taxi_id
        last_ts = self._last_ts.value()
        if last_ts is not None and event_ts_ms <= last_ts:
            return
        self._last_ts.update(event_ts_ms)

        lat = data.get("lat")
        lon = data.get("lon")
        speed = data.get("speed_kmh", 0.0)
        taxi_id = data.get("taxi_id", "unknown")
        status = data.get("status", "unknown")
        snap_dist_m = data.get("snap_dist_m")

        if lat is None or lon is None:
            return

        # Validate coordinates and speed
        b = self._bounds
        if not (b[0] <= lat <= b[1] and b[2] <= lon <= b[3]):
            return
        if speed is not None and (speed < 0 or speed > 150.0):
            return
        if snap_dist_m is not None and float(snap_dist_m) > 333.0:
            return

        # Trust producer's pre-computed H3 cell
        cell = data.get("h3_index")
        if not cell:
            return
        zone_id = None
        if cell in self.h3_lookup:
            zone_id = self.h3_lookup[cell]["zone_id"]
        if zone_id is None:
            return

        # Anonymize: snap to H3 cell center (~87m precision, res 9)
        import h3
        h3_lat, h3_lon = h3.cell_to_latlng(cell)
        display_lat, display_lon = h3_lat, h3_lon

        # Parse canonical event_time
        try:
            event_time = datetime.fromisoformat(event_time_str.replace("Z", "+00:00"))
        except ValueError:
            return

        # Write to Cassandra (async — high throughput path)
        try:
            self.session.execute_async(
                self.insert_stmt,
                ("casablanca", zone_id, event_time, taxi_id,
                 display_lat, display_lon, speed, status, cell)
            )
        except Exception as e:
            logger.error(f"Cassandra write error: {e}")

        # Build output for processed.gps
        output = {
            "taxi_id": taxi_id,
            "zone_id": zone_id,
            "h3_index": cell,
            "event_time": event_time_str,
            "lat": display_lat,
            "lon": display_lon,
            "speed_kmh": speed,
            "status": status,
        }
        yield json.dumps(output)


def extract_key(value):
    """Extract taxi_id from raw GPS JSON for key_by partitioning."""
    try:
        data = json.loads(value)
        taxi_id = data.get("taxi_id")
        if taxi_id:
            return (taxi_id, value)
    except Exception:
        pass
    return None


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
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
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

    # Pre-map: extract taxi_id for keyed deduplication
    keyed_stream = (
        gps_stream
        .map(extract_key, output_type=Types.TUPLE([Types.STRING(), Types.STRING()]))
        .filter(lambda x: x is not None)
    )

    # Deduplicate → validate → zone assign → anonymize → Cassandra write
    processed_stream = (
        keyed_stream
        .key_by(lambda x: x[0])
        .process(GpsDeduplicator(), output_type=Types.STRING())
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
