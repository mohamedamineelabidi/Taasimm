"""
TaaSim — Flink Job 2: Demand Aggregator
=========================================
Inputs:
  - processed.gps  (Kafka) → normalized GPS positions from Job 1
  - raw.trips      (Kafka) → trip requests from producer

Logic:
  - 30-second TUMBLING windows per (city, zone_id)
  - Count active vehicles (GPS events) and pending requests (trip events)
  - Compute supply/demand ratio = active_vehicles / max(1, pending_requests)
  - Emit per-window aggregates to:
      → Cassandra: demand_zones table
      → Kafka: processed.demand topic

Checkpointing: 60s interval, EXACTLY_ONCE, RocksDB to s3://curated/flink-checkpoints
"""

import json
import os
import logging
from datetime import datetime, timezone

from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.datastream.state import ValueStateDescriptor
from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext
from pyflink.datastream.connectors.kafka import (
    KafkaSource, KafkaSink, KafkaRecordSerializationSchema,
    KafkaOffsetsInitializer, DeliveryGuarantee,
)
from pyflink.common import WatermarkStrategy, Time, Types, Duration
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.window import TumblingProcessingTimeWindows
from pyflink.datastream.functions import ProcessWindowFunction
from pyflink.datastream.state import ReducingStateDescriptor

from cassandra.cluster import Cluster
from cassandra.policies import DCAwareRoundRobinPolicy

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("DemandAggregator")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "cassandra")
WINDOW_SECONDS = 30
CITY = "casablanca"


class GpsTimestampAssigner:
    def extract_timestamp(self, value, record_timestamp):
        try:
            event = json.loads(value)
            ts = event.get("timestamp", 0)
            return int(ts) * 1000
        except Exception:
            return record_timestamp


class TripTimestampAssigner:
    def extract_timestamp(self, value, record_timestamp):
        try:
            event = json.loads(value)
            requested_at = event.get("requested_at", "")
            if requested_at:
                dt = datetime.fromisoformat(requested_at.replace("Z", "+00:00"))
                return int(dt.timestamp() * 1000)
        except Exception:
            pass
        return record_timestamp


class DemandWindowFunction(ProcessWindowFunction):
    """
    Processes a 30s tumbling window for a (city, zone_id) key.
    Aggregates GPS pings as active_vehicles and trip requests as pending_requests.
    """

    def __init__(self):
        self._cassandra_session = None
        self._insert_stmt = None
        self._kafka_producer = None

    def open(self, runtime_context: RuntimeContext):
        try:
            cluster = Cluster(
                [CASSANDRA_HOST],
                load_balancing_policy=DCAwareRoundRobinPolicy(local_dc="datacenter1"),
                protocol_version=4,
            )
            self._cassandra_session = cluster.connect("taasim")
            self._insert_stmt = self._cassandra_session.prepare(
                """
                INSERT INTO demand_zones
                    (city, zone_id, window_start, active_vehicles, pending_requests, ratio, forecast_demand)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                USING TTL 604800
                """
            )
            logger.info("Cassandra connected in DemandWindowFunction")
        except Exception as e:
            logger.error(f"Cassandra connect failed: {e}")

    def process(self, key, context, elements):
        city, zone_id = key
        gps_count = 0
        trip_count = 0
        window_start_ms = context.window().start

        for elem in elements:
            try:
                # elem is a tuple (city, zone_id, json_str)
                raw_json = elem[2] if isinstance(elem, (tuple, list)) else elem
                event = json.loads(raw_json)
                if event.get("_type") == "trip":
                    trip_count += 1
                else:
                    gps_count += 1
            except Exception:
                pass

        ratio = gps_count / max(1, trip_count)
        window_start_dt = datetime.fromtimestamp(window_start_ms / 1000, tz=timezone.utc)
        forecast_demand = float(trip_count)

        # Write to Cassandra
        if self._cassandra_session:
            try:
                self._cassandra_session.execute(
                    self._insert_stmt,
                    (city, zone_id, window_start_dt, gps_count, trip_count, ratio, forecast_demand)
                )
            except Exception as e:
                logger.error(f"Cassandra write failed: {e}")

        # Emit to Kafka processed.demand
        output = json.dumps({
            "city": city,
            "zone_id": zone_id,
            "window_start": window_start_dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "active_vehicles": gps_count,
            "pending_requests": trip_count,
            "ratio": round(ratio, 3),
        })
        yield output


def main():
    logger.info("=== Demand Aggregator Job Starting ===")

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
    env.set_parallelism(1)

    # ── Checkpointing ─────────────────────────────────────────
    env.enable_checkpointing(60_000)
    from pyflink.datastream import CheckpointingMode
    env.get_checkpoint_config().set_checkpointing_mode(CheckpointingMode.EXACTLY_ONCE)
    env.get_checkpoint_config().set_min_pause_between_checkpoints(30_000)
    env.get_checkpoint_config().set_checkpoint_timeout(120_000)

    # ── Watermark strategy: use ingestion time for both streams
    # GPS timestamps are 2013 (Porto replay); trips are 2026 real-time.
    # Use ingestion-time watermarks so both streams advance together.
    gps_watermark = WatermarkStrategy.for_monotonous_timestamps()
    trip_watermark = WatermarkStrategy.for_monotonous_timestamps()

    # ── GPS source (processed.gps from Job 1) ─────────────────
    gps_source = (
        KafkaSource.builder()
        .set_bootstrap_servers(KAFKA_BOOTSTRAP)
        .set_topics("processed.gps")
        .set_group_id("demand-aggregator-gps")
        .set_starting_offsets(KafkaOffsetsInitializer.latest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    # ── Trip request source (raw.trips) ───────────────────────
    trip_source = (
        KafkaSource.builder()
        .set_bootstrap_servers(KAFKA_BOOTSTRAP)
        .set_topics("raw.trips")
        .set_group_id("demand-aggregator-trips")
        .set_starting_offsets(KafkaOffsetsInitializer.latest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    # ── Kafka sink (processed.demand) ─────────────────────────
    demand_sink = (
        KafkaSink.builder()
        .set_bootstrap_servers(KAFKA_BOOTSTRAP)
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic("processed.demand")
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE)
        .build()
    )

    # ── GPS stream: tag with zone_id key (city, zone_id) ──────
    def tag_gps(value):
        try:
            event = json.loads(value)
            zone_id = event.get("zone_id", 0)
            return (CITY, zone_id, value)
        except Exception:
            return None

    def tag_trip(value):
        try:
            event = json.loads(value)
            # Mark as trip type and assign to origin zone
            event["_type"] = "trip"
            zone_id = event.get("origin_zone", 0)
            return (CITY, zone_id, json.dumps(event))
        except Exception:
            return None

    gps_stream = (
        env.from_source(gps_source, gps_watermark, "GPS Source (processed.gps)")
        .filter(lambda x: x is not None)
        .map(tag_gps, output_type=Types.TUPLE([Types.STRING(), Types.INT(), Types.STRING()]))
        .filter(lambda x: x is not None)
    )

    trip_stream = (
        env.from_source(trip_source, trip_watermark, "Trip Source (raw.trips)")
        .filter(lambda x: x is not None)
        .map(tag_trip, output_type=Types.TUPLE([Types.STRING(), Types.INT(), Types.STRING()]))
        .filter(lambda x: x is not None)
    )

    # ── Union GPS + trip streams, key by (city, zone_id) ──────
    combined = gps_stream.union(trip_stream)

    # Extract the raw event JSON, keyed by (city, zone_id)
    result = (
        combined
        .key_by(lambda x: (x[0], x[1]))
        .window(TumblingProcessingTimeWindows.of(Time.seconds(WINDOW_SECONDS)))
        .process(DemandWindowFunction(), output_type=Types.STRING())
    )

    result.sink_to(demand_sink)

    logger.info("Submitting Demand Aggregator Job...")
    env.execute("Demand Aggregator")


if __name__ == "__main__":
    main()
