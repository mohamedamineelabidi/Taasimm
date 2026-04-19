"""
TaaSim — Flink Job 3: Trip Matcher
=====================================
Inputs:
  - raw.trips      (Kafka) → trip requests
  - processed.gps  (Kafka) → normalized GPS positions with zone_id

Logic:
  1. Maintain a keyed state of available vehicles per zone (last GPS ping < 60s)
  2. On trip request: find nearest available vehicle in origin_zone
  3. If no vehicle in origin_zone within 5s: expand to adjacent zones (from zone_data.py)
  4. Assign match → compute ETA (distance / avg_speed)
  5. Emit to:
      → Cassandra: trips table
      → Kafka:     processed.matches topic

State TTL: vehicle positions expire after 60 seconds of inactivity.
"""

import json
import os
import math
import uuid
import logging
from datetime import datetime, timezone

from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.datastream.functions import KeyedProcessFunction, FlatMapFunction, RuntimeContext
from pyflink.datastream.state import MapStateDescriptor, StateTtlConfig
from pyflink.common import WatermarkStrategy, Types, Time, Duration
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.connectors.kafka import (
    KafkaSource, KafkaSink, KafkaRecordSerializationSchema,
    KafkaOffsetsInitializer, DeliveryGuarantee,
)

from cassandra.cluster import Cluster

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("TripMatcher")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "cassandra")
CITY = "casablanca"
AVG_SPEED_KMH = 30.0   # average vehicle speed for ETA calculation
VEHICLE_TTL_SECONDS = 60  # vehicles expire after 60s of no GPS ping


def haversine_km(lat1, lon1, lat2, lon2):
    """Compute distance in km between two GPS coordinates."""
    R = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat / 2) ** 2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon / 2) ** 2
    return R * 2 * math.asin(math.sqrt(a))


class TripTimestampAssigner:
    def extract_timestamp(self, value, record_timestamp):
        try:
            event = json.loads(value)
            # event_time is ISO-8601 UTC string (canonical field, renamed from requested_at)
            event_time = event.get("event_time", "")
            if event_time:
                dt = datetime.fromisoformat(event_time.replace("Z", "+00:00"))
                return int(dt.timestamp() * 1000)
        except Exception:
            pass
        return record_timestamp


class GpsTimestampAssigner:
    def extract_timestamp(self, value, record_timestamp):
        try:
            event = json.loads(value)
            # event_time is ISO-8601 UTC string from processed.gps
            event_time = event.get("event_time", "")
            if event_time:
                dt = datetime.fromisoformat(event_time.replace("Z", "+00:00"))
                return int(dt.timestamp() * 1000)
        except Exception:
            pass
        return record_timestamp


class TripMatcherFunction(KeyedProcessFunction):
    """
    Keyed by zone_id (int). Maintains available vehicles in the zone.
    On GPS event: update vehicle state.
    On trip request: find best vehicle → write match to Cassandra + Kafka.
    """

    def __init__(self):
        self._zones = None
        self._zone_adjacency = {}
        self._vehicles = None         # MapState: taxi_id -> {lat, lon, speed}
        self._cassandra_session = None
        self._trip_insert_stmt = None
        self._matched_trips = set()   # dedup: prevent same trip matched by multiple zones

    def open(self, runtime_context: RuntimeContext):
        # Load zone adjacency map from CSV directly (no zone_data module dependency)
        import csv
        try:
            self._zones = []
            with open("/opt/flink/data/zone_mapping.csv", "r") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    zone_id = int(row["zone_id"])
                    adj_raw = row.get("adjacent_zones", "")
                    self._zones.append({"zone_id": zone_id, "adjacent_zones": adj_raw})
                    if adj_raw:
                        self._zone_adjacency[zone_id] = [
                            int(x.strip()) for x in str(adj_raw).split(",") if x.strip().isdigit()
                        ]
                    else:
                        self._zone_adjacency[zone_id] = []
        except Exception as e:
            logger.error(f"Zone load failed: {e}")
            self._zones = []

        # Vehicle map state with TTL
        ttl_config = (
            StateTtlConfig
            .new_builder(Time.seconds(VEHICLE_TTL_SECONDS))
            .set_update_type(StateTtlConfig.UpdateType.OnCreateAndWrite)
            .set_state_visibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
            .build()
        )
        vehicle_desc = MapStateDescriptor(
            "vehicles",
            Types.STRING(),
            Types.STRING(),
        )
        vehicle_desc.enable_time_to_live(ttl_config)
        self._vehicles = runtime_context.get_map_state(vehicle_desc)

        # Cassandra connection
        self._connect_cassandra()

    def _connect_cassandra(self):
        """Connect (or reconnect) to Cassandra using the default LBP so the driver
        auto-detects the DC name — avoids hardcoding dc1 vs datacenter1."""
        try:
            cluster = Cluster([CASSANDRA_HOST], protocol_version=4)
            self._cassandra_session = cluster.connect("taasim")
            self._trip_insert_stmt = self._cassandra_session.prepare(
                """
                INSERT INTO trips
                    (city, date_bucket, created_at, trip_id, rider_id, taxi_id,
                     origin_zone, dest_zone, status, fare, eta_seconds,
                     origin_h3, dest_h3, origin_lat, origin_lon, dest_lat, dest_lon)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                IF NOT EXISTS
                """
            )
            logger.info("Cassandra connected in TripMatcherFunction")
        except Exception as e:
            logger.error(f"Cassandra connect failed: {e}")
            self._cassandra_session = None

    def process_element(self, value, ctx: KeyedProcessFunction.Context):
        try:
            # value is a tuple (zone_id, json_str)
            raw_json = value[1] if isinstance(value, (tuple, list)) else value
            event = json.loads(raw_json)
        except Exception:
            return

        event_type = event.get("_type", "gps")

        if event_type == "gps":
            # Store vehicle state -- only track available vehicles
            taxi_id = event.get("taxi_id", "")
            status = event.get("status", "")
            if taxi_id and status not in ("matched", "offline"):
                vehicle_data = json.dumps({
                    "lat": event.get("centroid_lat", event.get("lat", 0)),
                    "lon": event.get("centroid_lon", event.get("lon", 0)),
                    "speed_kmh": event.get("speed_kmh", 0),
                })
                self._vehicles.put(taxi_id, vehicle_data)

        elif event_type == "trip":
            # Dedup: skip if this trip was already matched (fanout from adjacent zones)
            trip_id = event.get("trip_id")
            if trip_id and trip_id in self._matched_trips:
                return

            # Attempt to match a vehicle
            zone_id = ctx.get_current_key()
            matched = self._find_best_vehicle(zone_id, event)

            if matched:
                taxi_id, eta_seconds, fare = matched
                # Remove matched vehicle from state to prevent ghost-taxi double-matching
                try:
                    self._vehicles.remove(taxi_id)
                except Exception:
                    pass
                if trip_id:
                    self._matched_trips.add(trip_id)
                    # Prevent unbounded growth
                    if len(self._matched_trips) > 50000:
                        self._matched_trips.clear()
                self._write_match(event, taxi_id, zone_id, eta_seconds, fare)
                yield json.dumps({
                    "trip_id": event.get("trip_id"),
                    "rider_id": event.get("rider_id"),
                    "taxi_id": taxi_id,
                    "origin_zone": zone_id,
                    "dest_zone": event.get("destination_zone", 0),
                    "eta_seconds": eta_seconds,
                    "fare": fare,
                    "matched_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "status": "matched",
                })
            else:
                # No vehicle found; write unmatched trip
                self._write_unmatched(event, zone_id)
                yield json.dumps({
                    "trip_id": event.get("trip_id"),
                    "rider_id": event.get("rider_id"),
                    "origin_zone": zone_id,
                    "status": "no_vehicle",
                    "matched_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                })

    def _find_best_vehicle(self, zone_id, trip_event):
        """Return (taxi_id, eta_seconds, fare) for best vehicle or None.

        Cost function: score = 0.7 * distance_km + 0.3 * idle_penalty
        idle_penalty = 1.0 if speed < 5 km/h (stationary), else 0.0
        """
        best_taxi = None
        best_score = float("inf")
        best_dist = 0.0

        o_lat = trip_event.get("origin_lat", 0)
        o_lon = trip_event.get("origin_lon", 0)

        try:
            for taxi_id, data_str in self._vehicles.items():
                data = json.loads(data_str)
                v_lat = data.get("lat", 0)
                v_lon = data.get("lon", 0)
                dist = haversine_km(o_lat, o_lon, v_lat, v_lon)
                speed = data.get("speed_kmh", 0)
                # Prefer moving vehicles over idle ones (lower score = better)
                idle_penalty = 1.0 if speed < 5 else 0.0
                score = 0.7 * dist + 0.3 * idle_penalty
                if score < best_score:
                    best_score = score
                    best_taxi = taxi_id
                    best_dist = dist
        except Exception as e:
            logger.error(f"Vehicle lookup failed: {e}")
            return None

        if best_taxi is None:
            return None

        # ETA based on pickup distance
        eta_seconds = max(60, int(best_dist / AVG_SPEED_KMH * 3600))
        # Fare: base 10 MAD + 3 MAD/km for estimated trip distance
        d_lat = trip_event.get("dest_lat", o_lat)
        d_lon = trip_event.get("dest_lon", o_lon)
        trip_km = max(1.0, haversine_km(o_lat, o_lon, d_lat, d_lon))
        fare = round(10.0 + 3.0 * trip_km, 2)
        return best_taxi, eta_seconds, fare

    def _write_match(self, event, taxi_id, zone_id, eta_seconds, fare):
        if not self._cassandra_session:
            self._connect_cassandra()
        if not self._cassandra_session:
            return
        try:
            # Use request event_time (not processing time) as created_at
            event_time_str = event.get("event_time", "")
            if event_time_str:
                created_at = datetime.fromisoformat(event_time_str.replace("Z", "+00:00"))
            else:
                created_at = datetime.now(timezone.utc)
            date_bucket = created_at.strftime("%Y-%m-%d")
            self._cassandra_session.execute(
                self._trip_insert_stmt,
                (
                    CITY,
                    date_bucket,
                    created_at,
                    uuid.UUID(event.get("trip_id", str(uuid.uuid4()))),
                    event.get("rider_id", "unknown"),
                    taxi_id,
                    zone_id,
                    event.get("destination_zone", 0),
                    "matched",
                    float(fare),
                    int(eta_seconds),
                    event.get("origin_h3"),
                    event.get("dest_h3"),
                    float(event.get("origin_lat", 0)),
                    float(event.get("origin_lon", 0)),
                    float(event.get("dest_lat", 0)),
                    float(event.get("dest_lon", 0)),
                )
            )
        except Exception as e:
            logger.error(f"Cassandra trip write failed: {e}")

    def _write_unmatched(self, event, zone_id):
        if not self._cassandra_session:
            self._connect_cassandra()
        if not self._cassandra_session:
            return
        try:
            # Use request event_time (not processing time) as created_at
            event_time_str = event.get("event_time", "")
            if event_time_str:
                created_at = datetime.fromisoformat(event_time_str.replace("Z", "+00:00"))
            else:
                created_at = datetime.now(timezone.utc)
            date_bucket = created_at.strftime("%Y-%m-%d")
            self._cassandra_session.execute(
                self._trip_insert_stmt,
                (
                    CITY,
                    date_bucket,
                    created_at,
                    uuid.UUID(event.get("trip_id", str(uuid.uuid4()))),
                    event.get("rider_id", "unknown"),
                    "",
                    zone_id,
                    event.get("destination_zone", 0),
                    "no_vehicle",
                    0.0,
                    0,
                    event.get("origin_h3"),
                    event.get("dest_h3"),
                    float(event.get("origin_lat", 0)),
                    float(event.get("origin_lon", 0)),
                    float(event.get("dest_lat", 0)),
                    float(event.get("dest_lon", 0)),
                )
            )
        except Exception as e:
            logger.error(f"Cassandra unmatched write failed: {e}")


class TripFanout(FlatMapFunction):
    """Emit trip events to origin zone + all adjacent zones for cross-zone matching."""

    def open(self, runtime_context: RuntimeContext):
        import csv
        self._adjacency = {}
        try:
            with open("/opt/flink/data/zone_mapping.csv", "r") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    zid = int(row["zone_id"])
                    adj = row.get("adjacent_zones", "")
                    self._adjacency[zid] = [
                        int(x.strip()) for x in str(adj).split(",") if x.strip().isdigit()
                    ] if adj else []
        except Exception:
            pass
        logger.info(f"TripFanout loaded adjacency for {len(self._adjacency)} zones")

    def flat_map(self, value):
        try:
            event = json.loads(value)
            event["_type"] = "trip"
            zone_id = int(event.get("origin_zone", 0))
            if zone_id == 0:
                return
            event_json = json.dumps(event)
            yield (zone_id, event_json)
            for adj in self._adjacency.get(zone_id, []):
                yield (adj, event_json)
        except Exception:
            return


def main():
    logger.info("=== Trip Matcher Job Starting ===")

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
    env.set_parallelism(1)

    env.enable_checkpointing(60_000)
    from pyflink.datastream import CheckpointingMode
    env.get_checkpoint_config().set_checkpointing_mode(CheckpointingMode.EXACTLY_ONCE)
    env.get_checkpoint_config().set_min_pause_between_checkpoints(30_000)

    gps_wm = (
        WatermarkStrategy
        .for_bounded_out_of_orderness(Duration.of_seconds(10))
        .with_timestamp_assigner(GpsTimestampAssigner())
        .with_idleness(Duration.of_seconds(15))
    )
    trip_wm = (
        WatermarkStrategy
        .for_bounded_out_of_orderness(Duration.of_seconds(10))
        .with_timestamp_assigner(TripTimestampAssigner())
        .with_idleness(Duration.of_seconds(15))
    )

    gps_source = (
        KafkaSource.builder()
        .set_bootstrap_servers(KAFKA_BOOTSTRAP)
        .set_topics("processed.gps")
        .set_group_id("trip-matcher-gps")
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    trip_source = (
        KafkaSource.builder()
        .set_bootstrap_servers(KAFKA_BOOTSTRAP)
        .set_topics("raw.trips")
        .set_group_id("trip-matcher-trips")
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    matches_sink = (
        KafkaSink.builder()
        .set_bootstrap_servers(KAFKA_BOOTSTRAP)
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic("processed.matches")
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE)
        .build()
    )

    def tag_gps(value):
        try:
            event = json.loads(value)
            zone_id = int(event.get("zone_id", 0))
            if zone_id == 0:
                return None
            return (zone_id, value)
        except Exception:
            return None

    trip_stream = (
        env.from_source(trip_source, trip_wm, "Trip Source")
        .flat_map(TripFanout(), output_type=Types.TUPLE([Types.INT(), Types.STRING()]))
    )

    gps_stream = (
        env.from_source(gps_source, gps_wm, "GPS Source")
        .map(tag_gps, output_type=Types.TUPLE([Types.INT(), Types.STRING()]))
        .filter(lambda x: x is not None)
    )

    result = (
        gps_stream
        .union(trip_stream)
        .key_by(lambda x: x[0])
        .process(
            TripMatcherFunction(),
            output_type=Types.STRING()
        )
    )

    result.sink_to(matches_sink)

    logger.info("Submitting Trip Matcher Job...")
    env.execute("Trip Matcher")


if __name__ == "__main__":
    main()
