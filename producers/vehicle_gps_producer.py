"""
TaaSim — Vehicle GPS Producer
================================
Replays Porto taxi POLYLINE trajectories at configurable speed (default 10×),
transforms coordinates to Casablanca bounding box, and publishes to Kafka
topic ``raw.gps``.

Features:
- Porto outlier filter (only metro-area trips)
- Linear bbox transform + Gaussian noise
- 5% GPS blackout probability (60–180s delay → out-of-order events)
- Kafka key = taxi_id (for partition affinity)

Usage:
    python vehicle_gps_producer.py [--max-trips 1000] [--speed 10]
"""

import argparse
import json
import logging
import random
import sys
import time
import uuid
from datetime import datetime, timezone

from kafka import KafkaProducer

from config import (
    KAFKA_BOOTSTRAP,
    TOPIC_GPS,
    PORTO_CSV_PATH,
    REPLAY_SPEED,
    BLACKOUT_PROB,
    BLACKOUT_DELAY,
    is_in_porto_metro,
    transform_to_casablanca,
    load_h3_lookup,
    assign_h3_zone,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [GPS] %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)


def parse_polyline(polyline_str):
    """Parse POLYLINE JSON string → list of [lon, lat] pairs."""
    try:
        coords = json.loads(polyline_str)
        if isinstance(coords, list) and len(coords) >= 2:
            return coords
    except (json.JSONDecodeError, TypeError):
        pass
    return None


def compute_speed(prev_lat, prev_lon, cur_lat, cur_lon, dt_seconds=15):
    """Estimate speed in km/h from two GPS points (haversine approx)."""
    import math
    R = 6371  # Earth radius in km
    dlat = math.radians(cur_lat - prev_lat)
    dlon = math.radians(cur_lon - prev_lon)
    a = (math.sin(dlat / 2) ** 2 +
         math.cos(math.radians(prev_lat)) * math.cos(math.radians(cur_lat)) *
         math.sin(dlon / 2) ** 2)
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    dist_km = R * c
    return round(dist_km / (dt_seconds / 3600), 1) if dt_seconds > 0 else 0.0


def create_producer():
    """Create Kafka producer with JSON serializer."""
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        key_serializer=lambda k: k.encode("utf-8"),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
        retries=3,
    )


def run(max_trips, speed):
    """Main producer loop."""
    import csv

    h3_lookup = load_h3_lookup()
    log.info("Loaded H3 lookup: %d cells", len(h3_lookup))

    producer = create_producer()
    log.info("Connected to Kafka at %s", KAFKA_BOOTSTRAP)
    log.info("Topic: %s | Speed: %dx | Max trips: %s",
             TOPIC_GPS, speed, max_trips or "unlimited")

    sent = 0
    skipped = 0
    blackout_events = 0
    trip_count = 0
    interval = 15.0 / speed  # original interval between GPS pings is 15s

    with open(PORTO_CSV_PATH, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row.get("MISSING_DATA", "").strip().upper() == "TRUE":
                continue

            polyline = parse_polyline(row.get("POLYLINE", "[]"))
            if polyline is None:
                continue

            # Check first point for Porto metro filter
            first_lon, first_lat = polyline[0]
            if not is_in_porto_metro(first_lat, first_lon):
                skipped += 1
                continue

            taxi_id = row.get("TAXI_ID", "unknown")
            trip_id = row.get("TRIP_ID", str(uuid.uuid4()))
            base_ts = int(row.get("TIMESTAMP", int(time.time())))

            # Determine if this vehicle has a GPS blackout
            has_blackout = random.random() < BLACKOUT_PROB
            blackout_delay = random.randint(*BLACKOUT_DELAY) if has_blackout else 0

            prev_lat, prev_lon = None, None
            for i, (lon, lat) in enumerate(polyline):
                if not is_in_porto_metro(lat, lon):
                    continue

                casa_lat, casa_lon = transform_to_casablanca(lat, lon)
                zone_id, zone_name, h3_cell = assign_h3_zone(
                    casa_lat, casa_lon, h3_lookup)
                if zone_id == 0:
                    continue  # skip points outside all zones (ocean/edge)
                event_ts = base_ts + i * 15  # each point is 15s apart

                speed_kmh = 0.0
                if prev_lat is not None:
                    speed_kmh = compute_speed(prev_lat, prev_lon,
                                              casa_lat, casa_lon, 15)
                prev_lat, prev_lon = casa_lat, casa_lon

                # Determine status
                if i == 0:
                    status = "pickup"
                elif i == len(polyline) - 1:
                    status = "dropoff"
                else:
                    status = "moving"

                event = {
                    "taxi_id": taxi_id,
                    "trip_id": trip_id,
                    "timestamp": event_ts,
                    "event_time": datetime.fromtimestamp(
                        event_ts, tz=timezone.utc
                    ).isoformat(),
                    "lat": round(casa_lat, 6),
                    "lon": round(casa_lon, 6),
                    "speed_kmh": speed_kmh,
                    "status": status,
                    "h3_index": h3_cell,
                    "zone_id": zone_id,
                    "zone_name": zone_name,
                }

                # Simulate GPS blackout: delay sending (creates out-of-order)
                if has_blackout and i > 0:
                    time.sleep(blackout_delay / speed)
                    blackout_events += 1
                    has_blackout = False  # one blackout per trip

                producer.send(TOPIC_GPS, key=taxi_id, value=event)
                sent += 1

                if sent % 500 == 0:
                    log.info("Sent %d GPS events (%d trips, %d skipped, %d blackouts)",
                             sent, trip_count, skipped, blackout_events)

                time.sleep(interval)

            trip_count += 1
            if max_trips and trip_count >= max_trips:
                break

    producer.flush()
    producer.close()
    log.info("=== DONE === Sent %d events from %d trips (%d skipped, %d blackouts)",
             sent, trip_count, skipped, blackout_events)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="TaaSim Vehicle GPS Producer")
    parser.add_argument("--max-trips", type=int, default=None,
                        help="Max number of trips to replay (default: all)")
    parser.add_argument("--speed", type=int, default=REPLAY_SPEED,
                        help=f"Replay speed multiplier (default: {REPLAY_SPEED})")
    args = parser.parse_args()
    run(args.max_trips, args.speed)
