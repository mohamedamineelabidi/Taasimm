"""
TaaSim — Trip Request Producer
================================
Generates simulated taxi trip-request events and publishes them to Kafka
topic ``raw.trips``.  The emission rate follows Porto's hourly demand curve
with peak multipliers at morning (7–9h) and evening (17–19h) rush hours.

Features:
- Hourly demand multiplier (derived from Porto EDA)
- Peak hours 7–9, 17–19 at 3–5× base rate
- Friday 12–14h reduced rate (0.6×)
- Random origin/destination zone from zone_mapping.csv
- Call type distribution: B=48%, C=40%, A=12% (matching Porto)

Usage:
    python trip_request_producer.py [--max-trips 500] [--base-rate 2.0]
"""

import argparse
import json
import logging
import os
import random
import sys
import time
import uuid
from datetime import datetime, timezone

from kafka import KafkaProducer

from config import (
    KAFKA_BOOTSTRAP,
    TOPIC_TRIPS,
    PHASE4_TRIPS_PARQUET,
    load_zone_mapping,
    assign_zone,
    load_h3_lookup,
    assign_h3_zone,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [TRIP] %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)

# ── Hourly demand multiplier (index = hour 0..23) ────────────────────
# Derived from Porto EDA: peak at 8-9 AM and 17-19 PM
HOURLY_MULTIPLIER = [
    0.3,  # 00
    0.2,  # 01
    0.15, # 02
    0.1,  # 03
    0.1,  # 04
    0.2,  # 05
    0.5,  # 06
    3.0,  # 07  ← morning rush start
    5.0,  # 08  ← morning peak
    4.0,  # 09  ← morning rush end
    2.0,  # 10
    1.5,  # 11
    1.2,  # 12
    1.0,  # 13
    1.0,  # 14
    1.5,  # 15
    2.5,  # 16
    4.5,  # 17  ← evening rush start
    5.0,  # 18  ← evening peak
    3.5,  # 19  ← evening rush end
    2.0,  # 20
    1.5,  # 21
    1.0,  # 22
    0.5,  # 23
]

# Call type distribution (Porto: B=48%, C=40%, A=12%)
CALL_TYPE_WEIGHTS = {"A": 0.12, "B": 0.48, "C": 0.40}


def pick_call_type():
    """Weighted random call type."""
    r = random.random()
    if r < 0.12:
        return "A"
    elif r < 0.60:
        return "B"
    else:
        return "C"


def create_producer():
    """Create Kafka producer with JSON serializer."""
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        key_serializer=lambda k: k.encode("utf-8"),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
        retries=3,
    )


def get_demand_multiplier(hour, weekday):
    """Get demand multiplier for given hour and weekday (0=Mon, 6=Sun)."""
    mult = HOURLY_MULTIPLIER[hour]
    # Friday 12–14h: reduced rate
    if weekday == 4 and 12 <= hour <= 13:
        mult *= 0.6
    return mult


def run(max_trips, base_rate):
    """Main producer loop."""
    zones = load_zone_mapping()
    if not zones:
        log.error("No zones loaded from zone_mapping.csv")
        sys.exit(1)
    log.info("Loaded %d zones", len(zones))

    h3_lookup = load_h3_lookup()
    log.info("Loaded H3 lookup: %d cells", len(h3_lookup))

    producer = create_producer()
    log.info("Connected to Kafka at %s", KAFKA_BOOTSTRAP)
    log.info("Topic: %s | Base rate: %.1f trips/sec | Max: %s",
             TOPIC_TRIPS, base_rate, max_trips or "unlimited")

    sent = 0
    rider_pool_size = 5000  # simulated rider pool

    try:
        while True:
            now = datetime.now(timezone.utc)
            hour = now.hour
            weekday = now.weekday()
            multiplier = get_demand_multiplier(hour, weekday)

            # Inter-arrival time: base_rate * multiplier events per second
            effective_rate = base_rate * multiplier
            sleep_time = 1.0 / effective_rate if effective_rate > 0 else 1.0

            # Pick random origin and destination zones (different)
            origin_zone = random.choice(zones)
            dest_zone = random.choice(zones)
            while dest_zone["zone_id"] == origin_zone["zone_id"] and len(zones) > 1:
                dest_zone = random.choice(zones)

            trip_id = str(uuid.uuid4())
            rider_id = f"rider_{random.randint(1, rider_pool_size):05d}"

            # Randomize coordinates within zone bounds (not always centroid)
            o_lat = random.uniform(origin_zone["lat_min"], origin_zone["lat_max"])
            o_lon = random.uniform(origin_zone["lon_min"], origin_zone["lon_max"])
            d_lat = random.uniform(dest_zone["lat_min"], dest_zone["lat_max"])
            d_lon = random.uniform(dest_zone["lon_min"], dest_zone["lon_max"])
            _, _, origin_h3 = assign_h3_zone(o_lat, o_lon, h3_lookup)
            _, _, dest_h3 = assign_h3_zone(d_lat, d_lon, h3_lookup)

            event = {
                "trip_id": trip_id,
                "rider_id": rider_id,
                "origin_zone": origin_zone["zone_id"],
                "origin_lat": round(o_lat, 6),
                "origin_lon": round(o_lon, 6),
                "origin_h3": origin_h3,
                "destination_zone": dest_zone["zone_id"],
                "dest_lat": round(d_lat, 6),
                "dest_lon": round(d_lon, 6),
                "dest_h3": dest_h3,
                "event_time": now.isoformat(),
                "call_type": pick_call_type(),
            }

            producer.send(TOPIC_TRIPS, key=rider_id, value=event)
            sent += 1

            if sent % 100 == 0:
                log.info("Sent %d trip requests (rate=%.1f/s, hour=%d, mult=%.1fx)",
                         sent, effective_rate, hour, multiplier)

            if max_trips and sent >= max_trips:
                break

            time.sleep(sleep_time)

    except KeyboardInterrupt:
        log.info("Interrupted by user")
    finally:
        producer.flush()
        producer.close()
        log.info("=== DONE === Sent %d trip request events", sent)


def run_from_parquet(parquet_path, max_trips, speed):
    """Replay Phase-4 Casa synth trip requests at wall-clock rebased cadence.

    Reads casa_trip_requests.parquet (500k rows, 90 days), sorts by request_time,
    rebases offset = now - min(request_time), and sleeps between events to reproduce
    the 08/13/19 Casa demand peaks at `speed`x real time.
    """
    import pandas as pd

    if not os.path.exists(parquet_path):
        log.error("Parquet not found: %s", parquet_path)
        sys.exit(1)
    log.info("Loading %s", parquet_path)
    df = pd.read_parquet(parquet_path)
    df = df.sort_values("request_time").reset_index(drop=True)
    if max_trips:
        df = df.head(max_trips)
    log.info("Loaded %d rows (%s -> %s)", len(df), df["request_time"].min(), df["request_time"].max())

    zones = {z["zone_id"]: z for z in load_zone_mapping()}
    h3_lookup = load_h3_lookup()

    # Wall-clock rebase: make first trip fire ~now
    t0_src = df["request_time"].iloc[0].to_pydatetime()
    t0_wall = datetime.now(timezone.utc)
    log.info("Rebase: src=%s -> wall=%s (speed=%.1fx)", t0_src, t0_wall, speed)

    producer = create_producer()
    log.info("Connected to Kafka at %s | topic=%s", KAFKA_BOOTSTRAP, TOPIC_TRIPS)

    sent = 0
    rider_pool = 5000
    try:
        for row in df.itertuples(index=False):
            # Scheduled wall-clock emit time
            dt_src = (row.request_time.to_pydatetime() - t0_src).total_seconds()
            wall_target = t0_wall.timestamp() + dt_src / speed
            lag = wall_target - datetime.now(timezone.utc).timestamp()
            if lag > 0:
                time.sleep(lag)

            o = zones.get(int(row.origin_zone_id))
            d = zones.get(int(row.dest_zone_id))
            if o is None or d is None:
                continue
            o_lat = random.uniform(o["lat_min"], o["lat_max"])
            o_lon = random.uniform(o["lon_min"], o["lon_max"])
            d_lat = random.uniform(d["lat_min"], d["lat_max"])
            d_lon = random.uniform(d["lon_min"], d["lon_max"])
            _, _, o_h3 = assign_h3_zone(o_lat, o_lon, h3_lookup)
            _, _, d_h3 = assign_h3_zone(d_lat, d_lon, h3_lookup)

            now = datetime.now(timezone.utc)
            rider_id = f"rider_{random.randint(1, rider_pool):05d}"
            event = {
                "trip_id": str(row.trip_id),
                "rider_id": rider_id,
                "origin_zone": int(row.origin_zone_id),
                "origin_zone_name": str(row.origin_zone_name),
                "origin_class": str(row.origin_class),
                "origin_lat": round(o_lat, 6),
                "origin_lon": round(o_lon, 6),
                "origin_h3": o_h3,
                "destination_zone": int(row.dest_zone_id),
                "dest_zone_name": str(row.dest_zone_name),
                "dest_class": str(row.dest_class),
                "dest_lat": round(d_lat, 6),
                "dest_lon": round(d_lon, 6),
                "dest_h3": d_h3,
                "passenger_count": int(row.passenger_count),
                "distance_km": float(row.distance_km),
                "duration_s": int(row.duration_s),
                "fleet_type": str(row.fleet_type),
                "fare_mad": float(row.fare_mad),
                "call_type": str(row.call_type),
                "event_time": now.isoformat(),
                "source": "casa_synth_v1",
            }
            producer.send(TOPIC_TRIPS, key=rider_id, value=event)
            sent += 1
            if sent % 100 == 0:
                log.info("Sent %d trips (hour=%d, last_zone=%d->%s, fare=%.1f MAD, %s)",
                         sent, now.hour, int(row.origin_zone_id),
                         int(row.dest_zone_id), float(row.fare_mad), str(row.fleet_type))
    except KeyboardInterrupt:
        log.info("Interrupted by user")
    finally:
        producer.flush()
        producer.close()
        log.info("=== DONE === Replayed %d trip request events from parquet", sent)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="TaaSim Trip Request Producer")
    parser.add_argument("--source", choices=["casa_synth", "random"], default="casa_synth",
                        help="casa_synth: replay Phase-4 parquet (default). random: legacy synthetic.")
    parser.add_argument("--max-trips", type=int, default=None,
                        help="Max trip requests to emit (default: run through all rows)")
    parser.add_argument("--base-rate", type=float, default=2.0,
                        help="[random mode] Base trip request rate in events/sec (default: 2.0)")
    parser.add_argument("--speed", type=float, default=10.0,
                        help="[casa_synth mode] Replay speed multiplier (default: 10x)")
    parser.add_argument("--parquet", default=PHASE4_TRIPS_PARQUET,
                        help="[casa_synth mode] Path to trip-requests parquet")
    args = parser.parse_args()
    if args.source == "casa_synth":
        run_from_parquet(args.parquet, args.max_trips, args.speed)
    else:
        run(args.max_trips, args.base_rate)
