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
import random
import sys
import time
import uuid
from datetime import datetime, timezone

from kafka import KafkaProducer

from config import (
    KAFKA_BOOTSTRAP,
    TOPIC_TRIPS,
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
CALL_TYPES = ["A", "B", "B", "B", "B", "C", "C", "C", "C"]
# Weighted sampling: 4/9 B ≈ 44%, 4/9 C ≈ 44%, 1/9 A ≈ 11%
# Closer match via explicit weights:
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

            # Compute centroids and H3 cells for origin/destination
            o_lat = (origin_zone["lat_min"] + origin_zone["lat_max"]) / 2
            o_lon = (origin_zone["lon_min"] + origin_zone["lon_max"]) / 2
            d_lat = (dest_zone["lat_min"] + dest_zone["lat_max"]) / 2
            d_lon = (dest_zone["lon_min"] + dest_zone["lon_max"]) / 2
            _, _, origin_h3 = assign_h3_zone(o_lat, o_lon, h3_lookup)
            _, _, dest_h3 = assign_h3_zone(d_lat, d_lon, h3_lookup)

            event = {
                "trip_id": trip_id,
                "rider_id": rider_id,
                "origin_zone": origin_zone["zone_id"],
                "origin_zone_name": origin_zone["name"],
                "origin_lat": round(o_lat, 6),
                "origin_lon": round(o_lon, 6),
                "origin_h3": origin_h3,
                "destination_zone": dest_zone["zone_id"],
                "destination_zone_name": dest_zone["name"],
                "dest_lat": round(d_lat, 6),
                "dest_lon": round(d_lon, 6),
                "dest_h3": dest_h3,
                "requested_at": now.isoformat(),
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


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="TaaSim Trip Request Producer")
    parser.add_argument("--max-trips", type=int, default=None,
                        help="Max trip requests to generate (default: run forever)")
    parser.add_argument("--base-rate", type=float, default=2.0,
                        help="Base trip request rate in events/sec (default: 2.0)")
    args = parser.parse_args()
    run(args.max_trips, args.base_rate)
