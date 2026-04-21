"""
TaaSim — Vehicle GPS Producer
================================
Replays taxi trajectories at configurable speed (default 10×) and publishes
to Kafka topic ``raw.gps``.

Modes:
- **live** (default): Read Porto CSVs, apply linear bbox transform + noise + road snap
- **curated**: Replay pre-projected road-matched trajectories from Parquet

Features:
- Porto outlier filter (only metro-area trips) [live mode]
- Road-constrained trajectories with map matching [curated mode]
- 5% GPS blackout probability (60–180s delay → out-of-order events)
- Kafka key = taxi_id (for partition affinity)

Usage:
    python vehicle_gps_producer.py [--mode live|curated] [--max-trips 1000] [--speed 10]
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
    TOPIC_GPS,
    PORTO_CSV_PATH,
    DATA_DIR,
    REPLAY_SPEED,
    BLACKOUT_PROB,
    BLACKOUT_DELAY,
    NOISE_SIGMA,
    PHASE3_TRAJ_PARQUET,
    PHASE4_TRIPS_PARQUET,
    GPS_TRAJ_INDEX_PATH,
    is_in_porto_metro,
    transform_to_casablanca,
    load_h3_lookup,
    assign_h3_zone,
    snap_to_road,
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
            # Rebase to current wall-clock time so Flink watermarks
            # and Cassandra TTLs work with real timestamps, not 2013.
            base_ts = int(time.time())

            # Determine if this vehicle has a GPS blackout
            has_blackout = random.random() < BLACKOUT_PROB
            blackout_delay = random.randint(*BLACKOUT_DELAY) if has_blackout else 0

            prev_lat, prev_lon = None, None
            for i, (lon, lat) in enumerate(polyline):
                if not is_in_porto_metro(lat, lon):
                    continue

                casa_lat, casa_lon = transform_to_casablanca(lat, lon)
                casa_lat, casa_lon, snap_dist_m, snapped_valid = snap_to_road(
                    casa_lat, casa_lon, h3_lookup=h3_lookup
                )
                if not snapped_valid:
                    continue
                zone_id, _, h3_cell = assign_h3_zone(
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
                    "event_time": datetime.fromtimestamp(
                        event_ts, tz=timezone.utc
                    ).isoformat(),
                    "lat": round(casa_lat, 6),
                    "lon": round(casa_lon, 6),
                    "speed_kmh": speed_kmh,
                    "status": status,
                    "h3_index": h3_cell,
                    "zone_id": zone_id,
                    "snap_dist_m": round(snap_dist_m, 2),
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


def run_curated(max_trips, speed, curated_path=None):
    """Replay curated (pre-projected, road-matched) trajectories from Parquet."""
    import os
    import pandas as pd

    path = curated_path or os.path.join(DATA_DIR, "curated_trajectories.parquet")
    if not os.path.exists(path):
        log.error("Curated trajectories not found: %s", path)
        log.error("Run scripts/offline_projector.py first.")
        sys.exit(1)

    df = pd.read_parquet(path)
    log.info("Loaded curated trajectories: %d rows from %s", len(df), path)

    h3_lookup = load_h3_lookup()
    producer = create_producer()
    log.info("Connected to Kafka at %s", KAFKA_BOOTSTRAP)
    log.info("Topic: %s | Speed: %dx | Mode: curated | Max trips: %s",
             TOPIC_GPS, speed, max_trips or "unlimited")

    sent = 0
    blackout_events = 0
    trip_count = 0
    interval = 15.0 / speed

    for trip_id, trip_df in df.groupby("trip_id", sort=False):
        trip_df = trip_df.sort_values("seq")
        taxi_id = f"taxi_{hash(trip_id) % 10000:04d}"
        base_ts = int(time.time())

        has_blackout = random.random() < BLACKOUT_PROB
        blackout_delay = random.randint(*BLACKOUT_DELAY) if has_blackout else 0

        prev_lat, prev_lon = None, None
        points = trip_df.to_dict("records")
        for i, pt in enumerate(points):
            lat = pt["lat"]
            lon = pt["lon"]
            zone_id = pt["zone_id"]

            if zone_id == 0:
                continue

            # Assign H3 cell
            _, _, h3_cell = assign_h3_zone(lat, lon, h3_lookup)
            event_ts = base_ts + pt.get("offset_s", i * 15)

            speed_kmh = 0.0
            if prev_lat is not None:
                speed_kmh = compute_speed(prev_lat, prev_lon, lat, lon, 15)
            prev_lat, prev_lon = lat, lon

            status = pt.get("status", "moving")

            event = {
                "taxi_id": taxi_id,
                "trip_id": str(trip_id),
                "event_time": datetime.fromtimestamp(
                    event_ts, tz=timezone.utc
                ).isoformat(),
                "lat": round(lat, 6),
                "lon": round(lon, 6),
                "speed_kmh": speed_kmh,
                "status": status,
                "h3_index": h3_cell,
                "zone_id": zone_id,
                "snap_dist_m": 0.0,
            }

            if has_blackout and i > 0:
                time.sleep(blackout_delay / speed)
                blackout_events += 1
                has_blackout = False

            producer.send(TOPIC_GPS, key=taxi_id, value=event)
            sent += 1

            if sent % 500 == 0:
                log.info("Sent %d GPS events (%d trips, %d blackouts)",
                         sent, trip_count, blackout_events)

            time.sleep(interval)

        trip_count += 1
        if max_trips and trip_count >= max_trips:
            break

    producer.flush()
    producer.close()
    log.info("=== DONE === Sent %d events from %d curated trips (%d blackouts)",
             sent, trip_count, blackout_events)


# ─────────────────────────────────────────────────────────────────────
# Coupled mode: Phase 4 trips drive timing, Phase 3 polylines drive geometry
# ─────────────────────────────────────────────────────────────────────

def _parse_wkt_linestring(wkt: str):
    """Parse 'LINESTRING (lon lat, lon lat, ...)' -> list of (lat, lon)."""
    inner = wkt[wkt.index("(") + 1: wkt.rindex(")")]
    out = []
    for pair in inner.split(","):
        lon, lat = pair.strip().split()
        out.append((float(lat), float(lon)))
    return out


def _pick_trajectory(idx, origin_zone_id, dest_zone_id, origin_class, dest_class):
    """Zone-pair match -> tier-pair fallback -> random."""
    zp = f"{origin_zone_id}-{dest_zone_id}"
    cands = idx["by_zone_pair"].get(zp)
    if cands:
        return random.choice(cands)
    tp = f"{origin_class}-{dest_class}"
    cands = idx["by_tier_pair"].get(tp)
    if cands:
        return random.choice(cands)
    return random.choice(list(idx["trajectories"].keys()))


def run_coupled(max_trips, speed, ping_interval_s, fleet_size):
    """Coupled replay: Phase-4 trip requests + Phase-3 OSRM polylines.

    Each Phase-4 trip is matched to a Phase-3 route by (origin_zone, dest_zone)
    with tier-pair fallback. The polyline is played back as 4-second GPS pings
    onto topic raw.gps. Taxi ids cycle through a bounded pool of `fleet_size`.
    Heap-driven scheduler keeps many trips in flight concurrently at `speed`x.
    """
    import heapq
    import math
    import numpy as np
    import pandas as pd

    if not os.path.exists(PHASE4_TRIPS_PARQUET):
        log.error("Missing %s — run Phase 4 notebook first", PHASE4_TRIPS_PARQUET)
        sys.exit(1)
    if not os.path.exists(GPS_TRAJ_INDEX_PATH):
        log.error("Missing %s — run scripts/build_trajectory_index.py first",
                  GPS_TRAJ_INDEX_PATH)
        sys.exit(1)

    log.info("Loading Phase-4 trips: %s", PHASE4_TRIPS_PARQUET)
    df = pd.read_parquet(PHASE4_TRIPS_PARQUET).sort_values("request_time").reset_index(drop=True)
    if max_trips:
        df = df.head(max_trips)
    log.info("Loaded %d trip requests", len(df))

    with open(GPS_TRAJ_INDEX_PATH, "r", encoding="utf-8") as f:
        idx = json.load(f)
    log.info("Loaded trajectory index: %d polylines, %d zone-pairs, %d tier-pairs",
             len(idx["trajectories"]), len(idx["by_zone_pair"]), len(idx["by_tier_pair"]))

    h3_lookup = load_h3_lookup()
    producer = create_producer()
    log.info("Connected to Kafka %s | topic=%s | speed=%sx | ping=%.1fs | fleet=%d",
             KAFKA_BOOTSTRAP, TOPIC_GPS, speed, ping_interval_s, fleet_size)

    # Wall-clock rebase aligned with trip_request_producer
    t0_src = df["request_time"].iloc[0].to_pydatetime()
    t0_wall = time.time()

    def src_to_wall(src_ts_epoch):
        dt = src_ts_epoch - t0_src.timestamp()
        return t0_wall + dt / speed

    # Fleet pool — round-robin taxi_ids
    fleet_cursor = [0]
    def next_taxi_id():
        tid = f"taxi_{fleet_cursor[0]:04d}"
        fleet_cursor[0] = (fleet_cursor[0] + 1) % fleet_size
        return tid

    # Heap: (wall_time, seq, taxi_id, trip_id, event_dict)
    heap: list = []
    counter = 0
    sent = 0
    trips_expanded = 0
    blackouts = 0

    def expand_trip(row):
        nonlocal counter, trips_expanded, blackouts
        tid = _pick_trajectory(idx, int(row.origin_zone_id), int(row.dest_zone_id),
                               str(row.origin_class), str(row.dest_class))
        traj = idx["trajectories"][tid]
        pts = _parse_wkt_linestring(traj["wkt"])
        if len(pts) < 2:
            return
        taxi = next_taxi_id()
        trip_id = str(row.trip_id)
        # Distribute pings uniformly across route duration (seconds in source time)
        total_dur_s = max(float(row.duration_s), float(traj["duration_s"]))
        n_pings = max(2, int(total_dur_s / ping_interval_s))
        # Interpolate polyline at n_pings equally-spaced positions (by point index)
        base_src = row.request_time.to_pydatetime().timestamp()
        has_blackout = random.random() < BLACKOUT_PROB
        blackout_at = random.randint(1, n_pings - 1) if has_blackout else -1
        blackout_delay_s = random.randint(*BLACKOUT_DELAY) if has_blackout else 0

        for k in range(n_pings):
            frac = k / (n_pings - 1) if n_pings > 1 else 0.0
            idx_f = frac * (len(pts) - 1)
            i0 = int(math.floor(idx_f))
            i1 = min(i0 + 1, len(pts) - 1)
            t = idx_f - i0
            lat = pts[i0][0] * (1 - t) + pts[i1][0] * t
            lon = pts[i0][1] * (1 - t) + pts[i1][1] * t
            # GPS noise ~20m
            lat += np.random.normal(0, NOISE_SIGMA)
            lon += np.random.normal(0, NOISE_SIGMA)

            src_ts = base_src + k * ping_interval_s
            wall_ts = src_to_wall(src_ts)
            if has_blackout and k >= blackout_at:
                wall_ts += blackout_delay_s / speed
                if k == blackout_at:
                    blackouts += 1

            status = "pickup" if k == 0 else ("dropoff" if k == n_pings - 1 else "moving")
            zone_id, _, h3_cell = assign_h3_zone(lat, lon, h3_lookup)
            event = {
                "taxi_id": taxi,
                "trip_id": trip_id,
                "event_time": datetime.fromtimestamp(src_ts + (k == 0) * 0, tz=timezone.utc).isoformat(),
                # ^ event_time uses the *source* time (rebased by Flink if needed);
                # wall_ts below drives producer scheduling only.
                "lat": round(lat, 6),
                "lon": round(lon, 6),
                "speed_kmh": 0.0,   # computed downstream if needed
                "status": status,
                "h3_index": h3_cell,
                "zone_id": zone_id,
                "snap_dist_m": 0.0,
                "source": "coupled_v1",
            }
            # Overwrite event_time with wall-clock so Flink watermarks advance now.
            event["event_time"] = datetime.fromtimestamp(wall_ts, tz=timezone.utc).isoformat()
            heapq.heappush(heap, (wall_ts, counter, taxi, trip_id, event))
            counter += 1
        trips_expanded += 1

    # Seed: expand all trips within first 5 minutes of source window, then top up as we drain
    LOOKAHEAD_SRC_S = 300.0
    trip_iter = iter(df.itertuples(index=False))
    pending_row = None

    def maybe_expand_more():
        nonlocal pending_row
        # Consume trips whose request_time is within lookahead of the latest scheduled wall_ts
        now_wall = time.time()
        horizon_src = t0_src.timestamp() + (now_wall - t0_wall) * speed + LOOKAHEAD_SRC_S
        while True:
            if pending_row is None:
                try:
                    pending_row = next(trip_iter)
                except StopIteration:
                    return False
            rts = pending_row.request_time.to_pydatetime().timestamp()
            if rts > horizon_src:
                return True
            expand_trip(pending_row)
            pending_row = None

    more = True
    try:
        while True:
            more = maybe_expand_more()
            if not heap:
                if not more:
                    break
                time.sleep(0.1)
                continue
            wall_ts, _, taxi, trip_id, event = heapq.heappop(heap)
            lag = wall_ts - time.time()
            if lag > 0:
                time.sleep(lag)
            producer.send(TOPIC_GPS, key=taxi, value=event)
            sent += 1
            if sent % 500 == 0:
                log.info("Sent %d GPS events | trips expanded=%d | heap=%d | blackouts=%d",
                         sent, trips_expanded, len(heap), blackouts)
    except KeyboardInterrupt:
        log.info("Interrupted by user")
    finally:
        producer.flush()
        producer.close()
        log.info("=== DONE === Sent %d GPS events from %d trips (%d blackouts)",
                 sent, trips_expanded, blackouts)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="TaaSim Vehicle GPS Producer")
    parser.add_argument("--mode", choices=["live", "curated", "coupled"], default="coupled",
                        help="live=Porto raw, curated=pre-projected, coupled=Phase4 trips + Phase3 routes (default)")
    parser.add_argument("--max-trips", type=int, default=None,
                        help="Max number of trips to replay (default: all)")
    parser.add_argument("--speed", type=float, default=float(REPLAY_SPEED),
                        help=f"Replay speed multiplier (default: {REPLAY_SPEED})")
    parser.add_argument("--curated-path", type=str, default=None,
                        help="Path to curated Parquet file (default: data/curated_trajectories.parquet)")
    parser.add_argument("--ping-interval", type=float, default=4.0,
                        help="[coupled] GPS ping cadence in seconds along each trip (default: 4s)")
    parser.add_argument("--fleet-size", type=int, default=500,
                        help="[coupled] Size of the rotating taxi_id pool (default: 500)")
    args = parser.parse_args()

    if args.mode == "coupled":
        run_coupled(args.max_trips, args.speed, args.ping_interval, args.fleet_size)
    elif args.mode == "curated":
        run_curated(args.max_trips, args.speed, args.curated_path)
    else:
        run(args.max_trips, args.speed)
