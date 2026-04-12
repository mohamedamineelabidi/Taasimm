"""
TaaSim Kafka Producer Configuration
====================================
Shared constants, bounding-box transform, and zone mapping loader
used by both vehicle_gps_producer.py and trip_request_producer.py.
"""

import csv
import os
import numpy as np

# ── Kafka ────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC_GPS = "raw.gps"
TOPIC_TRIPS = "raw.trips"

# ── Porto bounding box (transform range — v5, tightened to P5–P90) ────────
PORTO_LAT_MIN, PORTO_LAT_MAX = 41.135, 41.174
PORTO_LON_MIN, PORTO_LON_MAX = -8.650, -8.585

PORTO_METRO_LAT = (41.10, 41.25)
PORTO_METRO_LON = (-8.72, -8.55)

# ── Casablanca bounding box (v2 — wider, matching Sprint1 spec) ──────
CASA_LAT_MIN, CASA_LAT_MAX = 33.450, 33.680
CASA_LON_MIN, CASA_LON_MAX = -7.720, -7.480

# ── GPS noise & blackout ─────────────────────────────────────────────
NOISE_SIGMA = 0.0002        # ~20m Gaussian noise
BLACKOUT_PROB = 0.05         # 5% chance of GPS blackout per vehicle
BLACKOUT_DELAY = (60, 180)   # delay range in seconds

# ── Replay speed ─────────────────────────────────────────────────────
REPLAY_SPEED = 10            # 10x real-time

# ── Zone mapping CSV path ────────────────────────────────────────────
DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")
ZONE_MAPPING_PATH = os.path.join(DATA_DIR, "zone_mapping.csv")
PORTO_CSV_PATH = os.path.join(DATA_DIR, "train.csv")


def transform_coord(val, src_min, src_max, dst_min, dst_max):
    """Linear bounding-box transform with edge clamping."""
    ratio = (val - src_min) / (src_max - src_min)
    ratio = np.clip(ratio, 0.0, 1.0)
    return ratio * (dst_max - dst_min) + dst_min


def transform_to_casablanca(lat, lon):
    """Transform a Porto GPS coordinate to Casablanca space + noise."""
    casa_lat = transform_coord(lat, PORTO_LAT_MIN, PORTO_LAT_MAX,
                               CASA_LAT_MIN, CASA_LAT_MAX)
    casa_lon = transform_coord(lon, PORTO_LON_MIN, PORTO_LON_MAX,
                               CASA_LON_MIN, CASA_LON_MAX)
    casa_lat += np.random.normal(0, NOISE_SIGMA)
    casa_lon += np.random.normal(0, NOISE_SIGMA)
    return float(casa_lat), float(casa_lon)


def is_in_porto_metro(lat, lon):
    """Check if a GPS point falls inside Porto metropolitan area."""
    return (PORTO_METRO_LAT[0] <= lat <= PORTO_METRO_LAT[1] and
            PORTO_METRO_LON[0] <= lon <= PORTO_METRO_LON[1])


def load_zone_mapping():
    """Load zone_mapping.csv and return list of zone dicts."""
    zones = []
    with open(ZONE_MAPPING_PATH, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            zones.append({
                "zone_id": int(row["zone_id"]),
                "name": row["arrondissement_name"],
                "lat_min": float(row["casa_lat_min"]),
                "lat_max": float(row["casa_lat_max"]),
                "lon_min": float(row["casa_lon_min"]),
                "lon_max": float(row["casa_lon_max"]),
                "centroid_lat": float(row["casa_centroid_lat"]),
                "centroid_lon": float(row["casa_centroid_lon"]),
            })
    return zones


def assign_zone(lat, lon, zones):
    """Assign a (lat, lon) to a zone. Returns (zone_id, zone_name)."""
    for z in zones:
        if (z["lat_min"] <= lat <= z["lat_max"] and
                z["lon_min"] <= lon <= z["lon_max"]):
            return z["zone_id"], z["name"]
    return 0, "Outside"
