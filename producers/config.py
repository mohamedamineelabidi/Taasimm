"""
TaaSim Kafka Producer Configuration
====================================
Shared constants, bounding-box transform, zone mapping loader,
and H3 hexagonal zone assignment used by both producers.
"""

import csv
import json
import os
import numpy as np
import h3

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
# /data is the container mount; fall back to ../data for local dev
_local_data = os.path.join(os.path.dirname(__file__), "..", "data")
DATA_DIR = "/data" if os.path.isdir("/data") else _local_data
ZONE_MAPPING_PATH = os.path.join(DATA_DIR, "zone_mapping.csv")
PORTO_CSV_PATH = os.path.join(DATA_DIR, "train.csv")
H3_LOOKUP_PATH = os.path.join(DATA_DIR, "h3_zone_lookup.json")
H3_RESOLUTION = 9


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


# ── H3 Hexagonal Zone Assignment ────────────────────────────────────

def load_h3_lookup(path=None):
    """Load h3_zone_lookup.json → dict {h3_index: {"zone_id": int, "name": str}}."""
    p = path or H3_LOOKUP_PATH
    with open(p, "r", encoding="utf-8") as f:
        return json.load(f)


def assign_h3_zone(lat, lon, h3_lookup, resolution=H3_RESOLUTION, max_rings=5):
    """O(1) H3 zone lookup with grid_ring fallback.

    Returns (zone_id, zone_name, h3_index).
    Falls back to expanding rings if the exact cell isn't in the lookup.
    Returns (0, "Outside", h3_index) if no zone found within max_rings.
    """
    h3_cell = h3.latlng_to_cell(lat, lon, resolution)
    if h3_cell in h3_lookup:
        info = h3_lookup[h3_cell]
        return info["zone_id"], info["name"], h3_cell
    for k in range(1, max_rings + 1):
        for nb in h3.grid_ring(h3_cell, k):
            if nb in h3_lookup:
                info = h3_lookup[nb]
                return info["zone_id"], info["name"], h3_cell
    return 0, "Outside", h3_cell


# ── Road Snapping ────────────────────────────────────────────────────

ROAD_NODES_PATH = os.path.join(DATA_DIR, "casablanca_road_nodes.npy")

_road_tree = None
_road_arr = None
_h3_lookup_cache = None


def load_road_tree():
    """Load KDTree of Casablanca road nodes for snap_to_road(). Cached after first call.
    Returns (None, None) if casablanca_road_nodes.npy has not been generated yet.
    Run notebooks/03_h3_zone_remapping.ipynb to generate it.
    """
    global _road_tree, _road_arr
    if _road_tree is None:
        if not os.path.exists(ROAD_NODES_PATH):
            return None, None
        from scipy.spatial import cKDTree
        _road_arr = np.load(ROAD_NODES_PATH)
        _road_tree = cKDTree(_road_arr)
    return _road_tree, _road_arr


def _get_h3_lookup_cached():
    """Load h3 lookup once for strict land validation during snapping."""
    global _h3_lookup_cache
    if _h3_lookup_cache is None:
        _h3_lookup_cache = load_h3_lookup()
    return _h3_lookup_cache


def snap_to_road(lat, lon, h3_lookup=None, max_dist_deg=0.003):
    """Strict snap to nearest road node with distance + land validation.

    Returns (snapped_lat, snapped_lon, snap_dist_m, snapped_valid).
    - max_dist_deg=0.003 (~333m), aligned with notebook strict filtering.
    - snapped_valid=False when point is too far from roads or outside land zones.
    - Falls back to (lat, lon, 0.0, True) when road nodes file is unavailable.
    """
    tree, arr = load_road_tree()
    if tree is None:
        # Road nodes not generated yet — pass through without snapping
        return lat, lon, 0.0, True
    dist, idx = tree.query([lat, lon])
    snap_dist_m = float(dist * 111_000)
    if dist > max_dist_deg:
        return None, None, snap_dist_m, False

    snapped_lat, snapped_lon = float(arr[idx][0]), float(arr[idx][1])
    lookup = h3_lookup if h3_lookup is not None else _get_h3_lookup_cached()
    zone_id, _, _ = assign_h3_zone(snapped_lat, snapped_lon, lookup)
    if zone_id == 0:
        return None, None, snap_dist_m, False

    return snapped_lat, snapped_lon, snap_dist_m, True
