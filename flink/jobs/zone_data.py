"""Zone data helper — load zone_mapping.csv, H3 lookup, and provide zone assignment."""

import csv
import json
import os

# Paths inside the Flink container (mounted via docker-compose)
DEFAULT_ZONE_CSV = "/opt/flink/data/zone_mapping.csv"
DEFAULT_H3_JSON = "/opt/flink/data/h3_zone_lookup.json"


def load_zones(csv_path=None):
    """Load 16 Casablanca zones from CSV. Returns list of zone dicts."""
    path = csv_path or DEFAULT_ZONE_CSV
    zones = []
    with open(path, "r") as f:
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
                "adjacent_zones": row.get("adjacent_zones", ""),
            })
    return zones


def assign_zone(lat, lon, zones):
    """Assign a GPS coordinate to a zone via bounding-box lookup.
    Returns (zone_id, centroid_lat, centroid_lon) or None if outside all zones.
    """
    for z in zones:
        if z["lat_min"] <= lat <= z["lat_max"] and z["lon_min"] <= lon <= z["lon_max"]:
            return z["zone_id"], z["centroid_lat"], z["centroid_lon"]
    return None


# ── H3 Lookup ────────────────────────────────────────────────

def load_h3_lookup(json_path=None):
    """Load H3→zone lookup dict from JSON. Returns {h3_index: {zone_id, name}}."""
    path = json_path or DEFAULT_H3_JSON
    with open(path, "r") as f:
        return json.load(f)


def assign_h3_zone(lat, lon, h3_lookup, resolution=9, max_rings=5):
    """Assign GPS coordinate to zone via H3 O(1) lookup with ring fallback.
    Returns (zone_id, zone_name, h3_cell) or (0, 'unknown', h3_cell) if not found.
    """
    import h3
    cell = h3.latlng_to_cell(lat, lon, resolution)
    if cell in h3_lookup:
        info = h3_lookup[cell]
        return info["zone_id"], info["name"], cell
    for ring in range(1, max_rings + 1):
        for neighbor in h3.grid_ring(cell, ring):
            if neighbor in h3_lookup:
                info = h3_lookup[neighbor]
                return info["zone_id"], info["name"], cell
    return 0, "unknown", cell


# ── Validation ───────────────────────────────────────────────

CASA_LAT_MIN = 33.450
CASA_LAT_MAX = 33.680
CASA_LON_MIN = -7.720
CASA_LON_MAX = -7.480
MAX_SPEED_KMH = 150.0


def is_valid_gps(lat, lon, speed_kmh):
    """Validate GPS coordinates are within Casablanca bbox and speed is reasonable."""
    if lat < CASA_LAT_MIN or lat > CASA_LAT_MAX:
        return False
    if lon < CASA_LON_MIN or lon > CASA_LON_MAX:
        return False
    if speed_kmh is not None and speed_kmh > MAX_SPEED_KMH:
        return False
    return True
