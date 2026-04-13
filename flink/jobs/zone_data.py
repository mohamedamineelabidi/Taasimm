"""Zone data helper — load zone_mapping.csv and provide zone assignment + centroid lookup."""

import csv
import os

# Path inside the Flink container (mounted via docker-compose)
DEFAULT_ZONE_CSV = "/opt/flink/data/zone_mapping.csv"


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


# Casablanca bounding box for validation
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
