"""
TaaSim — Offline Trajectory Projector
=======================================
Processes Porto taxi trips in batches and projects them onto Casablanca roads
using:
  1. Displacement-preserving trajectory adaptation (turn angles, step distances)
  2. Road-network map matching (nearest-node + shortest-path reconstruction)
  3. Zone + H3 enrichment

Output: data/curated_trajectories.parquet — replay-ready for vehicle_gps_producer.py

Prerequisites:
  - Run scripts/generate_road_assets.py first (polygon, graph, road nodes)
  - Porto train.csv in data/

Usage:
    python scripts/offline_projector.py [--max-trips 50000] [--batch-size 500]
"""

import argparse
import csv
import json
import logging
import math
import os
import sys
import time

import numpy as np

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from producers.config import (
    PORTO_LAT_MIN, PORTO_LAT_MAX, PORTO_LON_MIN, PORTO_LON_MAX,
    CASA_LAT_MIN, CASA_LAT_MAX, CASA_LON_MIN, CASA_LON_MAX,
    PORTO_CSV_PATH, ZONE_MAPPING_PATH, DATA_DIR,
    H3_RESOLUTION,
    is_in_porto_metro, load_zone_mapping,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [PROJ] %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)

# ── Paths ────────────────────────────────────────────────────────────
POLYGON_PATH = os.path.join(DATA_DIR, "casablanca_polygon.geojson")
GRAPH_PATH = os.path.join(DATA_DIR, "casablanca_road_graph.graphml")
OUTPUT_PARQUET = os.path.join(DATA_DIR, "curated_trajectories.parquet")
CHECKPOINT_PATH = os.path.join(DATA_DIR, ".projector_checkpoint.json")

# ── Constants ────────────────────────────────────────────────────────
NOISE_SIGMA_DEG = 0.0001   # Small noise for adaptation (~11m)
GPS_INTERVAL_S = 15         # Porto GPS interval in seconds
MAX_SPEED_KMH = 150         # Speed filter threshold
SNAP_MAX_DIST_DEG = 0.003   # ~333m max snap distance


def parse_polyline(polyline_str):
    """Parse POLYLINE JSON string -> list of (lon, lat) tuples."""
    try:
        coords = json.loads(polyline_str)
        if isinstance(coords, list) and len(coords) >= 2:
            return [(c[0], c[1]) for c in coords]
    except (json.JSONDecodeError, TypeError):
        pass
    return None


def validate_trajectory(points):
    """Filter out anomalous GPS points (excessive speed)."""
    if len(points) < 2:
        return points, 0
    cleaned = [points[0]]
    dropped = 0
    for i in range(1, len(points)):
        lon0, lat0 = cleaned[-1]
        lon1, lat1 = points[i]
        dlat = math.radians(lat1 - lat0)
        dlon = math.radians(lon1 - lon0)
        a = (math.sin(dlat / 2) ** 2 +
             math.cos(math.radians(lat0)) * math.cos(math.radians(lat1)) *
             math.sin(dlon / 2) ** 2)
        dist_km = 6371 * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
        speed_kmh = dist_km / (GPS_INTERVAL_S / 3600) if GPS_INTERVAL_S > 0 else 0
        if speed_kmh <= MAX_SPEED_KMH:
            cleaned.append(points[i])
        else:
            dropped += 1
    return cleaned, dropped


def load_assets():
    """Load polygon, road graph, and zone mapping."""
    import osmnx as ox
    from shapely.geometry import shape

    if not os.path.exists(POLYGON_PATH):
        raise FileNotFoundError(
            f"Missing {POLYGON_PATH}. Run scripts/generate_road_assets.py first.")
    if not os.path.exists(GRAPH_PATH):
        raise FileNotFoundError(
            f"Missing {GRAPH_PATH}. Run scripts/generate_road_assets.py first.")

    with open(POLYGON_PATH) as f:
        polygon = shape(json.load(f)["geometry"])

    G = ox.load_graphml(GRAPH_PATH)
    log.info("Loaded road graph: %d nodes, %d edges",
             G.number_of_nodes(), G.number_of_edges())

    zones = load_zone_mapping()
    log.info("Loaded %d zones", len(zones))

    return polygon, G, zones


def compute_scale_factors(polygon):
    """Compute displacement scale factors from polygon bounds."""
    porto_lat_span = PORTO_LAT_MAX - PORTO_LAT_MIN
    porto_lon_span = abs(PORTO_LON_MAX - PORTO_LON_MIN)
    b = polygon.bounds  # (minx, miny, maxx, maxy)
    casa_lat_span = b[3] - b[1]
    casa_lon_span = b[2] - b[0]
    lat_scale = casa_lat_span / porto_lat_span
    lon_scale = casa_lon_span / porto_lon_span
    log.info("Scale factors: lat=%.3f, lon=%.3f", lat_scale, lon_scale)
    return lat_scale, lon_scale


def compute_zone_weights(zones):
    """Compute sampling weights per zone.

    Uses area^0.25 (4th root) to aggressively dampen the dominance of
    large central zones where road-network routing naturally concentrates
    traffic. Combined with nearest-centroid fallback in zone assignment,
    this achieves near-uniform distribution across all 16 zones.
    """
    weights = {}
    for z in zones:
        area = (z["lat_max"] - z["lat_min"]) * (z["lon_max"] - z["lon_min"])
        weights[z["zone_id"]] = area ** 0.25
    total = sum(weights.values())
    return {k: v / total for k, v in weights.items()}


def sample_start(zones, weights, polygon, rng):
    """Sample a start point from weighted zone distribution."""
    from shapely.geometry import Point
    zone_ids = list(weights.keys())
    probs = np.array([weights[z] for z in zone_ids])
    probs = probs / probs.sum()

    for _ in range(10):
        zid = rng.choice(zone_ids, p=probs)
        z = next(z for z in zones if z["zone_id"] == zid)
        lat = z["centroid_lat"] + rng.normal(0, 0.003)
        lon = z["centroid_lon"] + rng.normal(0, 0.003)
        if polygon.contains(Point(lon, lat)):
            return lon, lat

    c = polygon.centroid
    return c.x, c.y


def adapt_trajectory(porto_points, zones, weights, polygon, rng,
                     lat_scale, lon_scale):
    """Adapt Porto trajectory to Casablanca geography.

    Preserves: turn angles, step distances (scaled), trajectory length.
    Adapted: start position weighted by zone, polygon containment enforced.
    """
    from shapely.geometry import Point
    if len(porto_points) < 2:
        return []

    displacements = [
        ((porto_points[i][0] - porto_points[i - 1][0]) * lon_scale,
         (porto_points[i][1] - porto_points[i - 1][1]) * lat_scale)
        for i in range(1, len(porto_points))
    ]

    start_lon, start_lat = sample_start(zones, weights, polygon, rng)
    result = [(start_lon, start_lat)]
    cur_lon, cur_lat = start_lon, start_lat
    minx, miny, maxx, maxy = polygon.bounds

    for dlon, dlat in displacements:
        new_lon = cur_lon + dlon + rng.normal(0, NOISE_SIGMA_DEG)
        new_lat = cur_lat + dlat + rng.normal(0, NOISE_SIGMA_DEG)

        # Reflect if outside polygon bounds
        if not (minx <= new_lon <= maxx):
            new_lon = cur_lon - dlon
        if not (miny <= new_lat <= maxy):
            new_lat = cur_lat - dlat

        # Polygon containment: stay in place if outside
        if not polygon.contains(Point(new_lon, new_lat)):
            new_lon, new_lat = cur_lon, cur_lat

        cur_lon, cur_lat = new_lon, new_lat
        result.append((cur_lon, cur_lat))
    return result


def map_match_trajectory(points, G):
    """Snap trajectory to road network using nearest-node + shortest-path.

    Returns list of (lon, lat) on real Casablanca roads.
    """
    import osmnx as ox
    import networkx as nx

    if len(points) < 2:
        return points

    lons = [p[0] for p in points]
    lats = [p[1] for p in points]
    node_ids = ox.distance.nearest_nodes(G, lons, lats)

    matched = []
    prev_node = None
    for node_id in node_ids:
        nd = G.nodes[node_id]
        if prev_node is not None and prev_node != node_id:
            try:
                path = nx.shortest_path(G, prev_node, node_id, weight="length")
                for pn in path[1:]:
                    pnd = G.nodes[pn]
                    matched.append((pnd["x"], pnd["y"]))
            except nx.NetworkXNoPath:
                # Fallback: direct snap to target node
                matched.append((nd["x"], nd["y"]))
        else:
            matched.append((nd["x"], nd["y"]))
        prev_node = node_id
    return matched


def assign_zone_simple(lat, lon, zones):
    """Assign zone by bounding box. Returns (zone_id, zone_name)."""
    for z in zones:
        if (z["lat_min"] <= lat <= z["lat_max"] and
                z["lon_min"] <= lon <= z["lon_max"]):
            return z["zone_id"], z["name"]
    # Nearest centroid fallback
    min_dist = float("inf")
    best = (0, "Outside")
    for z in zones:
        d = (lat - z["centroid_lat"]) ** 2 + (lon - z["centroid_lon"]) ** 2
        if d < min_dist:
            min_dist = d
            best = (z["zone_id"], z["name"])
    return best


def process_trip(trip_id, polyline_str, zones, weights, polygon, G,
                 lat_scale, lon_scale, rng):
    """Full pipeline for one Porto trip -> curated trajectory rows."""
    porto_pts = parse_polyline(polyline_str)
    if not porto_pts:
        return None, "parse_fail"

    # Filter to Porto metro
    porto_pts = [(lon, lat) for lon, lat in porto_pts
                 if is_in_porto_metro(lat, lon)]
    if len(porto_pts) < 2:
        return None, "too_short"

    porto_pts, dropped = validate_trajectory(porto_pts)
    if len(porto_pts) < 2:
        return None, "speed_filter"

    casa_pts = adapt_trajectory(porto_pts, zones, weights, polygon, rng,
                                lat_scale, lon_scale)
    if len(casa_pts) < 2:
        return None, "adapt_fail"

    matched = map_match_trajectory(casa_pts, G)
    if not matched or len(matched) < 2:
        return None, "match_fail"

    # Build output rows
    rows = []
    for i, (lon, lat) in enumerate(matched):
        zone_id, zone_name = assign_zone_simple(lat, lon, zones)
        status = "pickup" if i == 0 else ("dropoff" if i == len(matched) - 1 else "moving")
        rows.append({
            "trip_id": trip_id,
            "seq": i,
            "lat": round(lat, 7),
            "lon": round(lon, 7),
            "offset_s": i * GPS_INTERVAL_S,
            "zone_id": zone_id,
            "zone_name": zone_name,
            "status": status,
            "snap_method": "map_match",
        })
    return rows, "ok"


def save_checkpoint(trip_count, row_offset):
    """Save progress checkpoint for resumability."""
    with open(CHECKPOINT_PATH, "w") as f:
        json.dump({"trip_count": trip_count, "row_offset": row_offset}, f)


def load_checkpoint():
    """Load progress checkpoint if exists."""
    if os.path.exists(CHECKPOINT_PATH):
        with open(CHECKPOINT_PATH) as f:
            return json.load(f)
    return None


def run(max_trips, batch_size, resume=True):
    """Main offline projection loop."""
    polygon, G, zones = load_assets()
    lat_scale, lon_scale = compute_scale_factors(polygon)
    weights = compute_zone_weights(zones)
    rng = np.random.default_rng(seed=42)

    # Check for checkpoint
    start_row = 0
    existing_rows = []
    if resume:
        ckpt = load_checkpoint()
        if ckpt:
            start_row = ckpt["row_offset"]
            log.info("Resuming from row %d (trip %d)", start_row, ckpt["trip_count"])
            if os.path.exists(OUTPUT_PARQUET):
                import pandas as pd
                existing_rows = pd.read_parquet(OUTPUT_PARQUET).to_dict("records")
                log.info("Loaded %d existing rows", len(existing_rows))

    all_rows = existing_rows
    trip_count = 0
    processed = 0
    counters = {"ok": 0, "parse_fail": 0, "too_short": 0,
                "speed_filter": 0, "adapt_fail": 0, "match_fail": 0}

    t0 = time.time()
    with open(PORTO_CSV_PATH, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row_idx, row in enumerate(reader):
            if row_idx < start_row:
                continue

            if str(row.get("MISSING_DATA", "")).strip().upper() == "TRUE":
                continue

            trip_id = row.get("TRIP_ID", str(row_idx))
            polyline = row.get("POLYLINE", "[]")

            rows, status = process_trip(
                trip_id, polyline, zones, weights, polygon, G,
                lat_scale, lon_scale, rng
            )
            counters[status] += 1

            if rows:
                all_rows.extend(rows)
                processed += 1

            trip_count += 1

            if trip_count % batch_size == 0:
                elapsed = time.time() - t0
                rate = trip_count / elapsed if elapsed > 0 else 0
                log.info("Processed %d trips (%d ok, %.1f trips/s) | rows=%d",
                         trip_count, processed, rate, len(all_rows))
                save_checkpoint(trip_count, row_idx + 1)

            if max_trips and trip_count >= max_trips:
                break

    # Write final output
    import pandas as pd
    df = pd.DataFrame(all_rows)
    df.to_parquet(OUTPUT_PARQUET, index=False)

    elapsed = time.time() - t0
    log.info("=" * 60)
    log.info("DONE: %d trips -> %d ok -> %d rows in %.1fs",
             trip_count, processed, len(df), elapsed)
    log.info("Counters: %s", counters)
    log.info("Output: %s (%.1f MB)", OUTPUT_PARQUET,
             os.path.getsize(OUTPUT_PARQUET) / 1e6)
    log.info("=" * 60)

    # Clean up checkpoint
    if os.path.exists(CHECKPOINT_PATH):
        os.remove(CHECKPOINT_PATH)

    return df


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="TaaSim Offline Trajectory Projector")
    parser.add_argument("--max-trips", type=int, default=5000,
                        help="Max number of Porto trips to project (default: 5000)")
    parser.add_argument("--batch-size", type=int, default=100,
                        help="Progress checkpoint interval (default: 100)")
    parser.add_argument("--no-resume", action="store_true",
                        help="Start fresh, ignore any checkpoint")
    args = parser.parse_args()

    run(args.max_trips, args.batch_size, resume=not args.no_resume)
