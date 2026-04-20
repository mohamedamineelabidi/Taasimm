"""
TaaSim — Remapping Quality Gate
================================
Deterministic evaluator that reads transform constants from producers/config.py
(single source of truth) and reports Outside%, Clamped%, Gini with pass/fail gates.

Acceptance thresholds:
  Outside  <= 10%
  Clamped  <= 12%
  Gini     <= 0.45

Usage:
    python scripts/remap_quality_gate.py [--rows 100000] [--curated path/to/curated.parquet]

Exit code 0 = PASS, 1 = FAIL.
"""

import argparse
import csv
import json
import os
import sys

import numpy as np

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from producers.config import (
    PORTO_LAT_MIN, PORTO_LAT_MAX, PORTO_LON_MIN, PORTO_LON_MAX,
    CASA_LAT_MIN, CASA_LAT_MAX, CASA_LON_MIN, CASA_LON_MAX,
    PORTO_CSV_PATH,
    transform_coord, load_zone_mapping, is_in_porto_metro,
)

# ── Acceptance thresholds ────────────────────────────────────────────
GATE_OUTSIDE_MAX = 10.0   # %
GATE_CLAMPED_MAX = 12.0   # %
GATE_GINI_MAX    = 0.45


def gini_coefficient(values):
    """Compute Gini coefficient from an array of non-negative values."""
    vals = np.sort(np.asarray(values, dtype=float))
    n = len(vals)
    if n == 0 or vals.sum() == 0:
        return float("nan")
    cumsum = np.cumsum(vals)
    return 1.0 - 2.0 * cumsum.sum() / (n * vals.sum()) + 1.0 / n


def evaluate_live_transform(max_rows):
    """Evaluate quality of the current linear bounding-box transform."""
    zones = load_zone_mapping()

    lats, lons = [], []
    count = 0
    with open(PORTO_CSV_PATH, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            count += 1
            if count > max_rows:
                break
            if str(row.get("MISSING_DATA", "")).strip().upper() == "TRUE":
                continue
            try:
                coords = json.loads(row.get("POLYLINE", "[]"))
            except Exception:
                continue
            if not coords or len(coords) < 2:
                continue
            lon, lat = coords[0][0], coords[0][1]
            if not is_in_porto_metro(lat, lon):
                continue
            lats.append(lat)
            lons.append(lon)

    lats = np.array(lats)
    lons = np.array(lons)
    if len(lats) == 0:
        print("ERROR: No valid Porto trips found.")
        return False

    # Clamped = points outside Porto transform range
    clamped = np.sum(
        (lats < PORTO_LAT_MIN) | (lats > PORTO_LAT_MAX) |
        (lons < PORTO_LON_MIN) | (lons > PORTO_LON_MAX)
    )
    clamped_pct = clamped / len(lats) * 100

    # Transform to Casablanca
    casa_lat = transform_coord(lats, PORTO_LAT_MIN, PORTO_LAT_MAX,
                               CASA_LAT_MIN, CASA_LAT_MAX)
    casa_lon = transform_coord(lons, PORTO_LON_MIN, PORTO_LON_MAX,
                               CASA_LON_MIN, CASA_LON_MAX)

    # Assign zones
    zone_counts = {}
    outside = 0
    for lat, lon in zip(casa_lat, casa_lon):
        found = False
        for z in zones:
            if (z["lat_min"] <= lat <= z["lat_max"] and
                    z["lon_min"] <= lon <= z["lon_max"]):
                zone_counts[z["zone_id"]] = zone_counts.get(z["zone_id"], 0) + 1
                found = True
                break
        if not found:
            outside += 1

    outside_pct = outside / len(lats) * 100
    gini = gini_coefficient(list(zone_counts.values()))

    return _report(len(lats), clamped_pct, outside_pct, gini, zone_counts, "LIVE TRANSFORM")


def evaluate_curated(curated_path):
    """Evaluate quality of curated (offline-projected) trajectory data."""
    zones = load_zone_mapping()

    try:
        import pandas as pd
        df = pd.read_parquet(curated_path)
    except ImportError:
        print("ERROR: pandas + pyarrow required for --curated evaluation.")
        return False

    if "lat" not in df.columns or "lon" not in df.columns:
        print(f"ERROR: curated file missing lat/lon columns. Found: {list(df.columns)}")
        return False

    lats = df["lat"].values
    lons = df["lon"].values

    # For curated data, clamped = outside Casablanca bbox
    clamped = np.sum(
        (lats < CASA_LAT_MIN) | (lats > CASA_LAT_MAX) |
        (lons < CASA_LON_MIN) | (lons > CASA_LON_MAX)
    )
    clamped_pct = clamped / len(lats) * 100

    zone_counts = {}
    outside = 0
    for lat, lon in zip(lats, lons):
        found = False
        for z in zones:
            if (z["lat_min"] <= lat <= z["lat_max"] and
                    z["lon_min"] <= lon <= z["lon_max"]):
                zone_counts[z["zone_id"]] = zone_counts.get(z["zone_id"], 0) + 1
                found = True
                break
        if not found:
            outside += 1

    outside_pct = outside / len(lats) * 100
    gini = gini_coefficient(list(zone_counts.values()))

    return _report(len(lats), clamped_pct, outside_pct, gini, zone_counts, "CURATED OUTPUT")


def _report(n_samples, clamped_pct, outside_pct, gini, zone_counts, label):
    """Print quality report and return True if all gates pass."""
    print(f"\n{'=' * 60}")
    print(f"  REMAPPING QUALITY GATE — {label}")
    print(f"{'=' * 60}")
    print(f"  Samples          : {n_samples:,}")
    print(f"  Zones with events: {len(zone_counts)}/16")

    # Per-zone distribution
    print(f"\n  {'Zone':>6s}  {'Count':>8s}  {'Pct':>6s}  Bar")
    print(f"  {'-'*6}  {'-'*8}  {'-'*6}  {'-'*30}")
    total_inside = sum(zone_counts.values())
    for zid in sorted(zone_counts):
        cnt = zone_counts[zid]
        pct = cnt / total_inside * 100 if total_inside > 0 else 0
        bar = "#" * max(1, int(pct * 0.5))
        print(f"  Z{zid:4d}  {cnt:8,}  {pct:5.1f}%  {bar}")

    # Top-5 concentration
    top5 = sorted(zone_counts.items(), key=lambda kv: kv[1], reverse=True)[:5]
    top5_pct = sum(c for _, c in top5) / total_inside * 100 if total_inside > 0 else 0

    # Gates
    print(f"\n  {'Metric':<20s}  {'Value':>8s}  {'Gate':>8s}  {'Result':>8s}")
    print(f"  {'-'*20}  {'-'*8}  {'-'*8}  {'-'*8}")

    results = []

    ok = outside_pct <= GATE_OUTSIDE_MAX
    results.append(ok)
    tag = "PASS" if ok else "FAIL"
    print(f"  {'Outside %':<20s}  {outside_pct:7.2f}%  {GATE_OUTSIDE_MAX:7.1f}%  {tag:>8s}")

    ok = clamped_pct <= GATE_CLAMPED_MAX
    results.append(ok)
    tag = "PASS" if ok else "FAIL"
    print(f"  {'Clamped %':<20s}  {clamped_pct:7.2f}%  {GATE_CLAMPED_MAX:7.1f}%  {tag:>8s}")

    ok = gini <= GATE_GINI_MAX
    results.append(ok)
    tag = "PASS" if ok else "FAIL"
    print(f"  {'Gini':<20s}  {gini:8.3f}  {GATE_GINI_MAX:8.3f}  {tag:>8s}")

    ok = len(zone_counts) >= 14
    results.append(ok)
    tag = "PASS" if ok else "FAIL"
    print(f"  {'Zones >= 14':<20s}  {len(zone_counts):8d}  {'14':>8s}  {tag:>8s}")

    print(f"\n  Top-5 concentration: {top5_pct:.1f}%")

    all_pass = all(results)
    verdict = "PASS" if all_pass else "FAIL"
    print(f"\n  {'=' * 40}")
    print(f"  VERDICT: {verdict}")
    print(f"  {'=' * 40}\n")

    return all_pass


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="TaaSim Remapping Quality Gate")
    parser.add_argument("--rows", type=int, default=100000,
                        help="Max Porto rows to evaluate (default: 100000)")
    parser.add_argument("--curated", type=str, default=None,
                        help="Path to curated Parquet file to evaluate instead of live transform")
    args = parser.parse_args()

    if args.curated:
        passed = evaluate_curated(args.curated)
    else:
        passed = evaluate_live_transform(args.rows)

    sys.exit(0 if passed else 1)
