"""Diagnostic script: analyze current zone distribution and test candidate configs.

Imports transform constants from producers/config.py (single source of truth).
"""
import sys
import os
import numpy as np
import csv
import json

# Ensure producers package is importable from scripts/
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from producers.config import (
    PORTO_LAT_MIN, PORTO_LAT_MAX, PORTO_LON_MIN, PORTO_LON_MAX,
    PORTO_METRO_LAT, PORTO_METRO_LON,
    CASA_LAT_MIN, CASA_LAT_MAX, CASA_LON_MIN, CASA_LON_MAX,
    PORTO_CSV_PATH, ZONE_MAPPING_PATH,
    transform_coord, load_zone_mapping, is_in_porto_metro,
)

zones = load_zone_mapping()

print("=== CURRENT ZONE BOUNDARIES ===")
for z in zones:
    lat_r = z["lat_max"] - z["lat_min"]
    lon_r = z["lon_max"] - z["lon_min"]
    area = lat_r * lon_r
    print(f"  Z{z['zone_id']:2d} {z['name']:18s}  lat [{z['lat_min']:.3f}-{z['lat_max']:.3f}] ({lat_r:.3f})  lon [{z['lon_min']:.3f}-{z['lon_max']:.3f}] ({lon_r:.3f})  area={area:.5f}")

# Load Porto data (pure CSV)
print("\n=== LOADING PORTO DATA (first 100K rows) ===")
olats = []
olons = []
count = 0
with open(PORTO_CSV_PATH, encoding="utf-8") as f:
    reader = csv.DictReader(f)
    for row in reader:
        count += 1
        if count > 100000:
            break
        if row["MISSING_DATA"] == "True":
            continue
        try:
            coords = json.loads(row["POLYLINE"])
            if len(coords) >= 2:
                lat, lon = coords[0][1], coords[0][0]
                if is_in_porto_metro(lat, lon):
                    olats.append(lat)
                    olons.append(lon)
        except:
            pass

lats = np.array(olats)
lons = np.array(olons)
print(f"Filtered Porto trips: {len(lats)}")

print(f"\nPorto LAT percentiles:")
for p in [1, 5, 10, 25, 50, 75, 90, 95, 99]:
    print(f"  P{p:2d} = {np.percentile(lats, p):.4f}")
print(f"  Mean={np.mean(lats):.4f}  Std={np.std(lats):.4f}")


def tx(val, smin, smax, dmin, dmax):
    """Wrapper around config.transform_coord for vectorized use."""
    return transform_coord(val, smin, smax, dmin, dmax)


# Current distribution per band (using active config constants)
print(f"\n=== CURRENT CONFIG (Porto {PORTO_LAT_MIN:.3f}-{PORTO_LAT_MAX:.3f}) ===")
casa_lats = tx(lats, PORTO_LAT_MIN, PORTO_LAT_MAX, CASA_LAT_MIN, CASA_LAT_MAX)
casa_lons = tx(lons, PORTO_LON_MIN, PORTO_LON_MAX, CASA_LON_MIN, CASA_LON_MAX)
bands = [("South", 33.45, 33.52), ("Mid-S", 33.52, 33.57),
         ("Center", 33.57, 33.62), ("North", 33.62, 33.68)]
for name, bmin, bmax in bands:
    n = np.sum((casa_lats >= bmin) & (casa_lats < bmax))
    print(f"  {name:8s}: {n:6d} ({n / len(lats) * 100:5.1f}%)")
clamp = np.sum((lats < PORTO_LAT_MIN) | (lats > PORTO_LAT_MAX))
print(f"  Clamped:  {clamp:6d} ({clamp / len(lats) * 100:.1f}%)")

# Per-zone assignment with current config
print("\n=== PER-ZONE (CURRENT CONFIG) ===")
zone_ids = np.zeros(len(lats), dtype=int)
zone_names = np.full(len(lats), "Outside", dtype=object)
for z in zones:
    m = ((casa_lats >= z["lat_min"]) & (casa_lats <= z["lat_max"]) &
         (casa_lons >= z["lon_min"]) & (casa_lons <= z["lon_max"]))
    zone_ids[m] = z["zone_id"]
    zone_names[m] = z["name"]

counts = {}
for zid, zn in zip(zone_ids, zone_names):
    if zid > 0:
        counts[(zid, zn)] = counts.get((zid, zn), 0) + 1
total_inside = sum(counts.values())
for (zid, zn), cnt in sorted(counts.items()):
    bar = "#" * int(cnt / total_inside * 100)
    print(f"  Z{zid:2d} {zn:18s}: {cnt:6d} ({cnt / total_inside * 100:5.1f}%) {bar}")

vals = np.sort(np.array(list(counts.values()), dtype=float))
gini = 1 - 2 * np.cumsum(vals).sum() / (len(vals) * vals.sum()) + 1 / len(vals)
print(f"\n  Gini: {gini:.3f}  (< 0.3 good, 0.3-0.5 moderate, > 0.5 bad)")

# Test candidate configs
print("\n=== CANDIDATE CONFIGS (band % + clamping) ===")
header = f"{'Config':25s}  {'South':>6s} {'Mid-S':>6s} {'Centr':>6s} {'North':>6s}  {'Clamp':>5s}  {'MaxDiff':>7s}"
print(header)
print("-" * len(header))
cfgs = [
    (f"CURRENT {PORTO_LAT_MIN:.3f}-{PORTO_LAT_MAX:.3f}", PORTO_LAT_MIN, PORTO_LAT_MAX, [(33.45, 33.52), (33.52, 33.57), (33.57, 33.62), (33.62, 33.68)]),
    ("A .137-.174 std      ", 41.137, 41.174, [(33.45, 33.52), (33.52, 33.57), (33.57, 33.62), (33.62, 33.68)]),
    ("B .136-.176 adj      ", 41.136, 41.176, [(33.45, 33.525), (33.525, 33.575), (33.575, 33.625), (33.625, 33.68)]),
    ("C .138-.172 adj      ", 41.138, 41.172, [(33.45, 33.525), (33.525, 33.575), (33.575, 33.625), (33.625, 33.68)]),
    ("D .139-.173 adj      ", 41.139, 41.173, [(33.45, 33.525), (33.525, 33.575), (33.575, 33.625), (33.625, 33.68)]),
    ("E .140-.170 adj      ", 41.140, 41.170, [(33.45, 33.525), (33.525, 33.575), (33.575, 33.625), (33.625, 33.68)]),
    ("F .140-.172 adj      ", 41.140, 41.172, [(33.45, 33.525), (33.525, 33.575), (33.575, 33.625), (33.625, 33.68)]),
    ("G .139-.174 adj      ", 41.139, 41.174, [(33.45, 33.525), (33.525, 33.575), (33.575, 33.625), (33.625, 33.68)]),
    ("H .140-.174 shiftS   ", 41.140, 41.174, [(33.45, 33.530), (33.530, 33.580), (33.580, 33.630), (33.630, 33.68)]),
    ("I .140-.174 shiftS2  ", 41.140, 41.174, [(33.45, 33.535), (33.535, 33.585), (33.585, 33.635), (33.635, 33.68)]),
]
for label, pmin, pmax, bnds in cfgs:
    sim = tx(lats, pmin, pmax, 33.45, 33.68)
    clamp = np.sum((lats < pmin) | (lats > pmax))
    raw = []
    pcts = []
    for bmin, bmax in bnds:
        n = np.sum((sim >= bmin) & (sim < bmax))
        p = n / len(sim) * 100
        pcts.append(f"{p:5.1f}%")
        raw.append(p)
    mdiff = max(raw) - min(raw)
    cpct = clamp / len(sim) * 100
    print(f"{label}  {'  '.join(pcts)}   {cpct:4.1f}%  {mdiff:6.1f}")
