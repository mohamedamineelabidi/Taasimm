"""Final verification: load zone_mapping.csv + config.py values, run Porto data through, verify distribution."""
import numpy as np
import csv, json, sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from producers.config import (PORTO_LAT_MIN, PORTO_LAT_MAX, PORTO_LON_MIN, PORTO_LON_MAX,
                                CASA_LAT_MIN, CASA_LAT_MAX, CASA_LON_MIN, CASA_LON_MAX,
                                transform_coord, load_zone_mapping, assign_zone)

print(f"=== CONFIG VALUES ===")
print(f"Porto LAT: [{PORTO_LAT_MIN}, {PORTO_LAT_MAX}]")
print(f"Porto LON: [{PORTO_LON_MIN}, {PORTO_LON_MAX}]")
print(f"Casa  LAT: [{CASA_LAT_MIN}, {CASA_LAT_MAX}]")
print(f"Casa  LON: [{CASA_LON_MIN}, {CASA_LON_MAX}]")

zones = load_zone_mapping()
print(f"\n=== ZONE MAPPING ({len(zones)} zones) ===")
for z in zones:
    print(f"  Z{z['zone_id']:2d} {z['name']:18s}: lat[{z['lat_min']:.3f}-{z['lat_max']:.3f}] lon[{z['lon_min']:.3f}-{z['lon_max']:.3f}]")

# Load Porto data
olats, olons = [], []
with open("data/train.csv", encoding="utf-8") as f:
    for i, row in enumerate(csv.DictReader(f)):
        if i > 100000: break
        if row["MISSING_DATA"] == "True": continue
        try:
            coords = json.loads(row["POLYLINE"])
            if len(coords) >= 2:
                lat, lon = coords[0][1], coords[0][0]
                if 41.10 <= lat <= 41.25 and -8.72 <= lon <= -8.55:
                    olats.append(lat); olons.append(lon)
        except: pass

N = len(olats)
print(f"\n=== PORTO DATA: {N} trips ===")

# Transform and assign
rng = np.random.default_rng(42)
zone_counts = {z["zone_id"]: 0 for z in zones}
outside = 0

for i in range(N):
    casa_lat = float(transform_coord(olats[i], PORTO_LAT_MIN, PORTO_LAT_MAX, CASA_LAT_MIN, CASA_LAT_MAX))
    casa_lon = float(transform_coord(olons[i], PORTO_LON_MIN, PORTO_LON_MAX, CASA_LON_MIN, CASA_LON_MAX))
    casa_lat += rng.normal(0, 0.0002)
    casa_lon += rng.normal(0, 0.0002)
    zid, _ = assign_zone(casa_lat, casa_lon, zones)
    if zid > 0:
        zone_counts[zid] += 1
    else:
        outside += 1

total_inside = sum(zone_counts.values())
print(f"\n=== FINAL v5 DISTRIBUTION (production pipeline) ===")
vals = []
for z in zones:
    c = zone_counts[z["zone_id"]]
    pct = c / total_inside * 100
    vals.append(c)
    bar = "#" * int(pct * 2)
    print(f"  Z{z['zone_id']:2d} {z['name']:18s}: {c:6d} ({pct:5.1f}%) {bar}")

vals = np.sort(np.array(vals, dtype=float))
gini = 1 - 2 * np.cumsum(vals).sum() / (len(vals) * vals.sum()) + 1 / len(vals)

print(f"\n  Inside zones: {total_inside}/{N} ({total_inside/N*100:.1f}%)")
print(f"  Outside: {outside} ({outside/N*100:.1f}%)")
print(f"  Gini coefficient: {gini:.3f}")
print(f"  Max zone: {max(zone_counts.values())/total_inside*100:.1f}%")
print(f"  Min zone: {min(zone_counts.values())/total_inside*100:.1f}%")

# Band distribution
bands = {"South": (33.45, 33.515), "Mid-South": (33.515, 33.545), "Center": (33.545, 33.610), "North": (33.610, 33.680)}
print(f"\n=== BAND DISTRIBUTION ===")
for bname, (bmin, bmax) in bands.items():
    band_total = sum(zone_counts[z["zone_id"]] for z in zones if z["lat_min"] >= bmin and z["lat_max"] <= bmax)
    print(f"  {bname:12s}: {band_total:6d} ({band_total/total_inside*100:5.1f}%)")

print(f"\n{'PASS' if gini < 0.40 else 'FAIL'}: Gini = {gini:.3f} ({'< 0.4' if gini < 0.40 else '>= 0.4'})")
