"""Verify the final v5 zone configuration with realistic column widths."""
import numpy as np
import csv
import json

# Load Porto data
olats, olons = [], []
count = 0
with open("data/train.csv", encoding="utf-8") as f:
    for row in csv.DictReader(f):
        count += 1
        if count > 100000: break
        if row["MISSING_DATA"] == "True": continue
        try:
            coords = json.loads(row["POLYLINE"])
            if len(coords) >= 2:
                lat, lon = coords[0][1], coords[0][0]
                if 41.10 <= lat <= 41.25 and -8.72 <= lon <= -8.55:
                    olats.append(lat)
                    olons.append(lon)
        except: pass
lats = np.array(olats)
lons = np.array(olons)
N = len(lats)

def tx(val, smin, smax, dmin, dmax):
    r = np.clip((val - smin) / (smax - smin), 0, 1)
    return r * (dmax - dmin) + dmin

# === v5 CONFIG ===
PORTO_LAT_MIN, PORTO_LAT_MAX = 41.135, 41.174
PORTO_LON_MIN, PORTO_LON_MAX = -8.690, -8.560
CASA_LAT_MIN, CASA_LAT_MAX = 33.450, 33.680
CASA_LON_MIN, CASA_LON_MAX = -7.720, -7.480

# v5 zone definitions: realistic column widths
zones = [
    # South band (33.450 - 33.515)
    (1,  "Ain Chock",      33.450, 33.515, -7.720, -7.600),  # large suburb
    (2,  "Sidi Othmane",   33.450, 33.515, -7.600, -7.540),
    (3,  "Sidi Moumen",    33.450, 33.515, -7.540, -7.480),
    # Mid-South band (33.515 - 33.545)
    (4,  "Hay Hassani",    33.515, 33.545, -7.720, -7.660),
    (5,  "Sbata",          33.515, 33.545, -7.660, -7.600),
    (6,  "Ben Msik",       33.515, 33.545, -7.600, -7.540),
    (7,  "Moulay Rachid",  33.515, 33.545, -7.540, -7.480),
    # Center band (33.545 - 33.610)
    (8,  "Maarif",         33.545, 33.610, -7.720, -7.660),
    (9,  "Al Fida",        33.545, 33.610, -7.660, -7.610),
    (10, "Mers Sultan",    33.545, 33.610, -7.610, -7.570),
    (11, "Roches Noires",  33.545, 33.610, -7.570, -7.530),
    (12, "Hay Mohammadi",  33.545, 33.610, -7.530, -7.480),
    # North band (33.610 - 33.680)
    (13, "Anfa",           33.610, 33.680, -7.720, -7.660),
    (14, "Sidi Belyout",   33.610, 33.680, -7.660, -7.590),
    (15, "Ain Sebaa",      33.610, 33.680, -7.590, -7.530),
    (16, "Sidi Bernoussi", 33.610, 33.680, -7.530, -7.480),
]

# Transform
sim_lats = tx(lats, PORTO_LAT_MIN, PORTO_LAT_MAX, CASA_LAT_MIN, CASA_LAT_MAX)
sim_lons = tx(lons, PORTO_LON_MIN, PORTO_LON_MAX, CASA_LON_MIN, CASA_LON_MAX)
# Add noise like in notebook
rng = np.random.default_rng(42)
sim_lats += rng.normal(0, 0.0002, N)
sim_lons += rng.normal(0, 0.0002, N)

# Assign zones
zone_ids = np.zeros(N, dtype=int)
for zid, name, latmin, latmax, lonmin, lonmax in zones:
    m = (sim_lats >= latmin) & (sim_lats <= latmax) & (sim_lons >= lonmin) & (sim_lons <= lonmax)
    zone_ids[m] = zid

# Band distribution
print("=== v5 BAND DISTRIBUTION ===")
bands = [("South  [33.450-33.515]", 33.450, 33.515), ("Mid-S  [33.515-33.545]", 33.515, 33.545),
         ("Center [33.545-33.610]", 33.545, 33.610), ("North  [33.610-33.680]", 33.610, 33.680)]
for name, bmin, bmax in bands:
    n = np.sum((sim_lats >= bmin) & (sim_lats < bmax))
    print(f"  {name}: {n:6d} ({n/N*100:5.1f}%)")
clamp = np.sum((lats < PORTO_LAT_MIN) | (lats > PORTO_LAT_MAX))
print(f"  Clamped: {clamp} ({clamp/N*100:.1f}%)")

# Per-zone distribution
print(f"\n=== v5 PER-ZONE DISTRIBUTION ===")
counts = {}
for zid, name, *_ in zones:
    c = np.sum(zone_ids == zid)
    counts[zid] = c
total_inside = sum(counts.values())
for zid, name, latmin, latmax, lonmin, lonmax in zones:
    c = counts[zid]
    bar = "#" * int(c / total_inside * 100)
    print(f"  Z{zid:2d} {name:18s}: {c:6d} ({c/total_inside*100:5.1f}%) {bar}")

vals = np.sort(np.array(list(counts.values()), dtype=float))
gini = 1 - 2 * np.cumsum(vals).sum() / (len(vals) * vals.sum()) + 1 / len(vals)
print(f"\n  Gini: {gini:.3f}")
print(f"  Inside zones: {total_inside}/{N} ({total_inside/N*100:.1f}%)")

# Compare with v4
print(f"\n=== COMPARISON: v4 vs v5 ===")
sim_v4 = tx(lats, 41.085, 41.195, 33.450, 33.680)
v4_bands = [(33.45,33.52),(33.52,33.57),(33.57,33.62),(33.62,33.68)]
print(f"{'':18s}  {'v4':>8s}  {'v5':>8s}")
band_names = ["South", "Mid-South", "Center", "North"]
for i, (bname, (v4min,v4max), (v5name,v5min,v5max)) in enumerate(zip(band_names, v4_bands, bands)):
    v4n = np.sum((sim_v4 >= v4min) & (sim_v4 < v4max))
    v5n = np.sum((sim_lats >= v5min) & (sim_lats < v5max))
    print(f"  {bname:18s}  {v4n/N*100:6.1f}%  {v5n/N*100:6.1f}%")
v4_clamp = np.sum((lats < 41.085) | (lats > 41.195))
print(f"  {'Clamped':18s}  {v4_clamp/N*100:6.1f}%  {clamp/N*100:6.1f}%")

# Verify Porto core mapping
test = tx(41.148, PORTO_LAT_MIN, PORTO_LAT_MAX, CASA_LAT_MIN, CASA_LAT_MAX)
print(f"\n  Porto core (41.148) → Casa {test:.3f} → {'Center' if 33.545 <= test < 33.610 else 'Mid-S' if 33.515 <= test < 33.545 else 'other'} band")
test2 = tx(41.154, PORTO_LAT_MIN, PORTO_LAT_MAX, CASA_LAT_MIN, CASA_LAT_MAX)
print(f"  Porto median (41.154) → Casa {test2:.3f} → {'Center' if 33.545 <= test2 < 33.610 else 'Mid-S' if 33.515 <= test2 < 33.545 else 'other'} band")
