"""v5 verification — tighten Porto LON range + density-aware column widths."""
import numpy as np
import csv, json

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
lats = np.array(olats); lons = np.array(olons)
N = len(lats)

print(f"Porto LON stats: P5={np.percentile(lons,5):.4f} P10={np.percentile(lons,10):.4f} "
      f"P25={np.percentile(lons,25):.4f} P50={np.percentile(lons,50):.4f} "
      f"P75={np.percentile(lons,75):.4f} P90={np.percentile(lons,90):.4f} P95={np.percentile(lons,95):.4f}")

def tx(val, smin, smax, dmin, dmax):
    r = np.clip((val - smin) / (smax - smin), 0, 1)
    return r * (dmax - dmin) + dmin

# CONFIG VARIANTS TO TEST
configs = {
    "A: tighten_lon_only": {
        "plat": (41.135, 41.174), "plon": (-8.660, -8.575),
        "zones": [
            (1,  "Ain Chock",      33.450, 33.515, -7.720, -7.610),
            (2,  "Sidi Othmane",   33.450, 33.515, -7.610, -7.560),
            (3,  "Sidi Moumen",    33.450, 33.515, -7.560, -7.480),
            (4,  "Hay Hassani",    33.515, 33.545, -7.720, -7.640),
            (5,  "Sbata",          33.515, 33.545, -7.640, -7.580),
            (6,  "Ben Msik",       33.515, 33.545, -7.580, -7.530),
            (7,  "Moulay Rachid",  33.515, 33.545, -7.530, -7.480),
            (8,  "Maarif",         33.545, 33.610, -7.720, -7.650),
            (9,  "Al Fida",        33.545, 33.610, -7.650, -7.600),
            (10, "Mers Sultan",    33.545, 33.610, -7.600, -7.560),
            (11, "Roches Noires",  33.545, 33.610, -7.560, -7.520),
            (12, "Hay Mohammadi",  33.545, 33.610, -7.520, -7.480),
            (13, "Anfa",           33.610, 33.680, -7.720, -7.650),
            (14, "Sidi Belyout",   33.610, 33.680, -7.650, -7.590),
            (15, "Ain Sebaa",      33.610, 33.680, -7.590, -7.530),
            (16, "Sidi Bernoussi", 33.610, 33.680, -7.530, -7.480),
        ]
    },
    "B: density_aware_cols": {
        "plat": (41.135, 41.174), "plon": (-8.660, -8.575),
        "zones": [
            # South: 3 zones, wider west (less dense)
            (1,  "Ain Chock",      33.450, 33.515, -7.720, -7.620),
            (2,  "Sidi Othmane",   33.450, 33.515, -7.620, -7.550),
            (3,  "Sidi Moumen",    33.450, 33.515, -7.550, -7.480),
            # Mid-South: 4 zones
            (4,  "Hay Hassani",    33.515, 33.545, -7.720, -7.640),
            (5,  "Sbata",          33.515, 33.545, -7.640, -7.580),
            (6,  "Ben Msik",       33.515, 33.545, -7.580, -7.530),
            (7,  "Moulay Rachid",  33.515, 33.545, -7.530, -7.480),
            # Center: 5 zones, narrower in dense middle
            (8,  "Maarif",         33.545, 33.610, -7.720, -7.650),
            (9,  "Al Fida",        33.545, 33.610, -7.650, -7.600),
            (10, "Mers Sultan",    33.545, 33.610, -7.600, -7.560),
            (11, "Roches Noires",  33.545, 33.610, -7.560, -7.520),
            (12, "Hay Mohammadi",  33.545, 33.610, -7.520, -7.480),
            # North: 4 zones
            (13, "Anfa",           33.610, 33.680, -7.720, -7.650),
            (14, "Sidi Belyout",   33.610, 33.680, -7.650, -7.580),
            (15, "Ain Sebaa",      33.610, 33.680, -7.580, -7.530),
            (16, "Sidi Bernoussi", 33.610, 33.680, -7.530, -7.480),
        ]
    },
    "C: balanced_target": {
        "plat": (41.135, 41.174), "plon": (-8.655, -8.580),
        "zones": [
            # South: 3 zones
            (1,  "Ain Chock",      33.450, 33.515, -7.720, -7.620),
            (2,  "Sidi Othmane",   33.450, 33.515, -7.620, -7.550),
            (3,  "Sidi Moumen",    33.450, 33.515, -7.550, -7.480),
            # Mid-South: 4 zones
            (4,  "Hay Hassani",    33.515, 33.545, -7.720, -7.640),
            (5,  "Sbata",          33.515, 33.545, -7.640, -7.580),
            (6,  "Ben Msik",       33.515, 33.545, -7.580, -7.530),
            (7,  "Moulay Rachid",  33.515, 33.545, -7.530, -7.480),
            # Center: 5 zones
            (8,  "Maarif",         33.545, 33.610, -7.720, -7.650),
            (9,  "Al Fida",        33.545, 33.610, -7.650, -7.600),
            (10, "Mers Sultan",    33.545, 33.610, -7.600, -7.560),
            (11, "Roches Noires",  33.545, 33.610, -7.560, -7.520),
            (12, "Hay Mohammadi",  33.545, 33.610, -7.520, -7.480),
            # North: 4 zones
            (13, "Anfa",           33.610, 33.680, -7.720, -7.650),
            (14, "Sidi Belyout",   33.610, 33.680, -7.650, -7.580),
            (15, "Ain Sebaa",      33.610, 33.680, -7.580, -7.530),
            (16, "Sidi Bernoussi", 33.610, 33.680, -7.530, -7.480),
        ]
    },
}

rng = np.random.default_rng(42)

for cname, cfg in configs.items():
    plat_min, plat_max = cfg["plat"]
    plon_min, plon_max = cfg["plon"]
    
    sim_lats = tx(lats, plat_min, plat_max, 33.450, 33.680)
    sim_lons = tx(lons, plon_min, plon_max, -7.720, -7.480)
    sim_lats2 = sim_lats + rng.normal(0, 0.0002, N)
    sim_lons2 = sim_lons + rng.normal(0, 0.0002, N)
    
    zone_ids = np.zeros(N, dtype=int)
    for zid, name, latmin, latmax, lonmin, lonmax in cfg["zones"]:
        m = (sim_lats2 >= latmin) & (sim_lats2 <= latmax) & (sim_lons2 >= lonmin) & (sim_lons2 <= lonmax)
        zone_ids[m] = zid
    
    counts = {}
    for zid, name, *_ in cfg["zones"]:
        counts[zid] = int(np.sum(zone_ids == zid))
    total = sum(counts.values())
    
    vals = np.sort(np.array(list(counts.values()), dtype=float))
    gini = 1 - 2 * np.cumsum(vals).sum() / (len(vals) * vals.sum()) + 1 / len(vals)
    max_pct = max(counts.values()) / total * 100
    min_pct = min(counts.values()) / total * 100
    lat_clamp = np.sum((lats < plat_min) | (lats > plat_max)) / N * 100
    lon_clamp = np.sum((lons < plon_min) | (lons > plon_max)) / N * 100
    
    print(f"\n{'='*60}")
    print(f"CONFIG {cname}")
    print(f"Porto LAT [{plat_min}, {plat_max}], LON [{plon_min}, {plon_max}]")
    print(f"Gini={gini:.3f}  Max={max_pct:.1f}%  Min={min_pct:.1f}%  Ratio={max_pct/max(min_pct,0.01):.1f}:1")
    print(f"Lat clamp: {lat_clamp:.1f}%  Lon clamp: {lon_clamp:.1f}%  Total inside: {total/N*100:.1f}%")
    for zid, name, *_ in cfg["zones"]:
        c = counts[zid]
        bar = "#" * int(c / total * 80)
        print(f"  Z{zid:2d} {name:18s}: {c:6d} ({c/total*100:5.1f}%) {bar}")
