"""Find optimal Porto range + band boundaries + column widths for balanced zone distribution."""
import numpy as np
import csv
import json
from itertools import product

# Load zone mapping
zones = []
with open("data/zone_mapping.csv") as f:
    for row in csv.DictReader(f):
        zones.append({k: row[k] for k in row})

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
print(f"Porto trips: {N}")
print(f"LAT: P5={np.percentile(lats,5):.4f} P25={np.percentile(lats,25):.4f} P50={np.percentile(lats,50):.4f} P75={np.percentile(lats,75):.4f} P95={np.percentile(lats,95):.4f}")
print(f"LON: P5={np.percentile(lons,5):.4f} P25={np.percentile(lons,25):.4f} P50={np.percentile(lons,50):.4f} P75={np.percentile(lons,75):.4f} P95={np.percentile(lons,95):.4f}")

def tx(val, smin, smax, dmin, dmax):
    r = np.clip((val - smin) / (smax - smin), 0, 1)
    return r * (dmax - dmin) + dmin

CASA_LAT_MIN, CASA_LAT_MAX = 33.45, 33.68
CASA_LON_MIN, CASA_LON_MAX = -7.72, -7.48

# === SEARCH: Porto range × band boundaries ===
# Target: Center ~30-35%, Mid-S ~22-28%, North ~18-25%, South ~12-20%
# Constraint: clamping < 15%, MaxDiff < 20

print("\n=== GRID SEARCH: Porto range × band boundaries ===")
print(f"{'Porto':15s}  {'Bands':40s}  {'S':>5s} {'MS':>5s} {'C':>5s} {'N':>5s}  {'Clmp':>4s}  {'MDif':>5s}  {'Score':>5s}")
print("-" * 110)

best_score = 999
best_cfg = None

# Target distribution (realistic city pattern)
target = np.array([15.0, 23.0, 35.0, 22.0])  # S, MS, C, N (sums ~95%, rest is clamp)

for plat_min in [41.135, 41.136, 41.137, 41.138, 41.139, 41.140]:
    for plat_max in [41.172, 41.173, 41.174, 41.175, 41.176]:
        for b1 in [33.500, 33.505, 33.510, 33.515]:           # S/MS border
            for b2 in [33.545, 33.550, 33.555, 33.560]:       # MS/C border
                for b3 in [33.610, 33.615, 33.620, 33.625]:   # C/N border
                    bands = [(33.45, b1), (b1, b2), (b2, b3), (b3, 33.68)]
                    sim = tx(lats, plat_min, plat_max, 33.45, 33.68)
                    clamp = np.sum((lats < plat_min) | (lats > plat_max))
                    clamp_pct = clamp / N * 100
                    if clamp_pct > 15: continue
                    
                    pcts = []
                    for bmin, bmax in bands:
                        n = np.sum((sim >= bmin) & (sim < bmax))
                        pcts.append(n / N * 100)
                    
                    pcts = np.array(pcts)
                    if np.min(pcts) < 8: continue  # no band below 8%
                    
                    # Score: weighted distance from target + penalty for clamp
                    score = np.sum((pcts - target) ** 2) + clamp_pct * 2
                    
                    if score < best_score:
                        best_score = score
                        best_cfg = (plat_min, plat_max, b1, b2, b3, pcts, clamp_pct)

if best_cfg:
    plm, plx, b1, b2, b3, pcts, clp = best_cfg
    print(f"BEST: Porto [{plm:.3f}-{plx:.3f}]  Bands [{b1:.3f}/{b2:.3f}/{b3:.3f}]")
    print(f"  South={pcts[0]:.1f}% Mid-S={pcts[1]:.1f}% Center={pcts[2]:.1f}% North={pcts[3]:.1f}%  Clamp={clp:.1f}%  Score={best_score:.1f}")

    # Now find optimal column widths for each band
    print(f"\n=== OPTIMAL COLUMN WIDTHS PER BAND ===")
    
    sim_lats = tx(lats, plm, plx, 33.45, 33.68)
    sim_lons = tx(lons, -8.69, -8.56, -7.72, -7.48)
    
    band_defs = [
        ("South",  33.45, b1,   3, ["Ain Chock", "Sidi Othmane", "Sidi Moumen"]),
        ("Mid-S",  b1,    b2,   4, ["Hay Hassani", "Sbata", "Ben Msik", "Moulay Rachid"]),
        ("Center", b2,    b3,   5, ["Maarif", "Al Fida", "Mers Sultan", "Roches Noires", "Hay Mohammadi"]),
        ("North",  b3,    33.68, 4, ["Anfa", "Sidi Belyout", "Ain Sebaa", "Sidi Bernoussi"]),
    ]
    
    for band_name, lat_min, lat_max, n_cols, zone_names in band_defs:
        # Get points in this band
        in_band = (sim_lats >= lat_min) & (sim_lats < lat_max)
        band_lons = sim_lons[in_band]
        band_count = len(band_lons)
        
        if band_count == 0:
            print(f"\n  {band_name}: 0 points")
            continue
        
        print(f"\n  {band_name} ({band_count} pts, lat {lat_min:.3f}-{lat_max:.3f}):")
        
        # Find equal-density longitude boundaries
        if band_count > 0:
            lon_pcts = [i * 100 / n_cols for i in range(1, n_cols)]
            lon_boundaries = [CASA_LON_MIN] + [np.percentile(band_lons, p) for p in lon_pcts] + [CASA_LON_MAX]
            
            # Round to 3 decimal places
            lon_boundaries = [round(x, 3) for x in lon_boundaries]
            
            for i, name in enumerate(zone_names):
                lo, hi = lon_boundaries[i], lon_boundaries[i + 1]
                n_in = np.sum((band_lons >= lo) & (band_lons < hi))
                pct = n_in / band_count * 100
                print(f"    {name:18s}: lon [{lo:.3f}, {hi:.3f}] width={hi-lo:.3f}  {n_in:5d} ({pct:.1f}%)")
    
    # Print final zone_mapping.csv format
    print(f"\n=== PROPOSED zone_mapping.csv ===")
    print("zone_id,arrondissement_name,casa_lat_min,casa_lat_max,casa_lon_min,casa_lon_max,casa_centroid_lat,casa_centroid_lon,adjacent_zones")
    
    all_zones = []
    for band_name, lat_min, lat_max, n_cols, zone_names in band_defs:
        in_band = (sim_lats >= lat_min) & (sim_lats < lat_max)
        band_lons = sim_lons[in_band]
        
        lon_pcts = [i * 100 / n_cols for i in range(1, n_cols)]
        if len(band_lons) > 0:
            lon_boundaries = [CASA_LON_MIN] + [np.percentile(band_lons, p) for p in lon_pcts] + [CASA_LON_MAX]
            lon_boundaries = [round(x, 3) for x in lon_boundaries]
        else:
            # Uniform fallback
            step = (CASA_LON_MAX - CASA_LON_MIN) / n_cols
            lon_boundaries = [round(CASA_LON_MIN + i * step, 3) for i in range(n_cols + 1)]
        
        for i, name in enumerate(zone_names):
            zid = len(all_zones) + 1
            lo, hi = lon_boundaries[i], lon_boundaries[i + 1]
            clat = round((lat_min + lat_max) / 2, 4)
            clon = round((lo + hi) / 2, 4)
            all_zones.append((zid, name, lat_min, lat_max, lo, hi, clat, clon))
    
    # Adjacency: same as before (topology unchanged)
    adj_map = {
        1: "2,4,5", 2: "1,3,5,6,7", 3: "2,7",
        4: "1,5,8,9", 5: "1,2,4,6,9,10", 6: "2,5,7,10,11,12", 7: "2,3,6,12",
        8: "4,9,13,14", 9: "4,5,8,10,13,14", 10: "5,6,9,11,14,15", 11: "6,10,12,15,16", 12: "6,7,11,15,16",
        13: "8,9,14", 14: "8,9,10,13,15", 15: "10,11,12,14,16", 16: "11,12,15"
    }
    
    for zid, name, latmin, latmax, lonmin, lonmax, clat, clon in all_zones:
        adj = adj_map.get(zid, "")
        print(f"{zid},{name},{latmin},{latmax},{lonmin},{lonmax},{clat},{clon},\"{adj}\"")

else:
    print("No config found matching constraints!")
