"""v5 FINAL — density-equalized column boundaries per band.
For each latitude band, compute lon boundaries that split trips equally across columns.
Enforce minimum column width of 0.040 deg (~3.5 km) for geographic realism."""
import numpy as np
import csv, json

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
lats = np.array(olats); lons = np.array(olons)
N = len(lats)

def tx(val, smin, smax, dmin, dmax):
    r = np.clip((val - smin) / (smax - smin), 0, 1)
    return r * (dmax - dmin) + dmin

# === OPTIMAL LAT CONFIG (from grid search) ===
PLAT_MIN, PLAT_MAX = 41.135, 41.174
PLON_MIN, PLON_MAX = -8.690, -8.560  # keep original lon range (fewer clamped)
CLAT_MIN, CLAT_MAX = 33.450, 33.680
CLON_MIN, CLON_MAX = -7.720, -7.480

# Transform all points
sim_lats = tx(lats, PLAT_MIN, PLAT_MAX, CLAT_MIN, CLAT_MAX)
sim_lons = tx(lons, PLON_MIN, PLON_MAX, CLON_MIN, CLON_MAX)
rng = np.random.default_rng(42)
sim_lats += rng.normal(0, 0.0002, N)
sim_lons += rng.normal(0, 0.0002, N)

# Band boundaries from optimizer
bands = [
    ("South",     33.450, 33.515, 3, ["Ain Chock", "Sidi Othmane", "Sidi Moumen"]),
    ("Mid-South", 33.515, 33.545, 4, ["Hay Hassani", "Sbata", "Ben Msik", "Moulay Rachid"]),
    ("Center",    33.545, 33.610, 5, ["Maarif", "Al Fida", "Mers Sultan", "Roches Noires", "Hay Mohammadi"]),
    ("North",     33.610, 33.680, 4, ["Anfa", "Sidi Belyout", "Ain Sebaa", "Sidi Bernoussi"]),
]

MIN_COL_WIDTH = 0.040  # minimum 0.040 deg (~3.5 km)

print(f"Total trips: {N}")
print(f"Porto LAT [{PLAT_MIN}, {PLAT_MAX}], LON [{PLON_MIN}, {PLON_MAX}]")
print(f"Casablanca LON range: [{CLON_MIN}, {CLON_MAX}] = {CLON_MAX - CLON_MIN:.3f} deg")
print(f"Min column width: {MIN_COL_WIDTH} deg\n")

zones = []
zone_id = 1

for bname, blat_min, blat_max, ncols, names in bands:
    # Select points in this band
    band_mask = (sim_lats >= blat_min) & (sim_lats < blat_max)
    band_lons = sim_lons[band_mask]
    band_n = len(band_lons)
    
    print(f"\n{'='*60}")
    print(f"BAND: {bname} [{blat_min}-{blat_max}] — {band_n} trips ({band_n/N*100:.1f}%), {ncols} columns")
    
    if band_n == 0:
        # Fallback: equal width columns
        widths = [(CLON_MAX - CLON_MIN) / ncols] * ncols
        boundaries = [CLON_MIN + i * widths[0] for i in range(ncols + 1)]
    else:
        # Step 1: Compute ideal percentile-based boundaries for equal density
        percentiles = np.linspace(0, 100, ncols + 1)
        ideal_bounds = np.percentile(band_lons, percentiles)
        ideal_bounds[0] = CLON_MIN  # force full coverage
        ideal_bounds[-1] = CLON_MAX
        
        print(f"  Ideal equal-density boundaries: {[f'{b:.4f}' for b in ideal_bounds]}")
        ideal_widths = np.diff(ideal_bounds)
        print(f"  Ideal widths: {[f'{w:.4f}' for w in ideal_widths]}")
        
        # Step 2: Enforce minimum width by expanding narrow columns
        boundaries = list(ideal_bounds)
        
        # Iterative adjustment: push boundaries apart where columns are too narrow
        for iteration in range(10):
            widths = [boundaries[i+1] - boundaries[i] for i in range(ncols)]
            violations = [(i, MIN_COL_WIDTH - widths[i]) for i in range(ncols) if widths[i] < MIN_COL_WIDTH]
            if not violations:
                break
            
            for col_idx, deficit in violations:
                # Expand this column by pushing its neighbors
                # Split the deficit between left and right boundaries
                left_push = deficit / 2
                right_push = deficit / 2
                
                # Don't push beyond edges
                if col_idx == 0:  # leftmost column
                    right_push = deficit  # push right boundary only
                    left_push = 0
                elif col_idx == ncols - 1:  # rightmost column
                    left_push = deficit  # push left boundary only
                    right_push = 0
                
                if col_idx > 0:
                    boundaries[col_idx] = max(CLON_MIN + col_idx * MIN_COL_WIDTH * 0.5,
                                               boundaries[col_idx] - left_push)
                if col_idx < ncols - 1:
                    boundaries[col_idx + 1] = min(CLON_MAX - (ncols - col_idx - 1) * MIN_COL_WIDTH * 0.5,
                                                   boundaries[col_idx + 1] + right_push)
        
        boundaries[0] = CLON_MIN
        boundaries[-1] = CLON_MAX
    
    # Round to 3 decimal places for clean CSV
    boundaries = [round(b, 3) for b in boundaries]
    
    final_widths = [boundaries[i+1] - boundaries[i] for i in range(ncols)]
    print(f"  Final boundaries: {boundaries}")
    print(f"  Final widths: {[f'{w:.3f}' for w in final_widths]}")
    
    # Count per column
    for i, name in enumerate(names):
        col_mask = band_mask & (sim_lons >= boundaries[i]) & (sim_lons < boundaries[i+1])
        if i == ncols - 1:  # include right edge for last column
            col_mask = band_mask & (sim_lons >= boundaries[i]) & (sim_lons <= boundaries[i+1])
        count = int(np.sum(col_mask))
        centroid_lat = round((blat_min + blat_max) / 2, 4)
        centroid_lon = round((boundaries[i] + boundaries[i+1]) / 2, 4)
        
        zones.append({
            "zone_id": zone_id,
            "name": name,
            "lat_min": blat_min, "lat_max": blat_max,
            "lon_min": boundaries[i], "lon_max": boundaries[i+1],
            "centroid_lat": centroid_lat, "centroid_lon": centroid_lon,
            "count": count, "pct": count/N*100,
            "width": final_widths[i]
        })
        print(f"    Z{zone_id:2d} {name:18s}: lon [{boundaries[i]:.3f}, {boundaries[i+1]:.3f}]"
              f" w={final_widths[i]:.3f} — {count:6d} trips ({count/N*100:.1f}%)")
        zone_id += 1

# === SUMMARY ===
print(f"\n{'='*60}")
print(f"FINAL v5 DISTRIBUTION SUMMARY")
print(f"{'='*60}")

total_inside = sum(z["count"] for z in zones)
counts_arr = np.array([z["count"] for z in zones], dtype=float)
vals = np.sort(counts_arr)
gini = 1 - 2 * np.cumsum(vals).sum() / (len(vals) * vals.sum()) + 1 / len(vals)

for z in zones:
    bar = "#" * int(z["count"] / total_inside * 80)
    print(f"  Z{z['zone_id']:2d} {z['name']:18s}: {z['count']:6d} ({z['pct']:5.1f}%) w={z['width']:.3f} {bar}")

print(f"\n  Gini: {gini:.3f}")
print(f"  Max zone: {max(z['pct'] for z in zones):.1f}%")
print(f"  Min zone: {min(z['pct'] for z in zones):.1f}%")
print(f"  Ratio: {max(z['pct'] for z in zones) / max(min(z['pct'] for z in zones), 0.01):.1f}:1")
print(f"  Inside zones: {total_inside}/{N} ({total_inside/N*100:.1f}%)")
print(f"  Min column width: {min(z['width'] for z in zones):.3f} deg")

# === OUTPUT CSV ===
print(f"\n{'='*60}")
print(f"ZONE_MAPPING.CSV CONTENT")
print(f"{'='*60}")
# Adjacency stays the same as v4
adjacency = {
    1: "2;4;5", 2: "1;3;5;6;7", 3: "2;7",
    4: "1;5;8;9", 5: "1;2;4;6;9;10", 6: "2;5;7;10;11;12", 7: "2;3;6;12",
    8: "4;9;13;14", 9: "4;5;8;10;13;14", 10: "5;6;9;11;14;15",
    11: "6;10;12;15;16", 12: "6;7;11;15;16",
    13: "8;9;14", 14: "8;9;10;13;15", 15: "10;11;12;14;16", 16: "11;12;15"
}

header = "zone_id,arrondissement_name,casa_lat_min,casa_lat_max,casa_lon_min,casa_lon_max,casa_centroid_lat,casa_centroid_lon,adjacent_zones"
print(header)
csv_lines = [header]
for z in zones:
    adj = adjacency[z["zone_id"]]
    line = f"{z['zone_id']},{z['name']},{z['lat_min']},{z['lat_max']},{z['lon_min']},{z['lon_max']},{z['centroid_lat']},{z['centroid_lon']},{adj}"
    print(line)
    csv_lines.append(line)

# Save to file
with open("data/zone_mapping_v5.csv", "w") as f:
    f.write("\n".join(csv_lines) + "\n")
print(f"\nSaved to data/zone_mapping_v5.csv")
