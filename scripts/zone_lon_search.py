"""v5 FINAL — tighten BOTH Porto LAT and LON ranges + density-equalized columns."""
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

def tx(val, smin, smax, dmin, dmax):
    r = np.clip((val - smin) / (smax - smin), 0, 1)
    return r * (dmax - dmin) + dmin

CLAT_MIN, CLAT_MAX = 33.450, 33.680
CLON_MIN, CLON_MAX = -7.720, -7.480

# Test multiple Porto LON ranges
lon_configs = [
    ("LON_orig [-8.690,-8.560]", -8.690, -8.560),
    ("LON_A    [-8.660,-8.575]", -8.660, -8.575),
    ("LON_B    [-8.655,-8.580]", -8.655, -8.580),
    ("LON_C    [-8.650,-8.585]", -8.650, -8.585),
    ("LON_D    [-8.645,-8.590]", -8.645, -8.590),
    ("LON_E    [-8.640,-8.595]", -8.640, -8.595),
]

PLAT_MIN, PLAT_MAX = 41.135, 41.174
MIN_COL_WIDTH = 0.040

bands = [
    ("South",     33.450, 33.515, 3, ["Ain Chock", "Sidi Othmane", "Sidi Moumen"]),
    ("Mid-South", 33.515, 33.545, 4, ["Hay Hassani", "Sbata", "Ben Msik", "Moulay Rachid"]),
    ("Center",    33.545, 33.610, 5, ["Maarif", "Al Fida", "Mers Sultan", "Roches Noires", "Hay Mohammadi"]),
    ("North",     33.610, 33.680, 4, ["Anfa", "Sidi Belyout", "Ain Sebaa", "Sidi Bernoussi"]),
]

adjacency = {
    1: "2;4;5", 2: "1;3;5;6;7", 3: "2;7",
    4: "1;5;8;9", 5: "1;2;4;6;9;10", 6: "2;5;7;10;11;12", 7: "2;3;6;12",
    8: "4;9;13;14", 9: "4;5;8;10;13;14", 10: "5;6;9;11;14;15",
    11: "6;10;12;15;16", 12: "6;7;11;15;16",
    13: "8;9;14", 14: "8;9;10;13;15", 15: "10;11;12;14;16", 16: "11;12;15"
}

for lname, plon_min, plon_max in lon_configs:
    sim_lats = tx(lats, PLAT_MIN, PLAT_MAX, CLAT_MIN, CLAT_MAX)
    sim_lons = tx(lons, plon_min, plon_max, CLON_MIN, CLON_MAX)
    rng = np.random.default_rng(42)
    sim_lats += rng.normal(0, 0.0002, N)
    sim_lons += rng.normal(0, 0.0002, N)
    
    lat_clamp = np.sum((lats < PLAT_MIN) | (lats > PLAT_MAX)) / N * 100
    lon_clamp = np.sum((lons < plon_min) | (lons > plon_max)) / N * 100
    
    zones = []
    zone_id = 1
    
    for bname, blat_min, blat_max, ncols, names in bands:
        band_mask = (sim_lats >= blat_min) & (sim_lats < blat_max)
        band_lons = sim_lons[band_mask]
        band_n = len(band_lons)
        
        if band_n > 0:
            percentiles = np.linspace(0, 100, ncols + 1)
            ideal_bounds = np.percentile(band_lons, percentiles)
            ideal_bounds[0] = CLON_MIN
            ideal_bounds[-1] = CLON_MAX
            
            boundaries = list(ideal_bounds)
            for _ in range(10):
                widths = [boundaries[i+1] - boundaries[i] for i in range(ncols)]
                violations = [(i, MIN_COL_WIDTH - widths[i]) for i in range(ncols) if widths[i] < MIN_COL_WIDTH]
                if not violations: break
                for ci, deficit in violations:
                    left_push = deficit / 2 if ci > 0 else 0
                    right_push = deficit / 2 if ci < ncols - 1 else deficit if ci > 0 else 0
                    if ci == 0: right_push = deficit
                    if ci == ncols - 1 and ci > 0: left_push = deficit
                    if ci > 0: boundaries[ci] -= left_push
                    if ci < ncols - 1: boundaries[ci + 1] += right_push
            boundaries[0] = CLON_MIN
            boundaries[-1] = CLON_MAX
            boundaries = [round(b, 3) for b in boundaries]
        else:
            w = (CLON_MAX - CLON_MIN) / ncols
            boundaries = [round(CLON_MIN + i * w, 3) for i in range(ncols + 1)]
        
        for i, name in enumerate(names):
            col_mask = band_mask & (sim_lons >= boundaries[i]) & (sim_lons <= boundaries[i+1])
            zones.append({"zone_id": zone_id, "name": name, "count": int(np.sum(col_mask)),
                          "lat_min": blat_min, "lat_max": blat_max,
                          "lon_min": boundaries[i], "lon_max": boundaries[i+1]})
            zone_id += 1
    
    total_inside = sum(z["count"] for z in zones)
    counts_arr = np.sort(np.array([z["count"] for z in zones], dtype=float))
    gini = 1 - 2 * np.cumsum(counts_arr).sum() / (len(counts_arr) * counts_arr.sum()) + 1 / len(counts_arr)
    
    max_pct = max(z["count"] for z in zones) / total_inside * 100
    min_pct = min(z["count"] for z in zones) / total_inside * 100
    
    print(f"\n{'='*70}")
    print(f"{lname}  |  Gini={gini:.3f}  Max={max_pct:.1f}%  Min={min_pct:.1f}%  "
          f"LatClmp={lat_clamp:.1f}%  LonClmp={lon_clamp:.1f}%  Inside={total_inside/N*100:.1f}%")
    for z in zones:
        pct = z["count"] / total_inside * 100
        bar = "#" * int(pct * 2)
        print(f"  Z{z['zone_id']:2d} {z['name']:18s}: {z['count']:6d} ({pct:5.1f}%)  "
              f"lon[{z['lon_min']:.3f},{z['lon_max']:.3f}] {bar}")

    # For best config, output full CSV
    if lname.startswith("LON_C") or lname.startswith("LON_D"):
        print(f"\n  --- CSV OUTPUT ---")
        for z in zones:
            clat = round((z["lat_min"] + z["lat_max"]) / 2, 4)
            clon = round((z["lon_min"] + z["lon_max"]) / 2, 4)
            adj = adjacency[z["zone_id"]]
            print(f"  {z['zone_id']},{z['name']},{z['lat_min']},{z['lat_max']},{z['lon_min']},{z['lon_max']},{clat},{clon},{adj}")
