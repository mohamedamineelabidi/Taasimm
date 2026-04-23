"""Quick validation of data/casablanca_arrondissements_v4.geojson."""
import json
from pathlib import Path
from shapely.geometry import shape

p = Path("data/casablanca_arrondissements_v4.geojson")
fc = json.loads(p.read_text(encoding="utf-8"))

print(f"{'id':>3} {'name':<18} {'osm_id':>10} {'area_km2':>10} {'coords':>8}")
print("-" * 60)
for feat in sorted(fc["features"], key=lambda f: f["properties"]["zone_id"]):
    g = shape(feat["geometry"])
    # Rough area in km² using equirectangular approx at lat=33.6
    import math
    lat_rad = math.radians(33.6)
    km_per_deg_lat = 111.32
    km_per_deg_lon = 111.32 * math.cos(lat_rad)
    area_deg = g.area
    area_km2 = area_deg * km_per_deg_lat * km_per_deg_lon
    p_ = feat["properties"]
    # rough coord count
    coords = 0
    geom = feat["geometry"]
    if geom["type"] == "Polygon":
        coords = sum(len(r) for r in geom["coordinates"])
    elif geom["type"] == "MultiPolygon":
        coords = sum(len(r) for poly in geom["coordinates"] for r in poly)
    print(f"{p_['zone_id']:>3} {p_['name']:<18} {str(p_.get('osm_id','')):>10} {area_km2:>10.2f} {coords:>8}")
