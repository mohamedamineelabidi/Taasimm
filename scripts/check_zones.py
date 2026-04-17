"""Quick analysis of current zone geojson data."""
import json
from shapely.geometry import shape

with open("data/casablanca_arrondissements.geojson", encoding="utf-8") as f:
    fc = json.load(f)

for feat in fc["features"]:
    p = feat["properties"]
    geom = shape(feat["geometry"])
    b = geom.bounds  # (minx, miny, maxx, maxy)
    area_km2 = geom.area * (111**2) * 0.834
    src = "OSM" if p.get("osm_id") else "BBOX"
    c = geom.centroid
    print(
        f"Zone {p['zone_id']:2d} {p['name']:18s} [{src:4s}]  "
        f"centroid=({c.y:.4f}, {c.x:.4f})  "
        f"bbox=({b[1]:.4f}-{b[3]:.4f}, {b[0]:.4f} to {b[2]:.4f})  "
        f"area={area_km2:.1f} km2"
    )
