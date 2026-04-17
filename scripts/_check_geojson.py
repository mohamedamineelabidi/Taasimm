import json
with open("data/casablanca_arrondissements.geojson", encoding="utf-8") as f:
    fc = json.load(f)
osm = [x for x in fc["features"] if x["properties"].get("osm_id")]
bbox = [x for x in fc["features"] if not x["properties"].get("osm_id")]
print(f"Total: {len(fc['features'])} | OSM: {len(osm)} | Bbox: {len(bbox)}")
for x in osm:
    p = x["properties"]
    print(f"  OSM  Z{p['zone_id']:2d}: {p['name']}")
for x in bbox:
    p = x["properties"]
    print(f"  BBOX Z{p['zone_id']:2d}: {p['name']}")
