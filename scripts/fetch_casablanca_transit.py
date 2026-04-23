"""Fetch Casablanca transit geometry from OpenStreetMap.

Outputs `data/casablanca_transit.geojson` with a FeatureCollection containing:
- Tram lines     (LineString, properties: mode='tram', ref, name)
- Tram stops     (Point,      properties: mode='tram_stop', name)
- BRT/Casabusway (LineString, properties: mode='brt', ref, name)  -- route=bus + highway=busway
- BRT stops      (Point,      properties: mode='brt_stop', name)

Data source: Overpass API (public OSM mirror). No API key required.
"""
from __future__ import annotations

import json
import sys
import time
from pathlib import Path

import requests

ROOT = Path(__file__).resolve().parent.parent
OUT_PATH = ROOT / "data" / "casablanca_transit.geojson"

# Bounding box covering Casa + near suburbs (south, west, north, east)
BBOX = (33.45, -7.75, 33.68, -7.45)

OVERPASS_URL = "https://overpass-api.de/api/interpreter"
HEADERS = {"User-Agent": "TaaSim-geodata/1.0 (academic capstone)"}

# Overpass QL: tram lines + stops + BRT busway + bus stations
QUERY = f"""
[out:json][timeout:120];
(
  // Tram rails
  way["railway"="tram"]({BBOX[0]},{BBOX[1]},{BBOX[2]},{BBOX[3]});
  // Tram stops
  node["railway"="tram_stop"]({BBOX[0]},{BBOX[1]},{BBOX[2]},{BBOX[3]});
  node["public_transport"="stop_position"]["tram"="yes"]({BBOX[0]},{BBOX[1]},{BBOX[2]},{BBOX[3]});
  // BRT / dedicated busway infrastructure (Casabusway)
  way["highway"="busway"]({BBOX[0]},{BBOX[1]},{BBOX[2]},{BBOX[3]});
  way["busway"="lane"]({BBOX[0]},{BBOX[1]},{BBOX[2]},{BBOX[3]});
  // Bus stations / major terminals
  node["amenity"="bus_station"]({BBOX[0]},{BBOX[1]},{BBOX[2]},{BBOX[3]});
  node["highway"="bus_stop"]["public_transport"="station"]({BBOX[0]},{BBOX[1]},{BBOX[2]},{BBOX[3]});
);
out body geom;
"""


def classify(tags: dict) -> str | None:
    if tags.get("railway") == "tram":
        return "tram"
    if tags.get("railway") == "tram_stop" or (
        tags.get("public_transport") == "stop_position" and tags.get("tram") == "yes"
    ):
        return "tram_stop"
    if tags.get("highway") == "busway" or tags.get("busway") == "lane":
        return "brt"
    if tags.get("amenity") == "bus_station":
        return "brt_stop"
    return None


def main() -> int:
    print(f"Overpass query bbox={BBOX}")
    r = requests.post(OVERPASS_URL, data={"data": QUERY}, headers=HEADERS, timeout=180)
    r.raise_for_status()
    elements = r.json().get("elements", [])
    print(f"Got {len(elements)} elements")

    features = []
    for el in elements:
        tags = el.get("tags", {})
        mode = classify(tags)
        if not mode:
            continue
        props = {
            "mode": mode,
            "name": tags.get("name"),
            "ref": tags.get("ref"),
            "operator": tags.get("operator"),
            "osm_id": f"{el['type'][0].upper()}{el['id']}",
        }
        if el["type"] == "node":
            geom = {"type": "Point", "coordinates": [el["lon"], el["lat"]]}
        elif el["type"] == "way":
            coords = [[p["lon"], p["lat"]] for p in el.get("geometry", [])]
            if len(coords) < 2:
                continue
            geom = {"type": "LineString", "coordinates": coords}
        else:
            continue
        features.append({"type": "Feature", "properties": props, "geometry": geom})

    # Count by mode
    from collections import Counter
    c = Counter(f["properties"]["mode"] for f in features)
    print(f"Modes: {dict(c)}")

    fc = {"type": "FeatureCollection", "features": features}
    OUT_PATH.write_text(json.dumps(fc), encoding="utf-8")
    print(f"Wrote {len(features)} features -> {OUT_PATH}")
    return 0 if features else 1


if __name__ == "__main__":
    sys.exit(main())
