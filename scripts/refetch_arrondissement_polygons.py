"""Refetch accurate polygons for all 16 Casablanca arrondissements from OSM.

Nominatim returns the municipal boundary polygon when queried by
arrondissement name + Casablanca. We cache the resulting GeoJSON to
data/casablanca_arrondissements_v4.geojson so notebooks can load it
without repeating slow HTTP calls.

The previous data/casablanca_arrondissements.geojson has placeholder
rectangles for 7 of the 16 zones; this script supersedes it.
"""
from __future__ import annotations

import json
import sys
import time
from pathlib import Path

import requests

OUT_PATH = Path(__file__).resolve().parent.parent / "data" / "casablanca_arrondissements_v4.geojson"

# Nominatim search queries per zone_id. Kept explicit (not fuzzy) to avoid
# landing on a neighbouring arrondissement.
QUERIES: list[tuple[int, str, str]] = [
    (1, "Ain Chock", "Arrondissement d'Aïn Chock, Casablanca, Morocco"),
    (2, "Sidi Othmane", "Arrondissement Sidi Othmane, Casablanca, Morocco"),
    (3, "Sidi Moumen", "Arrondissement Sidi Moumen, Casablanca, Morocco"),
    (4, "Hay Hassani", "Arrondissement Hay Hassani, Casablanca, Morocco"),
    (5, "Sbata", "Arrondissement de Sbata, Casablanca, Morocco"),
    (6, "Ben Msik", "Arrondissement de Ben M'Sick, Casablanca, Morocco"),
    (7, "Moulay Rachid", "Arrondissement Moulay Rachid, Casablanca, Morocco"),
    (8, "Maarif", "Arrondissement Maârif, Casablanca, Morocco"),
    (9, "Al Fida", "Arrondissement d'Al Fida, Casablanca, Morocco"),
    (10, "Mers Sultan", "Arrondissement Mers Sultan, Casablanca, Morocco"),
    (11, "Roches Noires", "Assoukhour Assawda, Casablanca, Morocco"),
    (12, "Hay Mohammadi", "Arrondissement Hay Mohammadi, Casablanca, Morocco"),
    (13, "Anfa", "Arrondissement Anfa, Casablanca, Morocco"),
    (14, "Sidi Belyout", "Arrondissement de Sidi Belyout, Casablanca, Morocco"),
    (15, "Ain Sebaa", "Arrondissement d'Aïn Sebaâ, Casablanca, Morocco"),
    (16, "Sidi Bernoussi", "Arrondissement Sidi Bernoussi, Casablanca, Morocco"),
]

NOMINATIM_URL = "https://nominatim.openstreetmap.org/search"
LOOKUP_URL = "https://nominatim.openstreetmap.org/lookup"
HEADERS = {"User-Agent": "TaaSim-geodata/1.0 (academic capstone; contact: taasim-dev)"}

# Fallback queries tried in order when the primary query yields no polygon.
FALLBACKS: dict[int, list[str]] = {
    2: ["Sidi Othmane, Ben M'Sick, Casablanca", "Sidi Othmane, Casablanca"],
    4: ["Hay Hassani, Casablanca, Morocco", "Préfecture d'arrondissement de Hay Hassani"],
    13: ["Anfa, Casablanca-Anfa, Casablanca", "Préfecture d'arrondissements de Casablanca-Anfa"],
}

# Known OSM relation IDs for last-resort direct lookup.
KNOWN_RELATIONS: dict[int, int] = {
    4: 4743065,  # Hay Hassani (Préfecture d'arrondissement de Hay Hassani)
}


def _pick_polygon(results: list[dict]) -> tuple[int | None, str, dict]:
    for res in results:
        geom = res.get("geojson", {})
        if geom.get("type") in ("Polygon", "MultiPolygon"):
            return int(res["osm_id"]), res.get("display_name", ""), geom
    return None, "", {}


def fetch_polygon(query: str) -> tuple[int | None, str, dict]:
    params = {
        "q": query,
        "format": "json",
        "polygon_geojson": 1,
        "limit": 5,
        "addressdetails": 0,
    }
    r = requests.get(NOMINATIM_URL, params=params, headers=HEADERS, timeout=30)
    r.raise_for_status()
    return _pick_polygon(r.json())


def lookup_relation(osm_relation_id: int) -> tuple[int | None, str, dict]:
    params = {"osm_ids": f"R{osm_relation_id}", "format": "json", "polygon_geojson": 1}
    r = requests.get(LOOKUP_URL, params=params, headers=HEADERS, timeout=30)
    r.raise_for_status()
    return _pick_polygon(r.json())


def main() -> int:
    features = []
    misses: list[tuple[int, str]] = []
    for zone_id, name, query in QUERIES:
        print(f"[{zone_id:2d}] {name:18s} -> {query}")
        osm_id: int | None = None
        display_name = ""
        geom: dict = {}
        try:
            osm_id, display_name, geom = fetch_polygon(query)
        except Exception as exc:
            print(f"    FAIL primary: {exc}")
        time.sleep(1.1)
        if not geom:
            for alt in FALLBACKS.get(zone_id, []):
                print(f"    fallback -> {alt}")
                try:
                    osm_id, display_name, geom = fetch_polygon(alt)
                except Exception as exc:
                    print(f"    FAIL alt: {exc}")
                time.sleep(1.1)
                if geom:
                    break
        if not geom and zone_id in KNOWN_RELATIONS:
            rel = KNOWN_RELATIONS[zone_id]
            print(f"    direct OSM relation lookup R{rel}")
            try:
                osm_id, display_name, geom = lookup_relation(rel)
            except Exception as exc:
                print(f"    FAIL lookup: {exc}")
            time.sleep(1.1)
        if not geom:
            print("    no polygon match")
            misses.append((zone_id, name))
        else:
            features.append(
                {
                    "type": "Feature",
                    "properties": {
                        "zone_id": zone_id,
                        "name": name,
                        "osm_id": osm_id,
                        "display_name": display_name,
                    },
                    "geometry": geom,
                }
            )

    fc = {"type": "FeatureCollection", "features": features}
    OUT_PATH.write_text(json.dumps(fc), encoding="utf-8")
    print(f"\nWrote {len(features)}/{len(QUERIES)} polygons to {OUT_PATH}")
    if misses:
        print("Missed:", misses)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
