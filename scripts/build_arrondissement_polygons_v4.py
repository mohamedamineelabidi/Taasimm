"""Build v4 arrondissement polygons for Casablanca.

Strategy (robust, deterministic):
1. Direct OSM relation lookup (hardcoded, manually verified relation IDs) for the
   9 arrondissements that have a dedicated boundary in OSM.
2. Voronoi tessellation from arrondissement centroids, clipped to the Casablanca
   city polygon (data/casablanca_polygon.geojson), for the 7 arrondissements
   that do NOT have a dedicated OSM polygon. Voronoi cells are then trimmed
   to exclude areas already covered by real OSM polygons (so no double coverage).

Output: data/casablanca_arrondissements_v4.geojson
"""
from __future__ import annotations

import json
import sys
import time
from pathlib import Path

import pandas as pd
import requests
from scipy.spatial import Voronoi
from shapely.geometry import Polygon, MultiPolygon, shape, mapping, box
from shapely.ops import unary_union

ROOT = Path(__file__).resolve().parent.parent
DATA = ROOT / "data"
OUT_PATH = DATA / "casablanca_arrondissements_v4.geojson"
CITY_BOUND_PATH = DATA / "casablanca_polygon.geojson"
ZONE_MAP_PATH = DATA / "zone_mapping.csv"  # legacy centroids

# Manually verified OSM relation IDs for arrondissements that have a real polygon.
# (zone_id, name, osm_relation_id). Zones NOT listed here -> built via Voronoi.
OSM_RELATIONS: dict[int, tuple[str, int]] = {
    1: ("Ain Chock", 2801442),
    4: ("Hay Hassani", 4743065),
    5: ("Sbata", 2801415),
    6: ("Ben Msik", 2801410),
    8: ("Maarif", 2801474),
    9: ("Al Fida", 2801452),
    11: ("Roches Noires", 2801457),
    14: ("Sidi Belyout", 4743250),
    15: ("Ain Sebaa", 2801460),
}

# All 16 zones (names as in zone_mapping.csv)
ALL_ZONES = [
    (1, "Ain Chock"),
    (2, "Sidi Othmane"),
    (3, "Sidi Moumen"),
    (4, "Hay Hassani"),
    (5, "Sbata"),
    (6, "Ben Msik"),
    (7, "Moulay Rachid"),
    (8, "Maarif"),
    (9, "Al Fida"),
    (10, "Mers Sultan"),
    (11, "Roches Noires"),
    (12, "Hay Mohammadi"),
    (13, "Anfa"),
    (14, "Sidi Belyout"),
    (15, "Ain Sebaa"),
    (16, "Sidi Bernoussi"),
]

LOOKUP_URL = "https://nominatim.openstreetmap.org/lookup"
HEADERS = {"User-Agent": "TaaSim-geodata/1.0 (academic capstone)"}


def fetch_osm_polygon(relation_id: int) -> tuple[str, dict] | None:
    params = {"osm_ids": f"R{relation_id}", "format": "json", "polygon_geojson": 1}
    r = requests.get(LOOKUP_URL, params=params, headers=HEADERS, timeout=30)
    r.raise_for_status()
    for res in r.json():
        geom = res.get("geojson", {})
        if geom.get("type") in ("Polygon", "MultiPolygon"):
            return res.get("display_name", ""), geom
    return None


def load_centroids() -> pd.DataFrame:
    return pd.read_csv(ZONE_MAP_PATH)[["zone_id", "arrondissement_name", "casa_centroid_lat", "casa_centroid_lon"]]


def voronoi_cells(centroids_df: pd.DataFrame, city_poly: Polygon) -> dict[int, Polygon]:
    """Compute Voronoi cells for centroids, clipped to city polygon."""
    # Pad with 4 distant boundary points so Voronoi cells near the edge are bounded.
    minx, miny, maxx, maxy = city_poly.bounds
    pad = max(maxx - minx, maxy - miny)
    pts = centroids_df[["casa_centroid_lon", "casa_centroid_lat"]].to_numpy()
    ghost = [
        [minx - pad, miny - pad],
        [minx - pad, maxy + pad],
        [maxx + pad, miny - pad],
        [maxx + pad, maxy + pad],
    ]
    all_pts = list(pts) + ghost
    vor = Voronoi(all_pts)

    cells: dict[int, Polygon] = {}
    for i, zone_id in enumerate(centroids_df["zone_id"].tolist()):
        region_idx = vor.point_region[i]
        region = vor.regions[region_idx]
        if -1 in region or not region:
            continue
        poly_pts = [vor.vertices[j] for j in region]
        poly = Polygon(poly_pts)
        clipped = poly.intersection(city_poly)
        if not clipped.is_empty:
            cells[int(zone_id)] = clipped
    return cells


def _to_polygon(geom: dict) -> Polygon | MultiPolygon:
    return shape(geom)


def main() -> int:
    # 1. Load city boundary
    city_fc = json.loads(CITY_BOUND_PATH.read_text(encoding="utf-8"))
    city_geom = _to_polygon(city_fc["geometry"])

    # 2. Fetch OSM polygons (9)
    osm_polys: dict[int, tuple[str, int, Polygon | MultiPolygon]] = {}
    for zid, (name, rel_id) in OSM_RELATIONS.items():
        print(f"[OSM {zid:2d}] {name} -> R{rel_id}")
        res = fetch_osm_polygon(rel_id)
        time.sleep(1.1)
        if not res:
            print("  ! OSM lookup failed, will fallback to Voronoi")
            continue
        display_name, geom = res
        osm_polys[zid] = (display_name, rel_id, _to_polygon(geom))

    # 3. Compute Voronoi cells for the 7 missing zones using centroids
    centroids = load_centroids()
    missing_ids = [zid for zid, _ in ALL_ZONES if zid not in osm_polys]
    print(f"\nVoronoi fallback for zones: {missing_ids}")
    vcells = voronoi_cells(centroids, city_geom)

    # 4. Subtract OSM polygons from Voronoi cells so no double coverage
    osm_union = unary_union([p for (_, _, p) in osm_polys.values()])
    for zid in missing_ids:
        if zid in vcells:
            vcells[zid] = vcells[zid].difference(osm_union)

    # 5. Build output FeatureCollection
    features = []
    name_lookup = dict(ALL_ZONES)
    for zid, name in ALL_ZONES:
        if zid in osm_polys:
            display_name, rel_id, geom = osm_polys[zid]
            features.append({
                "type": "Feature",
                "properties": {
                    "zone_id": zid,
                    "name": name,
                    "osm_id": rel_id,
                    "source": "osm_relation",
                    "display_name": display_name,
                },
                "geometry": mapping(geom),
            })
        else:
            geom = vcells.get(zid)
            if geom is None or geom.is_empty:
                print(f"  ! {name} (zone {zid}) has no Voronoi cell; skipping")
                continue
            features.append({
                "type": "Feature",
                "properties": {
                    "zone_id": zid,
                    "name": name,
                    "osm_id": None,
                    "source": "voronoi_clipped_city",
                    "display_name": f"{name} (Voronoi cell within Casablanca city boundary)",
                },
                "geometry": mapping(geom),
            })

    fc = {"type": "FeatureCollection", "features": features}
    OUT_PATH.write_text(json.dumps(fc), encoding="utf-8")
    print(f"\nWrote {len(features)}/16 polygons to {OUT_PATH}")
    return 0 if len(features) == 16 else 1


if __name__ == "__main__":
    sys.exit(main())
