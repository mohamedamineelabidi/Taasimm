"""
TaaSim — Generate Casablanca Road Assets
==========================================
Downloads the Casablanca road network via OSMnx and saves:
  - data/casablanca_polygon.geojson    (city boundary)
  - data/casablanca_road_graph.graphml (road graph)
  - data/casablanca_road_nodes.npy     (lat/lon array for KDTree snapping)

These are prerequisites for the offline trajectory projector.

Usage:
    python scripts/generate_road_assets.py [--force]
"""

import argparse
import json
import os
import sys

import numpy as np

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from producers.config import DATA_DIR

POLYGON_PATH = os.path.join(DATA_DIR, "casablanca_polygon.geojson")
GRAPH_PATH = os.path.join(DATA_DIR, "casablanca_road_graph.graphml")
NODES_PATH = os.path.join(DATA_DIR, "casablanca_road_nodes.npy")


def generate(force=False):
    import osmnx as ox
    from shapely.geometry import mapping

    # 1. Casablanca polygon
    if os.path.exists(POLYGON_PATH) and not force:
        print(f"[skip] Polygon exists: {POLYGON_PATH}")
        from shapely.geometry import shape
        with open(POLYGON_PATH) as f:
            polygon = shape(json.load(f)["geometry"])
    else:
        print("[1/3] Geocoding Casablanca boundary via OSMnx ...")
        gdf = ox.geocode_to_gdf("Casablanca, Morocco")
        polygon = gdf.geometry.iloc[0]
        geojson = {"type": "Feature", "geometry": mapping(polygon), "properties": {}}
        with open(POLYGON_PATH, "w") as f:
            json.dump(geojson, f)
        print(f"  -> Saved: {POLYGON_PATH}")
        b = polygon.bounds
        print(f"  -> Bounds: lon [{b[0]:.4f}, {b[2]:.4f}]  lat [{b[1]:.4f}, {b[3]:.4f}]")

    # 2. Road graph
    if os.path.exists(GRAPH_PATH) and not force:
        print(f"[skip] Graph exists: {GRAPH_PATH}")
        G = ox.load_graphml(GRAPH_PATH)
    else:
        print("[2/3] Downloading road network from OSM ...")
        G = ox.graph_from_polygon(polygon, network_type="drive", simplify=True)
        ox.save_graphml(G, GRAPH_PATH)
        print(f"  -> Saved: {GRAPH_PATH}")
    print(f"  -> Graph: {G.number_of_nodes()} nodes, {G.number_of_edges()} edges")

    # 3. Road nodes array
    if os.path.exists(NODES_PATH) and not force:
        print(f"[skip] Nodes array exists: {NODES_PATH}")
        arr = np.load(NODES_PATH)
    else:
        print("[3/3] Extracting road node coordinates ...")
        nodes = [(data["y"], data["x"]) for _, data in G.nodes(data=True)]
        arr = np.array(nodes, dtype=np.float64)
        np.save(NODES_PATH, arr)
        print(f"  -> Saved: {NODES_PATH}")
    print(f"  -> Nodes array shape: {arr.shape}")

    print("\nAll road assets ready.")
    return polygon, G, arr


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate Casablanca road assets")
    parser.add_argument("--force", action="store_true",
                        help="Regenerate even if files exist")
    args = parser.parse_args()
    generate(force=args.force)
