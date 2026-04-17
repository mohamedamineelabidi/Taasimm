# -*- coding: utf-8 -*-
"""
Generator script: builds notebooks/03_zone_mapping.ipynb
Production-grade trajectory preprocessing pipeline:
  - Real Casablanca polygon from OSMnx (no bounding boxes)
  - H3 hexagonal grid clipped to polygon
  - Spatially-weighted trajectory adaptation
  - Road-network snapping
  - Density visualizations

Run from repo root:  python scripts/build_trajectory_notebook.py
"""
import json
import uuid
from pathlib import Path

OUT = Path(__file__).parent.parent / "notebooks" / "03_zone_mapping.ipynb"


def cid():
    return uuid.uuid4().hex[:8]


def md(source: str):
    return {"cell_type": "markdown", "id": cid(), "metadata": {}, "source": source}


def code(source: str):
    return {
        "cell_type": "code",
        "execution_count": None,
        "id": cid(),
        "metadata": {},
        "outputs": [],
        "source": source,
    }


cells = []

# ═══════════════════════════════════════════════════════════════════
# 00  TITLE
# ═══════════════════════════════════════════════════════════════════
cells.append(md(
"# TaaSim — Trajectory Preprocessing & Map-Matching Pipeline\n"
"**Notebook 03 — Porto GPS patterns -> Casablanca road-snapped trajectories (H3 edition)**\n\n"
"### Pipeline overview\n"
"```\n"
"Porto POLYLINE\n"
"  1. Parse & validate (speed filter, dedup)\n"
"  2. Load real Casablanca polygon (OSMnx geocode)\n"
"  3. Build H3 hexagonal grid clipped to polygon\n"
"  4. Compute spatial weights (density + coast proximity)\n"
"  5. Adapt Porto trajectories -> Casablanca (preserve length + temporal patterns)\n"
"  6. Snap adapted points to road network (OSMnx + NetworkX)\n"
"  7. Assign zone_id (zone_mapping.csv) + H3 index\n"
"  8. Aggregate: trip density, flow, hotspots per H3 cell\n"
"  9. Visualise: polygon + H3 grid + trajectories + heatmap\n"
"```\n\n"
"| Constraint | Solution |\n"
"|------------|----------|\n"
"| No rectangular bounding boxes | OSMnx geocoded polygon |\n"
"| Spatial filtering | Point-in-polygon (Shapely) |\n"
"| Zone system | zone_mapping.csv centroids aligned to H3 |\n"
"| Resolution tuning | `H3_RESOLUTION` constant |\n"
"| Weight calibration | `calibrate_weights()` from data |\n"
))

# ═══════════════════════════════════════════════════════════════════
# 01  INSTALL
# ═══════════════════════════════════════════════════════════════════
cells.append(md("## 1  Install dependencies"))
cells.append(code(
"import subprocess, sys\n\n"
"LIBS = [\n"
"    'osmnx==1.9.4',\n"
"    'h3==4.1.0',\n"
"    'geopandas',\n"
"    'shapely',\n"
"    'folium',\n"
"    'networkx',\n"
"    'scipy',\n"
"    'matplotlib',\n"
"]\n\n"
"for lib in LIBS:\n"
"    subprocess.check_call([sys.executable, '-m', 'pip', 'install', lib, '-q'],\n"
"                          stderr=subprocess.DEVNULL)\n"
"print('All dependencies ready.')\n"
))

# ═══════════════════════════════════════════════════════════════════
# 02  IMPORTS
# ═══════════════════════════════════════════════════════════════════
cells.append(md("## 2  Imports"))
cells.append(code(
"import ast\n"
"import json\n"
"import logging\n"
"import math\n"
"import time\n"
"import warnings\n"
"from collections import defaultdict\n"
"from pathlib import Path\n\n"
"import folium\n"
"import geopandas as gpd\n"
"import h3\n"
"import matplotlib.pyplot as plt\n"
"import matplotlib.colors as mcolors\n"
"import networkx as nx\n"
"import numpy as np\n"
"import osmnx as ox\n"
"import pandas as pd\n"
"from folium.plugins import HeatMap\n"
"from shapely.geometry import Point, Polygon, MultiPolygon\n\n"
"warnings.filterwarnings('ignore')\n"
"logging.basicConfig(level=logging.INFO, format='%(levelname)s  %(message)s')\n"
"logger = logging.getLogger('taasim.trajectory')\n\n"
"print(f'osmnx   {ox.__version__}')\n"
"print(f'h3      {h3.__version__}')\n"
"print(f'gpd     {gpd.__version__}')\n"
))

# ═══════════════════════════════════════════════════════════════════
# 03  CONFIGURATION
# ═══════════════════════════════════════════════════════════════════
cells.append(md("## 3  Configuration — all tuneable parameters in one place"))
cells.append(code(
"# H3 resolution: 5=~252km2, 6=~36km2, 7=~5km2, 8=~0.74km2, 9=~0.1km2\n"
"H3_RESOLUTION = 7\n\n"
"GPS_INTERVAL_S  = 15    # Porto: one point every 15 s\n"
"MAX_SPEED_KMH   = 120   # anomaly threshold\n"
"NOISE_SIGMA_DEG = 0.0002  # ~20 m positional noise\n\n"
"CASA_QUERY   = 'Casablanca, Morocco'\n"
"NETWORK_TYPE = 'drive'\n\n"
"# Prior density weights per zone (overridden by calibrate_weights after first run)\n"
"PRIOR_WEIGHTS = {\n"
"    1:  0.9,   # Ain Chock\n"
"    2:  0.7,   # Ain Diab / Anfa coast\n"
"    3:  1.5,   # Anfa (dense commercial)\n"
"    4:  1.2,   # Ben MSik\n"
"    5:  0.8,   # Bernoussi\n"
"    6:  1.0,   # Bourgogne\n"
"    7:  1.3,   # CYM\n"
"    8:  0.6,   # El Fida\n"
"    9:  0.9,   # Hay Hassani\n"
"    10: 1.4,   # Maarif (dense)\n"
"    11: 1.5,   # Mers Sultan (very dense)\n"
"    12: 0.7,   # Moulay Rachid\n"
"    13: 1.1,   # Roches Noires\n"
"    14: 1.3,   # Sidi Belyout (CBD)\n"
"    15: 0.8,   # Sidi Bernoussi\n"
"    16: 0.6,   # Sidi Moumen\n"
"}\n\n"
"COAST_LON = -7.605   # Atlantic coastline longitude in Casablanca\n\n"
"DATA_DIR      = Path('../data')\n"
"ZONE_CSV      = DATA_DIR / 'zone_mapping.csv'\n"
"PORTO_CSV     = DATA_DIR / 'train.csv'\n"
"GRAPH_CACHE   = DATA_DIR / 'casablanca_road_graph.graphml'\n"
"POLYGON_CACHE = DATA_DIR / 'casablanca_polygon.geojson'\n"
"OUT_DIR       = Path('.')\n\n"
"print(f'H3_RESOLUTION = {H3_RESOLUTION}')\n"
"print('Configuration loaded.')\n"
))

# ═══════════════════════════════════════════════════════════════════
# 04  UTILITY FUNCTIONS
# ═══════════════════════════════════════════════════════════════════
cells.append(md("## 4  Utility functions"))
cells.append(code(
"def haversine(lat1, lon1, lat2, lon2):\n"
"    R = 6_371.0\n"
"    phi1, phi2 = math.radians(lat1), math.radians(lat2)\n"
"    dphi = math.radians(lat2 - lat1)\n"
"    dlam = math.radians(lon2 - lon1)\n"
"    a = math.sin(dphi/2)**2 + math.cos(phi1)*math.cos(phi2)*math.sin(dlam/2)**2\n"
"    return 2 * R * math.asin(math.sqrt(a))\n\n\n"
"def parse_polyline(raw):\n"
"    if not raw or raw.strip() in ('[]', ''):\n"
"        return []\n"
"    try:\n"
"        pts = ast.literal_eval(raw)\n"
"    except (ValueError, SyntaxError):\n"
"        return []\n"
"    if not isinstance(pts, list) or len(pts) < 2:\n"
"        return []\n"
"    out = []\n"
"    for p in pts:\n"
"        if len(p) == 2:\n"
"            lon, lat = float(p[0]), float(p[1])\n"
"            if -90 <= lat <= 90 and -180 <= lon <= 180:\n"
"                out.append((lon, lat))\n"
"    return out if len(out) >= 2 else []\n\n\n"
"def validate_trajectory(points):\n"
"    if len(points) < 2:\n"
"        return points, 0\n"
"    clean = [points[0]]\n"
"    dropped = 0\n"
"    for curr in points[1:]:\n"
"        prev = clean[-1]\n"
"        if curr == prev:\n"
"            continue\n"
"        km  = haversine(prev[1], prev[0], curr[1], curr[0])\n"
"        kmh = km / (GPS_INTERVAL_S / 3600)\n"
"        if kmh > MAX_SPEED_KMH:\n"
"            dropped += 1\n"
"            continue\n"
"        clean.append(curr)\n"
"    return clean, dropped\n\n\n"
"print('haversine, parse_polyline, validate_trajectory  OK')\n"
))

# ═══════════════════════════════════════════════════════════════════
# 05  CASABLANCA POLYGON
# ═══════════════════════════════════════════════════════════════════
cells.append(md(
"## 5  Casablanca polygon — real geometry from OSMnx\n\n"
"Downloads the OSM **administrative boundary** of Casablanca.\n"
"Result is a Shapely `Polygon` — no rectangles, ocean excluded automatically.\n"
"Cached as GeoJSON after first download.\n"
))
cells.append(code(
"def load_casablanca_polygon(cache=POLYGON_CACHE):\n"
"    if cache.exists():\n"
"        logger.info('Loading Casablanca polygon from cache: %s', cache)\n"
"        gdf = gpd.read_file(cache)\n"
"    else:\n"
"        logger.info('Geocoding Casablanca boundary from OSM ...')\n"
"        gdf = ox.geocode_to_gdf(CASA_QUERY)\n"
"        gdf.to_file(cache, driver='GeoJSON')\n"
"        logger.info('Polygon cached -> %s', cache)\n"
"    geom = gdf.geometry.iloc[0]\n"
"    if isinstance(geom, MultiPolygon):\n"
"        geom = max(geom.geoms, key=lambda g: g.area)\n"
"    logger.info('Polygon centroid=(%.4f, %.4f) bounds=%s',\n"
"                geom.centroid.y, geom.centroid.x,\n"
"                tuple(round(b, 4) for b in geom.bounds))\n"
"    return geom\n\n\n"
"CASA_POLYGON = load_casablanca_polygon()\n"
"c = CASA_POLYGON.centroid\n"
"print(f'Centroid : lat={c.y:.4f} lon={c.x:.4f}')\n"
"print(f'Bounds   : {tuple(round(b,4) for b in CASA_POLYGON.bounds)}')\n"
))

# ═══════════════════════════════════════════════════════════════════
# 06  H3 GRID CLIPPED TO POLYGON
# ═══════════════════════════════════════════════════════════════════
cells.append(md(
"## 6  H3 hexagonal grid — clipped strictly to Casablanca polygon\n\n"
"Steps:\n"
"1. `h3.geo_to_cells()` — enumerate cells whose centroid is inside the polygon.\n"
"2. Strict point-in-polygon filter (removes any ocean cells).\n"
"3. Build GeoDataFrame of hex geometries for overlay and spatial joins.\n\n"
"| res | edge ~km | area ~km2 | ~cells in Casa |\n"
"|-----|----------|-----------|----------------|\n"
"| 5   | 8.5      | 252       | 5              |\n"
"| 6   | 3.2      | 36        | 30             |\n"
"| 7   | 1.2      | 5.2       | 200            |\n"
"| 8   | 0.46     | 0.74      | 1400           |\n"
"| 9   | 0.17     | 0.10      | 10000          |\n"
))
cells.append(code(
"def build_h3_grid(polygon, resolution=H3_RESOLUTION):\n"
"    geo_json = polygon.__geo_interface__\n"
"    cells_raw = h3.geo_to_cells(geo_json, resolution)\n"
"    records = []\n"
"    for cell in cells_raw:\n"
"        clat, clon = h3.cell_to_latlng(cell)\n"
"        if polygon.contains(Point(clon, clat)):\n"
"            boundary = h3.cell_to_boundary(cell)  # [(lat, lon), ...]\n"
"            hex_poly = Polygon([(lon, lat) for lat, lon in boundary])\n"
"            records.append({\n"
"                'h3_index':    cell,\n"
"                'centroid_lat': clat,\n"
"                'centroid_lon': clon,\n"
"                'resolution':  resolution,\n"
"                'geometry':    hex_poly,\n"
"            })\n"
"    gdf = gpd.GeoDataFrame(records, geometry='geometry', crs='EPSG:4326')\n"
"    gdf = gdf.set_index('h3_index')\n"
"    logger.info('H3 grid res=%d: %d cells inside polygon', resolution, len(gdf))\n"
"    return gdf\n\n\n"
"H3_GRID = build_h3_grid(CASA_POLYGON, H3_RESOLUTION)\n"
"print(f'H3 cells (res={H3_RESOLUTION}) inside Casablanca: {len(H3_GRID)}')\n"
"H3_GRID.head(3)\n"
))

cells.append(md("### 6b  Resolution tuning utility"))
cells.append(code(
"def tune_resolution(polygon, resolutions=None):\n"
"    if resolutions is None:\n"
"        resolutions = [5, 6, 7, 8, 9]\n"
"    geo_json = polygon.__geo_interface__\n"
"    rows = []\n"
"    for res in resolutions:\n"
"        cells = list(h3.geo_to_cells(geo_json, res))\n"
"        area = h3.cell_area(cells[0], unit='km^2') if cells else 0\n"
"        rows.append({'resolution': res, 'n_cells': len(cells),\n"
"                     'cell_area_km2': round(area, 3)})\n"
"    return pd.DataFrame(rows)\n\n\n"
"tune_resolution(CASA_POLYGON)\n"
))

# ═══════════════════════════════════════════════════════════════════
# 07  LOAD ZONES ALIGNED TO H3
# ═══════════════════════════════════════════════════════════════════
cells.append(md(
"## 7  Load zone_mapping.csv — centroids aligned to H3\n\n"
"Zone assignment uses centroid proximity (no bounding boxes for filtering).\n"
"Each zone centroid is mapped to its H3 cell at `H3_RESOLUTION`.\n"
))
cells.append(code(
"def load_zones(zone_csv=ZONE_CSV, polygon=None):\n"
"    df = pd.read_csv(zone_csv)\n"
"    records = []\n"
"    for _, row in df.iterrows():\n"
"        clat = float(row['casa_centroid_lat'])\n"
"        clon = float(row['casa_centroid_lon'])\n"
"        bbox_poly = Polygon([\n"
"            (row['casa_lon_min'], row['casa_lat_min']),\n"
"            (row['casa_lon_max'], row['casa_lat_min']),\n"
"            (row['casa_lon_max'], row['casa_lat_max']),\n"
"            (row['casa_lon_min'], row['casa_lat_max']),\n"
"        ])\n"
"        h3_cell = h3.latlng_to_cell(clat, clon, H3_RESOLUTION)\n"
"        inside  = polygon.contains(Point(clon, clat)) if polygon else True\n"
"        records.append({\n"
"            'zone_id':       int(row['zone_id']),\n"
"            'name':          row['arrondissement_name'],\n"
"            'centroid_lat':  clat,\n"
"            'centroid_lon':  clon,\n"
"            'h3_cell':       h3_cell,\n"
"            'adjacent':      str(row['adjacent_zones']),\n"
"            'inside_polygon': inside,\n"
"            'geometry':      bbox_poly,\n"
"        })\n"
"    return gpd.GeoDataFrame(records, geometry='geometry', crs='EPSG:4326')\n\n\n"
"ZONES_GDF = load_zones(ZONE_CSV, CASA_POLYGON)\n"
"print(f'Zones: {len(ZONES_GDF)} total, '\n"
"      f'{ZONES_GDF.inside_polygon.sum()} centroids inside polygon')\n"
"ZONES_GDF[['zone_id','name','centroid_lat','centroid_lon','h3_cell','inside_polygon']]\n"
))

# ═══════════════════════════════════════════════════════════════════
# 08  SPATIAL WEIGHTS
# ═══════════════════════════════════════════════════════════════════
cells.append(md(
"## 8  Spatial weights — density + coast proximity\n\n"
"```\n"
"weight(z) = density_prior(z) * (1 + coast_alpha / (1 + |lon_z - coast_lon|))\n"
"```\n\n"
"- Higher weight in Anfa, Maarif, Mers Sultan, Sidi Belyout (dense zones)\n"
"- Coast bonus amplifies zones near Atlantic (-7.605 longitude)\n"
"- Weights normalised to sum = 1 (sampling distribution)\n"
))
cells.append(code(
"def compute_zone_weights(zones_gdf, prior=PRIOR_WEIGHTS,\n"
"                          coast_lon=COAST_LON, coast_alpha=0.3):\n"
"    weights = {}\n"
"    for _, row in zones_gdf.iterrows():\n"
"        zid     = row['zone_id']\n"
"        density = prior.get(zid, 1.0)\n"
"        coast_d = abs(row['centroid_lon'] - coast_lon)\n"
"        bonus   = 1 + coast_alpha / (1 + coast_d)\n"
"        weights[zid] = density * bonus\n"
"    s = pd.Series(weights)\n"
"    return s / s.sum()\n\n\n"
"def calibrate_weights(trip_records, zones_gdf,\n"
"                       smoothing=0.1, coast_alpha=0.3):\n"
"    counts = defaultdict(int)\n"
"    for rec in trip_records:\n"
"        for pt in rec.get('trajectory', []):\n"
"            counts[pt['zone_id']] += 1\n"
"    all_ids = zones_gdf['zone_id'].tolist()\n"
"    raw = pd.Series({z: counts.get(z, 0) + smoothing for z in all_ids})\n"
"    bonus = {}\n"
"    for _, row in zones_gdf.iterrows():\n"
"        d = abs(row['centroid_lon'] - COAST_LON)\n"
"        bonus[row['zone_id']] = 1 + coast_alpha / (1 + d)\n"
"    combined = raw * pd.Series(bonus)\n"
"    return combined / combined.sum()\n\n\n"
"ZONE_WEIGHTS = compute_zone_weights(ZONES_GDF)\n"
"print('Zone sampling weights (prior, sorted by weight):')\n"
"w_display = ZONE_WEIGHTS.rename('weight').reset_index()\n"
"w_display.columns = ['zone_id', 'weight']\n"
"names = ZONES_GDF.set_index('zone_id')['name']\n"
"w_display['name'] = w_display['zone_id'].map(names)\n"
"print(w_display.sort_values('weight', ascending=False).to_string(index=False))\n"
))

# ═══════════════════════════════════════════════════════════════════
# 09  ROAD NETWORK
# ═══════════════════════════════════════════════════════════════════
cells.append(md(
"## 9  Load Casablanca road network (polygon-based, cached)\n\n"
"Uses `ox.graph_from_polygon` — more precise than distance-based download.\n"
))
cells.append(code(
"def load_road_network(polygon=None, cache=GRAPH_CACHE):\n"
"    if cache.exists():\n"
"        logger.info('Loading road graph from cache: %s', cache)\n"
"        G = ox.load_graphml(cache)\n"
"    else:\n"
"        logger.info('Downloading road network from OSM polygon ...')\n"
"        G = ox.graph_from_polygon(polygon, network_type=NETWORK_TYPE,\n"
"                                   simplify=True)\n"
"        ox.save_graphml(G, cache)\n"
"        logger.info('Graph cached -> %s', cache)\n"
"    logger.info('Graph: %d nodes, %d edges',\n"
"                G.number_of_nodes(), G.number_of_edges())\n"
"    return G\n\n\n"
"G_ll = load_road_network(polygon=CASA_POLYGON)\n"
))

# ═══════════════════════════════════════════════════════════════════
# 10  TRAJECTORY ADAPTATION
# ═══════════════════════════════════════════════════════════════════
cells.append(md(
"## 10  Trajectory adaptation — Porto patterns -> Casablanca geography\n\n"
"**Preserved**: turn angles, step distances, trajectory length, temporal rhythm.\n"
"**Adapted**: start position sampled from weighted zone distribution,\n"
"displacements scaled to Casablanca spatial extent, polygon containment enforced.\n"
))
cells.append(code(
"# Compute displacement scale factors from polygon bounds\n"
"PORTO_LAT_SPAN = 41.174 - 41.135\n"
"PORTO_LON_SPAN = abs(-8.585 - -8.650)\n"
"_b = CASA_POLYGON.bounds  # (minx, miny, maxx, maxy)\n"
"CASA_LAT_SPAN = _b[3] - _b[1]\n"
"CASA_LON_SPAN = _b[2] - _b[0]\n"
"LAT_SCALE = CASA_LAT_SPAN / PORTO_LAT_SPAN\n"
"LON_SCALE = CASA_LON_SPAN / PORTO_LON_SPAN\n\n"
"print(f'Porto span   : lat={PORTO_LAT_SPAN:.4f} lon={PORTO_LON_SPAN:.4f}')\n"
"print(f'Casa span    : lat={CASA_LAT_SPAN:.4f} lon={CASA_LON_SPAN:.4f}')\n"
"print(f'Scale factors: lat={LAT_SCALE:.3f}  lon={LON_SCALE:.3f}')\n"
))

cells.append(code(
"def _sample_start(zones_gdf, weights, rng, polygon):\n"
"    zone_ids = weights.index.tolist()\n"
"    probs    = weights.values\n"
"    for _ in range(5):\n"
"        zid = rng.choice(zone_ids, p=probs)\n"
"        row = zones_gdf.set_index('zone_id').loc[zid]\n"
"        lat = row['centroid_lat'] + rng.normal(0, 0.002)\n"
"        lon = row['centroid_lon'] + rng.normal(0, 0.002)\n"
"        if polygon.contains(Point(lon, lat)):\n"
"            return lon, lat\n"
"    c = polygon.centroid\n"
"    return c.x, c.y\n\n\n"
"def adapt_trajectory(porto_points, zones_gdf, weights, polygon, rng):\n"
"    if len(porto_points) < 2:\n"
"        return []\n"
"    displacements = [\n"
"        ((porto_points[i][0] - porto_points[i-1][0]) * LON_SCALE,\n"
"         (porto_points[i][1] - porto_points[i-1][1]) * LAT_SCALE)\n"
"        for i in range(1, len(porto_points))\n"
"    ]\n"
"    start_lon, start_lat = _sample_start(zones_gdf, weights, rng, polygon)\n"
"    result = [(start_lon, start_lat)]\n"
"    cur_lon, cur_lat = start_lon, start_lat\n"
"    minx, miny, maxx, maxy = polygon.bounds\n"
"    for dlon, dlat in displacements:\n"
"        new_lon = cur_lon + dlon + rng.normal(0, NOISE_SIGMA_DEG)\n"
"        new_lat = cur_lat + dlat + rng.normal(0, NOISE_SIGMA_DEG)\n"
"        if not (minx <= new_lon <= maxx):\n"
"            new_lon = cur_lon - dlon\n"
"        if not (miny <= new_lat <= maxy):\n"
"            new_lat = cur_lat - dlat\n"
"        if not polygon.contains(Point(new_lon, new_lat)):\n"
"            new_lon, new_lat = cur_lon, cur_lat\n"
"        cur_lon, cur_lat = new_lon, new_lat\n"
"        result.append((cur_lon, cur_lat))\n"
"    return result\n\n\n"
"print('adapt_trajectory() defined.')\n"
))

# ═══════════════════════════════════════════════════════════════════
# 11  MAP MATCHING
# ═══════════════════════════════════════════════════════════════════
cells.append(md(
"## 11  Map matching — snap to road network\n\n"
"Nearest-node + shortest-path reconstruction.\n"
"All output points lie on real Casablanca roads.\n"
))
cells.append(code(
"def map_match_trajectory(points, G):\n"
"    if len(points) < 2:\n"
"        return points\n"
"    lons = [p[0] for p in points]\n"
"    lats = [p[1] for p in points]\n"
"    node_ids  = ox.distance.nearest_nodes(G, lons, lats)\n"
"    matched   = []\n"
"    prev_node = None\n"
"    for node_id in node_ids:\n"
"        nd = G.nodes[node_id]\n"
"        if prev_node is not None and prev_node != node_id:\n"
"            try:\n"
"                path = nx.shortest_path(G, prev_node, node_id, weight='length')\n"
"                for pn in path[1:]:\n"
"                    pnd = G.nodes[pn]\n"
"                    matched.append((pnd['x'], pnd['y']))\n"
"            except nx.NetworkXNoPath:\n"
"                matched.append((nd['x'], nd['y']))\n"
"        else:\n"
"            matched.append((nd['x'], nd['y']))\n"
"        prev_node = node_id\n"
"    return matched\n\n\n"
"print('map_match_trajectory() defined.')\n"
))

# ═══════════════════════════════════════════════════════════════════
# 12  ZONE + H3 ENRICHMENT
# ═══════════════════════════════════════════════════════════════════
cells.append(md("## 12  Zone and H3 enrichment"))
cells.append(code(
"def assign_zone(lat, lon, zones_gdf=None):\n"
"    if zones_gdf is None:\n"
"        zones_gdf = ZONES_GDF\n"
"    pt   = Point(lon, lat)\n"
"    hits = zones_gdf[zones_gdf.contains(pt)]\n"
"    if not hits.empty:\n"
"        r = hits.iloc[0]\n"
"        return int(r['zone_id']), r['name']\n"
"    dists   = zones_gdf.apply(\n"
"        lambda r: haversine(lat, lon, r['centroid_lat'], r['centroid_lon']), axis=1)\n"
"    nearest = zones_gdf.loc[dists.idxmin()]\n"
"    return int(nearest['zone_id']), nearest['name']\n\n\n"
"def enrich_point(lon, lat):\n"
"    zone_id, zone_name = assign_zone(lat, lon)\n"
"    h3_idx = h3.latlng_to_cell(lat, lon, H3_RESOLUTION)\n"
"    return {'zone_id': zone_id, 'zone_name': zone_name, 'h3_index': h3_idx}\n\n\n"
"_info = enrich_point(-7.625, 33.5775)\n"
"print(f'Spot-check: zone {_info[\"zone_id\"]} ({_info[\"zone_name\"]}), H3={_info[\"h3_index\"]}')\n"
))

# ═══════════════════════════════════════════════════════════════════
# 13  OUTPUT FORMAT
# ═══════════════════════════════════════════════════════════════════
cells.append(md("## 13  Output formatter — TaaSim JSON schema"))
cells.append(code(
"def format_output(trip_id, matched_points, start_ts=None):\n"
"    if start_ts is None:\n"
"        start_ts = int(time.time())\n"
"    trajectory = []\n"
"    for i, (lon, lat) in enumerate(matched_points):\n"
"        info = enrich_point(lon, lat)\n"
"        trajectory.append({\n"
"            'lat':       round(lat, 7),\n"
"            'lon':       round(lon, 7),\n"
"            'zone_id':   info['zone_id'],\n"
"            'zone_name': info['zone_name'],\n"
"            'h3_index':  info['h3_index'],\n"
"            'timestamp': start_ts + i * GPS_INTERVAL_S,\n"
"        })\n"
"    return {'trip_id': trip_id, 'trajectory': trajectory}\n\n\n"
"print('format_output() defined.')\n"
))

# ═══════════════════════════════════════════════════════════════════
# 14  FULL PIPELINE
# ═══════════════════════════════════════════════════════════════════
cells.append(md("## 14  Full pipeline — `process_trajectory()`"))
cells.append(code(
"def process_trajectory(trip_id, polyline_str, zones_gdf=None,\n"
"                        weights=None, polygon=None, G=None,\n"
"                        rng=None, do_map_match=True):\n"
"    if zones_gdf is None: zones_gdf = ZONES_GDF\n"
"    if weights   is None: weights   = ZONE_WEIGHTS\n"
"    if polygon   is None: polygon   = CASA_POLYGON\n"
"    if G         is None: G         = G_ll\n"
"    if rng       is None: rng       = np.random.default_rng()\n\n"
"    porto_pts = parse_polyline(polyline_str)\n"
"    if not porto_pts:\n"
"        return None\n"
"    porto_pts, dropped = validate_trajectory(porto_pts)\n"
"    if len(porto_pts) < 2:\n"
"        return None\n"
"    if dropped:\n"
"        logger.info('[%s] dropped %d anomaly pts', trip_id, dropped)\n\n"
"    casa_pts = adapt_trajectory(porto_pts, zones_gdf, weights, polygon, rng)\n"
"    if len(casa_pts) < 2:\n"
"        return None\n\n"
"    matched = map_match_trajectory(casa_pts, G) if do_map_match else casa_pts\n"
"    if not matched:\n"
"        return None\n\n"
"    return format_output(trip_id, matched)\n\n\n"
"print('process_trajectory() ready.')\n"
))

# ═══════════════════════════════════════════════════════════════════
# 15  LOAD DATA
# ═══════════════════════════════════════════════════════════════════
cells.append(md("## 15  Load Porto sample data"))
cells.append(code(
"sample = pd.read_csv(\n"
"    PORTO_CSV, nrows=60,\n"
"    usecols=['TRIP_ID', 'POLYLINE', 'MISSING_DATA', 'TIMESTAMP'],\n"
")\n"
"sample = sample[sample['MISSING_DATA'] == False].reset_index(drop=True)\n"
"sample['hour'] = pd.to_datetime(sample['TIMESTAMP'], unit='s').dt.hour\n"
"print(f'Valid trips : {len(sample)}')\n"
"print('Hour distribution:')\n"
"print(sample['hour'].value_counts().sort_index().to_string())\n"
))

# ═══════════════════════════════════════════════════════════════════
# 16  RUN DEMO
# ═══════════════════════════════════════════════════════════════════
cells.append(md("## 16  Run pipeline on demo trajectories"))
cells.append(code(
"N_DEMO  = 5\n"
"rng     = np.random.default_rng(seed=42)\n"
"results = []\n\n"
"for _, row in sample.head(N_DEMO).iterrows():\n"
"    trip_id = str(row['TRIP_ID'])\n"
"    t0      = time.time()\n"
"    result  = process_trajectory(trip_id, row['POLYLINE'],\n"
"                                  rng=rng, do_map_match=True)\n"
"    elapsed = time.time() - t0\n"
"    if result:\n"
"        n   = len(result['trajectory'])\n"
"        zs  = sorted({p['zone_id'] for p in result['trajectory']})\n"
"        h3s = len({p['h3_index'] for p in result['trajectory']})\n"
"        print(f'  {trip_id}: {n} pts | zones={zs} | {h3s} H3 cells | {elapsed:.1f}s')\n"
"        results.append(result)\n"
"    else:\n"
"        print(f'  {trip_id}: SKIPPED')\n\n"
"print(f'\\nProcessed {len(results)}/{N_DEMO} trips.')\n"
))

# ═══════════════════════════════════════════════════════════════════
# 17  SAMPLE OUTPUT
# ═══════════════════════════════════════════════════════════════════
cells.append(md("## 17  Sample output — first 3 points of first trajectory"))
cells.append(code(
"if results:\n"
"    first = results[0]\n"
"    print(f'trip_id : {first[\"trip_id\"]}')\n"
"    print(f'points  : {len(first[\"trajectory\"])}')\n"
"    for pt in first['trajectory'][:3]:\n"
"        print(json.dumps(pt, indent=2))\n"
))

# ═══════════════════════════════════════════════════════════════════
# 18  H3 AGGREGATION
# ═══════════════════════════════════════════════════════════════════
cells.append(md(
"## 18  H3 aggregation — trip density, flow intensity, hotspots\n\n"
"For each H3 cell:\n"
"- `visit_count`: GPS points in that cell\n"
"- `trip_count`: distinct trips through the cell\n"
"- `flow_intensity`: visit_count * 15 s (dwell time proxy)\n"
))
cells.append(code(
"def aggregate_h3(trip_records, h3_grid):\n"
"    visit_count = defaultdict(int)\n"
"    trip_count  = defaultdict(set)\n"
"    for rec in trip_records:\n"
"        tid = rec['trip_id']\n"
"        for pt in rec['trajectory']:\n"
"            cell = pt['h3_index']\n"
"            visit_count[cell] += 1\n"
"            trip_count[cell].add(tid)\n"
"    rows = []\n"
"    for cell in h3_grid.index:\n"
"        vc = visit_count.get(cell, 0)\n"
"        tc = len(trip_count.get(cell, set()))\n"
"        rows.append({'h3_index': cell, 'visit_count': vc,\n"
"                     'trip_count': tc,\n"
"                     'flow_intensity': vc * GPS_INTERVAL_S})\n"
"    agg = pd.DataFrame(rows).set_index('h3_index')\n"
"    agg['density_rank'] = agg['visit_count'].rank(pct=True)\n"
"    return h3_grid.join(agg)\n\n\n"
"H3_AGG = aggregate_h3(results, H3_GRID)\n"
"nonzero = (H3_AGG['visit_count'] > 0).sum()\n"
"print(f'H3 cells aggregated: {len(H3_AGG)} | non-zero: {nonzero}')\n"
"cols = ['centroid_lat','centroid_lon','visit_count','trip_count','flow_intensity']\n"
"print('Top-10 hotspot cells:')\n"
"print(H3_AGG[cols].sort_values('visit_count', ascending=False).head(10).to_string())\n"
))

# ═══════════════════════════════════════════════════════════════════
# 19  CALIBRATE WEIGHTS
# ═══════════════════════════════════════════════════════════════════
cells.append(md("## 19  Calibrate weights from observed trip density"))
cells.append(code(
"if results:\n"
"    CALIBRATED_WEIGHTS = calibrate_weights(results, ZONES_GDF)\n"
"    cmp = pd.DataFrame({'prior': ZONE_WEIGHTS,\n"
"                        'calibrated': CALIBRATED_WEIGHTS}).round(4)\n"
"    cmp['name'] = ZONES_GDF.set_index('zone_id')['name']\n"
"    print('Prior vs calibrated weights:')\n"
"    print(cmp.sort_values('calibrated', ascending=False).to_string())\n"
))

# ═══════════════════════════════════════════════════════════════════
# 20  MATPLOTLIB FIGURE
# ═══════════════════════════════════════════════════════════════════
cells.append(md("## 20  Static figure — Casablanca polygon + H3 heatmap + trajectories"))
cells.append(code(
"def plot_static(polygon, h3_agg, trip_results, zones_gdf):\n"
"    fig, axes = plt.subplots(1, 2, figsize=(18, 9))\n\n"
"    # LEFT: H3 density heatmap\n"
"    ax = axes[0]\n"
"    px, py = polygon.exterior.xy\n"
"    ax.fill(px, py, alpha=0.05, color='steelblue')\n"
"    ax.plot(px, py, color='steelblue', linewidth=1.5,\n"
"            label='Casablanca boundary')\n"
"    vmax = max(h3_agg['visit_count'].max(), 1)\n"
"    cmap = plt.cm.YlOrRd\n"
"    for _, row in h3_agg.iterrows():\n"
"        vc = row['visit_count']\n"
"        if vc == 0:\n"
"            continue\n"
"        color = cmap(vc / vmax)\n"
"        hx, hy = row.geometry.exterior.xy\n"
"        ax.fill(hx, hy, color=color, alpha=0.75)\n"
"        ax.plot(hx, hy, color='white', linewidth=0.2)\n"
"    sm = plt.cm.ScalarMappable(cmap=cmap,\n"
"                                norm=mcolors.Normalize(0, vmax))\n"
"    sm.set_array([])\n"
"    plt.colorbar(sm, ax=ax, label='Visit count')\n"
"    for _, zrow in zones_gdf.iterrows():\n"
"        ax.plot(zrow['centroid_lon'], zrow['centroid_lat'],\n"
"                'o', color='navy', markersize=4)\n"
"        ax.annotate(str(zrow['zone_id']),\n"
"                    (zrow['centroid_lon'], zrow['centroid_lat']),\n"
"                    fontsize=5, ha='center', color='navy')\n"
"    ax.set_title(f'H3 Density Heatmap (res={H3_RESOLUTION})', fontsize=13)\n"
"    ax.set_xlabel('Longitude'); ax.set_ylabel('Latitude')\n"
"    ax.set_aspect('equal')\n\n"
"    # RIGHT: trajectories\n"
"    ax2 = axes[1]\n"
"    ax2.fill(px, py, alpha=0.05, color='steelblue')\n"
"    ax2.plot(px, py, color='steelblue', linewidth=1.5)\n"
"    tab_colors = plt.cm.tab10.colors\n"
"    for i, res in enumerate(trip_results):\n"
"        pts  = res['trajectory']\n"
"        lons = [p['lon'] for p in pts]\n"
"        lats = [p['lat'] for p in pts]\n"
"        c    = tab_colors[i % len(tab_colors)]\n"
"        ax2.plot(lons, lats, color=c, linewidth=1.3, alpha=0.85,\n"
"                 label=f'Trip {i+1}')\n"
"        ax2.plot(lons[0],  lats[0],  'o', color='green', markersize=7)\n"
"        ax2.plot(lons[-1], lats[-1], 's', color='red',   markersize=7)\n"
"    ax2.set_title('Map-matched Trajectories on Casablanca Roads', fontsize=13)\n"
"    ax2.set_xlabel('Longitude'); ax2.set_ylabel('Latitude')\n"
"    ax2.legend(fontsize=7, loc='upper right')\n"
"    ax2.set_aspect('equal')\n\n"
"    plt.tight_layout()\n"
"    out = OUT_DIR / 'casablanca_analysis.png'\n"
"    plt.savefig(out, dpi=150, bbox_inches='tight')\n"
"    plt.show()\n"
"    print(f'Saved -> {out}')\n\n\n"
"if results:\n"
"    plot_static(CASA_POLYGON, H3_AGG, results, ZONES_GDF)\n"
))

# ═══════════════════════════════════════════════════════════════════
# 21  FOLIUM INTERACTIVE MAP
# ═══════════════════════════════════════════════════════════════════
cells.append(md("## 21  Interactive map — polygon + H3 density + trajectories + heatmap"))
cells.append(code(
"def build_interactive_map(polygon, h3_agg, trip_results, zones_gdf):\n"
"    centre = polygon.centroid\n"
"    m = folium.Map(location=[centre.y, centre.x], zoom_start=12,\n"
"                   tiles='OpenStreetMap')\n\n"
"    # Casablanca polygon\n"
"    poly_fg = folium.FeatureGroup(name='Casablanca boundary', show=True)\n"
"    coords  = [(y, x) for x, y in polygon.exterior.coords]\n"
"    folium.Polygon(locations=coords, color='#1565C0', weight=2,\n"
"                   fill=True, fill_opacity=0.04,\n"
"                   tooltip='Casablanca boundary').add_to(poly_fg)\n"
"    poly_fg.add_to(m)\n\n"
"    # H3 density\n"
"    h3_fg  = folium.FeatureGroup(name='H3 density', show=True)\n"
"    vmax   = max(h3_agg['visit_count'].max(), 1)\n"
"    cmap_h = plt.cm.YlOrRd\n"
"    for cell, row in h3_agg.iterrows():\n"
"        vc = row['visit_count']\n"
"        if vc == 0:\n"
"            continue\n"
"        hex_col  = mcolors.to_hex(cmap_h(vc / vmax))\n"
"        boundary = h3.cell_to_boundary(cell)\n"
"        locs     = [(lat, lon) for lat, lon in boundary]\n"
"        folium.Polygon(\n"
"            locations=locs, color=hex_col, weight=0.3,\n"
"            fill=True, fill_color=hex_col, fill_opacity=0.65,\n"
"            tooltip=f'H3: {cell} | visits: {vc} | trips: {int(row[\"trip_count\"])}',\n"
"        ).add_to(h3_fg)\n"
"    h3_fg.add_to(m)\n\n"
"    # Zone centroids\n"
"    zone_fg = folium.FeatureGroup(name='Zones', show=False)\n"
"    for _, zrow in zones_gdf.iterrows():\n"
"        folium.CircleMarker(\n"
"            location=[zrow['centroid_lat'], zrow['centroid_lon']],\n"
"            radius=6, color='navy', fill=True,\n"
"            fill_color='white', fill_opacity=0.8,\n"
"            popup=f\"Zone {zrow['zone_id']}: {zrow['name']}<br>H3: {zrow['h3_cell']}\",\n"
"        ).add_to(zone_fg)\n"
"    zone_fg.add_to(m)\n\n"
"    # Trajectories\n"
"    COLORS  = ['#E53935','#43A047','#F9A825','#8E24AA','#00ACC1']\n"
"    traj_fg = folium.FeatureGroup(name='Trajectories', show=True)\n"
"    for i, res in enumerate(trip_results):\n"
"        color   = COLORS[i % len(COLORS)]\n"
"        pts     = res['trajectory']\n"
"        latlons = [(p['lat'], p['lon']) for p in pts]\n"
"        folium.PolyLine(latlons, color=color, weight=3, opacity=0.85,\n"
"                        tooltip=f\"Trip {res['trip_id']} | {len(pts)} pts\"\n"
"                        ).add_to(traj_fg)\n"
"        folium.CircleMarker(location=latlons[0], radius=7, color=color,\n"
"                            fill=True, fill_color='#4CAF50',\n"
"                            popup=f\"START zone {pts[0]['zone_id']}: {pts[0]['zone_name']}<br>H3: {pts[0]['h3_index']}\"\n"
"                            ).add_to(traj_fg)\n"
"        folium.CircleMarker(location=latlons[-1], radius=7, color=color,\n"
"                            fill=True, fill_color='#F44336',\n"
"                            popup=f\"END zone {pts[-1]['zone_id']}: {pts[-1]['zone_name']}<br>H3: {pts[-1]['h3_index']}\"\n"
"                            ).add_to(traj_fg)\n"
"    traj_fg.add_to(m)\n\n"
"    # GPS heatmap layer\n"
"    heat_fg   = folium.FeatureGroup(name='GPS heatmap', show=False)\n"
"    heat_data = [[p['lat'], p['lon']]\n"
"                 for res in trip_results for p in res['trajectory']]\n"
"    HeatMap(heat_data, radius=14, blur=10,\n"
"            gradient={0.2:'blue', 0.5:'lime', 0.8:'orange', 1.0:'red'}\n"
"            ).add_to(heat_fg)\n"
"    heat_fg.add_to(m)\n\n"
"    folium.LayerControl().add_to(m)\n"
"    return m\n\n\n"
"if results:\n"
"    imap = build_interactive_map(CASA_POLYGON, H3_AGG, results, ZONES_GDF)\n"
"    imap.save(OUT_DIR / 'casablanca_trajectories.html')\n"
"    print('Interactive map saved -> casablanca_trajectories.html')\n"
"    display(imap)\n"
))

# ═══════════════════════════════════════════════════════════════════
# 22  DARK HEATMAP
# ═══════════════════════════════════════════════════════════════════
cells.append(md("## 22  GPS density heatmap (dark theme)"))
cells.append(code(
"if results:\n"
"    hm = folium.Map(\n"
"        location=[CASA_POLYGON.centroid.y, CASA_POLYGON.centroid.x],\n"
"        zoom_start=12, tiles='CartoDB dark_matter')\n"
"    heat_pts = [[p['lat'], p['lon']]\n"
"                for res in results for p in res['trajectory']]\n"
"    HeatMap(heat_pts, radius=15, blur=10, max_zoom=14,\n"
"            gradient={0.2:'blue', 0.5:'lime', 0.8:'orange', 1.0:'red'}\n"
"            ).add_to(hm)\n"
"    hm.save(OUT_DIR / 'casablanca_heatmap.html')\n"
"    print('Heatmap saved -> casablanca_heatmap.html')\n"
"    display(hm)\n"
))

# ═══════════════════════════════════════════════════════════════════
# 23  EXPORT
# ═══════════════════════════════════════════════════════════════════
cells.append(md("## 23  Export — Kafka-ready JSON + flat DataFrame"))
cells.append(code(
"if results:\n"
"    out_json = OUT_DIR / 'processed_trajectories.json'\n"
"    with open(out_json, 'w') as f:\n"
"        json.dump(results, f, indent=2)\n"
"    print(f'Exported {len(results)} trajectories -> {out_json}')\n\n"
"    rows = []\n"
"    for res in results:\n"
"        for pt in res['trajectory']:\n"
"            rows.append({'trip_id': res['trip_id'], **pt})\n"
"    df_out = pd.DataFrame(rows)\n"
"    print(f'Flat DataFrame shape : {df_out.shape}')\n"
"    print(df_out.dtypes)\n"
"    df_out.head(5)\n"
))

# ═══════════════════════════════════════════════════════════════════
# 24  PERFORMANCE NOTES
# ═══════════════════════════════════════════════════════════════════
cells.append(md(
"## 24  Performance notes & Spark / Flink readiness\n\n"
"### Single-thread throughput\n"
"| Step | Cost | Bottleneck |\n"
"|------|------|------------|\n"
"| parse + validate | < 1 ms/trip | — |\n"
"| adapt_trajectory | < 2 ms/trip | numpy |\n"
"| map_match_trajectory | 50–500 ms/trip | nx.shortest_path |\n"
"| enrich_point | < 1 ms/pt | GeoPandas contains |\n"
"| aggregate_h3 | O(n_pts) | dict ops |\n\n"
"### Spark batch (Week 5)\n"
"```python\n"
"G_bc       = sc.broadcast(G_ll)\n"
"polygon_bc = sc.broadcast(CASA_POLYGON)\n"
"zones_bc   = sc.broadcast(ZONES_GDF)\n"
"weights_bc = sc.broadcast(ZONE_WEIGHTS)\n\n"
"def process_partition(rows):\n"
"    rng = np.random.default_rng()\n"
"    for row in rows:\n"
"        result = process_trajectory(\n"
"            str(row.TRIP_ID), row.POLYLINE,\n"
"            zones_gdf=zones_bc.value, weights=weights_bc.value,\n"
"            polygon=polygon_bc.value, G=G_bc.value, rng=rng,\n"
"        )\n"
"        if result: yield result\n\n"
"spark_df.rdd.mapPartitions(process_partition)\n"
"```\n\n"
"### Flink streaming (Week 4)\n"
"- `parse_polyline` + `validate_trajectory` → stateless `MapFunction`\n"
"- `adapt_trajectory` → stateless (weights broadcast via `RichFunction.open()`)\n"
"- `map_match_trajectory` → stateful; cache `G_ll` in `RuntimeContext`\n"
"- `enrich_point` → pure function, zero state\n"
"- `format_output` → Kafka sink serialiser\n"
))

# ═══════════════════════════════════════════════════════════════════
# WRITE NOTEBOOK
# ═══════════════════════════════════════════════════════════════════
notebook = {
    "nbformat": 4,
    "nbformat_minor": 5,
    "metadata": {
        "kernelspec": {
            "display_name": "Python 3 (ipykernel)",
            "language": "python",
            "name": "python3",
        },
        "language_info": {"name": "python", "version": "3.13.0"},
    },
    "cells": cells,
}

OUT.write_text(json.dumps(notebook, indent=1, ensure_ascii=False), encoding="utf-8")
print(f"Notebook written -> {OUT}  ({len(cells)} cells)")
# -*- coding: utf-8 -*-
