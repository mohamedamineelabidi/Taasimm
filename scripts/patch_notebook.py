# -*- coding: utf-8 -*-
"""
Patch 03_zone_mapping.ipynb in-place:
  - Cell 22: update section-10 markdown
  - Cell 24: keep only _sample_start (remove adapt_trajectory)
  - Cell 25: new section-11 markdown (graph-based routing)
  - Cell 26: replace map_match_trajectory with 5 routing functions
  - Cell 32: update process_trajectory (use build_trajectory + validate_route)
  - Cell 36: update demo runner (RANDOMNESS / N_WAYPOINTS / print km)
  - Cell 51: update performance notes
"""
import json
from pathlib import Path

NB = Path(__file__).parent.parent / "notebooks" / "03_zone_mapping.ipynb"
nb = json.loads(NB.read_text(encoding="utf-8"))
cells = nb["cells"]

# ── helpers ──────────────────────────────────────────────────────────────────
def md(src):
    return src  # source is a plain string; we join/split as needed

def set_source(cell, text):
    # ipynb stores source as a list of strings (each line ends with \n except last)
    lines = text.split("\n")
    cell["source"] = [l + "\n" for l in lines[:-1]] + ([lines[-1]] if lines[-1] else [])

# ── Cell 22 (index 21) — section-10 markdown ─────────────────────────────────
set_source(cells[21], """\
## 10  Spatial helpers — displacement scales & start sampling

`LAT_SCALE` / `LON_SCALE` map Porto displacement vectors to Casablanca space.
`_sample_start()` picks a realistic taxi origin from the weighted zone distribution.
Both are consumed by `build_trajectory()` in section 11.\
""")

# ── Cell 24 (index 23) — _sample_start only (adapt_trajectory removed) ───────
set_source(cells[23], """\
def _sample_start(zones_gdf, weights, rng, polygon):
    \"\"\"Sample a realistic start position from the weighted zone distribution.\"\"\"
    zone_ids = weights.index.tolist()
    probs    = weights.values
    for _ in range(5):
        zid = rng.choice(zone_ids, p=probs)
        row = zones_gdf.set_index('zone_id').loc[zid]
        lat = row['centroid_lat'] + rng.normal(0, 0.002)
        lon = row['centroid_lon'] + rng.normal(0, 0.002)
        if polygon.contains(Point(lon, lat)):
            return lon, lat
    c = polygon.centroid
    return c.x, c.y


print('_sample_start()  OK')\
""")

# ── Cell 25 (index 24) — section-11 markdown ─────────────────────────────────
set_source(cells[24], """\
## 11  Graph-based routing — realistic road trajectories

Replaces point-by-point snapping with **continuous graph routing**.
Each trajectory is built by routing between *adapted waypoints* on the real road network.

| Function | Role |
|----------|------|
| `snap_to_graph()` | Nearest road node via OSMnx |
| `add_randomness()` | Perturb edge weights → driver variability |
| `compute_route()` | `nx.shortest_path` on perturbed graph, recovers full edge geometry |
| `validate_route()` | Reject zig-zag / too-short / too-long paths |
| `build_trajectory()` | Porto waypoints → Casablanca route (end-to-end) |

**Randomness level**: `0` = strict shortest path · `0.3` = realistic variability · `0.5+` = exploratory\
""")

# ── Cell 26 (index 25) — replace map_match_trajectory with 5 routing fns ─────
set_source(cells[25], """\
# ── 1. Snap any coordinate to the nearest road node ──────────────────────────
def snap_to_graph(lon, lat, G):
    \"\"\"Return the nearest graph node id to (lon, lat).\"\"\"
    return ox.distance.nearest_nodes(G, lon, lat)


# ── 2. Driver variability — perturb edge weights ──────────────────────────────
def add_randomness(G, rng, noise_factor=0.3):
    \"\"\"
    Return a copy of G with perturbed 'perturbed_length' on every edge.
    noise_factor: 0 = no noise, ~0.5 = high variability.
    Routing on perturbed weights simulates different drivers choosing
    slightly different roads for the same origin-destination pair.
    \"\"\"
    G_noisy = G.copy()
    for u, v, k, data in G_noisy.edges(keys=True, data=True):
        length = data.get('length', 1.0)
        noise  = 1.0 + rng.uniform(-noise_factor, noise_factor)
        G_noisy[u][v][k]['perturbed_length'] = max(length * noise, 0.1)
    return G_noisy


# ── 3. Route between two nodes, recovering full edge geometry ─────────────────
def compute_route(G, source_node, target_node, rng=None, randomness=0.3):
    \"\"\"
    Compute a realistic road route between two graph nodes.

    - Uses perturbed edge weights for driver variability (randomness > 0).
    - Recovers full (lon, lat) geometry from each edge's 'geometry' attribute,
      so the path follows the actual road curve — NOT straight lines.
    - Falls back to strict shortest path if the perturbed path fails.
    - Returns list of (lon, lat).
    \"\"\"
    if source_node == target_node:
        nd = G.nodes[source_node]
        return [(nd['x'], nd['y'])]

    if rng is not None and randomness > 0:
        G_r    = add_randomness(G, rng, noise_factor=randomness * 0.5)
        weight = 'perturbed_length'
    else:
        G_r    = G
        weight = 'length'

    try:
        path = nx.shortest_path(G_r, source_node, target_node, weight=weight)
    except nx.NetworkXNoPath:
        try:
            path = nx.shortest_path(G, source_node, target_node, weight='length')
        except nx.NetworkXNoPath:
            sn, tn = G.nodes[source_node], G.nodes[target_node]
            return [(sn['x'], sn['y']), (tn['x'], tn['y'])]

    # Walk the path and collect geometry from each edge
    coords = []
    for i in range(len(path) - 1):
        u, v      = path[i], path[i + 1]
        edge_data = min(G_r[u][v].values(),
                        key=lambda d: d.get(weight, d.get('length', 1.0)))
        if 'geometry' in edge_data:
            pts = list(edge_data['geometry'].coords)   # actual road polyline
            if coords:
                pts = pts[1:]          # drop duplicated junction node
            coords.extend(pts)
        else:
            un, vn = G_r.nodes[u], G_r.nodes[v]
            if not coords:
                coords.append((un['x'], un['y']))
            coords.append((vn['x'], vn['y']))

    return coords or [(G.nodes[source_node]['x'], G.nodes[source_node]['y'])]


# ── 4. Reject unrealistic trajectories ───────────────────────────────────────
def validate_route(points, max_total_km=60, min_points=3):
    \"\"\"
    Validate a generated trajectory for realism.
    Returns (is_valid: bool, reason: str).
    Checks: minimum point count · length bounds · zig-zag ratio.
    \"\"\"
    if len(points) < min_points:
        return False, f'Too few points ({len(points)})'

    total_km = sum(
        haversine(points[i][1], points[i][0],
                  points[i + 1][1], points[i + 1][0])
        for i in range(len(points) - 1)
    )
    if total_km < 0.05:
        return False, f'Route too short ({total_km:.3f} km)'
    if total_km > max_total_km:
        return False, f'Route too long ({total_km:.1f} km)'

    reversals = 0
    for i in range(1, len(points) - 1):
        dx1 = points[i][0]   - points[i-1][0]
        dy1 = points[i][1]   - points[i-1][1]
        dx2 = points[i+1][0] - points[i][0]
        dy2 = points[i+1][1] - points[i][1]
        dot = dx1 * dx2 + dy1 * dy2
        mag = math.sqrt(dx1**2 + dy1**2) * math.sqrt(dx2**2 + dy2**2)
        if mag > 1e-12 and dot / mag < -0.85:
            reversals += 1
    if reversals / max(len(points) - 2, 1) > 0.30:
        return False, f'Zig-zag ({reversals} reversals in {len(points)} pts)'

    return True, 'OK'


# ── 5. Full Porto→Casablanca route builder ────────────────────────────────────
def build_trajectory(porto_points, G, zones_gdf, weights, polygon,
                     rng=None, randomness=0.3, n_waypoints=4):
    \"\"\"
    Build a realistic Casablanca trajectory from a Porto behavioral pattern.

    Steps
    -----
    1. Sample uniformly-spaced waypoints from the Porto trajectory.
    2. Adapt waypoints to Casablanca space (scaled displacements + weighted start).
    3. Snap each adapted waypoint to the nearest road node.
    4. Route between consecutive nodes using randomised shortest path.
    5. Concatenate edge geometries → one continuous road trajectory.

    Parameters
    ----------
    randomness  : 0 = strict shortest path, 0.3 = realistic variability
    n_waypoints : intermediate routing targets between start and end
    \"\"\"
    if rng is None:
        rng = np.random.default_rng()
    if len(porto_points) < 2:
        return []

    # Step 1 — uniformly-spaced waypoints from Porto polyline
    n_pts     = min(n_waypoints + 2, len(porto_points))
    indices   = np.linspace(0, len(porto_points) - 1, n_pts, dtype=int)
    porto_wps = [porto_points[i] for i in indices]

    # Step 2 — adapt waypoints to Casablanca coordinate space
    start_lon, start_lat    = _sample_start(zones_gdf, weights, rng, polygon)
    casa_wps                = [(start_lon, start_lat)]
    cur_lon, cur_lat        = start_lon, start_lat
    minx, miny, maxx, maxy = polygon.bounds

    for i in range(1, len(porto_wps)):
        dlon    = (porto_wps[i][0] - porto_wps[i-1][0]) * LON_SCALE
        dlat    = (porto_wps[i][1] - porto_wps[i-1][1]) * LAT_SCALE
        new_lon = float(np.clip(cur_lon + dlon + rng.normal(0, NOISE_SIGMA_DEG), minx, maxx))
        new_lat = float(np.clip(cur_lat + dlat + rng.normal(0, NOISE_SIGMA_DEG), miny, maxy))
        cur_lon, cur_lat = new_lon, new_lat
        casa_wps.append((cur_lon, cur_lat))

    # Step 3 — snap adapted waypoints to graph nodes
    node_ids     = [snap_to_graph(wp[0], wp[1], G) for wp in casa_wps]
    unique_nodes = [node_ids[0]]
    for nid in node_ids[1:]:
        if nid != unique_nodes[-1]:
            unique_nodes.append(nid)
    if len(unique_nodes) < 2:
        return []

    # Steps 4 & 5 — route between waypoints, concatenate edge geometry
    full_coords = []
    for i in range(len(unique_nodes) - 1):
        seg = compute_route(G, unique_nodes[i], unique_nodes[i + 1],
                            rng=rng, randomness=randomness)
        if full_coords and seg:
            seg = seg[1:]   # drop junction duplicate
        full_coords.extend(seg)

    return full_coords


print('snap_to_graph | add_randomness | compute_route | validate_route | build_trajectory  OK')\
""")

# ── Cell 32 (index 31) — process_trajectory ──────────────────────────────────
set_source(cells[31], """\
def process_trajectory(trip_id, polyline_str, zones_gdf=None,
                        weights=None, polygon=None, G=None,
                        rng=None, randomness=0.3, n_waypoints=4):
    \"\"\"
    Full pipeline: Porto POLYLINE → validated Casablanca road trajectory.

    Parameters
    ----------
    randomness  : 0 = strict shortest path · 0.3-0.5 = driver variability
    n_waypoints : intermediate routing targets (more = richer turn patterns)
    \"\"\"
    if zones_gdf is None: zones_gdf = ZONES_GDF
    if weights   is None: weights   = ZONE_WEIGHTS
    if polygon   is None: polygon   = CASA_POLYGON
    if G         is None: G         = G_ll
    if rng       is None: rng       = np.random.default_rng()

    porto_pts = parse_polyline(polyline_str)
    if not porto_pts:
        return None
    porto_pts, dropped = validate_trajectory(porto_pts)
    if len(porto_pts) < 2:
        return None
    if dropped:
        logger.info('[%s] dropped %d anomaly pts', trip_id, dropped)

    # Build continuous road-network trajectory (no independent point snapping)
    matched = build_trajectory(porto_pts, G, zones_gdf, weights, polygon,
                               rng=rng, randomness=randomness,
                               n_waypoints=n_waypoints)
    if len(matched) < 2:
        logger.warning('[%s] build_trajectory returned empty route', trip_id)
        return None

    valid, reason = validate_route(matched)
    if not valid:
        logger.warning('[%s] route rejected: %s', trip_id, reason)
        return None

    return format_output(trip_id, matched)


print('process_trajectory() ready — graph-based routing.')\
""")

# ── Cell 36 (index 35) — demo runner ─────────────────────────────────────────
set_source(cells[35], """\
RANDOMNESS  = 0.35   # 0 = strict shortest path · 0.5 = high driver variability
N_WAYPOINTS = 5      # intermediate routing targets (more = richer turn patterns)
N_DEMO      = 25
rng         = np.random.default_rng(seed=42)
results     = []

for _, row in sample.head(N_DEMO).iterrows():
    trip_id = str(row['TRIP_ID'])
    t0      = time.time()
    result  = process_trajectory(
        trip_id, row['POLYLINE'],
        rng=rng, randomness=RANDOMNESS, n_waypoints=N_WAYPOINTS,
    )
    elapsed = time.time() - t0
    if result:
        pts = result['trajectory']
        n   = len(pts)
        km  = sum(haversine(pts[i]['lat'],   pts[i]['lon'],
                            pts[i+1]['lat'], pts[i+1]['lon'])
                  for i in range(n - 1))
        zs  = sorted({p['zone_id'] for p in pts})
        h3s = len({p['h3_index'] for p in pts})
        print(f'  {trip_id}: {n} pts | {km:.2f} km | zones={zs} | {h3s} H3 | {elapsed:.1f}s')
        results.append(result)
    else:
        print(f'  {trip_id}: SKIPPED')

print(f'\\nProcessed {len(results)}/{N_DEMO} trips  (randomness={RANDOMNESS}).')\
""")

# ── Cell 51 (index 50) — performance notes ───────────────────────────────────
set_source(cells[50], """\
## 24  Performance notes & Spark / Flink readiness

### Single-thread throughput
| Step | Typical cost | Bottleneck |
|------|-------------|------------|
| parse + validate | < 1 ms/trip | — |
| `_sample_start` + waypoint adapt | < 2 ms/trip | numpy |
| `add_randomness` (graph copy) | 200-800 ms/trip | graph size |
| `nx.shortest_path` x n_waypoints | 50-300 ms/trip | graph traversal |
| `enrich_point` x n_pts | < 1 ms/pt | GeoPandas |
| `aggregate_h3` | O(n_pts) | dict ops |

> **Optimisation tip**: pre-build one noisy graph copy per Spark partition and reuse it across all trips in that partition — avoids per-trip graph copies.

### Spark batch (Week 5)
```python
G_bc       = sc.broadcast(G_ll)
polygon_bc = sc.broadcast(CASA_POLYGON)
zones_bc   = sc.broadcast(ZONES_GDF)
weights_bc = sc.broadcast(ZONE_WEIGHTS)

def process_partition(rows):
    rng     = np.random.default_rng()
    G_noisy = add_randomness(G_bc.value, rng, noise_factor=0.15)  # one copy per partition
    for row in rows:
        result = process_trajectory(
            str(row.TRIP_ID), row.POLYLINE,
            zones_gdf=zones_bc.value, weights=weights_bc.value,
            polygon=polygon_bc.value, G=G_noisy, rng=rng,
            randomness=0.3, n_waypoints=4,
        )
        if result:
            yield result

spark_df.rdd.mapPartitions(process_partition)
```

### Flink streaming (Week 4)
- `parse_polyline` + `validate_trajectory` → stateless `MapFunction`
- `_sample_start` + waypoint adapt → stateless (weights broadcast via `RichFunction.open()`)
- `snap_to_graph` + `compute_route` → stateful; cache `G_ll` in `RuntimeContext`
- `validate_route` → pure function, zero state
- `enrich_point` + `format_output` → Kafka sink serialiser\
""")

# ── write back ────────────────────────────────────────────────────────────────
NB.write_text(json.dumps(nb, indent=1, ensure_ascii=False), encoding="utf-8")
print(f"Notebook patched -> {NB}  ({len(cells)} cells)")
