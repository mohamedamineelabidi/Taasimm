# TaaSim — Data Pipeline & Remapping Architecture

> **Purpose**: Single source of truth for every dataset, every transform, and every artifact used to turn raw international taxi traces into a realistic Casablanca mobility simulation.
>
> **Scope**: Phase 1 (zones v4) → Phase 2 (Porto trajectory warping v4) → Phase 3 (NYC → Casa trip synthesis) → Phase 4 (runtime coupling).
>
> Last updated: 2026-04-22.

---

## 0. Why remap at all?

TaaSim needs a **live** urban-mobility pipeline for Casablanca (16 arrondissements, ~3.3 M people). No open taxi dataset exists for Casa, so we synthesise one by combining three complementary international sources with local calibration data:

| We need | We don't have | We use instead | Why it works |
|---------|--------------|----------------|--------------|
| Realistic **motion** (GPS polylines along real streets) | Casa GPS traces | **Porto** taxi dataset (1.7 M trips, dense polylines) | Same street-network topology class (dense European/Mediterranean grid) |
| Realistic **demand temporal fingerprint** (24 h curve, day-of-week, OD structure) | Casa trip requests | **NYC TLC** Q1 2023 (~3 M trips) | Preserves peak-hour shape, same-zone ratio, duration law |
| Realistic **spatial weights** per zone (who rides where) | — | **HCP 2024** population + **Glovo** commerce H3 cells + **OSM** transit | Official census + observed economic activity + transit accessibility |

Everything else is just transforming, blending, and indexing these sources.

---

## 1. Raw Data Inventory

### 1.1 International traces (motion + temporal)
| Dataset | Location | Size | Role | Tracked in Git? |
|---------|----------|------|------|-----------------|
| **Porto taxi** | [data/train.csv](../data/train.csv) | 1.8 GiB (~1.7 M trips) | Source of GPS polylines | No (MinIO) |
| **NYC TLC Q1 2023** | [data/nyc-tlc/](../data/nyc-tlc/) | ~150 MiB (3 months Parquet) | Demand fingerprint donor | No (MinIO) |

### 1.2 Morocco calibration layers
| Dataset | Location | Size | Role | Tracked in Git? |
|---------|----------|------|------|-----------------|
| **HCP RGPH 2024 indicators** (6 CSVs) | [data/hcp-data-casa/](../data/hcp-data-casa/) | ~20 KiB | Province-level priors (demographics, households, literacy, employment) | No (raw) |
| **HCP arrondissement population** | [data/casa_arrondissement_population_2024.csv](../data/casa_arrondissement_population_2024.csv) | 1 KiB | Per-zone 2024 population (16 rows) | Yes |
| **Glovo opportunity score** | [data/opportunity-find/glovo_opportunity_score_from_commerce-2.geojson](../data/opportunity-find/glovo_opportunity_score_from_commerce-2.geojson) | ~700 KiB | 934 H3-res9 cells w/ commerce metrics | No |
| **Arrondissement polygons v4** | [data/casablanca_arrondissements_v4.geojson](../data/casablanca_arrondissements_v4.geojson) | ~300 KiB | 16 irregular Casa polygons (OSM-derived) | Yes |
| **Transit POIs** | [data/casablanca_transit.geojson](../data/casablanca_transit.geojson) | ~100 KiB | OSM tram/bus hubs (transit accessibility signal) | Yes |
| **Road graph** | [data/casablanca_road_graph.graphml](../data/casablanca_road_graph.graphml) | ~120 MiB | OSMnx driving graph (street segments + intersections) | No |
| **Road nodes cache** | [data/casablanca_road_nodes.npy](../data/casablanca_road_nodes.npy) | ~8 MiB | Pre-computed KDTree coords for snapping | No |

### 1.3 The `image.png` in HCP folder
[data/hcp-data-casa/image.png](../data/hcp-data-casa/image.png) is a screenshot of the HCP interactive portal showing the per-arrondissement population for 2024. This is the **primary source** for [data/casa_arrondissement_population_2024.csv](../data/casa_arrondissement_population_2024.csv) — transcribed manually because HCP's API doesn't expose arrondissement breakdowns.

---

## 2. Zone Remapping v4 — Casablanca Spatial Base

**Notebook**: [notebooks/02_zone_remapping_v4.ipynb](../notebooks/02_zone_remapping_v4.ipynb)
**Output**: [data/zone_mapping_v4.csv](../data/zone_mapping_v4.csv) (16 rows) + [notebooks/casablanca_ae_map.html](../notebooks/casablanca_ae_map.html)

### 2.1 Pipeline
```
casablanca_arrondissements_v4.geojson  ─┐
casa_arrondissement_population_2024    ─┤
glovo_opportunity_score_from_commerce  ─┼─►  Spatial join (H3→polygon)
casablanca_transit.geojson             ─┘
                                        │
                                        ▼
                         Per-zone feature matrix (16 × 4):
                           - commerce_weight  (Glovo sum)
                           - pop_density       (HCP / area_km²)
                           - poi_diversity     (Shannon entropy of POI types)
                           - transit_hubs      (count of tram/bus stops)
                                        │
                                        ▼
                       A–E urban-activity score (weighted sum)
                       score = 0.30·commerce + 0.40·pop_density
                             + 0.20·poi_diversity + 0.10·transit_hubs
                                        │
                                        ▼
                         Quantile-based tier assignment (16 zones → 5 tiers):
                           - A (top 15%)    → 2 zones: Sidi Belyout, Sbata
                           - B (next 20%)   → 3 zones
                           - C (middle 30%) → 5 zones
                           - D (next 20%)   → 3 zones
                           - E (bottom 15%) → 3 zones
```

### 2.2 Why 4 components with these weights?
| Component | Weight | Rationale |
|-----------|--------|-----------|
| `pop_density` | 0.40 | Taxi demand scales roughly linearly with residents per km² |
| `commerce` | 0.30 | Glovo signal captures business districts, shopping, leisure (demand drivers beyond residents) |
| `poi_diversity` | 0.20 | Mixed-use zones generate cross-zone trips (vs. pure residential) |
| `transit_hubs` | 0.10 | Taxi demand *inverse* to transit would underweight hubs (riders transfer at tram stations) |

### 2.3 Sanity checks (in the notebook)
- Each zone has ≥1 polygon, no overlaps, covers 100% of Casa urban footprint
- A/E population ratio ≈ 3.65× (matches HCP income inequality)
- Adjacency graph is connected (no isolated zones)
- `zone_mapping_v4.csv` columns: `zone_id, arrondissement_name, ae_class, population_2024, area_km2, centroid_lat, centroid_lon, adjacent_zones, commerce_score, transit_hubs`

### 2.4 Downstream consumers
- Phase 2 (trajectory warping) — uses `ae_class` for the 25 tier-pair strata
- Phase 3 (NYC synthesis) — uses `population_2024` and `ae_class` multipliers
- Flink Job 3 (trip matcher) — uses `adjacent_zones` for fallback matching
- Producers — `centroid_lat/lon` for GPS anonymization

---

## 3. HCP Data Processing

**Loader**: [scripts/hcp_loader.py](../scripts/hcp_loader.py)
**Consumed by**: [notebooks/02_zone_remapping_v4.ipynb](../notebooks/02_zone_remapping_v4.ipynb), [notebooks/04_nyc_to_casa_synthesis.ipynb](../notebooks/04_nyc_to_casa_synthesis.ipynb)

### 3.1 The 6 raw CSVs
All 6 files in [data/hcp-data-casa/](../data/hcp-data-casa/) share the same shape but carry different indicators:
```
Titre de l'indicateur | Milieu     | Sexe (optional) | Valeur(s)
---------------------+------------+-----------------+-----------
Population municipale | Ensemble   | Ensemble        | 2,812,504
Population municipale | Ensemble   | Masculin        | 1,378,...
Part 15-59 ans (%)    | Ensemble   | Ensemble        |       64.2
...
```
Files with `Sexe` column = **demographics**. Files without = **household characteristics**.

### 3.2 Loader functions

```python
# scripts/hcp_loader.py

def load_province_priors() -> Dict[str, float]:
    """~17 uniform priors for ALL 16 arrondissements.
    Used where HCP doesn't publish zone breakdowns."""
    # Keys: population_province, population_female, population_male,
    #       share_under_15_pct, share_15_59_pct, share_60_plus_pct,
    #       fertility_icf, households_total, household_size_mean,
    #       share_apartment_pct, share_bidonville_pct,
    #       share_owner_pct, share_renter_pct,
    #       electricity_access_pct, water_access_pct,
    #       salaried_share_pct, school_enrolment_6_11_pct,
    #       trilingual_literacy_pct

def load_arrondissement_population() -> pd.DataFrame:
    """Per-zone 2024 population, 16 rows.
    Transcribed from image.png (HCP portal screenshot)."""
```

### 3.3 Why "uniform priors" are OK
HCP publishes most indicators only at **Préfecture de Casablanca** level (not arrondissement). Applying these province-wide means to every zone:
- **Doesn't distort relative demand** — population density already drives spatial heterogeneity
- **Keeps fare/pricing calibration honest** — same household income assumption across zones is a conservative null hypothesis
- **Preserves audit trail** — any future per-zone indicator can plug in without rewiring

---

## 4. Glovo Opportunity Score

**File**: [data/opportunity-find/glovo_opportunity_score_from_commerce-2.geojson](../data/opportunity-find/glovo_opportunity_score_from_commerce-2.geojson)
**Consumed by**: [notebooks/02_zone_remapping_v4.ipynb](../notebooks/02_zone_remapping_v4.ipynb) (commerce component of A–E score)

### 4.1 What it contains
934 H3-resolution-9 hexagons covering Casa, each with:
| Field | Description |
|-------|-------------|
| `h3_index` | H3 res-9 cell ID (edge ≈ 175 m) |
| `opportunity_score` | Glovo's internal demand-opportunity score (0-100) |
| `commerce_count` | Number of registered merchants in cell |
| `commerce_weight_sum` | Weighted sum (premium merchants count more) |
| `commerce_types` | JSON: `{"restaurant": 12, "pharmacy": 3, ...}` |

### 4.2 How it enters Phase 1
```python
# notebooks/02_zone_remapping_v4.ipynb (excerpt)
commerce_gdf = gpd.read_file('data/opportunity-find/glovo_opportunity_score_from_commerce-2.geojson')
# Spatial join: assign each H3 cell to the arrondissement polygon containing its centroid
commerce_per_zone = gpd.sjoin(commerce_gdf, arr_polygons, predicate='within')
zone_commerce = commerce_per_zone.groupby('arrondissement_name').agg(
    commerce_weight=('commerce_weight_sum', 'sum'),
    commerce_count=('commerce_count', 'sum'),
)
```
Then `commerce_weight` is z-score normalized and plugged into the A–E score with weight 0.30.

### 4.3 Why Glovo specifically?
- Open public dataset (no licensing cost)
- Already spatially indexed (H3 res-9 is the right granularity — smaller than an arrondissement, larger than a street block)
- `opportunity_score` encodes implicit ground truth from Glovo's 2022-2023 Casa operations

---

## 5. Phase 2 — Porto Trajectory Warping (v4)

**Notebook**: [notebooks/03_porto_trajectory_warping.ipynb](../notebooks/03_porto_trajectory_warping.ipynb)
**Output**: [data/curated_trajectories_v4.parquet](../data/curated_trajectories_v4.parquet) (500 rows × ~15 columns)

### 5.1 Why we don't use Porto coordinates directly
Porto bbox: `(41.085-41.195 N, -8.69 to -8.56 W)`. Casa bbox: `(33.45-33.68 N, -7.72 to -7.48 W)`. A pure linear affine transform (v3 approach) creates polylines that **cross buildings** and ignore the Casa street grid — visually obvious on Grafana. v4 fixes this.

### 5.2 v4 algorithm (end-to-end)
```
STEP 1: Load Porto CSV
  → filter valid POLYLINE (≥3 points, duration 2-60 min)
  → keep only trips entirely within Porto bbox

STEP 2: Sample stratified by Porto trip length
  → 500 trips total, evenly split across duration deciles
  → preserves variety of short/medium/long trips

STEP 3: Affine-warp Porto polyline → candidate Casa polyline
  → linear transform (stretch + shift) → rough Casa coords

STEP 4: OSRM snap-to-road
  → send candidate polyline endpoints + waypoints to local OSRM server
  → OSRM returns actual driving path on Casablanca road graph
  → polyline now follows real streets (no building crossings)

STEP 5: Assign (origin_zone, dest_zone)
  → spatial join polyline endpoints against arrondissements_v4
  → compute origin_ae_class, dest_ae_class from zone_mapping_v4

STEP 6: Balance the 25 tier-pair strata
  → ensure every (A-A, A-B, ..., E-E) bucket has ≥ N trips
  → re-sample Porto trips to fill sparse pairs if needed

STEP 7: Emit curated_trajectories_v4.parquet
  Columns: trip_id, porto_trip_id, origin_zone, dest_zone,
           origin_ae, dest_ae, polyline_wkt (LINESTRING),
           duration_s, distance_m, n_waypoints
```

### 5.3 Key implementation details (cells in notebook)
| Step | Cell | What it does |
|------|------|-------------|
| Load + clean | #VSC-f7bf8cab | 1.7M Porto trips → ~950k valid |
| Stratified sample | #VSC-1997e0a0 | 500 trips across length deciles |
| Affine warp | #VSC-4c685cd3 | Linear Porto→Casa transform with jitter |
| OSRM cache | #VSC-a7723d73 | `data/.osrm_route_cache.json` (avoids re-querying OSRM) |
| Zone assignment | #VSC-9ed2fad0 | Spatial-join endpoints → zones |
| Tier balance | #VSC-c9387968 | Stratify 25 tier-pairs, fill gaps |
| Export | #VSC-6548034b | Write parquet + interactive Leaflet viz |

### 5.4 OSRM setup
Runs **locally** on the host (not in Docker stack) at `http://localhost:5000`. Cache persisted at [data/.osrm_route_cache.json](../data/.osrm_route_cache.json) so re-runs are idempotent. If OSRM isn't available, the notebook falls back to graph-based shortest path using [data/casablanca_road_graph.graphml](../data/casablanca_road_graph.graphml).

### 5.5 Visualization
[notebooks/casablanca_trajectories_v4.html](../notebooks/casablanca_trajectories_v4.html) — Folium map with:
- All 500 polylines colored by origin A–E class
- Origin/destination markers clustered by zone
- Zone polygons overlay with tier coloring
- Tram/bus buffers in translucent blue

---

## 6. Phase 3 — NYC → Casa Trip Synthesis

**Notebook**: [notebooks/04_nyc_to_casa_synthesis.ipynb](../notebooks/04_nyc_to_casa_synthesis.ipynb)
**Outputs** (all under [data/casa_synthesis/](../data/casa_synthesis/)):
| File | Rows | Purpose |
|------|------|---------|
| `casa_trip_requests.parquet` | **500,000** | Main output: 90-day synthetic trip stream |
| `casa_hourly_demand.parquet` | 2,160 | Aggregated demand per zone per hour |
| `casa_od_matrix.parquet` | 256 | 16×16 zone-pair demand matrix |
| `nyc_to_casa_zone_bridge.csv` | 1,020 | NYC TLC taxi zones → Casa zone mapping |
| `eda_figures.png` | — | 8-panel validation figure |

### 6.1 Pipeline
```
NYC TLC Q1 2023 (3M trips, Parquet)
         │
         ├─► Temporal fingerprint
         │     - hourly curve (24 weights, normalized)
         │     - day-of-week weights (7, normalized)
         │     - duration distribution (log-normal fit)
         │     - same-zone ratio (~20%)
         │
         └─► Spatial bridge: NYC taxi zones → Casa zones
                - 263 NYC zones grouped into 16 Casa-equivalent clusters
                - by population + density + borough-type similarity
                - persisted to nyc_to_casa_zone_bridge.csv

HCP per-arrondissement population
         │
         └─► Base origin weight per zone

v4 A–E class (from zone_mapping_v4.csv)
         │
         └─► Demand multiplier:
                A=1.8, B=1.3, C=1.0, D=0.7, E=0.5

Combined into origin probability:
   origin_p[zone] = population[zone] × ae_mult[zone]
                  / Σ(population × ae_mult)

Destination pool per tier:
   - Given origin zone with tier T,
     sample destination from pool weighted by population within tier T ±1

Taxi type split (Moroccan fleet model):
   - 70% Petit taxi (intra-arrondissement, 2 km max, 1.6 MAD/km)
   - 30% Grand taxi (inter-arrondissement, up to 30 km, 10-40 MAD/seat fixed)
   → Determined by haversine(origin, dest)

Fare calculation:
   - Petit: base 7 MAD + 1.6 MAD/km (metered)
   - Grand: fixed by OD pair, 10-40 MAD

90-day timeline:
   - Start: 2026-04-21 00:00:00 UTC
   - End:   2026-07-19 23:59:59 UTC
   - Event_time sampled from hourly × dow distribution
   - ~5,555 trips/day, ~230 trips/hour peak
```

### 6.2 Output schema: `casa_trip_requests.parquet`
```
trip_id          string  UUID4
rider_id         string  rider_00001..rider_99999 (10k unique)
origin_zone      int     1-16
origin_zone_name string  e.g. "Sidi Belyout"
origin_lat       float   centroid lat ± jitter
origin_lon       float   centroid lon ± jitter
origin_h3        string  H3 res-9 of origin
destination_zone int     1-16
dest_lat         float
dest_lon         float
dest_h3          string
event_time       timestamp  UTC (90-day span)
call_type        string  A=dispatch, B=stand, C=street hail
fleet_type       string  petit | grand
fare_mad         float
distance_km      float   haversine
duration_s       int     sampled from NYC log-normal
origin_class     string  A-E
dest_class       string  A-E
```

### 6.3 8-panel validation (all passed)
[data/casa_synthesis/eda_figures.png](../data/casa_synthesis/eda_figures.png):
1. Hourly demand curve — twin peaks at 08:00 and 18:00 ✓
2. Day-of-week — Fri highest, Sun lowest ✓
3. Fleet split — 70.6% Petit / 29.4% Grand ✓
4. Origin distribution — A-tier zones dominate ✓
5. A/E per-capita demand ratio — 3.65× ✓
6. Fare distributions — Petit median 12.6 MAD, Grand median 25.0 MAD ✓
7. Trip duration CDF — matches NYC log-normal ✓
8. Same-zone ratio — 19.9% ✓

---

## 7. Phase 4 — Runtime Coupling

**Builder**: [scripts/build_trajectory_index.py](../scripts/build_trajectory_index.py)
**Output**: [data/casa_synthesis/gps_trajectory_index.json](../data/casa_synthesis/gps_trajectory_index.json) (2.35 MB)

### 7.1 Why we need an index
Phase 3 produces 500k trips with (origin_zone, dest_zone, event_time). Phase 2 produces 500 polylines with (origin_zone, dest_zone). At runtime, each incoming trip needs a polyline — we can't do a live spatial join 500k times. The index is a pre-computed lookup.

### 7.2 Index structure
```json
{
  "by_zone_pair": {
    "(3, 10)": [12, 47, 89, 234],   // trip_idx list
    "(5, 2)":  [7, 33, 128],
    ...
  },
  "by_tier_pair": {
    "(A, A)": [1, 55, 200],
    "(A, B)": [3, 78, ...],
    ...
  },
  "polylines": [
    {"trip_id": ..., "origin_zone": 3, "dest_zone": 10,
     "origin_ae": "C", "dest_ae": "B",
     "coords": [[lat, lon], ...], "duration_s": 842},
    ...
  ]
}
```
- **160 zone-pairs** covered (some zone pairs have no Porto analog)
- **25 tier-pairs** covered (full A-E × A-E matrix)
- Producer uses zone-pair match first, falls back to tier-pair, then random

### 7.3 GPS producer consumption (runtime)
```python
# producers/vehicle_gps_producer.py (coupled mode)
for trip in phase4_trips:
    polyline = pick_trajectory(
        trip["origin_zone"], trip["destination_zone"],
        trip["origin_class"], trip["dest_class"]
    )  # index lookup
    # schedule GPS pings every 4s along polyline
    # add ±20m Gaussian jitter + 5% blackout
    emit_to_kafka("raw.gps", ping)
```

---

## 8. Directory Structure (What Lives Where)

```
Taasimm/
├── data/
│   ├── hcp-data-casa/              # HCP census raw CSVs + portal screenshot
│   │   ├── 20260421_*.csv          # 6 indicator files (MOR RGPH 2024)
│   │   └── image.png               # portal screenshot (per-arr pop source)
│   ├── opportunity-find/           # Glovo commerce data
│   │   └── glovo_opportunity_score_from_commerce-2.geojson
│   ├── casa_synthesis/             # Phase 3 + Phase 4 outputs
│   │   ├── casa_trip_requests.parquet      # 500k synthetic trips
│   │   ├── casa_hourly_demand.parquet
│   │   ├── casa_od_matrix.parquet
│   │   ├── nyc_to_casa_zone_bridge.csv
│   │   ├── gps_trajectory_index.json       # Phase 4 index
│   │   └── eda_figures.png
│   ├── nyc-tlc/                    # NYC TLC raw Parquet (MinIO)
│   ├── train.csv                   # Porto raw CSV (MinIO)
│   ├── casablanca_arrondissements_v4.geojson
│   ├── casablanca_transit.geojson
│   ├── casablanca_road_graph.graphml        # OSMnx graph (MinIO)
│   ├── casablanca_road_nodes.npy
│   ├── casa_arrondissement_population_2024.csv
│   ├── curated_trajectories_v4.parquet      # Phase 2 output
│   ├── zone_mapping_v4.csv                  # Phase 1 output
│   └── .osrm_route_cache.json              # OSRM cache (gitignored)
│
├── notebooks/
│   ├── 01_porto_eda.ipynb                   # Week 1 EDA
│   ├── 02_zone_remapping.ipynb              # v3 (legacy)
│   ├── 02_zone_remapping_v4.ipynb           # v4 (current — A-E scoring)
│   ├── 03_h3_zone_remapping.ipynb           # H3 exploration
│   ├── 03_porto_trajectory_warping.ipynb    # Phase 2: warp + OSRM
│   ├── 03_zone_mapping.ipynb                # earlier exploration
│   ├── 04_nyc_etl_and_ml.ipynb              # NYC ETL preliminary
│   ├── 04_nyc_to_casa_synthesis.ipynb       # Phase 3: synthesize 500k trips
│   ├── casablanca_ae_map.html               # Phase 1 viz
│   └── casablanca_trajectories_v4.html      # Phase 2 viz
│
├── scripts/
│   ├── hcp_loader.py                        # HCP CSV loader (priors + pop)
│   ├── build_arrondissement_polygons_v4.py  # OSM → v4 geojson
│   ├── refetch_arrondissement_polygons.py   # OSM fetch retries
│   ├── _validate_v4_polygons.py             # polygon QA
│   ├── fetch_casablanca_transit.py          # OSM Overpass → transit geojson
│   ├── build_trajectory_index.py            # Phase 4: build index JSON
│   ├── generate_road_assets.py              # road graph + nodes .npy
│   ├── offline_projector.py                 # affine Porto→Casa helper
│   ├── remap_quality_gate.py                # zone-remap QA checks
│   └── verify_week3.py                      # Flink job verification
│
├── producers/
│   ├── config.py                            # paths + bbox + zone loader
│   ├── trip_request_producer.py             # --source casa_synth
│   ├── vehicle_gps_producer.py              # --mode coupled
│   └── Dockerfile                           # python:3.13-slim + deps
│
├── flink/jobs/
│   ├── gps_normalizer.py                    # Job 1: zone assign + centroid snap
│   ├── demand_aggregator.py                 # Job 2: 30s window + supply/demand
│   └── trip_matcher.py                      # Job 3: nearest vehicle + fallback
│
└── documents/
    ├── 00_master_status.md
    ├── 06_next_steps.md                     # sprint log
    ├── 07_adr_v1.md                         # Architecture Decision Record
    └── 11_data_pipeline_architecture.md     # ← this file
```

---

## 9. End-to-End Data Flow Diagram

```
┌──────────────────────────────────────────────────────────────────────────────┐
│  LAYER 1 — RAW INPUTS                                                        │
├──────────────────────────────────────────────────────────────────────────────┤
│  Porto taxi CSV        NYC TLC Parquet      HCP CSVs + screenshot           │
│  (1.7M trips)          (3M trips Q1 2023)   (6 ind. + pop.png)              │
│       │                      │                    │                          │
│       │                      │                    │                          │
│  OSM arrondissement    Glovo H3 geojson     OSM transit geojson             │
│  polygons v4           (934 cells)          (tram/bus)                       │
│       │                      │                    │                          │
└───────┼──────────────────────┼────────────────────┼──────────────────────────┘
        │                      │                    │
        │                      ▼                    ▼
        │     ┌──────────────────────────────────────────────────────┐
        │     │  PHASE 1 — Zone remapping v4                          │
        │     │  [02_zone_remapping_v4.ipynb]                         │
        │     │  A-E score = 0.30·commerce + 0.40·pop_density         │
        │     │            + 0.20·poi_div + 0.10·transit              │
        │     │                                                        │
        │     │  → zone_mapping_v4.csv (16 rows, A-E tiers)           │
        │     └──────────────┬────────────────────────────────────────┘
        │                    │
        ▼                    ▼
┌───────────────────────────────────────┐  ┌──────────────────────────────────┐
│  PHASE 2 — Porto trajectory warping   │  │  PHASE 3 — NYC→Casa synthesis    │
│  [03_porto_trajectory_warping.ipynb]  │  │  [04_nyc_to_casa_synthesis.ipynb]│
│                                        │  │                                  │
│  Porto polyline                        │  │  NYC temporal fingerprint        │
│   → affine warp → OSRM snap            │  │  × HCP per-zone pop              │
│   → 500 polylines stratified           │  │  × A-E demand multipliers        │
│     over 25 tier-pairs                 │  │  × Petit/Grand fleet split       │
│                                        │  │                                  │
│  → curated_trajectories_v4.parquet     │  │  → casa_trip_requests.parquet    │
│    (500 rows)                          │  │    (500k rows, 90 days)          │
└───────────────────────────┬────────────┘  └───────────────┬──────────────────┘
                            │                               │
                            └───────────────┬───────────────┘
                                            ▼
                 ┌──────────────────────────────────────────┐
                 │  PHASE 4 — Build trajectory index        │
                 │  [scripts/build_trajectory_index.py]     │
                 │                                          │
                 │  Index polylines by:                     │
                 │    - (origin_zone, dest_zone) → 160 pairs│
                 │    - (origin_tier, dest_tier) → 25 pairs │
                 │                                          │
                 │  → gps_trajectory_index.json (2.35 MB)   │
                 └──────────────────┬───────────────────────┘
                                    │
                                    ▼
                 ┌──────────────────────────────────────────┐
                 │  LAYER 4 — PRODUCERS (runtime)           │
                 │                                          │
                 │  trip_request_producer --source casa_synth│
                 │    replays casa_trip_requests.parquet    │
                 │    → Kafka raw.trips                     │
                 │                                          │
                 │  vehicle_gps_producer --mode coupled     │
                 │    pairs each trip w/ indexed polyline   │
                 │    → Kafka raw.gps                       │
                 └──────────────────┬───────────────────────┘
                                    │
                                    ▼
                 ┌──────────────────────────────────────────┐
                 │  LAYER 5 — FLINK STREAMING               │
                 │  Job 1: GPS normalizer (centroid snap)   │
                 │  Job 2: Demand aggregator (30s windows)  │
                 │  Job 3: Trip matcher (nearest + adjacent)│
                 └──────────────────┬───────────────────────┘
                                    │
                                    ▼
                 ┌──────────────────────────────────────────┐
                 │  LAYER 6 — CASSANDRA + GRAFANA           │
                 │  - vehicle_positions (24h TTL)           │
                 │  - trips (date_bucket partitioned)       │
                 │  - demand_zones (7d TTL)                 │
                 │                                          │
                 │  Grafana: TaaSim — Live Pipeline         │
                 └──────────────────────────────────────────┘
```

---

## 10. Quality Gates & Reproducibility

| Gate | Tool | Where |
|------|------|-------|
| Zone polygon validity | `_validate_v4_polygons.py` | Phase 1 |
| A–E tier balance (3-5 per tier) | notebook assertions | Phase 1 |
| Porto trip coverage (25 tier-pairs) | notebook assertions | Phase 2 |
| OSRM cache hit rate | `.osrm_route_cache.json` diagnostics | Phase 2 |
| NYC fingerprint extraction | `remap_quality_gate.py` | Phase 3 |
| 8 synthesis validation checks | EDA figure (visual + assertion) | Phase 3 |
| Trajectory index coverage | `build_trajectory_index.py` logs | Phase 4 |
| End-to-end Cassandra zone coverage | `scripts/verify_week3.py` | Runtime |

### Re-running from scratch
```powershell
# 1. Upload raw datasets to MinIO (one-time)
.\upload-datasets.ps1

# 2. Regenerate zone map (if polygons change)
jupyter execute notebooks/02_zone_remapping_v4.ipynb

# 3. Regenerate Porto trajectories (needs OSRM local)
jupyter execute notebooks/03_porto_trajectory_warping.ipynb

# 4. Regenerate NYC→Casa synthesis
jupyter execute notebooks/04_nyc_to_casa_synthesis.ipynb

# 5. Rebuild runtime index
.venv\Scripts\python.exe scripts\build_trajectory_index.py

# 6. Restart producers with new data
docker compose up -d --build --force-recreate gps-producer trip-producer
```

---

## 11. Open Questions / Future Work

- **Per-arrondissement HCP indicators**: request directly from HCP to replace province-wide priors
- **Real-time weather** (is_raining feature) — currently stubbed in ML pipeline
- **Multi-city scaling**: the same Porto + NYC + HCP + Glovo recipe should work for Rabat, Marrakech with minimal changes (just swap the per-zone CSVs)
- **Trajectory index refresh**: currently 500 polylines, could expand to 2000 for tighter tier coverage
- **Gamification of coupled mode**: add idle-cruising taxis to raise Job 3 match rate beyond 21%
