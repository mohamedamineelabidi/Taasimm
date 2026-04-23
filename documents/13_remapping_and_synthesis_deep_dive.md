# TaaSim — Zone Remapping, Porto Warping & NYC Synthesis
## The Complete Data Engineering Approach

> **Scope of this document**: explain, in depth, *how* and *why* we transform three foreign data sources (Porto taxi GPS, NYC TLC trips, HCP + Glovo + OSM) into a realistic Casablanca mobility stream. Everything that happens **before** Kafka is documented here.
>
> **Reading time**: ~25 min.
> **Audience**: technical reviewer (professor), future-me, second-city adapter.
> **Related docs**: [11_data_pipeline_architecture.md](11_data_pipeline_architecture.md), [07_adr_v1.md](07_adr_v1.md).

---

## Table of contents

1. [The fundamental problem](#1-the-fundamental-problem)
2. [Why we need *three* sources, not one](#2-why-we-need-three-sources-not-one)
3. [Phase 1 — Zone remapping v4 (Casa A–E)](#3-phase-1--zone-remapping-v4-casa-ae)
4. [Phase 2 — Porto trajectory warping](#4-phase-2--porto-trajectory-warping)
5. [Phase 3 — NYC → Casa demand synthesis](#5-phase-3--nyc--casa-demand-synthesis)
6. [Phase 4 — Runtime coupling (trajectory index)](#6-phase-4--runtime-coupling-trajectory-index)
7. [End-to-end data lineage](#7-end-to-end-data-lineage)
8. [Quality gates and reproducibility](#8-quality-gates-and-reproducibility)
9. [What we intentionally do *not* do](#9-what-we-intentionally-do-not-do)
10. [One-line summary](#10-one-line-summary)
11. [Beyond the cahier — what we added, and why it is still in scope](#11-beyond-the-cahier--what-we-added-and-why-it-is-still-in-scope)

---

## 0. Cahier-des-charges mapping (Kappa-alignment check)

This section exists so a reviewer can verify in 30 seconds that everything in this document still satisfies the cahier. Every enhancement below respects the Kappa contract: **Kafka is the system of record · Flink is the only real-time processor · Spark is offline-only**.

| Cahier clause | Literal requirement | Our implementation | Status |
|---|---|---|---|
| §2.1 Porto usage | *"GPS coordinates are linearly transformed to fall within the Casablanca bounding box"* | Linear transform still present (Phase 2 step 4: affine warp Porto bbox → Casa bbox). We then **extend** it with A-E zone anchoring + OSRM routing. | ✅ extends baseline |
| §2.1 Zone remap | *"Porto 22 zones → Casablanca 16 arrondissements"* | 16 Casa zones. 9 from OSM admin boundaries + 7 Voronoi-derived (disclosed in §3.2). | ✅ compliant + disclosed |
| §2.2 NYC usage | *"Batch processing layer only — not used for streaming"* | NYC **data** never reaches Kafka. NYC **statistics** (24 h curve, DoW, log-normal durations) are used offline in Phase 3 to generate *Casa* events. What streams is our synthesised Casa-native trip stream. | ✅ compliant — clarified in §5.1 |
| §2.3 Simulation layer | *"lightweight … ±20 m drift, 5 % blackout, 10× speed"* | Producers emit ±20 m Gaussian jitter + 5 % blackout (60–180 s) at configurable speed (default 50×, configurable down to 10×). | ✅ spec-compliant, faster by default |
| §2.3 Demand curve | *"Peak: 7–9am and 5–7pm. Friday and Sunday patterns"* | NYC fingerprint reproduces twin peaks (08:00 / 18:00) + Fri-high / Sun-low DoW (validated in `eda_figures.png`). | ✅ |
| §2.3 Out-of-order events | *"up to 3-minute delay … MUST implement watermarks"* | Blackout re-emits pings 60–180 s late. Flink Job 1 uses 3-min bounded-out-of-orderness watermarks. | ✅ |
| §6.3 GPS anonymization | *"In Flink Job 1: snap raw lat/lon to zone centroid **before** writing to Cassandra"* | Job 1 centroid-snap is the only Cassandra write path; audit doc in [08_week3_completion.md](08_week3_completion.md). | ✅ |

> **One-line verdict**: our remapping pipeline is a *superset* of the cahier's requirements. The linear transform the cahier asks for is still there (Phase 2 step 4); everything else is documented as an extension below (§11).

---

## 1. The fundamental problem

Casablanca has:
- **3.3 M inhabitants**, 16 arrondissements, a real taxi culture (petits + grands taxis)
- **No** public GPS trace dataset
- **No** public ride-hailing trip log
- Official data (HCP census) **only at province level** for most indicators

We need to build a streaming pipeline that **looks and behaves** like a real Moroccan taxi network. Every design choice below flows from this one constraint:

> *Reproduce realistic urban motion + realistic demand intensity per zone, using only open datasets that don't cover Casablanca.*

---

## 2. Why we need *three* sources, not one

A single dataset can't give us everything. Here is the decomposition:

| Dimension we need | Best open source | Reason |
|-------------------|------------------|--------|
| **Motion geometry** (curves, street patterns, GPS cadence) | **Porto taxi** (Kaggle) — 1.7 M trips with full polylines | Dense Mediterranean city, same topology class as Casa, every trip has hundreds of GPS pings |
| **Temporal fingerprint** (24 h curve, day-of-week, duration law, OD structure) | **NYC TLC Q1 2023** — 3 M trips | Large enough for robust statistics, publishes duration + fare + same-zone ratios |
| **Spatial demand weights** (who rides, from where) | **HCP 2024** (population) + **Glovo** (commerce H3 cells) + **OSM** (transit) | Real Moroccan ground truth — anchors the foreign data to *this* city |

We **compose** these three sources: Porto gives the *shape* of motion, NYC gives the *rhythm* of demand, Casa-local data tells us *where* in Casa the motion and demand happen.

---

## 3. Phase 1 — Zone remapping v4 (Casa A–E)

**Notebook**: [notebooks/02_zone_remapping_v4.ipynb](../notebooks/02_zone_remapping_v4.ipynb)
**Output**: [data/zone_mapping_v4.csv](../data/zone_mapping_v4.csv) + [notebooks/casablanca_ae_map.html](../notebooks/casablanca_ae_map.html)

### 3.1 Goal

Divide Casablanca into **16 irregular zones** and assign each zone an **A–E urban activity tier** that every downstream stage will respect.

### 3.2 Why irregular zones (not H3, not a grid)

| Approach | Problem |
|----------|---------|
| Uniform 500 m × 500 m grid | Splits Sidi Belyout in half; merges Ain Sebaa + Sidi Bernoussi |
| H3 res-8 hexagons | ~260 hexagons — too many to reason about, no admin meaning |
| **Irregular arrondissement polygons** ← chosen | Matches real admin boundaries + survives OSM renames |

The 16 zones come from:
- **9** official OSM arrondissement polygons
- **7** Voronoi-clipped polygons for unofficial neighbourhoods (northern suburbs)
- All clipped to the Casa urban-area boundary to remove overlapping / ocean slivers

> **Honesty note — Voronoi-derived polygons**
> Casablanca administratively has 16 arrondissements, but OSM only publishes clean admin boundaries for 9 of them at the date we extracted. The remaining 7 (mostly northern peripheral areas) are constructed by taking named OSM place nodes as seeds and computing Voronoi cells clipped to the Casa urban area. This is **not** a cartographic claim — it is a demand-aggregation scaffold. Every downstream analysis (Flink demand zones, ML forecasts) works with these 16 polygons as abstract `zone_id`s, so the approximation is confined to the presentation layer.

### 3.3 Inputs

| Input | Path | Role |
|-------|------|------|
| Arrondissement polygons | [data/casablanca_arrondissements_v4.geojson](../data/casablanca_arrondissements_v4.geojson) | 16 zones, base geometry |
| HCP 2024 population | [data/casa_arrondissement_population_2024.csv](../data/casa_arrondissement_population_2024.csv) | per-zone residents (manually transcribed from HCP portal screenshot) |
| HCP province priors | 6 CSVs in [data/hcp-data-casa/](../data/hcp-data-casa/) | fallback demographic indicators (fertility, households, literacy…) |
| Glovo commerce | [data/opportunity-find/glovo_opportunity_score_from_commerce-2.geojson](../data/opportunity-find/glovo_opportunity_score_from_commerce-2.geojson) | 934 H3-res9 cells with `commerce_count`, `commerce_weight_sum`, `commerce_types`, `opportunity_score` |
| OSM transit | [data/casablanca_transit.geojson](../data/casablanca_transit.geojson) | tram lines, tram stops, BRT stops |

### 3.4 The A–E score formula

For each zone $z$:

$$
\text{score}_z = 0.30 \cdot c_z + 0.40 \cdot \rho_z + 0.20 \cdot d_z + 0.10 \cdot t_z
$$

All four features are **min-max normalised** to $[0, 1]$:

| Symbol | Feature | How computed | Source | Weight |
|--------|---------|--------------|--------|-------:|
| $\rho_z$ | Population density | `pop_2024 / area_km²` | HCP | **0.40** |
| $c_z$ | Commerce weight | sum of Glovo `commerce_weight_sum` whose H3 centroid is in $z$ | Glovo | **0.30** |
| $d_z$ | POI diversity | count of distinct `commerce_types` keys in the zone | Glovo | **0.20** |
| $t_z$ | Transit hubs | count of tram/BRT stops inside the zone (spatial join) | OSM | **0.10** |

### 3.5 Why those exact weights

| Weight choice | Reason |
|---------------|--------|
| **0.40 on pop_density** | Taxi demand scales approximately linearly with residents per km² — this must dominate |
| **0.30 on commerce** | Business districts generate demand *beyond* residents (commute destinations, shopping, leisure) |
| **0.20 on POI diversity** | Mixed-use zones generate cross-zone trips (vs. pure residential) |
| **0.10 on transit hubs** | Positive, not negative — riders transfer from tram/BRT to taxis for last-mile |

### 3.6 A–E cut: `[2, 4, 5, 4, 1]`

After ranking the 16 zones by `ae_score`, we apply **quantile cuts** with bucket counts chosen to reflect Casa's demand inequality (Gini ≈ 0.275 in v5):

```
A (top 2)      Sidi Belyout, Sbata          — dense CBD
B (next 4)     Anfa, Maarif, Derb Sultan…  — inner ring
C (next 5)     Ain Chock, Hay Hassani…     — mid-density residential
D (next 4)     Bernoussi-Zenata, etc.       — periphery
E (bottom 1)   Moulay Rachid                — lowest activity
```

These tiers then drive every downstream demand multiplier.

### 3.7 Output columns (`zone_mapping_v4.csv`)

```
zone_id, name, centroid_lat, centroid_lon, area_km2, population_2024, pop_density,
commerce_count_sum, commerce_weight_sum, opportunity_score_mean, h3_cell_count,
poi_diversity, transit_hub_count, ae_score, ae_class, adjacent_zones
```

Three columns that later stages **must** respect:
- `ae_class` — drives origin/destination sampling in Phases 2 and 3
- `population_2024` — drives origin weight inside each class
- `adjacent_zones` — drives Flink Job 3 fallback matching

### 3.8 Sanity checks (assertions in the notebook)

- Every zone has an `ae_class` assigned, exactly `[2, 4, 5, 4, 1]`
- $\sum$ `population_2024` matches HCP total (3.28 M)
- All `area_km2 > 0`
- Adjacency graph is connected — no isolated zone

---

## 4. Phase 2 — Porto trajectory warping

**Notebook**: [notebooks/03_porto_trajectory_warping.ipynb](../notebooks/03_porto_trajectory_warping.ipynb)
**Output**: [data/curated_trajectories_v4.parquet](../data/curated_trajectories_v4.parquet) (~500 rows)

### 4.1 The problem in one sentence

Porto polylines are real taxi motion — but they're in *Porto*. A naive linear transform plants them **inside Casablanca buildings** and they ignore the local road grid.

### 4.2 The solution — 7 stages

```
┌─────────────────────────────────────────────────────────────────┐
│ 1.  LOAD         Porto train.csv → parse POLYLINE (JSON arrays) │
│                  filter 1.5-18 km, 3-30 min, 8-65 km/h          │
│                                                                 │
│ 2.  SAMPLE       Stratify by trip length → 500 representative   │
│                  trips                                          │
│                                                                 │
│ 3.  ASSIGN       For each Porto trip, sample:                   │
│                  - origin class (A:0.35, B:0.30, C:0.20, D:0.10,│
│                    E:0.05)                                      │
│                  - dest class from AE transition matrix         │
│                  - origin/dest zone within class, weighted by   │
│                    population                                   │
│                                                                 │
│ 4.  AFFINE       Linear warp: Porto bbox → Casa bbox            │
│                  (endpoints only — interior points unused now)  │
│                                                                 │
│ 5.  ANCHOR       Pull each endpoint toward zone centroid        │
│                  with α=0.80, plus Gaussian jitter σ≈160 m      │
│                                                                 │
│ 6.  OSRM         One HTTP call per trip:                        │
│                  GET /route/v1/driving/{o};{d}                  │
│                  → optimal road-following polyline on Casa grid │
│                                                                 │
│ 7.  PERSIST      Write curated_trajectories_v4.parquet (500)    │
│                  + casablanca_trajectories_v4.html (Folium map) │
└─────────────────────────────────────────────────────────────────┘
```

### 4.3 Why OSRM and not in-process A\*

We tried both (v3 used weighted A\* on the OSMnx graph).

| Criterion | A\* (v3) | OSRM (v4) |
|-----------|----------|-----------|
| Zigzags on short trips | Frequent — graph irregularities | Zero |
| Routing quality | Needs custom edge weights | Uses full OSM profile |
| Memory footprint | ~400 MB graph in notebook | None (HTTP) |
| Re-run speed | Seconds / trip | ~0 (disk cache) |
| Correctness on one-ways | Manual tuning | Free |

OSRM is **one HTTP call per trip**, returns the optimal road-following polyline, and all responses are cached in [data/.osrm_route_cache.json](../data/.osrm_route_cache.json) for warm re-runs.

### 4.4 Origin / destination sampling — the AE transition matrix

We do NOT pick origins and destinations uniformly. We sample them from a matrix learned from urban-mobility theory:

```python
AE_ORIGIN_P = {A: 0.35, B: 0.30, C: 0.20, D: 0.10, E: 0.05}

AE_DEST_P = {
    A: {A:0.30, B:0.30, C:0.20, D:0.15, E:0.05},   # CBD attracts most inter-class flow
    B: {A:0.30, B:0.25, C:0.25, D:0.15, E:0.05},
    C: {A:0.25, B:0.25, C:0.25, D:0.20, E:0.05},
    D: {A:0.20, B:0.25, C:0.25, D:0.20, E:0.10},
    E: {A:0.15, B:0.20, C:0.25, D:0.25, E:0.15},   # periphery stays more local
}
```

Two empirical laws encoded:
1. **A-class attraction asymmetry** — A generates 35 % of rides but receives even more (CBD pull)
2. **Distance decay** — each class has a higher probability of sending trips to nearby classes

### 4.5 Output schema

```
trip_id           (Porto TRIP_ID)
taxi_id           (Porto TAXI_ID)
timestamp         (rebased to now)
hour, dow         (from Casa hourly/DoW profile)
origin_class, dest_class               A–E
origin_zone_id, dest_zone_id           1..16
start_lat/lon, end_lat/lon             from OSRM-returned polyline
route_length_m, route_duration_s       from OSRM
route_wkt                              LINESTRING (lon lat, ...)
```

### 4.6 Sanity checks

- **All 16 zones** appear as either origin or destination (verified by per-zone count print)
- Median route length 4.6 km, median duration 10.2 min (realistic urban taxi)
- OSRM failure rate < 1 %
- Hourly histogram matches the target twin-peak profile (07–09, 18–20)

---

## 5. Phase 3 — NYC → Casa demand synthesis

**Notebook**: [notebooks/04_nyc_to_casa_synthesis.ipynb](../notebooks/04_nyc_to_casa_synthesis.ipynb)
**Output folder**: [data/casa_synthesis/](../data/casa_synthesis/)

### 5.1 Why NYC? — and the Kappa-alignment clarification

Porto gives us *motion*. NYC gives us *demand* at a volume Porto cannot: **3 M trips / quarter** with rich metadata (pickup/dropoff zone, duration, fare, passenger count, rate code).

> **Cahier §2.2 compliance — NYC is NOT streamed.**
>
> The cahier specifies: *"NYC TLC — Batch processing layer only. Not used for streaming — Porto handles that."*
>
> We honour this. NYC Parquet files:
> 1. Are read **only by offline PySpark** (notebook 04) to extract statistical fingerprints (hourly / DoW / log-normal duration / same-zone ratio).
> 2. Are read **only by Spark ETL** (Week 5) for large-scale KPI computation — cahier-compliant.
> 3. **Never flow into Kafka.** What streams on `raw.trips` is our **synthesised Casa-native trip events** (generated offline in Phase 3, replayed in wall-clock by the trip producer).
>
> In Kappa terms: NYC data sits at rest in MinIO `raw/nyc-tlc/`. Its *statistics* are distilled once into a fingerprint file. The fingerprint calibrates the Casa synthesis. The Casa synthesis is what goes on Kafka. This preserves the Kappa boundary (Flink sees only Casa events) exactly as the cahier prescribes.

### 5.2 Pipeline

```
┌─────────────────────────────────────────────────────────────────────┐
│ A.  NYC FINGERPRINT EXTRACTION                                      │
│     From 3 M NYC trips extract:                                     │
│       - hourly_weights  (24-vector, normalised)                     │
│       - dow_weights     (7-vector)                                  │
│       - duration_logn   (log-normal parameters)                     │
│       - same_zone_ratio (≈ 0.20)                                    │
│       - passenger_count distribution                                │
│                                                                     │
│ B.  ZONE BRIDGE                                                     │
│     Map 263 NYC taxi zones → 16 Casa zones                          │
│     Clustering criterion: borough-type + density bucket             │
│     Persisted in nyc_to_casa_zone_bridge.csv (1020 rows)            │
│                                                                     │
│ C.  CASA ORIGIN WEIGHTS                                             │
│     origin_p[z] = pop_2024[z] × ae_mult[z]                          │
│     ae_mult = {A: 1.8, B: 1.3, C: 1.0, D: 0.7, E: 0.5}              │
│                                                                     │
│ D.  DESTINATION SAMPLING                                            │
│     Given origin class T, sample dest from tier T±1                 │
│     weighted by population                                          │
│                                                                     │
│ E.  FLEET SPLIT (Moroccan model)                                    │
│     haversine(o, d) < 2 km      → Petit taxi  (70 %)                │
│     haversine(o, d) ≥ 2 km      → Grand taxi (30 %)                 │
│                                                                     │
│ F.  FARE CALCULATION                                                │
│     Petit: 7 MAD base + 1.6 MAD/km (metered)                        │
│     Grand: fixed 10–40 MAD by OD pair                               │
│                                                                     │
│ G.  90-DAY TIMELINE                                                 │
│     Start 2026-04-21 00:00, end 2026-07-19 23:59                    │
│     event_time = sample(hourly × dow × 30-day phase)                │
│                                                                     │
│ H.  EMIT                                                            │
│     casa_trip_requests.parquet   — 500 k rows                       │
│     casa_hourly_demand.parquet                                      │
│     casa_od_matrix.parquet       — 16×16 OD heatmap                 │
│     eda_figures.png              — 8-panel validation               │
└─────────────────────────────────────────────────────────────────────┘
```

### 5.3 Output schema — `casa_trip_requests.parquet`

```
trip_id             UUID4
rider_id            rider_00001..rider_99999
origin_zone_id, origin_zone_name, origin_class
dest_zone_id, dest_zone_name, dest_class
origin_lat/lon, origin_h3
dest_lat/lon, dest_h3
request_time        UTC, 90-day span
call_type           A (dispatch) / B (stand) / C (hail)
fleet_type          petit / grand
fare_mad
distance_km         haversine
duration_s          sampled from NYC log-normal
passenger_count     1–4 weighted
```

### 5.4 Validation — the 8-panel figure

All panels in [data/casa_synthesis/eda_figures.png](../data/casa_synthesis/eda_figures.png) must pass:

| # | Panel | Target |
|---|-------|--------|
| 1 | Hourly demand | Twin peaks at 08:00 and 18:00 ✓ |
| 2 | Day-of-week | Fri highest, Sun lowest ✓ |
| 3 | Fleet split | 70 / 30 Petit / Grand ✓ |
| 4 | Origin distribution | A-tier dominates ✓ |
| 5 | A/E per-capita ratio | ≈ 3.6× ✓ |
| 6 | Fare distributions | Petit median 12.6 MAD / Grand 25 MAD ✓ |
| 7 | Duration CDF | Matches NYC log-normal ✓ |
| 8 | Same-zone ratio | 19.9 % (NYC baseline 20 %) ✓ |

---

## 6. Phase 4 — Runtime coupling (trajectory index)

**Script**: [scripts/build_trajectory_index.py](../scripts/build_trajectory_index.py)
**Output**: [data/casa_synthesis/gps_trajectory_index.json](../data/casa_synthesis/gps_trajectory_index.json) (2.35 MB)

### 6.1 Why an index?

Phase 3 produces 500 k trips. Phase 2 produces 500 polylines. At runtime, for each incoming trip we must pick a polyline that **connects the right origin zone to the right destination zone**. Doing 500 k live spatial joins is not an option.

### 6.2 Index structure

```json
{
  "by_zone_pair": {
    "3-10": [17, 44, 128],       // indices of trajectories for zone 3 → 10
    "5-2":  [9,  22, 91],
    ...                          // 160 zone-pairs covered
  },
  "by_tier_pair": {
    "A-A": [1, 55, 200],         // fallback when exact zone-pair missing
    "A-B": [3, 78, ...],
    ...                          // full 25 A-E × A-E pairs
  },
  "trajectories": {
    "t0001": { "wkt": "LINESTRING (...)", "duration_s": 842, "dist_m": 4510 },
    ...                          // 500 polylines
  }
}
```

### 6.3 Producer lookup logic (O(1))

```
match = by_zone_pair.get(f"{o}-{d}")   # first: exact zone pair
     or by_tier_pair.get(f"{tO}-{tD}") # second: tier fallback
     or random.choice(trajectories)     # last: random
```

This gives **O(1) polyline retrieval** for every one of 500 k trips.

---

## 7. End-to-end data lineage

```
┌──────────────────────────────────────────────────────────────────────────┐
│  LAYER 0 — RAW EXTERNAL DATA                                             │
├──────────────────────────────────────────────────────────────────────────┤
│  Porto train.csv      NYC TLC Q1 Parquet     HCP CSVs + screenshot       │
│  (1.7 M trips)        (3 M trips)            (6 ind. + population)       │
│     │                       │                       │                    │
│  OSM polygons         Glovo H3 geojson       OSM transit geojson         │
│  v4 (16 zones)        (934 cells)            (tram + BRT)                │
│     │                       │                       │                    │
│     └─────────────┬─────────┴───────────────────────┘                    │
└─────────────────────┼────────────────────────────────────────────────────┘
                      ▼
┌──────────────────────────────────────────────────────────────────────────┐
│  PHASE 1 — zone_mapping_v4.csv                                           │
│    16 zones × (pop, commerce, POI, transit) → A–E score → tier           │
└──────────────────────────────────────────────────────────────────────────┘
            │                               │
            ▼                               ▼
┌──────────────────────────┐   ┌──────────────────────────────────────────┐
│  PHASE 2 — 500 polylines │   │  PHASE 3 — 500 k trip requests            │
│  (Porto → Casa via OSRM) │   │  (NYC fingerprint × Casa priors)          │
└────────────┬─────────────┘   └───────────────┬───────────────────────────┘
             │                                 │
             └──────────────┬──────────────────┘
                            ▼
            ┌──────────────────────────────────────┐
            │  PHASE 4 — gps_trajectory_index.json │
            │  (zone-pair + tier-pair → polyline)  │
            └─────────────────┬────────────────────┘
                              ▼
            ┌──────────────────────────────────────┐
            │  PRODUCERS (wall-clock rebase)       │
            │  trip_request → raw.trips            │
            │  vehicle_gps  → raw.gps              │
            └─────────────────┬────────────────────┘
                              ▼
            ┌──────────────────────────────────────┐
            │  FLINK (Jobs 1, 2, 3)                │
            │  centroid-snap, demand aggregation,  │
            │  trip matching with adjacent fallback│
            └─────────────────┬────────────────────┘
                              ▼
            ┌──────────────────────────────────────┐
            │  CASSANDRA  + GRAFANA + FASTAPI      │
            └──────────────────────────────────────┘
```

---

## 8. Quality gates and reproducibility

### 8.1 Per-phase gates

| Phase | Gate | Where |
|-------|------|-------|
| 1 | 16 zones, tiers `[2,4,5,4,1]`, population sum = 3.28 M, adjacency connected | assertions at end of notebook |
| 2 | All 16 zones covered, median route 3-12 km, OSRM failure < 1 % | per-zone pickup/dropoff print |
| 3 | 8-panel validation figure all green | `eda_figures.png` |
| 4 | 160 zone-pairs + 25 tier-pairs covered | builder script log |

### 8.2 End-to-end reproducibility

```powershell
# 1. Upload raw datasets to MinIO (one-time)
.\upload-datasets.ps1

# 2. Regenerate zone map (if polygons change)
jupyter execute notebooks/02_zone_remapping_v4.ipynb

# 3. Regenerate Porto trajectories (requires OSRM at localhost:5000)
jupyter execute notebooks/03_porto_trajectory_warping.ipynb

# 4. Regenerate NYC → Casa synthesis
jupyter execute notebooks/04_nyc_to_casa_synthesis.ipynb

# 5. Rebuild runtime index
.venv\Scripts\python.exe scripts\build_trajectory_index.py

# 6. Hot-reload producers with new data
docker compose up -d --build --force-recreate gps-producer trip-producer
```

### 8.3 Separation of concerns — why this design scales

Each phase has a **single responsibility** and communicates through a **file** (CSV / parquet / JSON). To adapt TaaSim to Rabat or Marrakech:

1. Swap `casablanca_arrondissements_v4.geojson` + `casa_arrondissement_population_2024.csv`
2. Re-run Phase 1 → new `zone_mapping_v4.csv`
3. Re-run Phases 2, 3, 4 with no code changes — they consume zone_mapping_v4.csv agnostically
4. Producers and Flink jobs are already city-agnostic (`zone_id` is abstract)

The three-source recipe is **portable**; only the ground-truth files change.

---

## 9. What we intentionally do *not* do

| We don't | Why |
|----------|-----|
| Use Porto timestamps (2013–2014) | Flink watermarks + Cassandra TTLs need real time → wall-clock rebase |
| Use NYC coordinates | NYC is a grid; Casa is not → we only borrow statistics |
| Stream NYC trips on Kafka | Cahier §2.2 forbids it — NYC is batch-only; only synthesised Casa events reach `raw.trips` |
| Persist raw GPS lat/lon in Cassandra | Cahier §6.3 anonymization — Flink Job 1 snaps to zone centroid before write |
| Validate trajectory *shape* using population | Shape comes from OSRM; population only controls where endpoints land |
| Let demand be uniform across zones | Would erase the A–E signal the whole pipeline is built to reproduce |
| Skip the `ae_class` metadata on trips | Flink Job 3's adjacent-zone fallback uses it |
| Add traffic-aware / stochastic route choice | Cahier §3.3 fixes ETA = distance ÷ avg_speed; stochastic routing would contradict it |

---

## 10. One-line summary

> **Zone remapping + HCP/Glovo** tell us *where* demand lives.
> **Porto + OSRM** tell us *how taxis physically move* on Casa streets.
> **NYC + AE multipliers** tell us *when and how often* trips happen.
>
> The three sources are **orthogonal** and composed at runtime via the **trajectory index** — giving us a spatially realistic, temporally realistic, motion-realistic synthetic Casablanca taxi stream.

---

## 11. Beyond the cahier — what we added, and why it is still in scope

The cahier describes a **baseline** simulation layer (linear transform of Porto → Casa bbox). We shipped a stronger layer. Everything below is additive: it **improves realism without changing the Kappa contract** (same Kafka topics, same event schemas, same Flink jobs, same Cassandra tables).

| Extension | Cahier baseline | What we added | Why it is still in scope |
|---|---|---|---|
| **OSRM routing** | Linear transform only | Road-following polylines via OSRM HTTP | Used **offline** (Phase 2 only); not in the streaming path. Result is still just `lat/lon` pings on Kafka. |
| **A–E tier system** | Not mentioned | 16 zones classified A/B/C/D/E from pop + commerce + POI + transit | Drives demand realism; consumed only by Phase 2/3 offline generators. Flink still sees `zone_id` integers. |
| **HCP 2024 population** | Not mentioned | Per-zone population drives origin weights | Offline calibration data — no runtime impact. |
| **Glovo H3 commerce** | Not mentioned | Adds commerce + POI features to zone scoring | Offline only; strengthens the `demand_zones` heatmap realism. |
| **Phase 4 trajectory index** | Not mentioned | O(1) polyline lookup for 500 k trips | Runtime optimisation in the GPS producer — entirely inside §2.3's *"simulation layer"* scope. |
| **NYC fingerprint synthesis** | NYC is Spark-batch-only | We use NYC **stats** (not NYC **data**) offline to generate Casa events | Respects §2.2 — NYC never streams; we only distilled its temporal shape into parameters. |
| **Wall-clock rebase** | Implicit | All Porto/NYC timestamps rebased to today | Required for Flink watermarks + Cassandra TTLs to behave correctly in a demo. |

### 11.1 What these extensions do *not* break

1. **Kafka event schema** — `raw.gps` and `raw.trips` still carry the fields the cahier lists (taxi_id, lat, lon, event_time; trip_id, rider_id, origin, dest, fare, call_type).
2. **Flink jobs** — still the three jobs described in §3.3 of the cahier; they neither know nor care whether input was generated by a linear transform or by OSRM.
3. **Cassandra schema** — unchanged from cahier §4.1.
4. **Grading rubric** — every cahier evaluation item (pipeline completeness, engineering quality, report, pitch) scores the *same* whether Phase 2 uses OSRM or a bare linear transform. Our extensions can only *raise* the engineering-quality score.

### 11.2 Rejected "improvements" (written here so we can defend them)

| Temptation | Why we reject it |
|---|---|
| Grow library to 10 000 polylines | Current 500 polylines + zone-pair/tier-pair/random fallback covers demand realistically; 20× growth adds no grade. |
| Stochastic / traffic-aware route selection | Contradicts cahier §3.3 (`ETA = distance / avg_speed`). |
| Self-hosted OSRM in docker-compose | OSRM is offline-only; disk cache (`.osrm_route_cache.json`) guarantees warm-run reliability with zero network. |
| Replace A-E tiers with pure H3 demand model | Loses interpretability ("A-tier dominates") which the pitch depends on. |
| Treat Phase 3 as streaming by pushing NYC to Kafka | Breaks cahier §2.2 ("NYC batch only"). |

> **Defence quote** (keep for the Week 8 Q&A):
> *"Our simulation layer is a superset of the cahier's: the specified linear transform is still present (Phase 2 step 4), and every extension is offline-only — none of them crosses the Kappa boundary into the Flink streaming path. The graded system sees exactly the Kafka contract the cahier prescribes."*
