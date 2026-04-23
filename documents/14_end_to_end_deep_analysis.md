# TaaSim — End-to-End Deep Architectural Analysis

> **Scope**: Every moving part — from Porto CSV on disk to Grafana pixel — explained mechanically, with code excerpts, schemas, flow diagrams, and critical evaluation.
>
> **Last updated**: 2026-04-22
> **Related docs**: [11_data_pipeline_architecture.md](11_data_pipeline_architecture.md), [13_remapping_and_synthesis_deep_dive.md](13_remapping_and_synthesis_deep_dive.md), [07_adr_v1.md](07_adr_v1.md), [00_master_status.md](00_master_status.md).

---

## Table of Contents

1. [Global Architecture & Data Flow](#1--global-architecture--data-flow)
2. [Trajectory Remapping Logic — Porto → Casablanca](#2--trajectory-remapping-logic--porto--casablanca)
3. [Regions & Spatial Modeling](#3--regions--spatial-modeling)
4. [Matching Logic (Flink Job 3)](#4--matching-logic-flink-job-3)
5. [Flink Processing — Job-by-Job](#5--flink-processing--job-by-job)
6. [Dataset Generation](#6--dataset-generation)
7. [File-by-File Breakdown](#7--file-by-file-breakdown)
8. [Storage Layer](#8--storage-layer)
9. [Monitoring (Grafana)](#9--monitoring-grafana)
10. [Critical Evaluation](#10--critical-evaluation)

---

## 1. 🔁 Global Architecture & Data Flow

### 1.1 Architectural style: **Kappa**
Kafka is the **only** system of record for live data. Flink is the **only** real-time processor. Spark is **offline only** (ETL + ML training). This contract is enforced: NYC data never enters Kafka; only synthesised Casa events do.

### 1.2 The 12-container stack

```
┌───────────────────────────────────────────────────────────────────────────┐
│ OFFLINE (one-time / batch)                                                │
│  Notebooks ──► data/casa_synthesis/*.parquet  (500k synthetic Casa trips) │
│  Notebooks ──► data/curated_trajectories_v4.parquet (500 OSRM polylines)  │
│  build_trajectory_index.py ──► gps_trajectory_index.json                  │
└─────────────────────────────┬─────────────────────────────────────────────┘
                              │ (mounted into producer containers)
                              ▼
┌───────────────────────────────────────────────────────────────────────────┐
│ LIVE STREAMING PATH                                                       │
│                                                                           │
│  trip-producer   ──► raw.trips    ┐                                       │
│                                   ├──► Flink Job 1 (GPS Normalizer)       │
│  gps-producer    ──► raw.gps      │     • dedup, zone assign, centroid    │
│                                   │       snap, watermark 3min            │
│                                   │     ├─► Cassandra: vehicle_positions  │
│                                   │     └─► Kafka: processed.gps          │
│                                   │                                       │
│                                   ├──► Flink Job 2 (Demand Aggregator)    │
│                                   │     • 30s tumbling window per zone    │
│                                   │     • union(processed.gps, raw.trips) │
│                                   │     ├─► Cassandra: demand_zones       │
│                                   │     └─► Kafka: processed.demand       │
│                                   │                                       │
│                                   └──► Flink Job 3 (Trip Matcher)         │
│                                         • keyed state: vehicles per zone  │
│                                         • adjacency fallback              │
│                                         ├─► Cassandra: trips              │
│                                         └─► Kafka: processed.matches      │
│                                                                           │
│  Kafka Connect S3 Sink  ──► MinIO s3://kafka-archive/{raw.gps,raw.trips}/ │
│  Flink checkpoints      ──► MinIO s3://curated/flink-checkpoints/         │
│                                                                           │
│  Cassandra ◄── Grafana (Cassandra datasource plugin)                      │
│  Cassandra ◄── FastAPI  (JWT, GBT model loaded from mldata/models/)       │
└───────────────────────────────────────────────────────────────────────────┘
```

### 1.3 Step-by-step data flow (one GPS ping)

1. **Producer** picks an active trip from `casa_trip_requests.parquet`, looks up its polyline in `gps_trajectory_index.json`, interpolates a position at the current wall-clock tick, adds ±20m Gaussian jitter (5% chance of 60–180s blackout delay), computes H3-res-9 cell → publishes JSON to `raw.gps`.
2. **Kafka** partitions by `taxi_id` across 4 partitions (7-day retention).
3. **Kafka Connect** mirrors the raw event to `s3://kafka-archive/raw.gps/YYYY/MM/DD/HH/*.parquet` (replay capability).
4. **Flink Job 1** consumes, extracts `event_time` for watermark (3-min bounded lateness), keys by `taxi_id`, deduplicates via `ValueState<last_ts>`, validates bounds + speed, looks up `h3_index → zone_id` in an in-memory broadcast map, snaps lat/lon to `(zone_centroid + hash-based jitter)`, writes async to Cassandra `vehicle_positions`, and forwards to `processed.gps`.
5. **Flink Job 2** unions `processed.gps` + `raw.trips` on a `(city, zone_id)` key, opens a 30-second event-time tumbling window, counts **distinct** `taxi_id` and trip events, computes `ratio = requests / max(1, vehicles)`, writes `demand_zones` (TTL 7d) and forwards to `processed.demand`.
6. **Flink Job 3** maintains `MapState<zone_id, Map<taxi_id, position>>` (TTL 60s). On each trip request it scores vehicles in the origin zone by `0.7·distance_km + 0.3·idle_penalty`, falls back to `adjacent_zones` after 5s, removes matched vehicle from state (prevents double-booking), computes `eta_seconds` and fare, writes `trips` and forwards to `processed.matches`.
7. **Grafana** queries Cassandra every ~5s (Geomap panel on `vehicle_positions`, bar/heatmap on `demand_zones`) → user sees live taxi dots and zone heatmap.
8. **FastAPI** serves `/api/demand/forecast` — GBT model loaded once from `s3a://mldata/models/demand_v1/` at startup; queries Cassandra for live features, returns prediction in <500ms.

---

## 2. 🌍 Trajectory Remapping Logic — Porto → Casablanca

Remapping happens in **four phases**, all offline, all reproducible.

### 2.1 Phase 1 — Casa zone skeleton (`02_zone_remapping_v4.ipynb`)

Produces [../data/zone_mapping_v4.csv](../data/zone_mapping_v4.csv) with **16 irregular arrondissement polygons** (9 OSM official + 7 Voronoi-clipped) scored on four features and quantile-cut into A–E tiers:

$$\text{score}_z = 0.40 \cdot \rho_z + 0.30 \cdot c_z + 0.20 \cdot d_z + 0.10 \cdot t_z$$

- $\rho_z$: population density (HCP 2024 / area_km²)
- $c_z$: Glovo commerce weight (934 H3 res-9 cells spatially joined)
- $d_z$: POI diversity (Shannon-ish count of distinct commerce types)
- $t_z$: tram/BRT stop count (OSM)

Tiers: **A** (top 2 zones: Sidi Belyout, Sbata) → **E** (Moulay Rachid). The A/E per-capita demand ratio ≈ **3.65×** matches HCP income inequality data — a real ground-truth sanity check.

### 2.2 Phase 2 — Trajectory warping (`03_porto_trajectory_warping.ipynb`)

**Input**: Porto `train.csv` (1.7M trips with dense POLYLINE arrays).
**Output**: `data/curated_trajectories_v4.parquet` — 500 stratified Casa polylines.

**7-stage algorithm**:

| Stage | Transform | Purpose |
|-------|-----------|---------|
| 1. Load & filter | `duration ∈ [3, 30]min`, `length ∈ [1.5, 18]km`, `speed ∈ [8, 65]km/h` | Discard unrealistic Porto trips |
| 2. Stratified sample | 500 trips across duration deciles | Preserve short/medium/long variety |
| 3. AE-class sampling | Draw origin class from `{A:.35, B:.30, C:.20, D:.10, E:.05}`; dest from 5×5 transition matrix | Encodes CBD-pull + distance-decay laws |
| 4. Affine warp | `casa_lat = lerp(porto_lat, [41.135,41.174], [33.45,33.68])` (endpoints only) | Linear bbox transform (cahier §2.1) |
| 5. Anchor | Pull endpoints toward zone centroid with α=0.80 + Gaussian jitter σ≈160m | Align to actual A–E zones |
| 6. OSRM snap | `GET /route/v1/driving/{o};{d}` → road-following polyline | **Fixes buildings-crossing bug of v3** |
| 7. Persist | Parquet + cached at `data/.osrm_route_cache.json` | Idempotent re-runs |

**Why OSRM beats in-process A\***: v3 used OSMnx + weighted A\* → visible zigzags on short trips. OSRM uses full OSM driving profile → zero zigzags, handles one-ways, <1% failure rate.

### 2.3 Filtering unrealistic trajectories

Three defensive gates:
- **Source-side**: Porto `MISSING_DATA=False`, duration/speed bounds, origin inside Porto metro bbox.
- **OSRM-side**: Route must succeed; snap-distance cap 333m.
- **Land validation**: Every endpoint must fall inside a Casa arrondissement polygon (spatial join).

### 2.4 Behavioral validation

The AE transition matrix encodes two empirical urban-mobility laws:
- **A-attraction asymmetry**: A generates 35% of rides but receives more (CBD pull).
- **Distance decay**: Each class skews dest toward same or adjacent tier.

Low-activity zones (E-tier: Moulay Rachid) have correspondingly low taxi density in the simulation — not artificially boosted. This is visible in the per-zone origin bar chart in `eda_figures.png` (red = A, purple = E).

### 2.5 Is the remapping realistic?

**Verdict — partially realistic, honestly calibrated**:

| Aspect | Realism | Evidence |
|--------|---------|----------|
| Motion geometry (street-following polylines) | ✅ High | OSRM produces real Casa driving paths |
| Temporal demand (hourly/DoW curve) | ✅ High | NYC fingerprint validated in 8-panel figure; twin peaks 08:00/18:00 |
| Per-zone demand weights | 🟡 Medium | HCP pop + Glovo commerce are real priors, but HCP indicators beyond pop are province-level (uniformly applied) |
| Zone boundaries | 🟡 Medium | 9/16 are real OSM admins, 7/16 are Voronoi approximations (explicitly disclosed) |
| OD structure | 🟡 Medium | AE transition matrix is plausible but not measured from Casa data |
| Fleet model | ✅ High | Moroccan petit/grand split (70/30), real tariff model (7+1.6 MAD/km petit; 10–40 MAD grand) |

---

## 3. 🗺️ Regions & Spatial Modeling

### 3.1 Zone definition stack

| Layer | Granularity | Used for |
|-------|-------------|----------|
| **16 arrondissement polygons** | Zone-level | Demand aggregation, Cassandra partition key, Grafana heatmap |
| **H3 res-9 cells** (~174m hex) | Sub-zone | Fast GPS→zone lookup in producer (O(1) via `h3_zone_lookup.json`) |
| **Zone centroid + bounds** | Point | Cassandra anonymization, Flink jitter clamp |
| **Adjacency graph** | Zone-pair | Flink Job 3 fallback matching |

### 3.2 How a raw GPS point becomes a zone

```python
# Producer-side (O(1))
h3_cell = h3.latlng_to_cell(lat, lon, 9)
zone_id = h3_zone_lookup.get(h3_cell)
if zone_id is None:                 # fallback: ring search up to radius 5
    for r in range(1, 6):
        for nb in h3.grid_ring(h3_cell, r):
            if nb in h3_zone_lookup: ...

# Flink-side (trust producer, verify bounds)
if not (33.45 <= lat <= 33.68 and -7.72 <= lon <= -7.48): drop
zone = h3_zone_lookup[h3_cell]      # broadcast map
```

### 3.3 Role of Porto and Casablanca

- **Porto** is a *motion donor*. Its GPS polylines shape the Casa trajectories but its coordinates never appear in Kafka. Only the **geometry topology** is borrowed; endpoints are re-anchored to Casa zones before OSRM snapping.
- **Casablanca** is the *target universe*. All streaming, all Cassandra state, all dashboards operate exclusively in Casa lat/lon + zone_id space.

---

## 4. 🔗 Matching Logic (Flink Job 3)

### 4.1 Data structures
- `MapState<zone_id, Map<taxi_id, {lat, lon, speed_kmh}>>` — TTL 60s, auto-expires offline taxis.
- In-memory `Set<trip_id>` of already-matched trips (reset at 50k) — prevents Kafka-retry double-matches.

### 4.2 Scoring function
```
cost = 0.7 * haversine_km(trip_origin, vehicle_pos)
     + 0.3 * (1.0 if vehicle.speed_kmh < 5 else 0.0)   # idle bonus
```
Lower is better. Idle bonus encourages matching stationary vehicles (not ones mid-trip).

### 4.3 Matching flow
```
on trip_request(trip_id, origin_zone):
    if trip_id in matched_set: return                   # idempotency
    candidates = state[origin_zone]
    if not candidates:
        wait up to 5s (via timer)
        candidates = union(state[z] for z in adjacent_zones[origin_zone])
    if not candidates:
        emit {status: "no_vehicle"} → processed.matches
        write trips(status="no_vehicle") → Cassandra
        return
    best = argmin(cost(v) for v in candidates)
    state[best.zone].remove(best.taxi_id)               # release lock
    matched_set.add(trip_id)
    emit {status: "matched", taxi_id, eta, fare} → processed.matches
    write trips(...) → Cassandra
```

### 4.4 Observed match rate
~**21%** under current simulation density. The remaining 79% are `no_vehicle` because taxis only emit GPS while on an active trip (the coupled-mode design). This is documented in the copilot-instructions as a known artifact of the simulation model — solvable by adding an "idle cruising" mode for off-trip taxis.

---

## 5. ⚡ Flink Processing — Job-by-Job

| Concern | Job 1 | Job 2 | Job 3 |
|---------|-------|-------|-------|
| **Input** | `raw.gps` | `processed.gps` + `raw.trips` | `processed.gps` + `raw.trips` |
| **Watermark** | BoundedOutOfOrderness 3min | 10s + 15s idleness | 10s + 15s idleness |
| **Key** | `taxi_id` | `(city, zone_id)` | `zone_id` |
| **State** | `ValueState<last_ts>` TTL 5m | none (window-scoped) | `MapState<zone_id, vehicles>` TTL 60s |
| **Window** | none (per-event) | `Tumbling(30s)` event-time | none + 5s processing-time timer |
| **Transforms** | dedup → bounds/speed check → h3→zone → centroid+jitter snap | `COUNT(DISTINCT taxi_id)`, `COUNT(trips)`, ratio | nearest-neighbor score + adjacency fallback |
| **Cassandra sink** | `vehicle_positions` (TTL 24h) | `demand_zones` (TTL 7d) | `trips` (no TTL) |
| **Kafka sink** | `processed.gps` | `processed.demand` | `processed.matches` |
| **Checkpoint** | RocksDB → `s3a://curated/flink-checkpoints/` every 60s, EXACTLY_ONCE | same | same |

### 5.1 Duplicate handling
- **GPS**: `last_ts` state skip — vehicles ping every 4s but Kafka retries can replay; any `event_time ≤ last_seen` is dropped.
- **Windowing correctness**: Job 2 counts `DISTINCT taxi_id` (not raw ping count) — the P6 fix recorded in the commit log. Without this, a taxi emitting 7 pings in 30s would inflate `active_vehicles` by 7×.
- **Trip matching**: dual defense — Cassandra `IF NOT EXISTS` + in-memory matched set.

### 5.2 Late events
Producer injects 5% blackouts with 60–180s delay. Flink's 3-min watermark accepts all of these; anything beyond 3min is dropped. Verified by `scripts/test_late_events.py` which sends a 2-min-old event (accepted) and a 5-min-old event (dropped).

### 5.3 GPS Normalizer centroid snap (anonymization)

```python
# Deterministic jitter per taxi_id (hash-based):
h = int(hashlib.md5(taxi_id.encode()).hexdigest()[:8], 16)
angle = (h % 360) * math.pi / 180
radius = 0.0008 + (h % 1000) / 1e6      # ~80–180m in degrees
jitter_lat = radius * math.cos(angle)
jitter_lon = radius * math.sin(angle)
display_lat = centroid_lat + jitter_lat
display_lon = centroid_lon + jitter_lon
# Clamp to zone bounds
display_lat = clamp(display_lat, zone_bounds.lat_min, zone_bounds.lat_max)
display_lon = clamp(display_lon, zone_bounds.lon_min, zone_bounds.lon_max)
```

Privacy: all vehicles snap to centroid; usability: stable per-taxi jitter spreads dots on the Grafana map.

---

## 6. 🧱 Dataset Generation

Live Kafka events come from **three offline artifacts**:

| Artifact | Script / Notebook | Content | Size |
|----------|-------------------|---------|------|
| `casa_trip_requests.parquet` | `04_nyc_to_casa_synthesis.ipynb` | 500k trips, 90 days, NYC fingerprint × Casa spatial weights | ~30 MiB |
| `curated_trajectories_v4.parquet` | `03_porto_trajectory_warping.ipynb` | 500 OSRM polylines stratified by AE tier pair | ~5 MiB |
| `gps_trajectory_index.json` | `scripts/build_trajectory_index.py` | Lookup: zone-pair (160) + tier-pair (25) → polyline indices | 2.35 MiB |

### 6.1 Synthesis assumptions
1. **NYC temporal curve transfers** — peak hours, DoW pattern, log-normal trip durations, ~20% same-zone ratio.
2. **HCP population is the dominant origin weight** — modulated by AE multipliers `{A:1.8, B:1.3, C:1.0, D:0.7, E:0.5}`.
3. **Moroccan fleet model**: `haversine < 2km → petit (70%)`, else `grand (30%)`. Petit: metered 7 MAD + 1.6/km. Grand: fixed 10–40 MAD.
4. **Wall-clock rebase** — parquet event_time is re-anchored to "now" at producer start so Flink watermarks and Cassandra TTLs function in real-time.

### 6.2 Replay cadence
- `--speed 50` default → 1 wall-clock hour = 50 simulated hours.
- `--fleet-size 2000` → sustains ≥150 concurrent active taxis for healthy match rate.
- Ping interval: 4s between GPS pings per active trip.

---

## 7. 📁 File-by-File Breakdown

### 7.1 Producers
- **`producers/config.py`** — shared constants: Kafka bootstrap, topic names, Casa bbox, H3 lookup loader, zone-mapping path, coupled-mode enums.
- **`producers/vehicle_gps_producer.py`** — two modes: `coupled` (default, replays Phase-4 trips on Phase-2 polylines) and `live` (random-walk within zone). Emits GPS JSON to `raw.gps` with 4s cadence, ±20m jitter, 5% blackout.
- **`producers/trip_request_producer.py`** — two modes: `casa_synth` (default, reads `casa_trip_requests.parquet`) and `random` (synthetic uniform). Emits trip JSON to `raw.trips` at wall-clock-scaled rate.
- **`producers/Dockerfile`** — python:3.11-slim + kafka-python + h3 + pandas + pyarrow.

### 7.2 Flink
- **`flink/jobs/gps_normalizer.py`** — Job 1 (dedup, validate, zone, anonymize).
- **`flink/jobs/demand_aggregator.py`** — Job 2 (tumbling window, distinct count).
- **`flink/jobs/trip_matcher.py`** — Job 3 (stateful matching + adjacency fallback).
- **`flink/jobs/zone_data.py`** — shared: loads zone CSV, H3 lookup, zone centroids, bounds, adjacency.
- **`flink/entrypoint-wrapper.sh`** — copies mounted extra JARs (s3, kafka, cassandra connectors) into `/opt/flink/lib/` before starting Flink.

### 7.3 Spark
- **`spark/etl_porto.py`** — Porto CSV → dedup → bbox transform → validate → Parquet at `s3a://curated/trips/` partitioned by `year_month`.
- **`spark/etl_nyc.py`** — 3 months NYC Parquet → schema-drift normalization → outlier filter → per-zone/hour aggregation.
- **`spark/compute_kpis.py`** — 6 KPI datasets: `trips_per_zone`, `hourly_demand`, `daily_pattern`, `zone_hour_heatmap`, `coverage_gaps`, `call_type_breakdown`.
- **`spark/feature_engineering.py`** — 30-min slot aggregation, `demand_lag_1d/7d`, `rolling_7d_mean` → `s3a://mldata/features/`.
- **`spark/train_demand_model.py`** — GBT training (maxDepth=5, maxIter=50), temporal split, beats naive baseline by 45.8% (RMSE 3.71 vs 6.84).

### 7.4 Scripts (highlights)
- **`build_trajectory_index.py`** — builds dual-key lookup JSON for coupled mode.
- **`build_arrondissement_polygons_v4.py`** — 9 OSM + 7 Voronoi zones.
- **`submit-flink-jobs.ps1`** / **`verify-flink-jobs.ps1`** — deployment + end-to-end health.
- **`register-connectors.ps1`** — Kafka Connect S3 Sink REST registration.
- **`ensure-cassandra-schema.ps1`** — idempotent schema migration.
- **`test_late_events.py`** — watermark validation.

### 7.5 Notebooks
| Notebook | Purpose |
|----------|---------|
| `01_porto_eda.ipynb` | Porto EDA: schema, durations (median 14min), call types (A/B/C 12/48/40%), peak hours |
| `02_zone_remapping_v4.ipynb` | **Current** — 16 zones × A–E scoring with Glovo+HCP+OSM |
| `03_porto_trajectory_warping.ipynb` | 7-stage Porto→Casa warping with OSRM |
| `03_h3_zone_remapping.ipynb` | H3-based zone alternative (not adopted) |
| `04_nyc_etl_and_ml.ipynb` | NYC ETL + GBT training exploration |
| `04_nyc_to_casa_synthesis.ipynb` | **Current** — 500k Casa trip synthesis from NYC fingerprint |

### 7.6 API
- **`api/main.py`** — FastAPI + JWT (HS256, 24h, `rider`/`admin` roles). Endpoints: `POST /api/demand/forecast` (loads GBT at startup, <500ms), `GET /api/vehicles/{zone_id}`, `POST /api/trips`, `GET /api/health`.

---

## 8. 🗄️ Storage Layer

### 8.1 Cassandra (keyspace `taasim`)

| Table | Partition Key | Clustering | TTL | Rationale |
|-------|--------------|-----------|-----|-----------|
| `vehicle_positions` | `(city, zone_id)` | `event_time DESC, taxi_id ASC` | 24h | Grafana queries "vehicles in zone X right now" → single partition scan |
| `trips` | `(city, date_bucket)` | `created_at DESC, trip_id ASC` | none | Historical archive; `date_bucket` prevents unbounded partitions |
| `demand_zones` | `(city, zone_id)` | `window_start DESC` | 7d | Heatmap queries "recent demand for zone X" → single partition |

All partition keys are query-driven (not normalization-driven), per Cassandra best practice. Documented in [07_adr_v1.md](07_adr_v1.md).

### 8.2 MinIO (S3)

| Bucket/Prefix | Writer | Reader | Content |
|---------------|--------|--------|---------|
| `raw/porto-trips/` | Manual | Spark ETL | 1.8 GiB CSV |
| `raw/nyc-tlc/` | Manual | Spark ETL | 150 MiB Parquet (3 months) |
| `raw/kafka-archive/` | Kafka Connect S3 Sink | Spark ETL / replay | Mirror of `raw.gps` + `raw.trips` |
| `curated/trips/` | Spark ETL | Spark ML | Cleaned geo-enriched Parquet |
| `curated/flink-checkpoints/` | Flink | Flink (recovery) | RocksDB state |
| `mldata/features/` | Spark | Spark MLlib | Feature matrix |
| `mldata/models/demand_v1/` | Spark MLlib | FastAPI | PipelineModel |

---

## 9. 📊 Monitoring (Grafana)

**Dashboard** ([../config/grafana-dashboard.json](../config/grafana-dashboard.json)): Cassandra datasource plugin (`hadesarchitect-cassandra-datasource`).

| Panel | Query | Purpose |
|-------|-------|---------|
| Vehicle Geomap | `SELECT lat, lon, taxi_id FROM vehicle_positions WHERE city='casablanca' AND zone_id IN (1..16)` | Live taxi dots on Casa map |
| Zone Demand Bar | `SELECT zone_id, pending_requests, active_vehicles FROM demand_zones` (latest per zone) | Supply/demand per zone |
| Match Events Table | `SELECT created_at, trip_id, status, eta_seconds FROM trips` | Recent trip outcomes |
| KPI Heatmap | Grid of zone × hour demand | Demand rhythm |

Refresh every 5s. Measured live SLAs: GPS freshness ≈ 4s (target <15s ✅), match P95 ≈ 1.2s (target <5s ✅).

---

## 10. 📌 Critical Evaluation

### 10.1 Weaknesses / unrealistic assumptions

| # | Issue | Severity | Evidence |
|---|-------|----------|----------|
| 1 | **Only 9/16 zones are "real"** — 7 are Voronoi approximations where OSM lacks clean admin polygons | Medium | [13_remapping_and_synthesis_deep_dive.md](13_remapping_and_synthesis_deep_dive.md) §3.2 |
| 2 | **HCP non-population indicators are province-uniform** across all 16 zones (fertility, literacy, household size) | Medium | `scripts/hcp_loader.py` returns single dict |
| 3 | **AE transition matrix is theoretical, not measured** — we don't have Casa OD ground truth | Medium | `03_porto_trajectory_warping.ipynb` §4.4 |
| 4 | **Only 500 polyline templates** for 500k trips → each polyline is reused ~1000× | Medium | `gps_trajectory_index.json` |
| 5 | **Taxis emit GPS only during active trips** → ~79% `no_vehicle` match rate | High | Job 3 state model |
| 6 | **Linear bbox transform distorts Porto's 39×9km footprint into Casa's 23×26km** | Low (mitigated by OSRM re-routing) | `etl_porto.py::transform_coord` |
| 7 | **Centroid anonymization loses precision** — every taxi shown at zone centroid + jitter, not real position | By design (privacy) — but limits analytic value |
| 8 | **No weather / traffic / events features** in ML model | Medium | `feature_engineering.py` |
| 9 | **Kafka Connect has no dead-letter queue** configured — malformed events silently drop | Low | `connect-s3-sink-gps.json` |
| 10 | **No Kafka topic ACLs** — cahier §6.3 `raw.*` producer-only ACL not yet applied | Medium (Week 7 pending) | master status |

### 10.2 Concrete improvements (prioritized)

1. **Add idle-cruising mode to GPS producer** — when not on a trip, fleet emits random-walk pings within assigned zone. Raises match rate from 21% → ~70%.
2. **Weight polyline reuse by tier-pair rarity** — current index picks uniformly from matches; tier-pair-aware sampling prevents over-reuse of 5 A→A polylines.
3. **Introduce per-arrondissement HCP interpolation** — use spatial demographic smoothing (e.g., dasymetric mapping via Glovo commerce as covariate) to vary non-population indicators across zones.
4. **Add a DLQ topic** (`raw.gps.dlq`) and JMX metric for parse failures.
5. **Add Kafka SASL + ACLs** per cahier §6.3 — Week 7.
6. **Add weather/traffic enrichment** — OpenWeatherMap API + Casa holiday calendar in ML features; likely pushes R² from 0.75 → ~0.85.
7. **Upgrade zone polygons to v5** using paid HCP/admin cadastral data for all 16 arrondissements (eliminate Voronoi fallback).
8. **Implement checkpoint-recovery demo** — kill Flink TM, verify Job 1 resumes from MinIO checkpoint (cahier §3.3 demo requirement, still pending).
9. **Stress test**: run producer at `--speed 500` for 10 min; measure Kafka lag + Cassandra write latency under burst.
10. **Replace in-memory `matched_set`** in Job 3 with Flink keyed state — current Set leaks memory at scale (reset at 50k is a hack).

### 10.3 What's genuinely strong

- Clean Kappa boundary — NYC never touches Kafka; cahier compliance is provable.
- Real OSRM road-following polylines — no building-crossing artifacts.
- Measurable ground-truth calibration — A/E ratio 3.65× matches HCP income data.
- Full offline reproducibility — every artifact derives from versioned notebook + script.
- Proper ML hygiene — temporal split, baseline comparison (45.8% gain), model versioning.
- Cassandra schema is query-driven with justified partition keys.

---

## TL;DR

TaaSim is a textbook Kappa streaming stack where the **creative engineering is front-loaded** (phases 1–4 data synthesis) and the **live pipeline is deliberately boring** (standard Flink event-time, watermarks, stateful joins, Cassandra serving). The system is architecturally sound and cahier-compliant; its main weaknesses are data-realism (Voronoi zones, theoretical OD matrix, 500 polyline templates, GPS only during active trips) — all tractable with incremental improvements that don't require re-architecting.
