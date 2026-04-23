# Next Steps and Update Log

Last updated: 2026-04-19

## Immediate Next Steps (Week 7 — Security & Integration)

1. JWT auth hardening: rotate secrets, HTTPS/TLS for API
2. GPS anonymization audit: verify centroid snapping in Flink Job 1
3. Integration test: end-to-end pipeline (producer → Flink → Cassandra → API)
4. SLA measurement: trip match <5s P95, GPS freshness <15s, API <500ms at 20 req/s
5. Checkpoint recovery test: kill Flink TM, verify resume from checkpoint

## Pending Verifications

- Verify trip matching accuracy (nearest vehicle assignment)
- Verify adjacent zone fallback works when no vehicle in request zone
- Verify demand zone window triggers every 30s
- Verify S3 Sink archival over extended producer runs (8+ hours)

## Update Log

- 2026-04-21: **Grafana dashboard modernized** — [config/grafana-dashboard.json](config/grafana-dashboard.json) updated: title → "TaaSim — Live Pipeline (Casablanca)", refresh 10s→5s, time window now-1h→now-15m, added `$date_bucket` template variable (custom list: 2026-04-21/20/19/18, default today). Replaced 3 hardcoded `date_bucket='2026-04-19'` and 1 `='2026-04-18'` in trip panels with `$date_bucket`. Grafana auto-reloaded via provisioning (30s interval). Verified via REST API: 7 panels, 1 variable, 5s refresh. Dashboard now shows today's data by default and lets user switch date bucket from UI.

- 2026-04-21: **Match rate tuning** — Increased fleet to 2000 taxis and producer speed to 50× (both trip + GPS producers) in `docker-compose.yml`. Truncated Cassandra tables for clean measurement. After 3 min warm-up: **21.0% matched / 79.0% no_vehicle** (2,003 matched / 7,521 unmatched), up from initial 4.2% at fleet=500 speed=10. Flink jobs stable (25 consecutive checkpoints, 0 failures). All 16 zones populated. Root cause of remaining no_vehicle: coupled-mode taxis only emit GPS during active trips (~75 pings then drop); at 50× speed ~193 trips expanded in 3 min → ~150 concurrent taxis across 16 zones. Would need idle-cruising behavior in GPS producer to raise further (out of scope for Week 3).
- 2026-04-21: **Flink output semantics validated** — (1) GPS Normalizer: centroid snap confirmed (taxi_0192 consistently at 33.60512/-7.54537 in zone 15); (2) Demand Aggregator: 30s windows emit per-zone `active_vehicles`/`pending_requests`/`ratio` (zone 5 sample: ratio=1 when 2 vehicles / 2 requests); (3) Trip Matcher: schema valid (`taxi_id`, `eta_seconds`, `fare`, `status`∈{matched,no_vehicle}); sample match fare=54.63 MAD, ETA=60s. TripFanout correctly sends trip to origin + adjacent zones. Matching cost = 0.7·dist_km + 0.3·idle_penalty. No distance cap (could add later).
- 2026-04-21: Phase 4 wiring complete — Trip producer now replays casa_trip_requests.parquet via `--source casa_synth` (default) with wall-clock rebase + speed control; GPS producer gains `--mode coupled` (default) that pairs each Phase-4 trip with a Phase-3 OSRM polyline via data/casa_synthesis/gps_trajectory_index.json (500 routes, 160 zone-pairs, 25 tier-pairs — full A–E coverage). Fleet pool 500 taxis, 4s ping cadence, ±20m noise + 5% blackout preserved. Smoke-tested locally (5 trips → 29 GPS events). New file scripts/build_trajectory_index.py. Edited files: producers/config.py (paths), producers/trip_request_producer.py (run_from_parquet), producers/vehicle_gps_producer.py (run_coupled).
- 2026-04-20: Phase 4 NYC→Casa synthesis complete — 500k Casa trip requests over 90 days (2026-04-21 to 2026-07-19) from NYC TLC Q1 2023 fingerprint. 8/8 validation checks pass: fleet 70.6% Petit / 29.4% Grand, A/E per-capita ratio 3.65×, morning+evening peaks [8,18,19], same-zone ≤19.9%, Petit median 12.6 MAD, Grand median 25.0 MAD. Artifacts in data/casa_synthesis/: casa_trip_requests.parquet (15.9 MB), casa_hourly_demand.parquet (0.63 MB), casa_od_matrix.parquet (0.01 MB), nyc_to_casa_zone_bridge.csv (1020 rows), eda_figures.png. Notebook: 04_nyc_to_casa_synthesis.ipynb.
- 2026-04-19: Week 6 ML Pipeline complete — Feature engineering (183,981 rows), GBT model (RMSE 3.71, R² 0.75, 45.8% improvement), FastAPI API with JWT auth. Model saved to s3a://mldata/models/demand_v1/. Docker: added taasim-api service + numpy persistence. Evidence: 10_week6_ml_pipeline.md.
- 2026-04-19: Week 5 Spark ETL complete — Porto ETL (43 MiB, 12 partitions), NYC ETL (4.3 MiB, 192K demand rows), KPI Analytics (6 datasets). All outputs verified in MinIO curated/. Docker RAM upgraded (worker 4g, master 3g limit). Evidence doc: 09_week5_spark_etl.md.
- 2026-04-17: Week 3 complete — All 3 Flink jobs deployed and running. TaskManagers scaled to 3 (12 slots). GPS Normalizer + Demand Aggregator + Trip Matcher operational. Kafka UI added. Data flow verified end-to-end. Created verification evidence doc (08_week3_completion.md).
- 2026-04-17: Flink Job 2 (Demand Aggregator) deployed: processes.gps + raw.trips → 30s tumbling windows → demand_zones Cassandra + processed.demand topic.
- 2026-04-17: Flink Job 3 (Trip Matcher) deployed: raw.trips + processed.gps → nearest vehicle matching → trips Cassandra + processed.matches topic.
- 2026-04-12: Week 3 Sprint 3 completed — all 4 deliverables done.
- 2026-04-12: Custom Flink Docker image built (PyFlink 1.18.1 + cassandra-driver + kafka-python).
- 2026-04-12: Flink Job 1 GPS Normalizer deployed and running (2/2 tasks, job f1100660).
- 2026-04-12: Centroid anonymization verified — raw GPS snapped to zone centroids in Cassandra.
- 2026-04-12: Flink checkpointing to MinIO verified — 10+ checkpoints in s3://curated/flink-checkpoints/.
- 2026-04-12: Late event watermark test created (scripts/test_late_events.py).
- 2026-04-12: Grafana vehicle tracking dashboard deployed (Geomap + zone bar chart + table).
- 2026-04-12: Week 2 Sprint 2 completed — all 3 deliverables done.
- 2026-04-12: Kafka Connect S3 Sink deployed and verified (raw.gps + raw.trips → kafka-archive/).
- 2026-04-12: Cassandra schema INSERT + SELECT tested on all 3 tables.
- 2026-04-12: ADR v1 created (documents/07_adr_v1.md) — Kappa, partition keys, MinIO, retention.
- 2026-04-12: Updated copilot-instructions.md with Week 2 progress (12 containers, S3 Sink configs, ADR).
- 2026-04-03: Rewrote copilot-instructions.md with full 11-section architecture reference (343 lines).
- 2026-04-03: Full infrastructure verification passed — all 11 containers healthy (Kafka, MinIO, Cassandra, Flink, Spark, Grafana, Jupyter).
- 2026-04-03: Added change tracking guidelines to copilot-instructions.md.
- 2026-04-03: Added full status documentation package under documents/.
- 2026-04-03: Completed v3 geographic remapping and validated notebook outputs.
- 2026-04-03: Identified Docker daemon runtime blocker for producer smoke test.
