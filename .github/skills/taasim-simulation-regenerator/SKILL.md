---
name: taasim-simulation-regenerator
description: Use this skill when the user asks to regenerate Casablanca simulation artifacts end-to-end (zone maps, H3 lookup, warped trajectories, trajectory index, and NYC to Casa synthetic trips) with correct git policy.
---

# TaaSim Simulation Regenerator

## Trigger
Use this skill when simulation artifacts are stale, missing, or need deterministic regeneration for demos, tests, or sprint handoff.

## Workflow
1. Regenerate v4 geographic base:
   - Build arrondissement polygons with scripts/build_arrondissement_polygons_v4.py.
   - Re-run notebooks/02_zone_remapping_v4.ipynb to refresh data/zone_mapping_v4.csv.
2. Regenerate H3 lookup:
   - Re-run notebooks/03_h3_zone_remapping.ipynb to refresh data/h3_zone_lookup.json.
3. Regenerate Porto trajectory warping:
   - Re-run notebooks/03_porto_trajectory_warping.ipynb to refresh data/curated_trajectories_v4.parquet.
4. Rebuild OSRM-assisted cache/index artifacts:
   - Ensure data/.osrm_route_cache.json behavior is warm-cache and deterministic.
   - Run scripts/build_trajectory_index.py to produce data/casa_synthesis/gps_trajectory_index.json.
5. Regenerate NYC to Casa synthesis:
   - Re-run notebooks/04_nyc_to_casa_synthesis.ipynb to produce data/casa_synthesis/casa_trip_requests.parquet and related outputs.
6. Enforce commit policy for generated files:
   - confirm what is committed vs ignored before finalizing.

## Committed vs Ignored
Expected committed (core runtime metadata):
- data/zone_mapping_v4.csv
- data/h3_zone_lookup.json
- data/casa_synthesis/gps_trajectory_index.json

Expected ignored (large or regeneration outputs):
- data/casa_synthesis/casa_trip_requests.parquet
- data/curated_trajectories_v4.parquet
- data/.osrm_route_cache.json
- data/.projector_checkpoint.json
- data/nyc-tlc/
- data/hcp-data-casa/

## Validation Commands
```bash
python scripts/build_arrondissement_polygons_v4.py
python scripts/remap_quality_gate.py --rows 100000
python scripts/build_trajectory_index.py
rg -n "zone_mapping_v4|h3_zone_lookup|curated_trajectories_v4|casa_trip_requests|gps_trajectory_index|osrm_route_cache" data README.MD documents
Test-Path data/zone_mapping_v4.csv ; Test-Path data/h3_zone_lookup.json ; Test-Path data/casa_synthesis/gps_trajectory_index.json
git check-ignore data/casa_synthesis/casa_trip_requests.parquet data/curated_trajectories_v4.parquet data/.osrm_route_cache.json data/.projector_checkpoint.json data/nyc-tlc data/hcp-data-casa
```

## Output Contract
Return:
1. Regeneration status per phase (zone map, H3, warping, index, synthesis).
2. Artifact manifest with produced_path, producer_step, tracked_or_ignored.
3. Exact rerun order to reproduce identical outputs.
