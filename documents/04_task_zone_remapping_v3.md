# Task 4 - Zone Remapping Porto -> Casablanca (v3)

Status: Completed (major quality improvement)
Last updated: 2026-04-03

## Objective

Remap Porto GPS trips into Casablanca geography with realistic zone assignment.

## Problem Found in Earlier Version

- Uniform 4x4 mapping was mathematically valid but geographically illogical.
- Arrondissements were not placed according to real Casablanca structure.

## v3 Redesign Delivered

- Replaced uniform mapping with irregular, geography-aligned tessellation (16 zones)
- Added adjacency metadata for downstream matching logic
- Shifted Porto latitude transform range to center density in Casablanca urban core:
  - old: 41.140 to 41.185
  - new: 41.085 to 41.195

## Files Updated

- data/zone_mapping.csv
- producers/config.py
- notebooks/02_zone_remapping.ipynb (rebuilt and executed)
- data/zone_centroids.csv
- data/remapped_trips_sample.csv
- notebooks/casablanca_zone_map.html

## Validation Metrics (from executed notebook)

- Origins inside zones: 47,325 / 47,516 (99.6%)
- Destinations inside zones: 45,549 / 47,516 (95.9%)
- Edge clamping rate: 0.85%
- Bounding-box validation: PASS

## Result

- Remapping is now both technically valid and geographically coherent for Casablanca.
