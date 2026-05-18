---
name: TaaSim Data Rules
description: Use when editing data assets, notebooks, or generation scripts; enforces runtime-vs-study separation, dataset commit policy, and data README hygiene.
applyTo:
  - data/**
  - notebooks/**
  - scripts/**
---

# TaaSim Data Guardrails

## Scope
Apply these rules to data artifacts, notebook outputs, and script-generated files.

## Rules
- Keep runtime-required data clearly separated from raw or study-only data.
- Treat Porto and NYC raw datasets as offline donors, not live streaming inputs.
- Do not commit these paths unless explicitly requested in-task:
  - data/nyc-tlc/
  - data/hcp-data-casa/
  - data/train.csv
  - data/.osrm_route_cache.json
  - large generated Parquet outputs
- Before committing data changes, classify each changed artifact as:
  - runtime-required
  - study/regeneration
  - archive
  - removable
- Keep data/README.md updated whenever runtime-required files or data policy changes.

## Quick Validation
```bash
git check-ignore data/nyc-tlc data/hcp-data-casa data/train.csv data/.osrm_route_cache.json
rg -n "casa_trip_requests|curated_trajectories|h3_zone_lookup|zone_mapping" data/README.md
```
