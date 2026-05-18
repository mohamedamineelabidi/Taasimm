---
name: taasim-data-audit
description: Use this skill when the user asks to audit data and notebook artifacts, classify what is runtime-required vs regenerable/archive/removable, or clean repository footprint safely.
---

# TaaSim Data Audit

## Trigger
Use this skill when requests involve data folder hygiene, notebook output cleanup, or deciding what should stay tracked vs ignored.

## Workflow
1. Inventory tracked and ignored artifacts in data/ and notebooks/.
2. Verify these required checkpoints explicitly:
   - data/zone_mapping.csv
   - data/zone_mapping_v4.csv
   - data/h3_zone_lookup.json
   - data/casa_synthesis/casa_trip_requests.parquet
   - data/casa_synthesis/gps_trajectory_index.json
   - data/nyc-tlc/
   - data/hcp-data-casa/
3. Inspect notebook outputs and generated HTML assets.
4. Classify each checked artifact into one bucket only:
   - runtime-required
   - study/regeneration
   - archive
   - removable
5. For each non-runtime artifact, provide one action: keep ignored, regenerate, archive, or delete.
6. Report mismatches between intended policy (.gitignore/docs) and actual git tracking.

## Validation Commands
```bash
rg --files data notebooks
git check-ignore data/train.csv data/nyc-tlc data/hcp-data-casa data/casa_synthesis/casa_trip_requests.parquet
rg -n "\"outputs\": \[" notebooks --glob "*.ipynb"
rg -n "html|plotly|folium|iframe" notebooks --glob "*.ipynb" --glob "*.html"
git ls-files data notebooks
```

## Output Contract
Return:
1. Classification table with columns: path, class, reason, action, git_policy.
2. Drift list of tracked files that should be ignored (or the reverse).
3. Ordered cleanup plan with low-risk first.
