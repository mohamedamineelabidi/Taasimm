---
name: taasim-api-contract
description: Use this skill when API behavior, route docs, Cassandra integration, or ML fallback behavior is unclear or broken and the FastAPI contract must be corrected and documented.
---

# TaaSim API Contract

## Trigger
Use this skill when endpoints and docs diverge, deployment dependencies drift, or /api/trips behavior must be formally decided and enforced.

## Workflow
1. Inspect API surface and schemas in api/main.py.
2. Verify runtime dependencies in api/requirements.txt and image assumptions in api/Dockerfile.
3. Compare README API table against implemented routes and response behavior.
4. Cross-check Cassandra schema expectations against config/cassandra-init.cql and current API writes/reads.
5. Audit ML behavior:
   - startup model load path
   - heuristic fallback path
   - health exposure (model_loaded)
6. Decide and document /api/trips contract mode:
   - publish to Kafka raw.trips
   - write directly to Cassandra
   - stay demo-only (explicitly documented)
7. Apply smallest code+doc edits, then run contract verification.

## Required Checks
- api/main.py
- api/requirements.txt
- api/Dockerfile
- README API table
- Cassandra schema
- /api/trips integration mode decision

## Validation Commands
```bash
rg -n "@app\\.(get|post|put|delete)\\(" api/main.py
rg -n "/api/|auth|forecast|trips|zones|health" README.MD
rg -n "cassandra|kafka|raw.trips|model|fallback|PYSPARK_ENABLED|MODEL_PATH" api/main.py
rg -n "cassandra|kafka|pyspark|fastapi|uvicorn" api/requirements.txt
rg -n "FROM|COPY|CMD|ENV" api/Dockerfile
rg -n "CREATE TABLE|vehicle_positions|demand_zones|trips" config/cassandra-init.cql
docker compose config --quiet
```

## Output Contract
Return:
1. Contract diff table: endpoint, current_behavior, expected_behavior, required_change.
2. Explicit /api/trips decision and rationale.
3. Verification checklist (docs, code, runtime) with pass/fail markers.
