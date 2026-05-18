---
name: TaaSim API Rules
description: Use when editing FastAPI code, dependencies, or API docs; ensures route parity with README, safe metadata handling, deployable ML behavior, and explicit demo security boundaries.
applyTo:
  - api/**
---

# TaaSim API Guardrails

## Scope
Apply these rules to API routes, dependencies, container behavior, and API docs alignment.

## Rules
- API route behavior must match README API documentation.
- Do not hardcode stale zone metadata when runtime zone files are available.
- ML model loading must be Docker-deployable or clearly documented as fallback mode.
- Auth, CORS, and secret handling must remain demo-safe and explicitly documented.
- If an endpoint is demo-only, label it clearly in both code comments and README.

## Quick Validation
```bash
rg -n "@app\.(get|post|put|delete)\(" api/main.py
rg -n "/api/|auth|forecast|trips|zones|health" README.MD
rg -n "JWT_SECRET|PYSPARK_ENABLED|MODEL_PATH|fallback|zone_mapping" api/main.py
```
