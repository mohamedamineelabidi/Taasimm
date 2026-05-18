---
name: TaaSim Documentation Rules
description: Use when updating README or project documents; enforces code-backed claims, historical labeling for old week plans, and avoids production-ready statements without proof.
applyTo:
  - README.MD
  - documents/**
---

# TaaSim Documentation Guardrails

## Scope
Apply these rules to README and all project documentation updates.

## Rules
- Documentation claims must reflect current implementation, not intended roadmap state.
- Mark old roadmap/week planning material as historical when no longer current.
- Do not claim production readiness unless code and validation evidence prove it.
- When behavior changes, update README and the relevant documents in the same change set.
- If there is uncertainty, state assumptions and unresolved gaps explicitly.

## Quick Validation
```bash
rg -n "@app\.(get|post|put|delete)\(" api/main.py
rg -n "/api/|Kafka|Flink|Cassandra|Spark|production|ready" README.MD documents
rg -n "Week|roadmap|planned|upcoming|done" documents/00_master_status.md documents
```
