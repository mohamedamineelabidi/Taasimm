---
name: TaaSim Streaming Rules
description: Use when changing producers, Flink jobs, or compose wiring; protects Kafka topic contracts, event-time semantics, H3-zone consistency, and Cassandra write safety.
applyTo:
  - producers/**
  - flink/jobs/**
  - docker-compose.yml
---

# TaaSim Streaming Guardrails

## Scope
Apply these rules to Kafka producers, Flink streaming jobs, and stack wiring.

## Rules
- Preserve topic contracts exactly:
  - raw.gps
  - raw.trips
  - processed.gps
  - processed.demand
  - processed.matches
- Validate event_time parsing/propagation and watermark behavior after any schema or timing change.
- Keep H3 lookup and zone mapping usage consistent across producers and Flink jobs.
- Preserve zone adjacency behavior used by trip matching.
- Cassandra writes in streaming paths must be idempotent where possible.
- Do not introduce processing-time logic where event-time logic is already required.

## Quick Validation
```bash
rg -n "raw\.gps|raw\.trips|processed\.gps|processed\.demand|processed\.matches" producers flink/jobs docker-compose.yml
rg -n "event_time|WatermarkStrategy|for_bounded_out_of_orderness|dedup|h3|zone_mapping" flink/jobs producers
rg -n "INSERT INTO vehicle_positions|INSERT INTO demand_zones|INSERT INTO trips|IF NOT EXISTS" flink/jobs
```
