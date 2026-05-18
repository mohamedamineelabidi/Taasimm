---
name: taasim-streaming-debug
description: Use this skill when Kafka -> Flink -> Cassandra flow is stale, delayed, missing records, or behavior is inconsistent and you need structured runtime plus code-path debugging.
---

# TaaSim Streaming Debug

## Trigger
Use this skill when topics are empty, Flink jobs are unhealthy, Cassandra tables are stale, or event-time logic is suspected.

## Workflow
1. Validate compose wiring and active services with docker compose config and status.
2. Verify Kafka topic presence and sample payloads for:
   - raw.gps
   - raw.trips
   - processed.gps
   - processed.demand
   - processed.matches
3. Inspect Flink job code paths in:
   - flink/jobs/gps_normalizer.py
   - flink/jobs/demand_aggregator.py
   - flink/jobs/trip_matcher.py
4. Check correctness invariants:
   - event_time parsing and propagation
   - watermark strategy and lateness assumptions
   - H3 zone assignment path and fallback
   - dedup logic (drop duplicates, preserve valid new events)
5. Validate Cassandra sink writes into:
   - vehicle_positions
   - demand_zones
   - trips
6. Identify first broken stage and produce the smallest fix plus retest steps.

## Validation Commands
```bash
docker compose config --quiet
docker compose ps
docker exec taasim-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
docker exec taasim-kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic raw.gps --max-messages 5 --timeout-ms 10000
docker exec taasim-kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic processed.gps --max-messages 5 --timeout-ms 10000
docker exec taasim-flink-jm curl -s http://localhost:8081/jobs/overview
docker exec taasim-cassandra cqlsh -e "USE taasim; SELECT city, zone_id, event_time, taxi_id FROM vehicle_positions LIMIT 10;"
docker exec taasim-cassandra cqlsh -e "USE taasim; SELECT city, zone_id, window_start, ratio FROM demand_zones LIMIT 10;"
docker exec taasim-cassandra cqlsh -e "USE taasim; SELECT city, date_bucket, trip_id, status FROM trips LIMIT 10;"
rg -n "event_time|WatermarkStrategy|dedup|h3|zone" flink/jobs
```

## Output Contract
Return:
1. Stage health map (Kafka, Flink, Cassandra).
2. First confirmed fault point.
3. Fix recommendation with verification checklist.
