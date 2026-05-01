1--Configure Flink Checkpointing to MinIO

Flink checkpointing enabled: interval=60s, backend=RocksDB. Checkpoints stored at s3a://flink-checkpoints/. Verified by checking MinIO bucket after 2 minutes of job runtime. Checkpoint files visible in MinIO console.

Enable Flink checkpointing every 60 seconds with RocksDB state backend, storing checkpoints to MinIO via S3A. This is required for the Week 7 fault-tolerance demonstration.

2//Implement Flink Job 1: GPS Normalizer
Flink Job 1 running and checkpointing every 60s to MinIO. Kafka source on raw.gps with watermark (3-min max lateness). Coordinate validation: drops events outside Casablanca bbox and speed > 150 km/h. Zone mapping via broadcast state (zone_mapping.csv). Anonymization: lat/lon snapped to zone centroid before Cassandra write. Raw coordinates NOT persisted. Cassandra vehicle_positions table receiving data. Grafana panel shows live vehicle positions.Build the first Flink streaming job: GPS Normalizer. Consumes raw.gps Kafka topic, assigns BoundedOutOfOrdernessWatermarks with 3-minute max lateness, validates coordinates, maps GPS pings to Casablanca arrondissements using broadcast state, anonymizes coordinates to zone centroid, and sinks to Cassandra vehicle_positions table.

## 🎯 Objective

The GPS Normalizer is the entry point of the real-time pipeline. It must handle out-of-order events with watermarks — this is the core streaming engineering challenge of the project.

## 📋 Sub-tasks

- [ ]  Create Flink project (PyFlink or Java/Scala)
- [ ]  Kafka source: `FlinkKafkaConsumer` on `raw.gps` with JSON deserializer
- [ ]  Assign `BoundedOutOfOrdernessWatermarks` with maxOutOfOrderness=3 minutes
- [ ]  Implement coordinate validation: filter lon outside 33.4-33.7°N, lat outside 7.4-7.8°W, speed > 150 km/h
- [ ]  Load zone_mapping as broadcast state (`MapStateDescriptor`)
- [ ]  Zone assignment: for each GPS event, determine arrondissement via bounding box lookup
- [ ]  Anonymization: replace raw lat/lon with zone centroid before any write
- [ ]  Configure checkpointing: every 60s, RocksDB state backend, checkpoint to `s3a://flink-checkpoints/`
- [ ]  Cassandra sink: `CassandraAppendTableSink` to `vehicle_positions`
- [ ]  Forward to `processed.gps` Kafka topic (input for Job 2)
- [ ]  Test with injected late events: verify 3-min late GPS event is handled correctly
- [ ]  Connect Grafana to Cassandra, create vehicle positions Geomap panel

## ⚠️ Engineering Constraints

> Raw GPS coordinates MUST NEVER be persisted. Only zone centroid coordinates go to Cassandra.
> 

> The GPS producer emits out-of-order events — a processing-time approach will produce wrong results and FAIL evaluation.
> 

## 📚 Reference

- Flink Watermarks: https://flink.apache.org/docs/stable/dev/event_timestamps_watermarks/
- Flink Broadcast State: https://flink.apache.org/docs/stable/dev/stream/state/broadcast_state/




3//Flink Job 1: Watermark & Late Event TestTest script injecting GPS events with timestamp 2m50s in the past. Verify these events appear in Cassandra vehicle_positions. Test with events >3m late: verify they are dropped/handled as expected. Screenshot or log output documenting watermark behavior. Section in technical report.
Validate that Flink Job 1 correctly handles late-arriving GPS events within the 3-minute watermark window. Inject deliberately late events and verify they are processed, not dropped. Document the test with before/after evidence for the technical report.## 🎯 Objective

Provide empirical evidence that event-time processing with watermarks is working correctly. This is directly evaluated in Week 8 Q&A.

## 📋 Sub-tasks

- [ ]  Write test script: inject GPS event with `timestamp = now - 2m50s` (within watermark)
- [ ]  Verify event appears in Cassandra vehicle_positions
- [ ]  Write second test: inject GPS event with `timestamp = now - 4m` (outside watermark)
- [ ]  Verify event is handled as late (dropped or routed to side output)
- [ ]  Capture Flink UI metrics: watermark lag, late events counter
- [ ]  Document with screenshots for technical report

## 💡 Why this matters

A processing-time implementation will appear to work during demo but will produce **measurably incorrect demand aggregations** — the evaluator checks this specifically.