# Task 5 - Kafka Producers

Status: Implemented and previously validated; current smoke test blocked by Docker daemon
Last updated: 2026-04-03

## Objective

Implement realtime simulation producers for GPS and trip requests.

## Delivered

- producers/vehicle_gps_producer.py
- producers/trip_request_producer.py
- producers/config.py

## Features Implemented

### vehicle_gps_producer.py

- Replays GPS trajectories from Porto source data
- Applies Porto -> Casablanca transform
- Adds controlled coordinate noise
- Supports realistic event timing behavior

### trip_request_producer.py

- Emits synthetic trip request events
- Uses zone mapping for origin/destination zones
- Supports demand-driven generation behavior

## Current Smoke Test Result

- Command was run with max-trips=5
- Failure: kafka.errors.NoBrokersAvailable
- Root cause: Docker daemon is unavailable in this session, so Kafka is not reachable

## Conclusion

- Producer code is in place and integrated with new mapping
- Runtime infrastructure must be restarted for final live verification
