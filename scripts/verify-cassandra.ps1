# ============================================================
# TaaSim — Verify Cassandra Schema & Test Queries
# Run AFTER cassandra-init has completed
# ============================================================

$ErrorActionPreference = "Stop"

Write-Host "=== Verifying Cassandra Schema ===" -ForegroundColor Cyan

# Check tables exist
Write-Host ""
Write-Host "--- Listing tables in keyspace taasim ---"
docker exec taasim-cassandra cqlsh -e "USE taasim; DESCRIBE TABLES;"

# Insert sample data into vehicle_positions
Write-Host ""
Write-Host "--- Testing vehicle_positions (INSERT + SELECT) ---" -ForegroundColor Cyan
docker exec taasim-cassandra cqlsh -e @"
INSERT INTO taasim.vehicle_positions (city, zone_id, event_time, taxi_id, lat, lon, speed, status)
VALUES ('casa', 5, toTimestamp(now()), 'taxi-001', 33.573, -7.589, 35.2, 'available');

INSERT INTO taasim.vehicle_positions (city, zone_id, event_time, taxi_id, lat, lon, speed, status)
VALUES ('casa', 5, toTimestamp(now()), 'taxi-002', 33.571, -7.591, 0.0, 'occupied');

SELECT * FROM taasim.vehicle_positions WHERE city='casa' AND zone_id=5 ORDER BY event_time DESC LIMIT 10;
"@

# Insert sample data into trips
Write-Host ""
Write-Host "--- Testing trips (INSERT + SELECT) ---" -ForegroundColor Cyan
docker exec taasim-cassandra cqlsh -e @"
INSERT INTO taasim.trips (city, date_bucket, created_at, trip_id, rider_id, taxi_id, origin_zone, dest_zone, status, fare, eta_seconds)
VALUES ('casa', '2025-04-12', toTimestamp(now()), uuid(), 'rider-101', 'taxi-001', 5, 12, 'completed', 45.50, 420);

SELECT * FROM taasim.trips WHERE city='casa' AND date_bucket='2025-04-12' ORDER BY created_at DESC LIMIT 10;
"@

# Insert sample data into demand_zones
Write-Host ""
Write-Host "--- Testing demand_zones (INSERT + SELECT) ---" -ForegroundColor Cyan
docker exec taasim-cassandra cqlsh -e @"
INSERT INTO taasim.demand_zones (city, zone_id, window_start, active_vehicles, pending_requests, ratio, forecast_demand)
VALUES ('casa', 5, toTimestamp(now()), 8, 12, 0.667, 15.3);

SELECT * FROM taasim.demand_zones WHERE city='casa' AND zone_id=5 ORDER BY window_start DESC LIMIT 10;
"@

# Benchmark query
Write-Host ""
Write-Host "--- Benchmark: zone query with timing ---" -ForegroundColor Cyan
docker exec taasim-cassandra cqlsh -e "TRACING ON; SELECT * FROM taasim.vehicle_positions WHERE city='casa' AND zone_id=5 ORDER BY event_time DESC LIMIT 10;"

Write-Host ""
Write-Host "=== Cassandra Schema Verification Complete ===" -ForegroundColor Green
