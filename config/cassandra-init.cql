-- ============================================================
-- TaaSim — Cassandra Schema Initialization
-- Run once after Cassandra starts
-- ============================================================

-- Create keyspace
CREATE KEYSPACE IF NOT EXISTS taasim
  WITH replication = {
    'class': 'SimpleStrategy',
    'replication_factor': 1
  };

USE taasim;

-- ──────────────────────────────────────────────
-- Table 1: vehicle_positions
-- Query pattern: "All available vehicles in zone X"
-- Partition: (city, zone_id) → groups vehicles by zone
-- Clustering: event_time DESC → latest position first
-- ──────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS vehicle_positions (
    city        TEXT,
    zone_id     INT,
    event_time  TIMESTAMP,
    taxi_id     TEXT,
    lat         DOUBLE,
    lon         DOUBLE,
    speed       DOUBLE,
    status      TEXT,
    h3_index    TEXT,
    PRIMARY KEY ((city, zone_id), event_time, taxi_id)
) WITH CLUSTERING ORDER BY (event_time DESC, taxi_id ASC)
  AND default_time_to_live = 86400;  -- 24h TTL for positions

-- ──────────────────────────────────────────────
-- Table 2: trips
-- Query pattern: "Trip history for a given day"
-- Partition: (city, date_bucket) → bounded partition size
-- Clustering: created_at DESC → newest trips first
-- ──────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS trips (
    city            TEXT,
    date_bucket     TEXT,
    created_at      TIMESTAMP,
    trip_id         UUID,
    rider_id        TEXT,
    taxi_id         TEXT,
    origin_zone     INT,
    dest_zone       INT,
    status          TEXT,
    fare            DOUBLE,
    eta_seconds     INT,
    PRIMARY KEY ((city, date_bucket), created_at, trip_id)
) WITH CLUSTERING ORDER BY (created_at DESC, trip_id ASC);

-- ──────────────────────────────────────────────
-- Table 3: demand_zones
-- Query pattern: "Live demand heatmap per zone"
-- Partition: (city, zone_id) → per-zone demand data
-- Clustering: window_start DESC → latest window first
-- ──────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS demand_zones (
    city                TEXT,
    zone_id             INT,
    window_start        TIMESTAMP,
    active_vehicles     INT,
    pending_requests    INT,
    ratio               DOUBLE,
    forecast_demand     DOUBLE,
    PRIMARY KEY ((city, zone_id), window_start)
) WITH CLUSTERING ORDER BY (window_start DESC)
  AND default_time_to_live = 604800;  -- 7-day TTL
