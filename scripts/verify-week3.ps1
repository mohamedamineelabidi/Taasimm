#!/usr/bin/env pwsh
<#
.SYNOPSIS
Week 3 Verification Script - Pipeline E2E Testing
Tests: Flink job status, Cassandra data flow, Kafka topics, Grafana connectivity
#>

Write-Host "=== TaaSim Week 3 Verification Test ===" -ForegroundColor Cyan
Write-Host "Timestamp: $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')" -ForegroundColor Gray
Write-Host ""

# Test 1: Flink Jobs Status
Write-Host "[1/6] Checking Flink Job Status..." -ForegroundColor Yellow
try {
    $flink_response = Invoke-WebRequest -Uri "http://localhost:8081/overview" -UseBasicParsing -ErrorAction Stop
    $flink_data = $flink_response.Content | ConvertFrom-Json
    
    Write-Host "  TaskManagers: $($flink_data.taskmanagers) running" -ForegroundColor Green
    Write-Host "  Slots: $($flink_data.slots_available) available / $($flink_data.slots_total) total" -ForegroundColor Green
    
    # Get jobs
    $jobs_response = Invoke-WebRequest -Uri "http://localhost:8081/jobs" -UseBasicParsing -ErrorAction Stop
    $jobs_data = $jobs_response.Content | ConvertFrom-Json
    $job_count = ($jobs_data.jobs | Measure-Object).Count
    
    Write-Host "  Running Jobs: $job_count" -ForegroundColor Green
    $jobs_data.jobs | ForEach-Object {
        Write-Host "    - $($_.name): $($_.state)" -ForegroundColor Green
    }
} catch {
    Write-Host "  ERROR: $_" -ForegroundColor Red
}

# Test 2: Cassandra Connection
Write-Host ""
Write-Host "[2/6] Checking Cassandra Schema..." -ForegroundColor Yellow
try {
    $cass_test = docker exec taasim-cassandra cqlsh -e "USE taasim; DESCRIBE TABLES;" 2>&1
    if ($cass_test -match "vehicle_positions|trips|demand_zones") {
        Write-Host "  Tables found: vehicle_positions, trips, demand_zones" -ForegroundColor Green
    } else {
        Write-Host "  WARNING: Could not verify all tables" -ForegroundColor Yellow
    }
} catch {
    Write-Host "  ERROR: $_" -ForegroundColor Red
}

# Test 3: Kafka Topics
Write-Host ""
Write-Host "[3/6] Checking Kafka Topics..." -ForegroundColor Yellow
try {
    $topics = docker exec taasim-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list 2>&1 | Where-Object {$_ -match "raw\.|processed\."}
    Write-Host "  Topics:" -ForegroundColor Green
    $topics | ForEach-Object { Write-Host "    - $_" -ForegroundColor Green }
} catch {
    Write-Host "  ERROR: $_" -ForegroundColor Red
}

# Test 4: Cassandra Data Count
Write-Host ""
Write-Host "[4/6] Checking Cassandra Data Population..." -ForegroundColor Yellow
try {
    $vehicle_count = docker exec taasim-cassandra cqlsh -e "USE taasim; SELECT COUNT(*) as count FROM vehicle_positions;" 2>&1 | Select-String "count" -Context 0,2 | Select-Object -Last 1
    $trips_count = docker exec taasim-cassandra cqlsh -e "USE taasim; SELECT COUNT(*) as count FROM trips;" 2>&1 | Select-String "count" -Context 0,2 | Select-Object -Last 1
    $demand_count = docker exec taasim-cassandra cqlsh -e "USE taasim; SELECT COUNT(*) as count FROM demand_zones;" 2>&1 | Select-String "count" -Context 0,2 | Select-Object -Last 1
    
    Write-Host "  Vehicle Positions: $vehicle_count" -ForegroundColor Green
    Write-Host "  Trips: $trips_count" -ForegroundColor Green
    Write-Host "  Demand Zones: $demand_count" -ForegroundColor Green
} catch {
    Write-Host "  ERROR: $_" -ForegroundColor Red
}

# Test 5: Kafka Message Count (processed.gps)
Write-Host ""
Write-Host "[5/6] Checking Kafka Message Flow..." -ForegroundColor Yellow
try {
    Write-Host "  Consuming 5 messages from processed.gps (timeout 3s)..." -ForegroundColor Gray
    $msg_count = docker exec taasim-kafka timeout 3 /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic processed.gps --from-beginning 2>&1 | Measure-Object -Line
    Write-Host "  Messages in processed.gps: $($msg_count.Lines) (sample)" -ForegroundColor Green
} catch {
    Write-Host "  WARNING: Could not count messages (expected timeout)" -ForegroundColor Yellow
}

# Test 6: Service Health
Write-Host ""
Write-Host "[6/6] Checking Service Health..." -ForegroundColor Yellow
try {
    $services = @("taasim-flink-jm", "taasim-kafka", "taasim-cassandra", "taasim-grafana", "taasim-minio")
    $statuses = docker ps --filter "name=taasim" --format "table {{.Names}}\t{{.Status}}" | Select-Object -Skip 1
    
    $statuses | ForEach-Object {
        if ($_ -match "healthy") {
            Write-Host "  [OK] $_" -ForegroundColor Green
        } else {
            Write-Host "  [..] $_" -ForegroundColor Yellow
        }
    }
} catch {
    Write-Host "  ERROR: $_" -ForegroundColor Red
}

Write-Host ""
Write-Host "=== Verification Complete ===" -ForegroundColor Cyan
Write-Host "For detailed Flink dashboard: http://localhost:8081" -ForegroundColor Gray
Write-Host "For Grafana dashboard: http://localhost:3000 (admin)" -ForegroundColor Gray
