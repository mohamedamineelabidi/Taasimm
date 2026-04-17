#!/usr/bin/env pwsh
<#
.SYNOPSIS
    Verify the TaaSim Flink pipeline is running end-to-end.

.DESCRIPTION
    Checks:
      1. All 3 Flink jobs are in RUNNING state
      2. Kafka output topics have messages (processed.gps, processed.demand, processed.matches)
      3. Cassandra has recent rows in vehicle_positions, demand_zones, trips
      4. GPS freshness SLA: newest vehicle_positions row < 15s old
      5. Demand zone update SLA: newest demand_zones row < 60s old
#>

$ErrorActionPreference = "Continue"

Write-Host "=== TaaSim Flink Pipeline Verification ===" -ForegroundColor Cyan
Write-Host ""

$PASS = 0
$FAIL = 0

function Check($label, $ok, $detail = "") {
    if ($ok) {
        Write-Host "  [PASS] $label" -ForegroundColor Green
        if ($detail) { Write-Host "         $detail" -ForegroundColor DarkGreen }
        $script:PASS++
    } else {
        Write-Host "  [FAIL] $label" -ForegroundColor Red
        if ($detail) { Write-Host "         $detail" -ForegroundColor DarkRed }
        $script:FAIL++
    }
}

# ── 1. Flink Jobs ─────────────────────────────────────────────────────────────
Write-Host "--- Flink Jobs ---" -ForegroundColor Yellow
$jobsJson = docker exec taasim-flink-jm curl -s http://localhost:8081/jobs 2>$null
if (-not $jobsJson) {
    Check "Flink REST API reachable" $false "curl to :8081/jobs failed"
} else {
    $jobs = ($jobsJson | ConvertFrom-Json).jobs
    $runningJobs = $jobs | Where-Object { $_.status -eq "RUNNING" }
    Check "Flink REST API reachable" $true "found $($jobs.Count) total jobs"

    $expectedNames = @("GPS Normalizer", "Demand Aggregator", "Trip Matcher")
    foreach ($expected in $expectedNames) {
        $found = $false
        foreach ($job in $runningJobs) {
            $detail = docker exec taasim-flink-jm curl -s "http://localhost:8081/jobs/$($job.id)" 2>$null | ConvertFrom-Json
            if ($detail.name -like "*$($expected.Split(' ')[0])*") {
                $found = $true
                $uptime = [math]::Round($detail.duration / 1000)
                Check "Job RUNNING: $expected" $true "uptime ${uptime}s, job-id: $($job.id)"
                break
            }
        }
        if (-not $found) {
            Check "Job RUNNING: $expected" $false "not found in running jobs — submit with .\scripts\submit-flink-jobs.ps1"
        }
    }
}
Write-Host ""

# ── 2. Kafka Topics & Message Counts ──────────────────────────────────────────
Write-Host "--- Kafka Topics ---" -ForegroundColor Yellow
$topics = @("processed.gps", "processed.demand", "processed.matches")
foreach ($topic in $topics) {
    $offsets = docker exec taasim-kafka /opt/kafka/bin/kafka-run-class.sh `
        kafka.tools.GetOffsetShell `
        --bootstrap-server localhost:9092 `
        --topic $topic --time -1 2>$null
    if (-not $offsets) {
        Check "Topic has messages: $topic" $false "topic may not exist yet"
        continue
    }
    $totalMsgs = 0
    $offsets | ForEach-Object {
        $parts = $_ -split ":"
        if ($parts.Count -ge 3 -and $parts[2] -match '^\d+$') {
            $totalMsgs += [int]$parts[2]
        }
    }
    Check "Topic has messages: $topic" ($totalMsgs -gt 0) "$totalMsgs messages total"
}
Write-Host ""

# ── 3. Cassandra Tables ───────────────────────────────────────────────────────
Write-Host "--- Cassandra Tables ---" -ForegroundColor Yellow

# vehicle_positions — check row count
$vpCount = docker exec taasim-cassandra cqlsh -e `
    "SELECT COUNT(*) FROM taasim.vehicle_positions;" 2>$null | `
    Select-String "^\s+\d+" | ForEach-Object { $_.ToString().Trim() }
Check "Cassandra vehicle_positions has rows" ($vpCount -and [int]$vpCount -gt 0) "count: $vpCount"

# demand_zones — check row count
$dzCount = docker exec taasim-cassandra cqlsh -e `
    "SELECT COUNT(*) FROM taasim.demand_zones;" 2>$null | `
    Select-String "^\s+\d+" | ForEach-Object { $_.ToString().Trim() }
Check "Cassandra demand_zones has rows" ($dzCount -and [int]$dzCount -gt 0) "count: $dzCount"

# trips — check row count
$trCount = docker exec taasim-cassandra cqlsh -e `
    "SELECT COUNT(*) FROM taasim.trips;" 2>$null | `
    Select-String "^\s+\d+" | ForEach-Object { $_.ToString().Trim() }
Check "Cassandra trips has rows" ($trCount -and [int]$trCount -gt 0) "count: $trCount"

Write-Host ""

# ── 4. Freshness SLA ─────────────────────────────────────────────────────────
Write-Host "--- Freshness SLA ---" -ForegroundColor Yellow

# Latest vehicle position timestamp (GPS SLA: < 15s)
$latestGps = docker exec taasim-cassandra cqlsh -e `
    "SELECT event_time FROM taasim.vehicle_positions LIMIT 1;" 2>$null | `
    Select-String "\d{4}-\d{2}-\d{2}" | Select-Object -First 1
if ($latestGps) {
    try {
        $ts = [datetime]::Parse(($latestGps -replace '.*?(\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}).*', '$1'))
        $ageSeconds = ([datetime]::UtcNow - $ts).TotalSeconds
        Check "GPS freshness SLA (< 15s)" ($ageSeconds -lt 15) "last position ${ageSeconds:.0f}s ago"
    } catch {
        Check "GPS freshness SLA (< 15s)" $false "could not parse timestamp: $latestGps"
    }
} else {
    Check "GPS freshness SLA (< 15s)" $false "no rows in vehicle_positions"
}

# Latest demand window (demand SLA: < 60s)
$latestDemand = docker exec taasim-cassandra cqlsh -e `
    "SELECT window_start FROM taasim.demand_zones LIMIT 1;" 2>$null | `
    Select-String "\d{4}-\d{2}-\d{2}" | Select-Object -First 1
if ($latestDemand) {
    try {
        $ts = [datetime]::Parse(($latestDemand -replace '.*?(\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}).*', '$1'))
        $ageSeconds = ([datetime]::UtcNow - $ts).TotalSeconds
        Check "Demand freshness SLA (< 60s)" ($ageSeconds -lt 60) "last window ${ageSeconds:.0f}s ago"
    } catch {
        Check "Demand freshness SLA (< 60s)" $false "could not parse timestamp"
    }
} else {
    Check "Demand freshness SLA (< 60s)" $false "no rows in demand_zones"
}

Write-Host ""

# ── Summary ───────────────────────────────────────────────────────────────────
Write-Host "=== Summary ===" -ForegroundColor Cyan
Write-Host "  PASS: $PASS" -ForegroundColor Green
if ($FAIL -gt 0) {
    Write-Host "  FAIL: $FAIL" -ForegroundColor Red
    Write-Host ""
    Write-Host "  Next steps:" -ForegroundColor Yellow
    Write-Host "    1. Start producers:  .venv\Scripts\python.exe producers\vehicle_gps_producer.py --max-trips 50"
    Write-Host "    2. Start producers:  .venv\Scripts\python.exe producers\trip_request_producer.py --max-trips 20"
    Write-Host "    3. Submit jobs:      .\scripts\submit-flink-jobs.ps1"
    Write-Host "    4. Flink Web UI:     http://localhost:8081"
} else {
    Write-Host "  All checks passed — pipeline is healthy!" -ForegroundColor Green
}
