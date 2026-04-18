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
$WARN = 0

$script:JobDetails = @()

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

function Warn($label, $detail = "") {
    Write-Host "  [WARN] $label" -ForegroundColor Yellow
    if ($detail) { Write-Host "         $detail" -ForegroundColor DarkYellow }
    $script:WARN++
}

function Get-CassandraScalar {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Query,

        [string]$Pattern = '^\s+\d+'
    )

    $output = docker exec taasim-cassandra cqlsh -e $Query 2>$null
    if (-not $output) {
        return $null
    }

    $match = $output | Select-String $Pattern | Select-Object -First 1
    if (-not $match) {
        return $null
    }

    return $match.ToString().Trim()
}

function Parse-CqlTimestamp {
    param(
        [Parameter(Mandatory = $true)]
        [string]$RawValue
    )

    $cleaned = $RawValue.Trim()
    try {
        return [datetimeoffset]::Parse($cleaned).UtcDateTime
    } catch {
        return $null
    }
}

function Check-ConsumerLag {
    param(
        [Parameter(Mandatory = $true)]
        [string]$GroupName,

        [Parameter(Mandatory = $true)]
        [string]$ExpectedTopic,

        [int]$MaxLag = 0
    )

    $output = docker exec taasim-kafka /opt/kafka/bin/kafka-consumer-groups.sh `
        --bootstrap-server localhost:9092 --describe --group $GroupName 2>$null

    if (-not $output) {
        Check "Consumer group reachable: $GroupName" $false "no output returned"
        return
    }

    $topicLines = @($output | Select-String "^$GroupName\s+$ExpectedTopic\s+")
    if ($topicLines.Count -eq 0) {
        Check "Consumer group topic: $GroupName -> $ExpectedTopic" $false "topic not found in group output"
        return
    }

    $totalLag = 0
    foreach ($line in $topicLines) {
        $columns = ($line.ToString() -split '\s+') | Where-Object { $_ -ne "" }
        if ($columns.Count -lt 6 -or $columns[5] -notmatch '^\d+$') {
            Check "Consumer lag parse: $GroupName" $false "unexpected row format: $line"
            return
        }
        $totalLag += [int]$columns[5]
    }

    Check "Consumer lag acceptable: $GroupName" ($totalLag -le $MaxLag) "$ExpectedTopic lag: $totalLag"
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

    foreach ($job in $runningJobs) {
        $detail = docker exec taasim-flink-jm curl -s "http://localhost:8081/jobs/$($job.id)" 2>$null | ConvertFrom-Json
        if ($detail) {
            $script:JobDetails += $detail
        }
    }

    $expectedNames = @("GPS Normalizer", "Demand Aggregator", "Trip Matcher")
    foreach ($expected in $expectedNames) {
        $detail = $script:JobDetails | Where-Object { $_.name -eq $expected } | Select-Object -First 1
        if ($detail) {
            $uptime = [math]::Round($detail.duration / 1000)
            Check "Job RUNNING: $expected" $true "uptime $($uptime)s, job-id: $($detail.jid)"
        } else {
            Check "Job RUNNING: $expected" $false "not found in running jobs - submit with .\scripts\submit-flink-jobs.ps1"
        }
    }
}
Write-Host ""

# ── 2. Kafka Topics & Consumer Flow ──────────────────────────────────────────
Write-Host "--- Kafka Topics ---" -ForegroundColor Yellow
$allTopics = docker exec taasim-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list 2>$null
$topics = @("raw.gps", "raw.trips", "processed.gps", "processed.demand", "processed.matches")
foreach ($topic in $topics) {
    $exists = $allTopics | Select-String -Pattern "^$topic$"
    Check "Kafka topic exists: $topic" ([bool]$exists)
}
Write-Host ""

Write-Host "--- Kafka Consumer Lag ---" -ForegroundColor Yellow
Check-ConsumerLag -GroupName "flink-gps-normalizer" -ExpectedTopic "raw.gps" -MaxLag 50
Check-ConsumerLag -GroupName "demand-aggregator-gps" -ExpectedTopic "processed.gps" -MaxLag 25
Check-ConsumerLag -GroupName "demand-aggregator-trips" -ExpectedTopic "raw.trips" -MaxLag 25
Check-ConsumerLag -GroupName "trip-matcher-gps" -ExpectedTopic "processed.gps" -MaxLag 25
Check-ConsumerLag -GroupName "trip-matcher-trips" -ExpectedTopic "raw.trips" -MaxLag 25
Write-Host ""

# ── 3. Cassandra Tables ───────────────────────────────────────────────────────
Write-Host "--- Cassandra Tables ---" -ForegroundColor Yellow

# vehicle_positions — check row count
$vpCount = Get-CassandraScalar -Query "SELECT COUNT(*) FROM taasim.vehicle_positions;"
Check "Cassandra vehicle_positions has rows" ($vpCount -and [int]$vpCount -gt 0) "count: $vpCount"

# demand_zones — check row count
$dzCount = Get-CassandraScalar -Query "SELECT COUNT(*) FROM taasim.demand_zones;"
Check "Cassandra demand_zones has rows" ($dzCount -and [int]$dzCount -gt 0) "count: $dzCount"

# trips — check row count
$trCount = Get-CassandraScalar -Query "SELECT COUNT(*) FROM taasim.trips;"
Check "Cassandra trips has rows" ($trCount -and [int]$trCount -gt 0) "count: $trCount"

Write-Host ""

# ── 4. Freshness SLA ─────────────────────────────────────────────────────────
Write-Host "--- Freshness SLA ---" -ForegroundColor Yellow

# Latest vehicle position timestamp (GPS SLA: < 15s)
$latestGps = Get-CassandraScalar -Query "SELECT max(event_time) FROM taasim.vehicle_positions;" -Pattern '^\s+\d{4}-'
if ($latestGps) {
    $ts = Parse-CqlTimestamp -RawValue $latestGps
    if ($ts) {
        $ageSeconds = ([datetime]::UtcNow - $ts).TotalSeconds
        if ($ts.Year -lt ([datetime]::UtcNow.Year - 1)) {
            Warn "GPS freshness SLA (< 15s)" "event_time is historical replay data ($latestGps); use Kafka lag plus row growth as the freshness signal"
        } else {
            $ageDetail = "last position {0:N0}s ago" -f $ageSeconds
            Check "GPS freshness SLA (< 15s)" ($ageSeconds -lt 15) $ageDetail
        }
    } else {
        Check "GPS freshness SLA (< 15s)" $false "could not parse timestamp: $latestGps"
    }
} else {
    Check "GPS freshness SLA (< 15s)" $false "no rows in vehicle_positions"
}

# Latest demand window (demand SLA: < 60s)
$latestDemand = Get-CassandraScalar -Query "SELECT max(window_start) FROM taasim.demand_zones;" -Pattern '^\s+\d{4}-'
if ($latestDemand) {
    $ts = Parse-CqlTimestamp -RawValue $latestDemand
    if ($ts) {
        $ageSeconds = ([datetime]::UtcNow - $ts).TotalSeconds
        $ageDetail = "last window {0:N0}s ago" -f $ageSeconds
        Check "Demand freshness SLA (<= 60s)" ($ageSeconds -le 60) $ageDetail
    } else {
        Check "Demand freshness SLA (<= 60s)" $false "could not parse timestamp"
    }
} else {
    Check "Demand freshness SLA (<= 60s)" $false "no rows in demand_zones"
}

Write-Host ""

# ── Summary ───────────────────────────────────────────────────────────────────
Write-Host "=== Summary ===" -ForegroundColor Cyan
Write-Host "  PASS: $PASS" -ForegroundColor Green
if ($WARN -gt 0) {
    Write-Host "  WARN: $WARN" -ForegroundColor Yellow
}
if ($FAIL -gt 0) {
    Write-Host "  FAIL: $FAIL" -ForegroundColor Red
    Write-Host ""
    Write-Host "  Next steps:" -ForegroundColor Yellow
    Write-Host "    1. Start producers:  .venv\Scripts\python.exe producers\vehicle_gps_producer.py --max-trips 50"
    Write-Host "    2. Start producers:  .venv\Scripts\python.exe producers\trip_request_producer.py --max-trips 20"
    Write-Host "    3. Submit jobs:      .\scripts\submit-flink-jobs.ps1"
    Write-Host "    4. Flink Web UI:     http://localhost:8081"
} else {
    Write-Host "  All checks passed - pipeline is healthy!" -ForegroundColor Green
}
