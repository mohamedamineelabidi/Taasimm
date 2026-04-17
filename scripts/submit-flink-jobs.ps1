#!/usr/bin/env pwsh
<#
.SYNOPSIS
    Submit all three TaaSim PyFlink jobs to the Flink cluster.

.DESCRIPTION
    Submits Job 1 (GPS Normalizer), Job 2 (Demand Aggregator), and
    Job 3 (Trip Matcher) in detached mode via the Flink CLI inside
    the taasim-flink-jm container.

    Prerequisites:
      - docker compose up -d (stack must be running)
      - Kafka topics raw.gps and raw.trips must exist
      - Cassandra schema must be initialized (config/cassandra-init.cql)

.PARAMETER Jobs
    Which jobs to submit: "all", "1", "2", "3", or comma-separated e.g. "2,3"
    Default: "all"

.EXAMPLE
    .\scripts\submit-flink-jobs.ps1
    .\scripts\submit-flink-jobs.ps1 -Jobs 2,3
#>
param(
    [string]$Jobs = "all"
)

$ErrorActionPreference = "Stop"

$JM = "taasim-flink-jm"
$JOBS_DIR = "/opt/flink/jobs"

$JOB_FILES = @{
    1 = "gps_normalizer.py"
    2 = "demand_aggregator.py"
    3 = "trip_matcher.py"
}

$JOB_NAMES = @{
    1 = "GPS Normalizer"
    2 = "Demand Aggregator"
    3 = "Trip Matcher"
}

# ── Resolve which jobs to submit ─────────────────────────────────────────────
if ($Jobs -eq "all") {
    $jobIds = @(1, 2, 3)
} else {
    $jobIds = $Jobs -split "," | ForEach-Object { [int]$_.Trim() }
}

Write-Host "=== TaaSim Flink Job Submission ===" -ForegroundColor Cyan
Write-Host "Submitting jobs: $($jobIds -join ', ')" -ForegroundColor Cyan
Write-Host ""

# ── Check stack is running ────────────────────────────────────────────────────
Write-Host "[0/3] Checking Flink JobManager is healthy..." -ForegroundColor Yellow
$jmStatus = docker ps --filter "name=$JM" --filter "status=running" --format "{{.Names}}"
if (-not $jmStatus) {
    Write-Error "Container '$JM' is not running. Run: docker compose up -d"
    exit 1
}

$flinkOverview = docker exec $JM curl -s http://localhost:8081/overview 2>$null
if (-not $flinkOverview) {
    Write-Error "Flink REST API not responding at :8081. Wait for healthcheck to pass."
    exit 1
}
$overview = $flinkOverview | ConvertFrom-Json
Write-Host "  Flink version: $($overview.'flink-version')  |  TaskManagers: $($overview.'taskmanagers')  |  Free slots: $($overview.'slots-available')" -ForegroundColor Green
Write-Host ""

# ── Ensure output Kafka topics exist ─────────────────────────────────────────
Write-Host "[1/3] Ensuring output Kafka topics exist..." -ForegroundColor Yellow
$topicsToCreate = @("processed.gps", "processed.demand", "processed.matches")
foreach ($topic in $topicsToCreate) {
    $exists = docker exec taasim-kafka /opt/kafka/bin/kafka-topics.sh `
        --bootstrap-server localhost:9092 --list 2>$null | Select-String -Pattern "^$topic$"
    if (-not $exists) {
        Write-Host "  Creating topic: $topic" -ForegroundColor DarkYellow
        docker exec taasim-kafka /opt/kafka/bin/kafka-topics.sh `
            --bootstrap-server localhost:9092 `
            --create --topic $topic `
            --partitions 4 `
            --replication-factor 1 `
            --if-not-exists 2>&1 | Out-Null
        Write-Host "  Created: $topic" -ForegroundColor Green
    } else {
        Write-Host "  Exists:  $topic" -ForegroundColor Green
    }
}
Write-Host ""

# ── Cancel any existing running versions of the same jobs ────────────────────
Write-Host "[2/3] Checking for already-running jobs to avoid duplicates..." -ForegroundColor Yellow
$runningJobsJson = docker exec $JM curl -s http://localhost:8081/jobs 2>$null
if ($runningJobsJson) {
    $runningJobs = ($runningJobsJson | ConvertFrom-Json).jobs
    foreach ($job in $runningJobs) {
        if ($job.status -eq "RUNNING") {
            $detail = docker exec $JM curl -s "http://localhost:8081/jobs/$($job.id)" 2>$null | ConvertFrom-Json
            $name = $detail.name
            Write-Host "  Found running job: '$name' ($($job.id))" -ForegroundColor DarkYellow
        }
    }
    $runningCount = ($runningJobs | Where-Object { $_.status -eq "RUNNING" }).Count
    if ($runningCount -gt 0) {
        Write-Host "  $runningCount job(s) currently running. New submissions will run in parallel." -ForegroundColor DarkYellow
    } else {
        Write-Host "  No jobs currently running." -ForegroundColor Green
    }
}
Write-Host ""

# ── Submit jobs ───────────────────────────────────────────────────────────────
Write-Host "[3/3] Submitting PyFlink jobs..." -ForegroundColor Yellow
$submitted = @()
$failed = @()

foreach ($id in $jobIds) {
    $file = $JOB_FILES[$id]
    $name = $JOB_NAMES[$id]
    $path = "$JOBS_DIR/$file"

    Write-Host ""
    Write-Host "  Submitting Job ${id}: $name" -ForegroundColor Cyan
    Write-Host "    File: $path"

    # Check the file exists in the container
    $fileExists = docker exec $JM test -f $path; $exitCode = $LASTEXITCODE
    if ($exitCode -ne 0) {
        Write-Host "    ERROR: $path not found in container. Check volume mount." -ForegroundColor Red
        $failed += $name
        continue
    }

    # Submit in detached mode (-d)
    # Ignore warnings by filtering stderr through docker exec
    $submitOut = docker exec $JM bash -c "flink run -d -py $path 2>/dev/null" 2>&1
    
    # Check if we got a successful job submission (look for Job has been submitted or job ID output)
    if ($submitOut -match "Job has been submitted|JobID") {
        Write-Host "    SUCCESS: $submitOut" -ForegroundColor Green
        $submitted += $name
    } else {
        Write-Host "    FAILED: $submitOut" -ForegroundColor Red
        $failed += $name
    }
}

# ── Summary ───────────────────────────────────────────────────────────────────
Write-Host ""
Write-Host "=== Submission Summary ===" -ForegroundColor Cyan
if ($submitted.Count -gt 0) {
    Write-Host "  Submitted ($($submitted.Count)): $($submitted -join ', ')" -ForegroundColor Green
}
if ($failed.Count -gt 0) {
    Write-Host "  Failed ($($failed.Count)):    $($failed -join ', ')" -ForegroundColor Red
}

Write-Host ""
Write-Host "  Flink Web UI:  http://localhost:8081" -ForegroundColor Cyan
Write-Host "  Verify status: .\scripts\verify-flink-jobs.ps1" -ForegroundColor Cyan
