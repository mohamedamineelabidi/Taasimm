#!/usr/bin/env pwsh
<#
.SYNOPSIS
    Submit Spark ETL jobs to the TaaSim Spark cluster.

.DESCRIPTION
    Submits Porto ETL, NYC ETL, and/or KPI analytics jobs
    to the Spark master running inside Docker.

.PARAMETER Jobs
    Which jobs to run: "all", "porto", "nyc", "kpis"
    Default: "all"

.EXAMPLE
    .\scripts\submit-spark-jobs.ps1
    .\scripts\submit-spark-jobs.ps1 -Jobs porto
    .\scripts\submit-spark-jobs.ps1 -Jobs kpis
#>
param(
    [string]$Jobs = "all"
    # Valid values: "all", "porto", "nyc", "kpis", "features", "train", "verify", "ml"
)

$ErrorActionPreference = "Stop"
$MASTER = "taasim-spark-master"

# ── Verify Spark master is running ─────────────────────────────────
Write-Host "=== TaaSim Spark Job Submission ===" -ForegroundColor Cyan

$status = docker ps --filter "name=$MASTER" --filter "status=running" --format "{{.Names}}"
if (-not $status) {
    Write-Error "Container '$MASTER' is not running. Run: docker compose up -d"
    exit 1
}

$sparkInfo = docker exec $MASTER curl -s http://localhost:8080/json/ 2>$null | ConvertFrom-Json
Write-Host "  Spark master: $($sparkInfo.url)" -ForegroundColor Green
Write-Host "  Workers: $($sparkInfo.workers.Count)  |  Cores: $($sparkInfo.workers[0].cores)  |  Memory: $($sparkInfo.workers[0].memory) MB" -ForegroundColor Green
Write-Host ""

$SUBMIT = "/opt/spark/bin/spark-submit --master spark://spark-master:7077 --deploy-mode client"

# ── Porto ETL ────────────────────────────────────────────────────────
if ($Jobs -in @("all", "porto")) {
    Write-Host "[1] Submitting Porto ETL job..." -ForegroundColor Yellow
    $start = Get-Date
    docker exec $MASTER bash -c "$SUBMIT /opt/spark-jobs/etl_porto.py 2>&1"
    $elapsed = (Get-Date) - $start
    if ($LASTEXITCODE -eq 0) {
        Write-Host "  Porto ETL completed in $([math]::Round($elapsed.TotalSeconds))s" -ForegroundColor Green
    } else {
        Write-Host "  Porto ETL FAILED (exit $LASTEXITCODE)" -ForegroundColor Red
    }
    Write-Host ""
}

# ── NYC ETL ──────────────────────────────────────────────────────────
if ($Jobs -in @("all", "nyc")) {
    Write-Host "[2] Submitting NYC TLC ETL job..." -ForegroundColor Yellow
    $start = Get-Date
    docker exec $MASTER bash -c "$SUBMIT /opt/spark-jobs/etl_nyc.py 2>&1"
    $elapsed = (Get-Date) - $start
    if ($LASTEXITCODE -eq 0) {
        Write-Host "  NYC ETL completed in $([math]::Round($elapsed.TotalSeconds))s" -ForegroundColor Green
    } else {
        Write-Host "  NYC ETL FAILED (exit $LASTEXITCODE)" -ForegroundColor Red
    }
    Write-Host ""
}

# ── KPI Analytics ────────────────────────────────────────────────────
if ($Jobs -in @("all", "kpis")) {
    Write-Host "[3] Submitting KPI Analytics job..." -ForegroundColor Yellow
    Write-Host "    (requires Porto ETL output in s3a://curated/trips/)" -ForegroundColor DarkGray
    $start = Get-Date
    docker exec $MASTER bash -c "$SUBMIT /opt/spark-jobs/compute_kpis.py 2>&1"
    $elapsed = (Get-Date) - $start
    if ($LASTEXITCODE -eq 0) {
        Write-Host "  KPI Analytics completed in $([math]::Round($elapsed.TotalSeconds))s" -ForegroundColor Green
    } else {
        Write-Host "  KPI Analytics FAILED (exit $LASTEXITCODE)" -ForegroundColor Red
    }
    Write-Host ""
}

Write-Host "=== Submission Complete ===" -ForegroundColor Cyan
Write-Host "  Spark Web UI: http://localhost:8080" -ForegroundColor DarkGray
# ── Feature Engineering ──────────────────────────────────────────────
if ($Jobs -in @("all", "ml", "features")) {
    Write-Host "[4] Submitting Feature Engineering job..." -ForegroundColor Yellow
    Write-Host "    (requires Porto ETL output in s3a://curated/trips/)" -ForegroundColor DarkGray
    $start = Get-Date
    docker exec $MASTER bash -c "$SUBMIT /opt/spark-jobs/feature_engineering.py 2>&1"
    $elapsed = (Get-Date) - $start
    if ($LASTEXITCODE -eq 0) {
        Write-Host "  Feature Engineering completed in $([math]::Round($elapsed.TotalSeconds))s" -ForegroundColor Green
    } else {
        Write-Host "  Feature Engineering FAILED (exit $LASTEXITCODE)" -ForegroundColor Red
    }
    Write-Host ""
}

# ── Train Demand Model ───────────────────────────────────────────────
if ($Jobs -in @("all", "ml", "train")) {
    Write-Host "[5] Submitting Train Demand Model job..." -ForegroundColor Yellow
    Write-Host "    (requires feature matrix in s3a://mldata/features/)" -ForegroundColor DarkGray
    $start = Get-Date
    docker exec $MASTER bash -c "$SUBMIT /opt/spark-jobs/train_demand_model.py 2>&1"
    $elapsed = (Get-Date) - $start
    if ($LASTEXITCODE -eq 0) {
        Write-Host "  Train Demand Model completed in $([math]::Round($elapsed.TotalSeconds))s" -ForegroundColor Green
    } else {
        Write-Host "  Train Demand Model FAILED (exit $LASTEXITCODE)" -ForegroundColor Red
    }
    Write-Host ""
}

# ── Verify Model ─────────────────────────────────────────────────────
if ($Jobs -in @("ml", "verify")) {
    Write-Host "[6] Running Model Verification..." -ForegroundColor Yellow
    Write-Host "    (requires trained model in s3a://mldata/models/demand_v1/)" -ForegroundColor DarkGray
    $start = Get-Date
    docker exec $MASTER bash -c "$SUBMIT /opt/spark-jobs/verify_model.py 2>&1"
    $elapsed = (Get-Date) - $start
    if ($LASTEXITCODE -eq 0) {
        Write-Host "  Model Verification completed in $([math]::Round($elapsed.TotalSeconds))s" -ForegroundColor Green
    } else {
        Write-Host "  Model Verification FAILED (exit $LASTEXITCODE)" -ForegroundColor Red
    }
    Write-Host ""
}

Write-Host "=== Submission Complete ===" -ForegroundColor Cyan
Write-Host "  Spark Web UI: http://localhost:8080" -ForegroundColor DarkGray
Write-Host "  Check MinIO:  docker exec taasim-minio mc ls local/curated/ --recursive" -ForegroundColor DarkGray