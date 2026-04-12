# ============================================================
# TaaSim — Register Kafka Connect S3 Sink Connectors
# Run AFTER kafka-connect container is healthy
# ============================================================

$ErrorActionPreference = "Stop"
$CONNECT_URL = "http://localhost:8083"

Write-Host "=== Waiting for Kafka Connect REST API ===" -ForegroundColor Cyan

$maxRetries = 30
$retryCount = 0
while ($retryCount -lt $maxRetries) {
    try {
        $response = Invoke-RestMethod -Uri "$CONNECT_URL/connectors" -Method GET -TimeoutSec 5
        Write-Host "Kafka Connect is ready!" -ForegroundColor Green
        break
    } catch {
        $retryCount++
        Write-Host "  Waiting... ($retryCount/$maxRetries)"
        Start-Sleep -Seconds 5
    }
}

if ($retryCount -eq $maxRetries) {
    Write-Host "ERROR: Kafka Connect not available after $maxRetries retries" -ForegroundColor Red
    exit 1
}

# Register GPS S3 Sink connector
Write-Host ""
Write-Host "=== Registering S3 Sink: raw.gps ===" -ForegroundColor Cyan
$gpsConfig = Get-Content -Raw "config\connect-s3-sink-gps.json"
try {
    $existing = Invoke-RestMethod -Uri "$CONNECT_URL/connectors/s3-sink-raw-gps" -Method GET -ErrorAction SilentlyContinue
    Write-Host "  Connector 's3-sink-raw-gps' already exists. Updating..."
    $configOnly = ($gpsConfig | ConvertFrom-Json).config
    Invoke-RestMethod -Uri "$CONNECT_URL/connectors/s3-sink-raw-gps/config" `
        -Method PUT -Body ($configOnly | ConvertTo-Json -Depth 10) -ContentType "application/json"
} catch {
    Write-Host "  Creating new connector 's3-sink-raw-gps'..."
    Invoke-RestMethod -Uri "$CONNECT_URL/connectors" `
        -Method POST -Body $gpsConfig -ContentType "application/json"
}
Write-Host "  raw.gps sink registered!" -ForegroundColor Green

# Register Trips S3 Sink connector
Write-Host ""
Write-Host "=== Registering S3 Sink: raw.trips ===" -ForegroundColor Cyan
$tripsConfig = Get-Content -Raw "config\connect-s3-sink-trips.json"
try {
    $existing = Invoke-RestMethod -Uri "$CONNECT_URL/connectors/s3-sink-raw-trips" -Method GET -ErrorAction SilentlyContinue
    Write-Host "  Connector 's3-sink-raw-trips' already exists. Updating..."
    $configOnly = ($tripsConfig | ConvertFrom-Json).config
    Invoke-RestMethod -Uri "$CONNECT_URL/connectors/s3-sink-raw-trips/config" `
        -Method PUT -Body ($configOnly | ConvertTo-Json -Depth 10) -ContentType "application/json"
} catch {
    Write-Host "  Creating new connector 's3-sink-raw-trips'..."
    Invoke-RestMethod -Uri "$CONNECT_URL/connectors" `
        -Method POST -Body $tripsConfig -ContentType "application/json"
}
Write-Host "  raw.trips sink registered!" -ForegroundColor Green

# Show status
Write-Host ""
Write-Host "=== Connector Status ===" -ForegroundColor Cyan
$connectors = Invoke-RestMethod -Uri "$CONNECT_URL/connectors" -Method GET
foreach ($name in $connectors) {
    $status = Invoke-RestMethod -Uri "$CONNECT_URL/connectors/$name/status" -Method GET
    $state = $status.connector.state
    $taskStates = ($status.tasks | ForEach-Object { $_.state }) -join ", "
    Write-Host "  $name : connector=$state, tasks=[$taskStates]"
}

Write-Host ""
Write-Host "=== Done! Connectors registered ===" -ForegroundColor Green
Write-Host "GPS events will be archived to: s3a://kafka-archive/raw.gps/"
Write-Host "Trip events will be archived to: s3a://kafka-archive/raw.trips/"
