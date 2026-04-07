# =============================================================
# register-s3-sink.ps1
# Manually register (or refresh) the TaaSim S3 Sink connector.
# Run from the project root after kafka-connect is healthy.
# Usage: .\scripts\register-s3-sink.ps1
# =============================================================

$CONNECT_URL = $env:CONNECT_URL ?? "http://localhost:8083"
$CONFIG_FILE = "config\kafka-connect\s3-sink-connector.json"
$CONNECTOR_NAME = "taasim-s3-sink"

Write-Host "=== TaaSim Kafka Connect — S3 Sink Registration ===" -ForegroundColor Cyan
Write-Host "Connect REST: $CONNECT_URL"

# Wait until Connect is reachable
Write-Host "Waiting for Kafka Connect to be healthy..."
$maxRetries = 20
$retry = 0
do {
    Start-Sleep -Seconds 5
    $retry++
    try {
        $resp = Invoke-WebRequest -Uri "$CONNECT_URL/connectors" -UseBasicParsing -ErrorAction Stop
        break
    } catch {
        Write-Host "  Attempt $retry/$maxRetries — not ready yet"
    }
} while ($retry -lt $maxRetries)

if ($retry -ge $maxRetries) {
    Write-Error "Kafka Connect did not become healthy after $maxRetries attempts."
    exit 1
}

# Delete existing connector if present (idempotent re-registration)
try {
    $existing = Invoke-WebRequest -Uri "$CONNECT_URL/connectors/$CONNECTOR_NAME" -UseBasicParsing -ErrorAction Stop
    Write-Host "Deleting existing connector $CONNECTOR_NAME..."
    Invoke-WebRequest -Method Delete -Uri "$CONNECT_URL/connectors/$CONNECTOR_NAME" -UseBasicParsing | Out-Null
    Start-Sleep -Seconds 2
} catch {
    # Connector does not exist yet — OK
}

# Register connector
Write-Host "Registering connector from $CONFIG_FILE..."
$body = Get-Content $CONFIG_FILE -Raw
$resp = Invoke-WebRequest -Method Post `
    -Uri "$CONNECT_URL/connectors" `
    -ContentType "application/json" `
    -Body $body `
    -UseBasicParsing

if ($resp.StatusCode -in 200, 201) {
    Write-Host "=== Connector registered successfully (HTTP $($resp.StatusCode)) ===" -ForegroundColor Green
    $status = Invoke-WebRequest -Uri "$CONNECT_URL/connectors/$CONNECTOR_NAME/status" -UseBasicParsing
    Write-Host $status.Content
} else {
    Write-Error "Registration failed (HTTP $($resp.StatusCode)): $($resp.Content)"
    exit 1
}
