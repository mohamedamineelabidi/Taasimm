#!/usr/bin/env bash
# =============================================================
# register-s3-sink.sh (legacy compatibility)
# Registers the split S3 Sink connectors for raw.gps and raw.trips.
# Run this from the project root after kafka-connect is healthy.
# =============================================================

set -euo pipefail

CONNECT_URL="${CONNECT_URL:-http://localhost:8083}"
GPS_CONFIG="config/connect-s3-sink-gps.json"
TRIPS_CONFIG="config/connect-s3-sink-trips.json"

echo "=== TaaSim Kafka Connect — Split S3 Sink Registration ==="
echo "Connect REST: $CONNECT_URL"

# Wait until Connect is reachable
until curl -sf "$CONNECT_URL/connectors" > /dev/null; do
  echo "Waiting for Kafka Connect..."
  sleep 5
done

register_or_update() {
  local name="$1"
  local config_path="$2"

  echo "Registering connector: $name from $config_path"
  if curl -sf "$CONNECT_URL/connectors/$name" > /dev/null 2>&1; then
    curl -s -X PUT "$CONNECT_URL/connectors/$name/config" \
      -H "Content-Type: application/json" \
      -d "$(jq '.config' "$config_path")" > /tmp/connect-response.json
  else
    curl -s -X POST "$CONNECT_URL/connectors" \
      -H "Content-Type: application/json" \
      -d @"$config_path" > /tmp/connect-response.json
  fi
}

if ! command -v jq >/dev/null 2>&1; then
  echo "ERROR: jq is required by this script. Install jq or use scripts/register-connectors.ps1 on Windows."
  exit 1
fi

register_or_update "s3-sink-raw-gps" "$GPS_CONFIG"
register_or_update "s3-sink-raw-trips" "$TRIPS_CONFIG"

echo "=== Connector status ==="
curl -s "$CONNECT_URL/connectors/s3-sink-raw-gps/status"
echo
curl -s "$CONNECT_URL/connectors/s3-sink-raw-trips/status"
echo
