#!/usr/bin/env bash
# =============================================================
# register-s3-sink.sh
# Manually register (or refresh) the TaaSim S3 Sink connector.
# Run this from the project root after kafka-connect is healthy.
# =============================================================

set -e

CONNECT_URL="${CONNECT_URL:-http://localhost:8083}"
CONFIG_FILE="config/kafka-connect/s3-sink-connector.json"
CONNECTOR_NAME="taasim-s3-sink"

echo "=== TaaSim Kafka Connect — S3 Sink Registration ==="
echo "Connect REST: $CONNECT_URL"

# Wait until Connect is reachable
until curl -sf "$CONNECT_URL/connectors" > /dev/null; do
  echo "Waiting for Kafka Connect..."
  sleep 5
done

# Delete existing connector if present (idempotent)
if curl -sf "$CONNECT_URL/connectors/$CONNECTOR_NAME" > /dev/null 2>&1; then
  echo "Deleting existing connector $CONNECTOR_NAME..."
  curl -s -X DELETE "$CONNECT_URL/connectors/$CONNECTOR_NAME"
  sleep 2
fi

# Register connector
echo "Registering connector from $CONFIG_FILE..."
HTTP_STATUS=$(curl -s -o /tmp/connect-response.json \
  -w "%{http_code}" \
  -X POST "$CONNECT_URL/connectors" \
  -H "Content-Type: application/json" \
  -d @"$CONFIG_FILE")

if echo "$HTTP_STATUS" | grep -q "^2"; then
  echo "=== Connector registered successfully (HTTP $HTTP_STATUS) ==="
  curl -s "$CONNECT_URL/connectors/$CONNECTOR_NAME/status"
else
  echo "ERROR: Registration failed (HTTP $HTTP_STATUS)"
  cat /tmp/connect-response.json
  exit 1
fi
