#!/bin/bash

CONFIG_FILE="${CONNECT_CONNECTOR_CONFIG_FILE:-/etc/kafka-connect/pg-config.json}"
CONNECT_URL="http://localhost:8083"

echo "Checking connector status..."

# Проверяем, существует ли уже коннектор
if curl -s "$CONNECT_URL/connectors/$CONNECT_CONNECTOR_NAME" | grep -q "error_code"; then
  echo "Creating new connector: $CONNECT_CONNECTOR_NAME"
  curl -X POST -H "Content-Type: application/json" \
    --data @"$CONFIG_FILE" \
    "$CONNECT_URL/connectors"
else
  echo "Connector $CONNECT_CONNECTOR_NAME already exists"
fi