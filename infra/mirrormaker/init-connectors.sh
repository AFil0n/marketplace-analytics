#!/bin/bash

echo "=== Starting Kafka Connect Connectors Initialization ==="

# Ждем пока Kafka Connect станет доступен
echo "Waiting for Kafka Connect to be ready..."
MAX_WAIT=120
WAIT_TIME=0

while [ $WAIT_TIME -lt $MAX_WAIT ]; do
    if curl -s http://localhost:8083/ > /dev/null; then
        echo "✅ Kafka Connect is ready!"
        break
    fi
    echo "Waiting for Kafka Connect... ($((WAIT_TIME + 5))s)"
    sleep 5
    WAIT_TIME=$((WAIT_TIME + 5))
done

if [ $WAIT_TIME -ge $MAX_WAIT ]; then
    echo "❌ Kafka Connect not ready after $MAX_WAIT seconds"
    exit 1
fi

echo "=== Creating ALL MirrorMaker2 system topics in DR cluster ==="

# Создаем топики и в primary кластере на всякий случай
echo "=== Creating system topics in Primary cluster ==="
for topic in "${SYSTEM_TOPICS[@]}"; do
    kafka-topics --bootstrap-server kafka-0:1092 \
        --command-config /etc/kafka/secrets/client.properties \
        --create --topic "$topic" \
        --partitions 25 --replication-factor 2 \
        --config cleanup.policy=compact 2>/dev/null && echo "✅ Created $topic in primary" || echo "⚠️ Topic $topic may already exist in primary"
done

echo "=== Creating connectors ==="

# Сначала создаем heartbeat connector (он проще)
echo "Creating mirror-heartbeat-connector..."
HEARTBEAT_RESPONSE=$(curl -s -o /dev/null -w "%{http_code}" -X POST \
  -H "Content-Type: application/json" \
  -d @/etc/kafka-connect/connectors/mirror-heartbeat.json \
  http://localhost:8083/connectors)

if [ "$HEARTBEAT_RESPONSE" -eq 201 ]; then
    echo "✅ mirror-heartbeat-connector created successfully (HTTP 201)"
elif [ "$HEARTBEAT_RESPONSE" -eq 409 ]; then
    echo "✅ mirror-heartbeat-connector already exists (HTTP 409)"
else
    echo "❌ Failed to create mirror-heartbeat-connector (HTTP $HEARTBEAT_RESPONSE)"
fi

sleep 5

# Затем создаем source connector
echo "Creating mirror-source-connector..."
SOURCE_RESPONSE=$(curl -s -o /dev/null -w "%{http_code}" -X POST \
  -H "Content-Type: application/json" \
  -d @/etc/kafka-connect/connectors/mirror-source.json \
  http://localhost:8083/connectors)

if [ "$SOURCE_RESPONSE" -eq 201 ]; then
    echo "✅ mirror-source-connector created successfully (HTTP 201)"
elif [ "$SOURCE_RESPONSE" -eq 409 ]; then
    echo "✅ mirror-source-connector already exists (HTTP 409)"
else
    echo "❌ Failed to create mirror-source-connector (HTTP $SOURCE_RESPONSE)"
    # Покажем детали ошибки
    echo "Error details:"
    curl -X POST -H "Content-Type: application/json" \
      -d @/etc/kafka-connect/connectors/mirror-source.json \
      http://localhost:8083/connectors
fi

# Даем время на инициализацию
echo "=== Waiting for connectors to initialize (15 seconds) ==="
sleep 15

# Проверяем статусы
echo "=== Checking connectors status ==="

echo "--- mirror-source-connector status ---"
curl -s http://localhost:8083/connectors/mirror-source-connector/status | jq -r '.connector.state,.tasks[0].state' 2>/dev/null || curl -s http://localhost:8083/connectors/mirror-source-connector/status

echo "--- mirror-heartbeat-connector status ---"
curl -s http://localhost:8083/connectors/mirror-heartbeat-connector/status | jq -r '.connector.state,.tasks[0].state' 2>/dev/null || curl -s http://localhost:8083/connectors/mirror-heartbeat-connector/status

echo "=== Connectors initialization completed ==="