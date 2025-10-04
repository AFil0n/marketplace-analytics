#!/bin/bash
echo "Creating connectors..."
curl -X POST -H "Content-Type: application/json" -d @/etc/kafka-connect/connectors/mirror-source.json http://localhost:8083/connectors
sleep 5
curl -X POST -H "Content-Type: application/json" -d @/etc/kafka-connect/connectors/mirror-heartbeat.json http://localhost:8083/connectors

echo "Connectors created successfully!"