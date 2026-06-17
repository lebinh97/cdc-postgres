#!/bin/sh
# 1. Start Kafka Connect (background)
# 2. Wait for REST API
# 3. Register Debezium connector
# 4. Bring Connect to foreground

export CONNECT_REST_ADVERTISED_HOST_NAME=debezium-connect

# Start Connect using the image's built-in entrypoint
/docker-entrypoint.sh start &
PID=$!

# Wait until REST API is up
until curl -s http://localhost:8083; do sleep 3; done
echo "[startup] Kafka Connect is up — registering connector..."

# Register the connector
curl -s -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" -d @/connector.json
echo ""

# Show status
sleep 2
curl -s http://localhost:8083/connectors/cdc-postgres-connector/status
echo ""
echo "[startup] Done."

# Keep Connect running
wait $PID
