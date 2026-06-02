#!/bin/bash
set -e

CONNECT_URL="http://localhost:8083"
CONNECTOR_CONFIG="../connectors/postgres-connector.json"

echo "Waiting for Kafka Connect to be ready..."
until curl -sf "$CONNECT_URL/connectors" > /dev/null; do
    echo "  Not ready yet, retrying in 3s..."
    sleep 3
done

echo "Kafka Connect is ready."

echo "Registering Debezium PostgreSQL connector..."
curl -sf -X POST "$CONNECT_URL/connectors" \
  -H "Content-Type: application/json" \
  -d @"$CONNECTOR_CONFIG"

echo ""
echo "Done. Checking connector status..."
sleep 2
curl -sf "$CONNECT_URL/connectors/postgres-tickets-connector/status" | python3 -m json.tool 2>/dev/null || \
curl -sf "$CONNECT_URL/connectors/postgres-tickets-connector/status"

echo ""
echo "Debezium is now tailing PostgreSQL WAL."
echo "Any write to the 'tickets' table will appear in Kafka topic: dbserver1.public.tickets"
