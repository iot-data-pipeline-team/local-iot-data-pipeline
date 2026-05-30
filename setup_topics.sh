#!/usr/bin/env bash
# Creates required Kafka topics inside the running docker-compose stack.
# Run once before starting the producer or Spark job.

set -e

KAFKA_CONTAINER="${KAFKA_CONTAINER:-data-kafka-11}"
BOOTSTRAP="${KAFKA_BOOTSTRAP:-kafka1:9092}"

create_topic() {
  local topic="$1"
  local partitions="$2"
  local replication="$3"

  docker exec "$KAFKA_CONTAINER" kafka-topics \
    --create \
    --if-not-exists \
    --bootstrap-server "$BOOTSTRAP" \
    --replication-factor "$replication" \
    --partitions "$partitions" \
    --topic "$topic"
}

echo "Creating Kafka topics (container: $KAFKA_CONTAINER)..."

create_topic "sensor-events"     3 2
create_topic "sensor-processed"  3 1
create_topic "sensor-alerts"     3 1

echo ""
echo "Topics created. Listing all topics:"
docker exec "$KAFKA_CONTAINER" kafka-topics \
  --list \
  --bootstrap-server "$BOOTSTRAP"
