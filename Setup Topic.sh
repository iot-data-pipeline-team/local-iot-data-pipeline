# Creates required Kafka topics inside the running docker-compose stack.
# Run once before starting the producer or Spark job.
 
set -e

 
KAFKA_CONTAINER="data-kafka-11"
ZOOKEEPER="zookeeper:2181"
 
echo "Creating Kafka topics..."

docker exec "$KAFKA_CONTAINER" kafka-topics \
  --create \
  --if-not-exists \
  --bootstrap-server kafka1:9092 \
  --replication-factor 2 \
  --partitions 3 \
  --topic sensor-events


docker exec "$KAFKA_CONTAINER" kafka-topics \
  --create \
  --if-not-exists \
  --bootstrap-server kafka1:9092 \
  --replication-factor 1 \
  --partitions 3 \
  --topic sensor-processed


echo ""
echo "Topics created. Listing all topics:"
docker exec "$KAFKA_CONTAINER" kafka-topics \
  --list \
  --bootstrap-server kafka1:9092