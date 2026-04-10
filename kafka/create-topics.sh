#!/bin/bash
# Wait for Kafka to be ready
echo "Waiting for Kafka to be ready..."
cub kafka-ready -b localhost:9092 1 60 2>/dev/null || sleep 10

# Create eth-txns topic
kafka-topics --create --if-not-exists \
    --bootstrap-server localhost:9092 \
    --topic eth-txns \
    --partitions 6 \
    --replication-factor 1

echo "Kafka topic 'eth-txns' created (6 partitions, RF=1)"
