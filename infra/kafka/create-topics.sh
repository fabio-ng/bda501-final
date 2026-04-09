#!/bin/bash

# ============================================================
# Create Kafka Topics for Ethereum Phishing Detection Platform
# Run this script after Kafka is started
# ============================================================

KAFKA_BROKER="${KAFKA_BOOTSTRAP_SERVERS:-kafka:9092}"

echo "Creating Kafka topics on broker: $KAFKA_BROKER"

# Function to create topic
create_topic() {
    local topic=$1
    local partitions=$2
    local replication=$3
    local retention=$4

    echo "Creating topic: $topic (partitions=$partitions, replication=$replication, retention=$retention)"

    kafka-topics \
        --create \
        --bootstrap-server "$KAFKA_BROKER" \
        --topic "$topic" \
        --partitions "$partitions" \
        --replication-factor "$replication" \
        --config retention.ms="$retention" \
        --if-not-exists

    if [ $? -eq 0 ]; then
        echo "✓ Topic $topic created successfully"
    else
        echo "✗ Failed to create topic $topic (may already exist)"
    fi
}

# ── Main Topics ──

# eth.transactions: Raw Ethereum transactions for processing
# 7 days retention = 604800000 ms
create_topic "eth.transactions" 3 1 604800000

# eth.alerts: High-confidence phishing alerts from detector
# 30 days retention = 2592000000 ms
create_topic "eth.alerts" 3 1 2592000000

# eth.dead-letter: Failed messages for troubleshooting
# 30 days retention
create_topic "eth.dead-letter" 1 1 2592000000

echo ""
echo "All topics created. Listing topics:"
kafka-topics --list --bootstrap-server "$KAFKA_BROKER"

echo ""
echo "Topic creation complete!"
