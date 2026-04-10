"""Kafka producer — publishes validated ETH transactions to the eth-txns topic."""

import json
import logging

from kafka import KafkaProducer

logger = logging.getLogger(__name__)

REQUIRED_FIELDS = {
    "tx_hash",
    "block_number",
    "timestamp",
    "from",
    "to",
    "value_eth",
    "gas",
    "gas_price",
}


class TxnProducer:
    """Publishes one Kafka message per ETH transaction, keyed by tx_hash."""

    def __init__(self, bootstrap_servers: str, topic: str):
        self.topic = topic
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8") if k else None,
            acks="all",
            retries=3,
            linger_ms=50,
            batch_size=32768,
        )
        logger.info("Kafka producer connected to %s, topic=%s", bootstrap_servers, topic)

    def publish(self, txn: dict) -> None:
        """Validate schema and send a single transaction to Kafka."""
        missing = REQUIRED_FIELDS - txn.keys()
        if missing:
            raise ValueError(f"Transaction missing fields: {missing}")

        self.producer.send(
            self.topic,
            key=txn["tx_hash"],
            value=txn,
        )

    def flush(self) -> None:
        """Flush pending messages to broker."""
        self.producer.flush()

    def close(self) -> None:
        """Flush and close the producer."""
        self.producer.flush()
        self.producer.close()
        logger.info("Kafka producer closed")
