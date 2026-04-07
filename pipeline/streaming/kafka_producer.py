"""
Phase 4.6: Kafka Producer — Ingests new Ethereum transactions.

Polls the Etherscan API for new blocks and publishes transactions
to the 'new_transactions' Kafka topic.

Usage:
    export ETHERSCAN_API_KEY=<your-key>
    python kafka_producer.py
"""

import json
import time
import logging
import requests
from kafka import KafkaProducer

import sys
sys.path.append("..")
from config import (
    ETHERSCAN_API_KEY, ETHERSCAN_API_URL,
    KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC_NEW_TX,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# ──────────────────────────────────────────────
# Etherscan API helpers
# ──────────────────────────────────────────────

def get_latest_block_number() -> int:
    """Get the latest Ethereum block number from Etherscan."""
    resp = requests.get(ETHERSCAN_API_URL, params={
        "module": "proxy",
        "action": "eth_blockNumber",
        "apikey": ETHERSCAN_API_KEY,
    }, timeout=10)
    resp.raise_for_status()
    return int(resp.json()["result"], 16)


def get_block_transactions(block_number: int) -> list[dict]:
    """Fetch all transactions in a specific block."""
    resp = requests.get(ETHERSCAN_API_URL, params={
        "module": "proxy",
        "action": "eth_getBlockByNumber",
        "tag": hex(block_number),
        "boolean": "true",
        "apikey": ETHERSCAN_API_KEY,
    }, timeout=30)
    resp.raise_for_status()
    block = resp.json().get("result")
    if not block or not block.get("transactions"):
        return []

    transactions = []
    for tx in block["transactions"]:
        transactions.append({
            "tx_hash": tx.get("hash", ""),
            "from_address": (tx.get("from") or "").lower(),
            "to_address": (tx.get("to") or "").lower(),
            "value": int(tx.get("value", "0x0"), 16) / 1e18,  # Wei -> ETH
            "gas": int(tx.get("gas", "0x0"), 16),
            "gas_price": int(tx.get("gasPrice", "0x0"), 16),
            "block_number": block_number,
            "timestamp": int(block.get("timestamp", "0x0"), 16),
        })
    return transactions


# ──────────────────────────────────────────────
# Kafka Producer
# ──────────────────────────────────────────────

def create_producer() -> KafkaProducer:
    """Create a Kafka producer with JSON serialization."""
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        acks="all",
        retries=3,
        linger_ms=100,  # batch small messages
    )


def run_producer(poll_interval: int = 15):
    """
    Main loop: poll Etherscan for new blocks and publish transactions.

    Args:
        poll_interval: Seconds between polls (~15s = Ethereum block time).
    """
    producer = create_producer()
    last_block = get_latest_block_number() - 1
    logger.info(f"Starting producer from block {last_block + 1}")

    try:
        while True:
            current_block = get_latest_block_number()

            # Process any new blocks since last check
            while last_block < current_block:
                last_block += 1
                transactions = get_block_transactions(last_block)

                for tx in transactions:
                    # Key by sender address for partitioning
                    producer.send(
                        KAFKA_TOPIC_NEW_TX,
                        key=tx["from_address"],
                        value=tx,
                    )

                if transactions:
                    logger.info(
                        f"Block {last_block}: published {len(transactions)} transactions"
                    )

            producer.flush()
            time.sleep(poll_interval)

    except KeyboardInterrupt:
        logger.info("Shutting down producer.")
    finally:
        producer.close()


if __name__ == "__main__":
    if not ETHERSCAN_API_KEY:
        logger.error("Set ETHERSCAN_API_KEY environment variable.")
        sys.exit(1)
    run_producer()
