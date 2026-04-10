"""ETH RPC client — wraps web3.py to fetch blocks and parse transactions."""

import time
import logging
from decimal import Decimal
from datetime import datetime, timezone

from web3 import Web3

logger = logging.getLogger(__name__)

WEI_PER_ETH = Decimal("1000000000000000000")

# Retry config
MAX_RETRIES = 3
BACKOFF_BASE = 1  # seconds


class EthClient:
    """Thin wrapper around web3.py HTTP provider for block + transaction fetching."""

    def __init__(self, rpc_url: str):
        self.w3 = Web3(Web3.HTTPProvider(rpc_url, request_kwargs={"timeout": 30}))
        if not self.w3.is_connected():
            raise ConnectionError(f"Cannot connect to ETH RPC at {rpc_url}")
        logger.info("Connected to ETH RPC: %s", rpc_url[:40] + "...")

    def get_latest_block_number(self) -> int:
        """Return the latest block number on the chain."""
        return self._retry(lambda: self.w3.eth.block_number)

    def get_block_transactions(self, block_number: int) -> list[dict]:
        """Fetch a block and return parsed transaction dicts.

        Skips contract-creation txns (where `to` is None).
        """
        block = self._retry(
            lambda: self.w3.eth.get_block(block_number, full_transactions=True)
        )
        timestamp = datetime.fromtimestamp(block["timestamp"], tz=timezone.utc)

        transactions = []
        for tx in block["transactions"]:
            # Skip contract creation (to == None)
            if tx["to"] is None:
                continue

            transactions.append(
                {
                    "tx_hash": tx["hash"].hex(),
                    "block_number": block_number,
                    "timestamp": timestamp.isoformat(),
                    "from": tx["from"].lower(),
                    "to": tx["to"].lower(),
                    "value_eth": str(Decimal(tx["value"]) / WEI_PER_ETH),
                    "gas": tx["gas"],
                    "gas_price": tx["gasPrice"],
                }
            )

        logger.debug(
            "Block %d: %d txns (%d skipped contract-creation)",
            block_number,
            len(transactions),
            len(block["transactions"]) - len(transactions),
        )
        return transactions

    @staticmethod
    def _retry(fn, max_retries: int = MAX_RETRIES):
        """Execute fn with exponential backoff on failure."""
        for attempt in range(max_retries):
            try:
                return fn()
            except Exception as e:
                if attempt == max_retries - 1:
                    raise
                wait = BACKOFF_BASE * (2 ** attempt)
                logger.warning(
                    "RPC call failed (attempt %d/%d): %s — retrying in %ds",
                    attempt + 1,
                    max_retries,
                    e,
                    wait,
                )
                time.sleep(wait)
