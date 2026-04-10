"""Crawler main loop — polls ETH blocks, publishes to Kafka, flushes Parquet to GCS."""

import os
import sys
import signal
import time
import json
import logging
from datetime import datetime, timezone
from http.server import HTTPServer, BaseHTTPRequestHandler
from threading import Thread

from dotenv import load_dotenv

from eth_client import EthClient
from kafka_producer import TxnProducer
from gcs_writer import GcsParquetWriter
from checkpoint import CheckpointManager
from gap_detector import GapDetector

load_dotenv()

# ── Config ────────────────────────────────────
ETH_RPC_URL = os.environ["ETH_RPC_URL"]
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "eth-txns")
GCS_BUCKET = os.environ.get("GCS_BUCKET", "eth-bigdata-project")
CHECKPOINT_LOCAL = os.environ.get("CHECKPOINT_LOCAL", "checkpoints/last_block.txt")
FLUSH_INTERVAL = int(os.environ.get("FLUSH_INTERVAL_BLOCKS", "10"))
POLL_SLEEP = int(os.environ.get("POLL_SLEEP_SECONDS", "2"))
HEALTH_PORT = int(os.environ.get("HEALTH_PORT", "8001"))

# ── Logging ───────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger("crawler")

# ── Global state for health endpoint ──────────
_state = {
    "last_block": 0,
    "chain_head": 0,
    "start_time": time.time(),
    "status": "starting",
}


# ── Health endpoint ───────────────────────────
class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/healthz":
            lag = _state["chain_head"] - _state["last_block"]
            body = json.dumps(
                {
                    "last_block": _state["last_block"],
                    "chain_head": _state["chain_head"],
                    "lag": lag,
                    "status": "ok" if lag <= 100 else "lagging",
                    "uptime_seconds": int(time.time() - _state["start_time"]),
                }
            )
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(body.encode())
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, format, *args):
        pass  # suppress request logs


def start_health_server(port: int):
    server = HTTPServer(("0.0.0.0", port), HealthHandler)
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    logger.info("Health endpoint listening on :%d/healthz", port)


# ── Main ──────────────────────────────────────
def main():
    # Signal handling for graceful shutdown
    shutdown = False

    def handle_signal(signum, frame):
        nonlocal shutdown
        logger.info("Received signal %d — shutting down gracefully...", signum)
        shutdown = True

    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)

    # Start health server
    start_health_server(HEALTH_PORT)

    # Initialize components
    eth = EthClient(ETH_RPC_URL)
    kafka = TxnProducer(KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC)
    gcs = GcsParquetWriter(GCS_BUCKET)
    ckpt = CheckpointManager(
        local_path=CHECKPOINT_LOCAL,
        gcs_bucket=GCS_BUCKET,
    )

    # Load checkpoint
    last_block = ckpt.load()
    _state["last_block"] = last_block
    logger.info("Starting from block %d", last_block)

    # Gap detection + backfill
    chain_head = eth.get_latest_block_number()
    _state["chain_head"] = chain_head

    if last_block > 0 and last_block < chain_head:
        detector = GapDetector(GCS_BUCKET)
        gaps = detector.detect_gaps(last_block, chain_head)
        for gap_start, gap_end in gaps:
            logger.info("Backfilling gap: blocks %d–%d", gap_start, gap_end)
            _process_block_range(
                eth, kafka, gcs, ckpt, gap_start, gap_end, shutdown_check=lambda: shutdown
            )
            if shutdown:
                break

    # Live polling loop
    _state["status"] = "running"
    logger.info("Entering live polling loop")
    buffer = []
    blocks_since_flush = 0

    while not shutdown:
        try:
            chain_head = eth.get_latest_block_number()
            _state["chain_head"] = chain_head

            if last_block >= chain_head:
                time.sleep(POLL_SLEEP)
                continue

            # Process new blocks
            for block_num in range(last_block + 1, chain_head + 1):
                if shutdown:
                    break

                txns = eth.get_block_transactions(block_num)

                # Publish each txn to Kafka
                for txn in txns:
                    kafka.publish(txn)

                # Buffer for GCS
                buffer.extend(txns)
                blocks_since_flush += 1
                last_block = block_num
                _state["last_block"] = last_block

                # Flush every N blocks
                if blocks_since_flush >= FLUSH_INTERVAL:
                    _flush_buffer(gcs, ckpt, buffer, last_block)
                    buffer = []
                    blocks_since_flush = 0

        except KeyboardInterrupt:
            break
        except Exception as e:
            logger.error("Error in main loop: %s", e, exc_info=True)
            time.sleep(5)  # brief pause before retry

    # Graceful shutdown: flush remaining buffer
    if buffer:
        logger.info("Flushing remaining %d transactions before shutdown", len(buffer))
        _flush_buffer(gcs, ckpt, buffer, last_block)

    kafka.close()
    logger.info("Crawler stopped at block %d", last_block)


def _process_block_range(eth, kafka, gcs, ckpt, start, end, shutdown_check):
    """Process a contiguous range of blocks (used for backfill)."""
    buffer = []
    blocks_since_flush = 0

    for block_num in range(start, end + 1):
        if shutdown_check():
            break

        txns = eth.get_block_transactions(block_num)
        for txn in txns:
            kafka.publish(txn)
        buffer.extend(txns)
        blocks_since_flush += 1

        if blocks_since_flush >= FLUSH_INTERVAL:
            _flush_buffer(gcs, ckpt, buffer, block_num)
            buffer = []
            blocks_since_flush = 0

    # Flush remainder
    if buffer:
        _flush_buffer(gcs, ckpt, buffer, end)


def _flush_buffer(gcs, ckpt, buffer, block_number):
    """Flush transaction buffer to GCS and save checkpoint."""
    if not buffer:
        return

    # Group by date for partitioned writes
    by_date = {}
    for txn in buffer:
        # Parse ISO timestamp to date string
        dt = txn["timestamp"][:10]  # YYYY-MM-DD
        by_date.setdefault(dt, []).append(txn)

    total_rows = 0
    for date_str, txns in by_date.items():
        total_rows += gcs.flush(txns, date_str)

    ckpt.save(block_number)
    logger.info(
        "Flush: %d rows across %d partitions, checkpoint → block %d",
        total_rows,
        len(by_date),
        block_number,
    )


if __name__ == "__main__":
    main()
