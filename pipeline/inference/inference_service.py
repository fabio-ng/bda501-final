"""
Phase 4.10: GNN Inference Service

Loads the trained GraphSAGE model and provides scoring functions
for both batch and streaming inference.

Used by:
    - spark_streaming.py (streaming inference)
    - api/app.py (on-demand scoring)
"""

import os
import pickle
import logging
import threading
import time

import numpy as np
import torch
import torch.nn as nn
import torch.nn.functional as F
import dgl
from dgl.nn import SAGEConv

import sys
sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
from config import (
    IN_FEATS, HIDDEN_FEATS, OUT_FEATS, DROPOUT,
    MODEL_LOCAL_PATH, PHISHING_THRESHOLD,
    MODEL_REFRESH_INTERVAL_HOURS,
)

logger = logging.getLogger(__name__)


# ──────────────────────────────────────────────
# GraphSAGE Model (must match training definition)
# ──────────────────────────────────────────────

class GraphSAGE(nn.Module):
    """2-layer GraphSAGE for node classification."""

    def __init__(self, in_feats, hidden_feats, out_feats, dropout=0.5):
        super().__init__()
        self.conv1 = SAGEConv(in_feats, hidden_feats, aggregator_type="mean")
        self.conv2 = SAGEConv(hidden_feats, out_feats, aggregator_type="mean")
        self.dropout = nn.Dropout(dropout)

    def forward(self, blocks, x):
        h = self.conv1(blocks[0], x)
        h = F.relu(h)
        h = self.dropout(h)
        h = self.conv2(blocks[1], h)
        return h

    def forward_full(self, graph, x):
        """Full-graph forward pass (no mini-batching)."""
        h = self.conv1(graph, x)
        h = F.relu(h)
        h = self.dropout(h)
        h = self.conv2(graph, h)
        return h


# ──────────────────────────────────────────────
# Inference Service
# ──────────────────────────────────────────────

class PhishingInferenceService:
    """
    Manages model loading, graph data, and scoring.

    Loads the trained model + graph data and exposes a `score_addresses()`
    method for both streaming and batch inference.
    """

    def __init__(self, model_path: str, data_dir: str):
        self.model_path = model_path
        self.data_dir = data_dir
        self.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")

        self.model = None
        self.graph = None
        self.node_features = None
        self.node_to_id = None
        self.id_to_node = None
        self.scaler_mean = None
        self.scaler_scale = None
        self._model_load_time = 0

        self.load_model()
        self.load_graph_data()

    def load_model(self):
        """Load trained GraphSAGE model from checkpoint."""
        logger.info(f"Loading model from {self.model_path}")
        checkpoint = torch.load(self.model_path, map_location=self.device, weights_only=False)

        self.model = GraphSAGE(IN_FEATS, HIDDEN_FEATS, OUT_FEATS, DROPOUT)
        self.model.load_state_dict(checkpoint["model_state_dict"])
        self.model.to(self.device)
        self.model.eval()

        # Load scaler parameters for feature normalization
        self.scaler_mean = checkpoint.get("scaler_mean")
        self.scaler_scale = checkpoint.get("scaler_scale")

        self._model_load_time = time.time()
        metrics = checkpoint.get("metrics", {})
        logger.info(
            f"Model loaded. Test F1={metrics.get('test_f1', 'N/A')}, "
            f"AUC-ROC={metrics.get('test_auc_roc', 'N/A')}"
        )

    def load_graph_data(self):
        """Load graph structure and node features from numpy files."""
        logger.info(f"Loading graph data from {self.data_dir}")

        self.node_features = np.load(os.path.join(self.data_dir, "node_features.npy"))
        edge_index = np.load(os.path.join(self.data_dir, "edge_index.npy"))

        with open(os.path.join(self.data_dir, "node_to_id.pkl"), "rb") as f:
            self.node_to_id = pickle.load(f)
        self.id_to_node = {v: k for k, v in self.node_to_id.items()}

        # Build DGL graph
        self.graph = dgl.graph((edge_index[0], edge_index[1]),
                               num_nodes=len(self.node_features))
        self.graph.ndata["feat"] = torch.tensor(self.node_features, dtype=torch.float32)

        logger.info(
            f"Graph loaded: {self.graph.num_nodes():,} nodes, "
            f"{self.graph.num_edges():,} edges"
        )

    def needs_refresh(self) -> bool:
        """Check if model should be reloaded (older than refresh interval)."""
        elapsed_hours = (time.time() - self._model_load_time) / 3600
        return elapsed_hours >= MODEL_REFRESH_INTERVAL_HOURS

    def refresh_if_needed(self):
        """Reload model if refresh interval has passed."""
        if self.needs_refresh():
            logger.info("Model refresh interval reached. Reloading...")
            self.load_model()

    def compute_features_for_address(self, address: str, tx_data: dict) -> np.ndarray:
        """
        Compute a 12-dim feature vector for a new/updated address.

        If the address exists in the graph, returns its stored features.
        If new, computes features from the transaction data.
        """
        address = address.lower()

        if address in self.node_to_id:
            nid = self.node_to_id[address]
            return self.node_features[nid]

        # New address: create features from transaction context
        features = np.zeros(IN_FEATS, dtype=np.float32)
        features[0] = tx_data.get("in_degree", 1)       # in_degree
        features[1] = tx_data.get("out_degree", 1)       # out_degree
        features[2] = tx_data.get("value", 0)            # total_eth_received
        features[3] = tx_data.get("value", 0)            # total_eth_sent
        features[4] = tx_data.get("value", 0)            # avg_tx_value_in
        features[5] = tx_data.get("value", 0)            # avg_tx_value_out
        features[6] = tx_data.get("value", 0)            # max_tx_value
        features[7] = 1                                   # unique_in_neighbors
        features[8] = 1                                   # unique_out_neighbors
        features[9] = 0                                   # account_lifetime
        features[10] = 0                                  # tx_frequency
        features[11] = 0.5                                # in_out_ratio

        # Normalize using training scaler
        if self.scaler_mean is not None and self.scaler_scale is not None:
            features = (features - self.scaler_mean) / self.scaler_scale

        return features

    def extract_subgraph(self, node_ids: list[int], num_hops: int = 2) -> dgl.DGLGraph:
        """Extract a k-hop subgraph around the given node IDs."""
        seed_nodes = torch.tensor(node_ids, dtype=torch.long)

        # Sample 2-hop neighborhood
        sampler = dgl.dataloading.NeighborSampler([15, 10])
        dataloader = dgl.dataloading.DataLoader(
            self.graph, seed_nodes, sampler,
            batch_size=len(node_ids), shuffle=False, drop_last=False,
        )

        # Get the first (only) batch
        for input_nodes, output_nodes, blocks in dataloader:
            return blocks

        return None

    @torch.no_grad()
    def score_addresses(self, addresses: list[str],
                        tx_data_list: list[dict] = None) -> list[dict]:
        """
        Score a list of addresses for phishing probability.

        Args:
            addresses: List of Ethereum addresses to score.
            tx_data_list: Optional transaction data for feature computation.

        Returns:
            List of dicts with address, phishing_score, predicted_label.
        """
        self.refresh_if_needed()

        results = []
        known_ids = []
        known_addresses = []

        for i, addr in enumerate(addresses):
            addr = addr.lower()
            if addr in self.node_to_id:
                known_ids.append(self.node_to_id[addr])
                known_addresses.append(addr)

        if not known_ids:
            # All addresses are new/unknown — return default scores
            for addr in addresses:
                tx = (tx_data_list[addresses.index(addr)]
                      if tx_data_list else {})
                features = self.compute_features_for_address(addr, tx)
                results.append({
                    "address": addr.lower(),
                    "phishing_score": 0.0,
                    "predicted_label": 0,
                    "known": False,
                })
            return results

        # Extract subgraph and run inference
        blocks = self.extract_subgraph(known_ids)
        if blocks is None:
            return results

        blocks = [b.to(self.device) for b in blocks]
        feat = blocks[0].srcdata["feat"].to(self.device)

        logits = self.model(blocks, feat)
        probs = F.softmax(logits, dim=1)[:, 1]  # P(phishing)

        for i, addr in enumerate(known_addresses):
            score = probs[i].item()
            results.append({
                "address": addr,
                "phishing_score": round(score, 6),
                "predicted_label": 1 if score >= PHISHING_THRESHOLD else 0,
                "known": True,
            })

        return results

    def get_model_info(self) -> dict:
        """Return model metadata."""
        checkpoint = torch.load(self.model_path, map_location="cpu", weights_only=False)
        return {
            "model_path": self.model_path,
            "hyperparameters": checkpoint.get("hyperparameters", {}),
            "metrics": checkpoint.get("metrics", {}),
            "graph_nodes": self.graph.num_nodes(),
            "graph_edges": self.graph.num_edges(),
            "device": str(self.device),
            "loaded_at": self._model_load_time,
        }
