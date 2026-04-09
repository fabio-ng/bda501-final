"""
Feature schema definitions for address-level features.
"""

from typing import List, Optional

from pydantic import BaseModel, Field


# Feature names (12 dimensions)
NODE_FEATURE_NAMES: List[str] = [
    "in_degree",
    "out_degree",
    "total_eth_received",
    "total_eth_sent",
    "avg_tx_value_in",
    "avg_tx_value_out",
    "max_tx_value",
    "unique_in_neighbors",
    "unique_out_neighbors",
    "account_lifetime",
    "failed_tx_ratio",
    "avg_gas_used",
]

# Number of features
NUM_FEATURES = len(NODE_FEATURE_NAMES)


class AddressFeatures(BaseModel):
    """Pydantic model for address-level feature vector."""

    address: str = Field(..., description="Ethereum address")

    # Graph structure features
    in_degree: int = Field(..., description="Number of incoming transactions")
    out_degree: int = Field(..., description="Number of outgoing transactions")

    # Value transfer features
    total_eth_received: float = Field(..., description="Total ETH received (wei -> ETH)")
    total_eth_sent: float = Field(..., description="Total ETH sent (wei -> ETH)")
    avg_tx_value_in: float = Field(..., description="Average transaction value in")
    avg_tx_value_out: float = Field(..., description="Average transaction value out")
    max_tx_value: float = Field(..., description="Maximum transaction value")

    # Neighbor features
    unique_in_neighbors: int = Field(..., description="Count of unique senders")
    unique_out_neighbors: int = Field(..., description="Count of unique receivers")

    # Temporal features
    account_lifetime: float = Field(..., description="Account lifetime in seconds")

    # Behavior features
    failed_tx_ratio: float = Field(..., description="Ratio of failed transactions")
    avg_gas_used: float = Field(..., description="Average gas used per transaction")

    # Label (if available)
    label: Optional[int] = Field(None, description="1=phishing, 0=legitimate, None=unknown")

    class Config:
        """Pydantic config."""

        str_strip_whitespace = True


# Pandas dtype mapping for feature DataFrame
FEATURE_SCHEMA_PANDAS = {
    "address": "object",
    "in_degree": "int64",
    "out_degree": "int64",
    "total_eth_received": "float64",
    "total_eth_sent": "float64",
    "avg_tx_value_in": "float64",
    "avg_tx_value_out": "float64",
    "max_tx_value": "float64",
    "unique_in_neighbors": "int64",
    "unique_out_neighbors": "int64",
    "account_lifetime": "float64",
    "failed_tx_ratio": "float64",
    "avg_gas_used": "float64",
    "label": "Int64",  # Nullable integer
}
