# BDA501 Final Project: Ethereum Phishing Detection using GraphSAGE

A Graph Neural Network (GNN) based system for detecting phishing addresses on the Ethereum blockchain using the XBlock-ETH dataset.

## Project Overview

This project implements a phishing detection pipeline using GraphSAGE, a sampling and aggregation framework for GNNs. It processes Ethereum transaction networks to identify fraudulent addresses.

**Key Components:**
- Phase 1: Data Exploration & EDA
- Phase 2: Feature Engineering & Graph Construction
- Phase 3: GNN Model Development & Training (future)

## Directory Structure

```
bda501-final/
├── PROJECT_OVERVIEW.md                # This file
├── notebooks/
│   ├── 01_data_exploration.ipynb      # Phase 1: Dataset exploration and EDA
│   └── 02_feature_engineering.ipynb   # Phase 2: Feature engineering & graph build
├── scripts/
│   └── download_data.py               # Download and prepare dataset
├── data/
│   ├── raw/                           # Raw dataset from Kaggle
│   ├── processed/                     # Processed features and graph data
│   │   ├── node_features.npy          # Normalized node feature matrix (N x 12)
│   │   ├── edge_index.npy             # Edge connectivity indices (2 x E)
│   │   ├── labels.npy                 # Node labels (N,)
│   │   ├── node_to_id.pkl             # Address-to-index mapping
│   │   ├── feature_scaler.pkl         # Fitted StandardScaler
│   │   └── processing_report.json     # Processing summary
│   └── sample/                        # Sample data for development
│       ├── transactions_sample.csv    # 100 sample transactions
│       ├── addresses_sample.csv       # 50 sample addresses
│       └── phishing_labels.csv        # 50 address labels
├── src/                               # Source code (future)
└── reports/                           # Generated reports and visualizations
```

## Dataset

**Source:** XBlock-ETH from Kaggle  
**Dataset ID:** `xblock/ethereum-phishing-transaction-network`  
**URL:** https://www.kaggle.com/datasets/xblock/ethereum-phishing-transaction-network

**Data Components:**
- **Transactions:** Individual Ethereum transactions with from/to addresses, values, gas, timestamps
- **Addresses:** Unique addresses with aggregate statistics
- **Labels:** Known phishing and legitimate address labels

## Phase 1: Data Exploration & EDA

**File:** `notebooks/01_data_exploration.ipynb`

### Objectives:
1. Load XBlock-ETH dataset from Kaggle
2. Explore dataset structure and statistics
3. Analyze class distribution (phishing vs legitimate)
4. Visualize transaction patterns
5. Generate data quality report

### Key Analyses:
- Class distribution and imbalance metrics
- Transaction value statistics and distributions
- Network degree distributions
- Feature correlations
- Data quality assessment

### Outputs:
- Data quality report (JSON)
- Visualizations (histograms, distributions, heatmaps)

## Phase 2: Feature Engineering & Graph Construction

**File:** `notebooks/02_feature_engineering.ipynb`

### Objectives:
1. Load and clean raw data
2. Build directed transaction graph using NetworkX
3. Compute 12 node features per address
4. Normalize features with StandardScaler
5. Prepare data for GNN training

### Data Cleaning:
- Remove duplicate transactions/addresses
- Validate Ethereum address format
- Handle missing values
- Filter invalid entries

### 12 Node Features:
1. `in_degree` - Incoming transaction count
2. `out_degree` - Outgoing transaction count
3. `total_eth_received` - Total ETH received
4. `total_eth_sent` - Total ETH sent
5. `avg_tx_value_in` - Average incoming transaction value
6. `avg_tx_value_out` - Average outgoing transaction value
7. `max_tx_value` - Maximum transaction value
8. `unique_in_neighbors` - Unique source addresses
9. `unique_out_neighbors` - Unique destination addresses
10. `account_lifetime` - Days from first to last transaction
11. `failed_tx_ratio` - Ratio of failed transactions
12. `avg_gas_used` - Average gas consumption

### Graph Construction:
- Directed graph with addresses as nodes
- Transactions as weighted edges
- Edge weights = transaction values

### Normalization:
- StandardScaler applied to all features
- Mean = 0, Std = 1 per feature
- Scaler saved for test data

### Outputs:
- `node_features.npy` - Feature matrix (N × 12)
- `edge_index.npy` - Edge indices (2 × E)
- `labels.npy` - Node labels
- `node_to_id.pkl` - Address mapping
- `feature_scaler.pkl` - Fitted scaler
- `processing_report.json` - Summary statistics

## Setup & Usage

### Prerequisites
```bash
pip install pandas numpy matplotlib seaborn scikit-learn networkx
```

### Quick Start

1. **Prepare data:**
   ```bash
   python scripts/download_data.py
   ```

2. **Run Phase 1 (EDA):**
   ```bash
   jupyter notebook notebooks/01_data_exploration.ipynb
   ```

3. **Run Phase 2 (Feature Engineering):**
   ```bash
   jupyter notebook notebooks/02_feature_engineering.ipynb
   ```

## Data Files Format

### node_features.npy
- Shape: (num_nodes, 12)
- Type: float32
- Normalized features

### edge_index.npy
- Shape: (2, num_edges)
- Type: int64
- [source_indices, target_indices]

### labels.npy
- Shape: (num_nodes,)
- Type: int64
- Values: 0 (legitimate), 1 (phishing), -1 (unlabeled)

### node_to_id.pkl
- Python dictionary mapping addresses to indices
- Pickle format

### feature_scaler.pkl
- Fitted StandardScaler for normalization
- Pickle format

## Key Insights

- Dataset exhibits class imbalance
- Transaction values follow power-law distribution
- Network has sparse connectivity typical of blockchain
- Degree distribution suggests scale-free properties
- Multiple features correlate with transaction patterns

## Next Steps (Phase 3)

- Implement GraphSAGE model using PyTorch/DGL
- Train on processed features and graph
- Evaluate on test set
- Analyze learned node embeddings
- Fine-tune hyperparameters

## References

- XBlock-ETH Dataset: Ethereum Phishing Detection
- GraphSAGE: Inductive Representation Learning on Large Graphs
- NetworkX: Network analysis library
- PyTorch Geometric / DGL: GNN frameworks
