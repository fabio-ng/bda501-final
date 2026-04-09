"""
Data Download Script for Ethereum Phishing Detection Dataset
Downloads XBlock-ETH dataset from Kaggle and prepares it locally.
"""

import os
import pandas as pd
import numpy as np
from pathlib import Path

# Try to use kagglehub (newer approach) or fall back to kaggle API
try:
    import kagglehub
    USE_KAGGLEHUB = True
except ImportError:
    USE_KAGGLEHUB = False
    try:
        from kaggle.api.kaggle_api_extended import KaggleApi
    except ImportError:
        print("Warning: Neither kagglehub nor kaggle API is installed.")
        print("Install with: pip install kagglehub or kaggle")


def setup_directories():
    """Create necessary directories for data storage."""
    directories = [
        'data/raw',
        'data/processed',
        'data/sample'
    ]
    for directory in directories:
        Path(directory).mkdir(parents=True, exist_ok=True)
        print(f"Created/verified directory: {directory}")


def download_dataset():
    """
    Download XBlock-ETH dataset from Kaggle.
    Dataset: xblock/ethereum-phishing-transaction-network
    """
    print("\n" + "="*60)
    print("DOWNLOADING ETHEREUM PHISHING DATASET")
    print("="*60)

    dataset_name = "xblock/ethereum-phishing-transaction-network"
    output_path = "data/raw"

    try:
        if USE_KAGGLEHUB:
            print(f"\nUsing kagglehub to download: {dataset_name}")
            # Download using kagglehub
            path = kagglehub.dataset_download(dataset_name)
            print(f"Dataset downloaded to: {path}")
            return path
        else:
            # Fall back to kaggle API
            print(f"\nUsing Kaggle API to download: {dataset_name}")
            api = KaggleApi()
            api.authenticate()
            api.dataset_download_files(dataset_name, path=output_path, unzip=True)
            print(f"Dataset downloaded and extracted to: {output_path}")
            return output_path

    except Exception as e:
        print(f"Error downloading dataset: {e}")
        print("\nManual setup required:")
        print(f"1. Visit: https://www.kaggle.com/datasets/{dataset_name}")
        print(f"2. Download the dataset files")
        print(f"3. Extract to: {output_path}")
        return None


def explore_downloaded_data(data_path):
    """
    Explore the structure of downloaded data and report statistics.

    Expected files:
    - transactions.csv or similar
    - addresses.csv or similar
    - labels.csv or phishing_labels.csv
    """
    print("\n" + "="*60)
    print("EXPLORING DATASET STRUCTURE")
    print("="*60)

    if not os.path.exists(data_path):
        print(f"Path does not exist: {data_path}")
        return

    csv_files = []
    for root, dirs, files in os.walk(data_path):
        for file in files:
            if file.endswith('.csv'):
                csv_files.append(os.path.join(root, file))

    print(f"\nFound {len(csv_files)} CSV files:")
    for file_path in csv_files:
        print(f"  - {file_path}")

        try:
            df = pd.read_csv(file_path, nrows=5)
            print(f"    Shape: {pd.read_csv(file_path).shape}")
            print(f"    Columns: {list(df.columns)}")
            print()
        except Exception as e:
            print(f"    Error reading file: {e}\n")


def report_basic_stats(data_path):
    """
    Generate basic statistics about the dataset.
    """
    print("\n" + "="*60)
    print("DATASET STATISTICS")
    print("="*60)

    stats = {
        'total_csv_files': 0,
        'total_rows': 0,
        'total_columns': 0,
        'files_info': []
    }

    if not os.path.exists(data_path):
        print(f"Data path not found: {data_path}")
        return stats

    for root, dirs, files in os.walk(data_path):
        for file in files:
            if file.endswith('.csv'):
                file_path = os.path.join(root, file)
                try:
                    df = pd.read_csv(file_path)
                    stats['total_csv_files'] += 1
                    stats['total_rows'] += len(df)
                    stats['total_columns'] += len(df.columns)

                    file_info = {
                        'filename': file,
                        'rows': len(df),
                        'columns': len(df.columns),
                        'column_names': list(df.columns),
                        'dtypes': df.dtypes.to_dict()
                    }
                    stats['files_info'].append(file_info)

                    print(f"\nFile: {file}")
                    print(f"  Rows: {len(df)}")
                    print(f"  Columns: {len(df.columns)}")
                    print(f"  Column names: {list(df.columns)}")

                except Exception as e:
                    print(f"Error processing {file}: {e}")

    return stats


def create_sample_datasets():
    """
    Create small sample datasets for development/testing.
    These are minimal examples for notebook prototyping.
    """
    print("\n" + "="*60)
    print("CREATING SAMPLE DATASETS")
    print("="*60)

    # Sample transactions
    np.random.seed(42)
    n_transactions = 100

    sample_transactions = pd.DataFrame({
        'tx_hash': [f'0x{i:064x}' for i in range(n_transactions)],
        'from_address': [f'0x{np.random.randint(0, 1000000):040x}' for _ in range(n_transactions)],
        'to_address': [f'0x{np.random.randint(0, 1000000):040x}' for _ in range(n_transactions)],
        'value': np.random.exponential(1.5, n_transactions),
        'gas': np.random.randint(21000, 100000, n_transactions),
        'gas_price': np.random.exponential(50, n_transactions),
        'timestamp': pd.date_range('2020-01-01', periods=n_transactions, freq='H'),
        'is_contract': np.random.choice([0, 1], n_transactions, p=[0.7, 0.3])
    })

    sample_transactions.to_csv('data/sample/transactions_sample.csv', index=False)
    print(f"Created: data/sample/transactions_sample.csv ({n_transactions} rows)")

    # Sample addresses with labels
    n_addresses = 50
    sample_addresses = pd.DataFrame({
        'address': [f'0x{np.random.randint(0, 1000000):040x}' for _ in range(n_addresses)],
        'in_degree': np.random.zipf(1.5, n_addresses),
        'out_degree': np.random.zipf(1.5, n_addresses),
        'total_eth_received': np.random.exponential(2, n_addresses),
        'total_eth_sent': np.random.exponential(2, n_addresses),
        'is_contract': np.random.choice([0, 1], n_addresses, p=[0.7, 0.3]),
        'first_tx_timestamp': pd.date_range('2015-01-01', periods=n_addresses, freq='D')
    })

    sample_addresses.to_csv('data/sample/addresses_sample.csv', index=False)
    print(f"Created: data/sample/addresses_sample.csv ({n_addresses} rows)")

    # Known phishing labels
    phishing_labels = pd.DataFrame({
        'address': sample_addresses['address'].iloc[:10],  # 10 addresses labeled as phishing
        'label': 1
    })
    legitimate_labels = pd.DataFrame({
        'address': sample_addresses['address'].iloc[10:],  # Rest as legitimate
        'label': 0
    })

    labels = pd.concat([phishing_labels, legitimate_labels], ignore_index=True)
    labels.to_csv('data/sample/phishing_labels.csv', index=False)
    print(f"Created: data/sample/phishing_labels.csv ({len(labels)} rows)")
    print(f"  - Phishing addresses: {(labels['label'] == 1).sum()}")
    print(f"  - Legitimate addresses: {(labels['label'] == 0).sum()}")


def main():
    """Main execution function."""
    print("\n" + "="*60)
    print("ETHEREUM PHISHING DATASET PREPARATION")
    print("="*60)

    # Setup directories
    setup_directories()

    # Create sample datasets first (always useful for development)
    create_sample_datasets()

    # Try to download real dataset
    data_path = download_dataset()

    if data_path:
        explore_downloaded_data(data_path)
        stats = report_basic_stats(data_path)
        print(f"\nTotal CSV files: {stats['total_csv_files']}")
        print(f"Total rows across all files: {stats['total_rows']}")
        print(f"Total columns across all files: {stats['total_columns']}")
    else:
        print("\nDataset download skipped. Using sample data instead.")
        print("To use real data, manually download from Kaggle and place in data/raw/")

    print("\n" + "="*60)
    print("SETUP COMPLETE")
    print("="*60)
    print("\nNext steps:")
    print("1. Run: jupyter notebook notebooks/01_data_exploration.ipynb")
    print("2. This notebook will load data from data/raw/ or use data/sample/ samples")


if __name__ == "__main__":
    main()
