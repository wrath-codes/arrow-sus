#!/usr/bin/env python3
"""Test the configuration loading."""

from src.arrow_sus.metadata.config import DataSUSConfig


def test_config():
    config = DataSUSConfig()
    datasets = config.datasets
    print(f"Found {len(datasets)} datasets:")
    for dataset_id in list(datasets.keys())[:10]:  # Show first 10
        print(f"  {dataset_id}: {datasets[dataset_id]['name']}")


if __name__ == "__main__":
    test_config()
