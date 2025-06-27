#!/usr/bin/env python3
"""Extract all datasets from metadata.py and convert to our configuration format."""

import json
import re
from pathlib import Path


def extract_datasets_from_metadata():
    """Extract datasets dictionary from metadata.py by executing it."""
    # Import the metadata module to get the actual data
    import sys

    sys.path.append(".")

    import metadata

    return metadata.datasets, metadata.docs, metadata.auxiliary_tables


def convert_pattern_to_our_format(pattern_str):
    """Convert metadata.py regex patterns to our pattern names."""
    import metadata

    # Map actual regex patterns to our pattern names
    pattern_mapping = {
        metadata.uf_year2_month_pattern: "uf_year2_month",
        metadata.uf_year_pattern: "uf_year4",
        metadata.uf_year2_pattern: "uf_year2",
        metadata.year_pattern: "year4",
        metadata.uf_mapas_year_pattern: "uf_mapas_year",
        metadata.uf_cnv_pattern: "uf_cnv",
        metadata.uf_year2_month_pattern_sia_pa: "uf_year2_month_sia_pa",
    }

    return pattern_mapping.get(pattern_str, pattern_str)


def convert_datasets_to_our_format(datasets, docs, auxiliary_tables):
    """Convert metadata.py format to our DataSUSConfig format."""

    # Convert main datasets
    converted_datasets = {}
    for dataset_id, dataset_info in datasets.items():
        converted_dataset = {
            "name": dataset_info.get("name", dataset_id),
            "source": dataset_info.get("source", "unknown"),
        }

        # Convert periods to directories list
        periods = dataset_info.get("periods", [])
        if periods:
            directories = []
            for period in periods:
                dir_path = period.get("dir", "").replace("/dissemin/publicos", "")
                if dir_path:
                    directories.append(dir_path)
            converted_dataset["directories"] = directories

            # Use first period for other metadata
            first_period = periods[0]
            if "filename_prefix" in first_period:
                converted_dataset["filename_prefix"] = first_period["filename_prefix"]
            if "filename_pattern" in first_period:
                converted_dataset["filename_pattern"] = convert_pattern_to_our_format(
                    first_period["filename_pattern"]
                )

        converted_datasets[dataset_id] = converted_dataset

    # Convert documentation
    converted_docs = {}
    for doc_id, doc_info in docs.items():
        directories = doc_info.get("dir", [])
        if directories:
            converted_docs[doc_id] = {
                "directories": [
                    d.replace("/dissemin/publicos", "") for d in directories
                ]
            }

    # Convert auxiliary tables
    converted_aux = {}
    for aux_id, aux_info in auxiliary_tables.items():
        directories = aux_info.get("dir", [])
        if directories:
            converted_aux[aux_id] = {
                "directories": [
                    d.replace("/dissemin/publicos", "") for d in directories
                ]
            }

    return {
        "datasets": converted_datasets,
        "documentation": converted_docs,
        "auxiliary_tables": converted_aux,
    }


def main():
    """Main function."""
    print("Extracting datasets from metadata.py...")

    datasets, docs, auxiliary_tables = extract_datasets_from_metadata()

    print(f"Found {len(datasets)} datasets")
    print(f"Found {len(docs)} documentation entries")
    print(f"Found {len(auxiliary_tables)} auxiliary table entries")

    # Convert to our format
    converted = convert_datasets_to_our_format(datasets, docs, auxiliary_tables)

    # Save to file
    output_file = Path("extracted_metadata.json")
    with open(output_file, "w") as f:
        json.dump(converted, f, indent=2)

    print(f"Converted metadata saved to {output_file}")

    # Print sample datasets
    print("\nSample datasets:")
    for i, (dataset_id, dataset_info) in enumerate(converted["datasets"].items()):
        if i >= 10:  # Show first 10
            break
        print(f"  {dataset_id}: {dataset_info['name']}")


if __name__ == "__main__":
    main()
