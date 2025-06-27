#!/usr/bin/env python3
"""
Integration Patterns for DATASUS Metadata with Modern Data Stack

This example demonstrates how to integrate the high-performance DATASUS
metadata system with popular data engineering and analysis tools.
"""

from asyncio import run
from json import loads, dumps
from subprocess import run as subprocess_run, CalledProcessError, PIPE
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Any

# Import our high-performance metadata client
from arrow_sus.metadata import DataSUSMetadataClient, UFCode, DatasetSource


async def polars_integration():
    """Demonstrate integration with Polars for fast data analysis."""
    print("🐻‍❄️ Polars Integration Example")
    print("=" * 35)

    try:
        from polars import DataFrame as PlDataFrame
    except ImportError:
        print("❌ Polars not installed. Install with: uv add polars")
        return

    async with DataSUSMetadataClient() as client:
        # Get file metadata
        files = await client.search_files(uf=UFCode.SP, year=2023, dataset="sih-rd")

        if not files:
            print("No files found for analysis")
            return

        # Convert to Polars DataFrame
        file_data = [
            {
                "filename": f.filename,
                "size_mb": f.size_mb,
                "dataset": f.dataset,
                "uf": f.uf.value if hasattr(f, "uf") else "unknown",
                "datetime": f.datetime.isoformat() if hasattr(f, "datetime") else None,
                "full_path": f.full_path,
            }
            for f in files
        ]

        df = PlDataFrame(file_data)

        print(f"📊 Analyzing {len(df)} files with Polars:")
        print(f"  Total size: {df['size_mb'].sum():.2f} MB")
        print(f"  Average file size: {df['size_mb'].mean():.2f} MB")
        print(f"  Size range: {df['size_mb'].min():.2f} - {df['size_mb'].max():.2f} MB")

        # Group by dataset and analyze
        from polars import count, col

        summary = (
            df.group_by("dataset")
            .agg(
                [
                    count().alias("file_count"),
                    col("size_mb").sum().alias("total_size_mb"),
                    col("size_mb").mean().alias("avg_size_mb"),
                ]
            )
            .sort("total_size_mb", descending=True)
        )

        print("\n📈 Dataset Summary:")
        print(summary)


async def pandas_integration():
    """Demonstrate integration with Pandas for traditional data analysis."""
    print("\n🐼 Pandas Integration Example")
    print("=" * 35)

    try:
        from pandas import DataFrame as PdDataFrame
    except ImportError:
        print("❌ Pandas not installed. Install with: uv add pandas")
        return

    async with DataSUSMetadataClient() as client:
        # Get data from multiple states
        all_files = []
        states = [UFCode.SP, UFCode.RJ, UFCode.MG]

        for uf in states:
            files = await client.search_files(uf=uf, year=2023)
            for f in files[:10]:  # Limit for demo
                all_files.append(
                    {
                        "state": uf.value.upper(),
                        "filename": f.filename,
                        "size_mb": f.size_mb,
                        "dataset": getattr(f, "dataset", "unknown"),
                    }
                )

        if not all_files:
            print("No files found for analysis")
            return

        df = PdDataFrame(all_files)

        print(f"📊 Analyzing {len(df)} files with Pandas:")

        # State-wise analysis
        state_summary = (
            df.groupby("state")
            .agg({"size_mb": ["count", "sum", "mean"], "dataset": "nunique"})
            .round(2)
        )

        print("\n📍 State-wise Summary:")
        print(state_summary)

        # Dataset type analysis
        dataset_summary = (
            df.groupby("dataset")["size_mb"]
            .agg(["count", "sum"])
            .sort_values("sum", ascending=False)
        )
        print("\n📁 Dataset Type Summary:")
        print(dataset_summary.head())


async def dagster_integration():
    """Demonstrate integration with Dagster for data orchestration."""
    print("\n⚙️ Dagster Integration Pattern")
    print("=" * 35)

    # Since Dagster might not be installed, show the pattern
    dagster_code = '''
from dagster import asset, op, job, get_dagster_logger
from arrow_sus.metadata import DataSUSMetadataClient, UFCode
from asyncio import run

@op
def fetch_datasus_metadata(context, uf: str, year: int, dataset: str):
    """Fetch DATASUS metadata for downstream processing."""
    logger = get_dagster_logger()
    
    async def get_files():
        async with DataSUSMetadataClient() as client:
            files = await client.search_files(
                uf=UFCode[uf.upper()],
                year=year,
                dataset=dataset
            )
            return [
                {
                    "filename": f.filename,
                    "size_mb": f.size_mb,
                    "full_path": f.full_path
                }
                for f in files
            ]
    
    files = run(get_files())
    logger.info(f"Found {len(files)} files for {uf}/{year}/{dataset}")
    return files

@asset
def sih_sp_2023():
    """Hospital admissions data for São Paulo, 2023."""
    return fetch_datasus_metadata(None, "sp", 2023, "sih-rd")

@asset  
def processed_hospital_data(sih_sp_2023):
    """Process hospital admissions data."""
    # Your processing logic here
    return {"processed_files": len(sih_sp_2023)}

@job
def datasus_pipeline():
    processed_hospital_data()
'''

    print("Dagster pipeline pattern:")
    print(dagster_code)


async def airflow_integration():
    """Demonstrate integration with Apache Airflow."""
    print("\n🌪️ Apache Airflow Integration Pattern")
    print("=" * 40)

    airflow_code = '''
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from asyncio import run

def fetch_datasus_metadata(**context):
    """Airflow task to fetch DATASUS metadata."""
    from arrow_sus.metadata import DataSUSMetadataClient, UFCode
    
    async def get_metadata():
        async with DataSUSMetadataClient() as client:
            files = await client.search_files(
                uf=UFCode.SP,
                year=2023,
                dataset="sih-rd"
            )
            return len(files)
    
    file_count = run(get_metadata())
    context['task_instance'].xcom_push(key='file_count', value=file_count)
    return file_count

default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'datasus_metadata_dag',
    default_args=default_args,
    description='DATASUS metadata processing',
    schedule_interval='@daily',
    catchup=False
)

fetch_task = PythonOperator(
    task_id='fetch_metadata',
    python_callable=fetch_datasus_metadata,
    dag=dag
)
'''

    print("Apache Airflow DAG pattern:")
    print(airflow_code)


async def duckdb_integration():
    """Demonstrate integration with DuckDB for analytics."""
    print("\n🦆 DuckDB Integration Example")
    print("=" * 35)

    try:
        from duckdb import connect
    except ImportError:
        print("❌ DuckDB not installed. Install with: uv add duckdb")
        return

    async with DataSUSMetadataClient() as client:
        # Get file metadata
        files = await client.search_files(year=2023)

        if not files:
            print("No files found for analysis")
            return

        # Create DuckDB connection
        conn = connect()

        # Prepare data
        file_data = [
            (
                f.filename,
                f.size_mb,
                getattr(f, "dataset", "unknown"),
                getattr(f, "uf", UFCode.BR).value if hasattr(f, "uf") else "br",
                f.datetime.strftime("%Y-%m-%d")
                if hasattr(f, "datetime")
                else "2023-01-01",
            )
            for f in files[:100]  # Limit for demo
        ]

        # Create table and insert data
        conn.execute("""
            CREATE TABLE datasus_files (
                filename VARCHAR,
                size_mb DOUBLE,
                dataset VARCHAR,
                uf VARCHAR,
                date DATE
            )
        """)

        conn.executemany("INSERT INTO datasus_files VALUES (?, ?, ?, ?, ?)", file_data)

        # Run analytics queries
        print("📊 DuckDB Analytics:")

        # Query 1: Top datasets by size
        result = conn.execute("""
            SELECT dataset, COUNT(*) as file_count, SUM(size_mb) as total_size_mb
            FROM datasus_files 
            GROUP BY dataset 
            ORDER BY total_size_mb DESC 
            LIMIT 5
        """).fetchall()

        print("\nTop datasets by size:")
        for row in result:
            print(f"  {row[0]}: {row[1]} files, {row[2]:.2f} MB")

        # Query 2: Files by state
        result = conn.execute("""
            SELECT uf, COUNT(*) as file_count, AVG(size_mb) as avg_size_mb
            FROM datasus_files 
            GROUP BY uf 
            ORDER BY file_count DESC 
            LIMIT 5
        """).fetchall()

        print("\nFiles by state:")
        for row in result:
            print(f"  {row[0].upper()}: {row[1]} files, {row[2]:.2f} MB avg")


async def cli_wrapper_functions():
    """Demonstrate wrapper functions for CLI integration."""
    print("\n🖥️ CLI Wrapper Functions")
    print("=" * 30)

    def run_metadata_cli(command: str, **kwargs) -> Dict[str, Any]:
        """Run metadata CLI command and return JSON result."""
        cmd = ["uv", "run", "-m", "arrow_sus.metadata.cli", command]

        # Add arguments
        for key, value in kwargs.items():
            if isinstance(value, bool) and value:
                cmd.append(f"--{key.replace('_', '-')}")
            elif value is not None:
                cmd.extend([f"--{key.replace('_', '-')}", str(value)])

        cmd.append("--json")

        try:
            result = subprocess_run(
                cmd, stdout=PIPE, stderr=PIPE, text=True, check=True
            )
            return loads(result.stdout)
        except CalledProcessError as e:
            print(f"CLI command failed: {e}")
            return {}
        except ValueError:
            print("Failed to parse CLI output as JSON")
            return {}

    # Example usage
    print("📋 Getting dataset list via CLI wrapper:")
    datasets = run_metadata_cli("list-datasets", source="sih")
    print(f"Found {len(datasets)} SIH datasets")

    print("\n🔍 Searching files via CLI wrapper:")
    files = run_metadata_cli("search-files", uf="SP", year=2023, limit=5)
    print(f"Found {len(files)} files for SP in 2023")
    for file in files[:3]:
        print(
            f"  • {file.get('filename', 'unknown')} ({file.get('size_mb', 0):.1f} MB)"
        )


async def export_utilities():
    """Demonstrate export utilities for different formats."""
    print("\n📤 Export Utilities")
    print("=" * 20)

    async with DataSUSMetadataClient() as client:
        files = await client.search_files(uf=UFCode.RJ, year=2023)

        if not files:
            print("No files found for export")
            return

        # Export as CSV
        print("📄 Exporting as CSV:")
        csv_lines = ["filename,size_mb,dataset,full_path"]
        for f in files[:5]:
            csv_lines.append(
                f"{f.filename},{f.size_mb},{getattr(f, 'dataset', 'unknown')},{f.full_path}"
            )

        csv_content = "\n".join(csv_lines)
        print("Sample CSV content:")
        print(csv_content[:300] + "..." if len(csv_content) > 300 else csv_content)

        # Export as download script
        print("\n📥 Exporting as download script:")
        script_lines = [
            "#!/bin/bash",
            "# DATASUS download script",
            "set -e",
            "mkdir -p datasus_downloads",
            "cd datasus_downloads",
            "",
        ]

        for f in files[:3]:
            url = f"ftp://ftp.datasus.gov.br{f.full_path}"
            script_lines.append(f"echo 'Downloading {f.filename}...'")
            script_lines.append(f"wget -nc '{url}' || echo 'Failed: {f.filename}'")

        script_content = "\n".join(script_lines)
        print("Download script preview:")
        print(script_content)


if __name__ == "__main__":
    print("🔗 DATASUS Metadata Integration Patterns")
    print("=" * 50)

    # Run all integration examples
    run(polars_integration())
    run(pandas_integration())
    run(dagster_integration())
    run(airflow_integration())
    run(duckdb_integration())
    run(cli_wrapper_functions())
    run(export_utilities())

    print("\n🎯 Integration Summary:")
    print("• Polars: Fast, memory-efficient DataFrames with lazy evaluation")
    print("• Pandas: Traditional analysis with rich ecosystem integration")
    print("• Dagster: Modern data orchestration with type safety and lineage")
    print("• Apache Airflow: Battle-tested workflow orchestration")
    print("• DuckDB: High-performance analytics on metadata")
    print("• CLI wrappers: Subprocess integration for any language")
    print("• Export utilities: Multiple output formats for downstream tools")

    print("\n✨ The high-performance metadata system integrates seamlessly")
    print("   with the modern data stack for production workflows!")
