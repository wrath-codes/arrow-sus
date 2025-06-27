"""Command-line interface for DATASUS metadata operations."""

from asyncio import run
from json import loads, dumps
from sys import stderr
import orjson
from datetime import datetime
from pathlib import Path
from typing import List, Optional

import typer
from rich.console import Console
from rich.table import Table
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich.json import JSON

from .core.client import DataSUSMetadataClient
from .core.config import DataSUSConfig, CacheConfig, PerformanceConfig
from .core.models import UFCode, DatasetSource

app = typer.Typer(
    name="datasus-metadata",
    help="High-performance DATASUS metadata management CLI",
)
console = Console()


def create_client(
    cache_dir: Optional[Path] = None,
    max_connections: int = 10,
    cache_size_mb: int = 500,
) -> DataSUSMetadataClient:
    """Create a configured metadata client."""
    config = DataSUSConfig()
    cache_config = CacheConfig(memory_max_mb=cache_size_mb)
    performance_config = PerformanceConfig(max_ftp_connections=max_connections)

    return DataSUSMetadataClient(
        config=config,
        cache_config=cache_config,
        performance_config=performance_config,
        cache_dir=cache_dir,
    )


@app.command()
def list_datasets(
    source: Optional[str] = typer.Option(None, help="Filter by data source"),
    category: str = typer.Option(
        "data", help="Category: data, documentation, auxiliary"
    ),
    cache_dir: Optional[Path] = typer.Option(None, help="Cache directory"),
):
    """List all available datasets."""

    async def _list_datasets():
        async with create_client(cache_dir=cache_dir) as client:
            try:
                source_enum = DatasetSource(source) if source else None
            except ValueError:
                console.print(f"[red]Invalid source: {source}[/red]")
                console.print(
                    f"Valid sources: {', '.join([s.value for s in DatasetSource])}"
                )
                return

            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                console=console,
            ) as progress:
                task = progress.add_task("Fetching dataset list...", total=None)

                datasets = await client.list_available_datasets(
                    source=source_enum,
                    category=category,
                )

                progress.update(task, completed=True)

            if not datasets:
                console.print("[yellow]No datasets found[/yellow]")
                return

            table = Table(title=f"Available Datasets ({category})")
            table.add_column("Dataset", style="cyan")
            table.add_column("Source", style="magenta")

            for dataset in datasets:
                # Extract source from dataset name
                dataset_source = dataset.split("-")[0] if "-" in dataset else dataset
                table.add_row(dataset, dataset_source)

            console.print(table)

    run(_list_datasets())


@app.command()
def dataset_info(
    dataset: str = typer.Argument(..., help="Dataset name"),
    category: str = typer.Option(
        "data", help="Category: data, documentation, auxiliary"
    ),
    cache_dir: Optional[Path] = typer.Option(None, help="Cache directory"),
    json_output: bool = typer.Option(False, "--json", help="Output as JSON"),
):
    """Get detailed information about a dataset."""

    async def _dataset_info():
        async with create_client(cache_dir=cache_dir) as client:
            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                console=console,
            ) as progress:
                task = progress.add_task(f"Fetching info for {dataset}...", total=None)

                stats = await client.get_dataset_stats(
                    dataset_name=dataset,
                    category=category,
                )

                progress.update(task, completed=True)

            if not stats:
                console.print(f"[red]Dataset '{dataset}' not found[/red]")
                return

            if json_output:
                console.print(
                    JSON(orjson.dumps(stats, option=orjson.OPT_INDENT_2).decode())
                )
            else:
                # Create rich table display
                table = Table(title=f"Dataset Information: {dataset}")
                table.add_column("Property", style="cyan")
                table.add_column("Value", style="white")

                table.add_row("Name", stats["name"])
                table.add_row("Source", stats["source"])
                table.add_row("Total Files", f"{stats['total_files']:,}")
                table.add_row("Total Size", f"{stats['total_size_gb']:.2f} GB")
                table.add_row("First Period", stats["first_period"] or "N/A")
                table.add_row("Last Period", stats["last_period"] or "N/A")
                table.add_row("Supported UFs", ", ".join(stats["supported_ufs"]))
                table.add_row("File Extensions", ", ".join(stats["file_extensions"]))
                table.add_row("Last Updated", stats["last_updated"])

                console.print(table)

                # Files by UF
                if stats["files_by_uf"]:
                    uf_table = Table(title="Files by UF")
                    uf_table.add_column("UF", style="cyan")
                    uf_table.add_column("Files", style="white", justify="right")

                    for uf, count in sorted(stats["files_by_uf"].items()):
                        uf_table.add_row(uf, f"{count:,}")

                    console.print(uf_table)

    run(_dataset_info())


@app.command()
def search_files(
    dataset: Optional[str] = typer.Option(None, help="Dataset name"),
    uf: Optional[str] = typer.Option(None, help="UF code"),
    year: Optional[int] = typer.Option(None, help="Year"),
    month: Optional[int] = typer.Option(None, help="Month"),
    pattern: Optional[str] = typer.Option(None, help="Filename pattern"),
    min_size: Optional[float] = typer.Option(None, help="Minimum file size in MB"),
    max_size: Optional[float] = typer.Option(None, help="Maximum file size in MB"),
    limit: int = typer.Option(50, help="Maximum number of results"),
    cache_dir: Optional[Path] = typer.Option(None, help="Cache directory"),
    json_output: bool = typer.Option(False, "--json", help="Output as JSON"),
):
    """Search for files matching criteria."""

    async def _search_files():
        async with create_client(cache_dir=cache_dir) as client:
            # Validate UF
            uf_enum = None
            if uf:
                try:
                    uf_enum = UFCode(uf.lower())
                except ValueError:
                    console.print(f"[red]Invalid UF: {uf}[/red]")
                    console.print(
                        f"Valid UFs: {', '.join([u.value.upper() for u in UFCode])}"
                    )
                    return

            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                console=console,
            ) as progress:
                task = progress.add_task("Searching files...", total=None)

                files = await client.search_files(
                    dataset=dataset,
                    uf=uf_enum,
                    year=year,
                    month=month,
                    file_pattern=pattern,
                    min_size_mb=min_size,
                    max_size_mb=max_size,
                )

                progress.update(task, completed=True)

            # Limit results
            files = files[:limit]

            if not files:
                console.print("[yellow]No files found matching criteria[/yellow]")
                return

            if json_output:
                file_data = [
                    {
                        "filename": f.filename,
                        "dataset": f.dataset,
                        "size_mb": f.size_mb,
                        "uf": f.partition.uf.value.upper()
                        if f.partition and f.partition.uf
                        else None,
                        "year": f.partition.year if f.partition else None,
                        "month": f.partition.month if f.partition else None,
                        "full_path": f.full_path,
                        "datetime": f.datetime.isoformat(),
                    }
                    for f in files
                ]
                console.print(
                    JSON(orjson.dumps(file_data, option=orjson.OPT_INDENT_2).decode())
                )
            else:
                table = Table(title=f"Search Results ({len(files)} files)")
                table.add_column("Dataset", style="cyan")
                table.add_column("Filename", style="white")
                table.add_column("UF", style="magenta")
                table.add_column("Year", style="yellow")
                table.add_column("Month", style="green")
                table.add_column("Size (MB)", style="blue", justify="right")

                for file in files:
                    table.add_row(
                        file.dataset,
                        file.filename,
                        file.partition.uf.value.upper()
                        if file.partition and file.partition.uf
                        else "N/A",
                        str(file.partition.year)
                        if file.partition and file.partition.year
                        else "N/A",
                        str(file.partition.month)
                        if file.partition and file.partition.month
                        else "N/A",
                        f"{file.size_mb:.2f}" if file.size_mb else "N/A",
                    )

                console.print(table)

    run(_search_files())


@app.command()
def refresh_metadata(
    datasets: Optional[List[str]] = typer.Option(
        None, help="Specific datasets to refresh"
    ),
    force: bool = typer.Option(False, help="Force full refresh"),
    cache_dir: Optional[Path] = typer.Option(None, help="Cache directory"),
):
    """Refresh metadata for datasets."""

    async def _refresh_metadata():
        async with create_client(cache_dir=cache_dir) as client:
            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                console=console,
            ) as progress:
                task = progress.add_task("Refreshing metadata...", total=None)

                start_time = datetime.now()
                index = await client.refresh_metadata(
                    datasets=datasets,
                    force_full_refresh=force,
                )
                duration = datetime.now() - start_time

                progress.update(task, completed=True)

            console.print(
                f"[green]Metadata refresh completed in {duration.total_seconds():.2f}s[/green]"
            )
            console.print(f"Total datasets: {index.total_datasets}")
            console.print(f"Total files: {index.total_files:,}")
            console.print(f"Total size: {index.total_size_gb:.2f} GB")

    run(_refresh_metadata())


@app.command()
def cache_stats(
    cache_dir: Optional[Path] = typer.Option(None, help="Cache directory"),
    json_output: bool = typer.Option(False, "--json", help="Output as JSON"),
):
    """Show cache statistics."""

    async def _cache_stats():
        async with create_client(cache_dir=cache_dir) as client:
            stats = await client.get_cache_stats()

            if json_output:
                console.print(
                    JSON(
                        orjson.dumps(
                            stats,
                            option=orjson.OPT_INDENT_2 | orjson.OPT_SERIALIZE_DATETIME,
                        ).decode()
                    )
                )
            else:
                console.print("[bold cyan]Cache Statistics[/bold cyan]")

                if "memory" in stats and stats["memory"]:
                    memory_stats = stats["memory"]
                    console.print("\n[bold]Memory Cache:[/bold]")
                    console.print(f"  Hit ratio: {memory_stats.hit_ratio:.2%}")
                    console.print(f"  Hits: {memory_stats.hits:,}")
                    console.print(f"  Misses: {memory_stats.misses:,}")
                    console.print(f"  Evictions: {memory_stats.evictions:,}")
                    console.print(f"  Size: {memory_stats.size_mb:.2f} MB")
                    console.print(f"  Entries: {memory_stats.total_entries:,}")

                if "disk" in stats and stats["disk"]:
                    disk_stats = stats["disk"]
                    console.print("\n[bold]Disk Cache:[/bold]")
                    console.print(f"  Total files: {disk_stats['total_files']:,}")
                    console.print(f"  Size: {disk_stats['total_size_mb']:.2f} MB")
                    console.print(f"  Expired files: {disk_stats['expired_files']:,}")
                    console.print(f"  Cache dir: {disk_stats['cache_dir']}")

    run(_cache_stats())


@app.command()
def cleanup_cache(
    max_age_days: int = typer.Option(7, help="Maximum age in days for cache entries"),
    cache_dir: Optional[Path] = typer.Option(None, help="Cache directory"),
):
    """Clean up old cache entries."""

    async def _cleanup_cache():
        async with create_client(cache_dir=cache_dir) as client:
            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                console=console,
            ) as progress:
                task = progress.add_task("Cleaning up cache...", total=None)

                stats = await client.cleanup_cache(max_age_days=max_age_days)

                progress.update(task, completed=True)

            console.print("[green]Cache cleanup completed[/green]")
            console.print(f"Cleaned entries older than {max_age_days} days")

    run(_cleanup_cache())


@app.command()
def health_check(
    cache_dir: Optional[Path] = typer.Option(None, help="Cache directory"),
    json_output: bool = typer.Option(False, "--json", help="Output as JSON"),
):
    """Perform health check of the metadata system."""

    async def _health_check():
        async with create_client(cache_dir=cache_dir) as client:
            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                console=console,
            ) as progress:
                task = progress.add_task("Performing health check...", total=None)

                health = await client.health_check()

                progress.update(task, completed=True)

            if json_output:
                console.print(
                    JSON(
                        orjson.dumps(
                            health,
                            option=orjson.OPT_INDENT_2 | orjson.OPT_SERIALIZE_DATETIME,
                        ).decode()
                    )
                )
            else:
                status_color = {
                    "healthy": "green",
                    "degraded": "yellow",
                    "unhealthy": "red",
                }.get(health["status"], "white")

                console.print(
                    f"[bold {status_color}]System Status: {health['status'].upper()}[/bold {status_color}]"
                )
                console.print(f"Timestamp: {health['timestamp']}")

                console.print("\n[bold]Component Checks:[/bold]")
                for component, status in health["checks"].items():
                    color = "green" if status == "ok" else "red"
                    console.print(f"  {component}: [{color}]{status}[/{color}]")

                if "metadata_stats" in health:
                    stats = health["metadata_stats"]
                    console.print("\n[bold]Metadata Statistics:[/bold]")
                    console.print(f"  Datasets: {stats['total_datasets']:,}")
                    console.print(f"  Files: {stats['total_files']:,}")
                    console.print(f"  Size: {stats['total_size_gb']:.2f} GB")
                    console.print(f"  Last Updated: {stats['last_updated']}")

    run(_health_check())


if __name__ == "__main__":
    app()
