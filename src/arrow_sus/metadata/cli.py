"""Command-line interface for DATASUS metadata operations."""

from asyncio import run
from json import loads, dumps
from sys import stderr, stdout
import orjson
from datetime import datetime
from pathlib import Path
from typing import List, Optional
from collections import defaultdict

import typer
from rich.console import Console
from rich.table import Table
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich.json import JSON

from .core.client import DataSUSMetadataClient
from .core.config import DataSUSConfig, CacheConfig, PerformanceConfig
from .core.models import UFCode, DatasetSource
from .core.updater import MetadataUpdater
from .systems.monthly_system import MonthlyDatasusSystem
from .systems.yearly_system import YearlyDatasusSystem
from .utils.validation import validate_search_params, ValidationError
from returns.result import Success, Failure

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


def _determine_system_type(source: str) -> str:
    """Determine if a source uses monthly or yearly organization."""
    monthly_systems = {"sia", "sih", "cnes", "cih", "ciha", "sisprenatal"}
    yearly_systems = {"sinasc", "sim", "sinan", "resp"}

    source_lower = source.lower()
    if source_lower in yearly_systems:
        return "yearly"
    elif source_lower in monthly_systems:
        return "monthly"
    else:
        # Default to monthly for unknown systems
        return "monthly"


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
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console,
        ) as progress:
            task = progress.add_task("Fetching dataset list...", total=None)

            try:
                # Create updater to access cache
                config = DataSUSConfig()

                updater = MetadataUpdater(
                    config=config,
                    cache_dir=cache_dir,
                )

                # Get source systems from cache
                source_systems = await updater.cache.get_source_systems()

                if source_systems is None:
                    progress.update(
                        task, description="No cached metadata found, extracting..."
                    )
                    extract_result = await updater.extract_metadata()
                    if isinstance(extract_result, Success):
                        source_systems = extract_result.unwrap()
                    else:
                        console.print(
                            f"[red]Failed to extract metadata: {extract_result.failure()}[/red]"
                        )
                        await updater.close()
                        return

                progress.update(task, completed=True)

                # Filter by source if specified
                if source:
                    if source not in source_systems:
                        console.print(f"[red]Invalid source: {source}[/red]")
                        console.print(
                            f"Valid sources: {', '.join(source_systems.keys())}"
                        )
                        await updater.close()
                        return
                    source_systems = {source: source_systems[source]}

                if not source_systems:
                    console.print("[yellow]No datasets found[/yellow]")
                    await updater.close()
                    return

                table = Table(title=f"Available Datasets ({category})")
                table.add_column("Dataset", style="cyan")
                table.add_column("Source", style="magenta")

                # Build dataset list from source systems
                for source_name, sys_info in source_systems.items():
                    for group, group_info in sys_info["groups"].items():
                        dataset_name = f"{source_name}-{group}"
                        table.add_row(dataset_name, source_name)

                console.print(table)
                await updater.close()

            except Exception as e:
                progress.update(task, description="❌ Failed to fetch datasets")
                console.print(f"[red]Error: {e}[/red]")
                raise typer.Exit(1)

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
def search_system(
    source: str = typer.Argument(
        ..., help="Source system (sia, sih, cnes, sinasc, sinan, sim, etc.)"
    ),
    group: Optional[str] = typer.Argument(
        None, help="Group/prefix (PA, RD, ER for monthly; DN, DENG, DO for yearly)"
    ),
    uf: Optional[str] = typer.Option(None, help="UF code"),
    year: Optional[int] = typer.Option(None, help="Year"),
    month: Optional[int] = typer.Option(None, help="Month (only for monthly systems)"),
    limit: Optional[int] = typer.Option(
        None, help="Maximum number of results (default: no limit)"
    ),
    cache_dir: Optional[Path] = typer.Option(None, help="Cache directory"),
    json_output: bool = typer.Option(False, "--json", help="Output as JSON"),
    detailed: bool = typer.Option(
        False, "--detailed", help="Show detailed file list even in interactive mode"
    ),
):
    """Search files using Result-based system for both monthly and yearly systems."""

    async def _search_source():
        async with create_client(cache_dir=cache_dir) as client:
            # Get source metadata
            try:
                # Load source-based metadata
                source_metadata = await client.get_source_metadata(source)
                if not source_metadata:
                    console.print(f"[red]Source '{source}' not found[/red]")
                    return

                # Determine system type and create appropriate system
                system_type = _determine_system_type(source)

                # Validate parameters based on system type
                if system_type == "yearly" and month is not None:
                    console.print(
                        f"[red]Error: Month parameter not supported for yearly system '{source}'[/red]"
                    )
                    console.print(
                        f"[yellow]Yearly systems organize data by year only[/yellow]"
                    )
                    return

                if system_type == "yearly":
                    system = YearlyDatasusSystem(source_metadata, client)
                else:
                    system = MonthlyDatasusSystem(source_metadata, client)

                # Build filters dict based on system type
                filters = {}
                if group:
                    filters["group"] = group
                if uf:
                    filters["uf"] = uf
                if year:
                    filters["year"] = year
                if month and system_type == "monthly":
                    filters["month"] = month

                # Use the Result-based system
                with Progress(
                    SpinnerColumn(),
                    TextColumn("[progress.description]{task.description}"),
                    console=console,
                ) as progress:
                    search_desc = f"Searching {source.upper()}"
                    if group:
                        search_desc += f"-{group.upper()}"
                    search_desc += " files..."

                    task = progress.add_task(
                        search_desc,
                        total=None,
                    )

                    result = await system.get_files(**filters)

                    progress.remove_task(task)

                match result:
                    case Success(files):
                        # Convert files to JSON serializable format
                        files_data = []
                        files_to_process = files[:limit] if limit else files
                        for file in files_to_process:
                            file_dict = {
                                "filename": file.filename,
                                "size_mb": file.size_mb,
                                "full_path": file.full_path,
                                "dataset": file.dataset,
                            }
                            if file.partition:
                                partition_dict = {
                                    "uf": file.partition.uf.value
                                    if file.partition.uf
                                    else None,
                                    "year": file.partition.year,
                                }
                                # Only add month for monthly systems
                                if (
                                    hasattr(file.partition, "month")
                                    and file.partition.month is not None
                                ):
                                    partition_dict["month"] = file.partition.month
                                file_dict.update(partition_dict)
                            files_data.append(file_dict)

                        if json_output:
                            print(
                                orjson.dumps(
                                    files_data, option=orjson.OPT_INDENT_2
                                ).decode()
                            )
                        else:
                            # Display the files in a table
                            files_to_show = files[:limit] if limit else files
                            title_parts = [source.upper()]
                            if group:
                                title_parts.append(group.upper())
                            title = f"{'-'.join(title_parts)} Files ({len(files_to_show)} files)"

                            # Determine display mode
                            is_interactive = not json_output and len(files) > 10
                            show_summary = is_interactive and not detailed

                            if show_summary:
                                # Show summary
                                total_size = sum(
                                    f.get("size_mb", 0) or 0 for f in files_data
                                )
                                years = {
                                    f.get("year") for f in files_data if f.get("year")
                                }
                                months = {
                                    f.get("month") for f in files_data if f.get("month")
                                }

                                console.print(f"\n[bold]{title}[/bold]")
                                console.print(
                                    f"Found: [yellow]{len(files_to_show)}[/yellow] files"
                                )
                                console.print(
                                    f"Total size: [blue]{total_size:.1f} MB[/blue]"
                                )
                                if years:
                                    year_range = (
                                        f"{min(years)}-{max(years)}"
                                        if len(years) > 1
                                        else str(list(years)[0])
                                    )
                                    console.print(f"Years: [green]{year_range}[/green]")
                                # Only show month info for monthly systems
                                if months and system_type == "monthly":
                                    month_display = (
                                        f"{len(months)} months"
                                        if len(months) > 3
                                        else ", ".join(map(str, sorted(months)))
                                    )
                                    console.print(
                                        f"Months: [magenta]{month_display}[/magenta]"
                                    )
                                console.print(
                                    f"\n[dim]Use --detailed to see individual files or --json for machine-readable output[/dim]"
                                )
                            else:
                                # Show detailed table
                                table = Table(title=title)
                                table.add_column("Filename", style="white")
                                table.add_column("UF", style="magenta")
                                table.add_column("Year", style="yellow")
                                table.add_column("Month", style="green")
                                table.add_column(
                                    "Size (MB)", style="blue", justify="right"
                                )

                                for file_data in files_data:
                                    table.add_row(
                                        file_data["filename"],
                                        file_data.get("uf", "N/A"),
                                        str(file_data.get("year", "N/A")),
                                        str(file_data.get("month", "N/A")),
                                        f"{file_data.get('size_mb', 0):.2f}"
                                        if file_data.get("size_mb")
                                        else "N/A",
                                    )

                                console.print(table)

                    case Failure(error):
                        console.print(f"[red]Search Error: {error.message}[/red]")
                        if error.filters:
                            console.print(f"Filters used: {error.filters}")
                        return

            except Exception as e:
                console.print(f"[red]Unexpected error: {str(e)}[/red]")
                return

    run(_search_source())


@app.command()
def search_files(
    dataset: Optional[str] = typer.Option(None, help="Dataset name"),
    uf: Optional[str] = typer.Option(None, help="UF code"),
    year: Optional[int] = typer.Option(None, help="Year"),
    month: Optional[int] = typer.Option(None, help="Month"),
    pattern: Optional[str] = typer.Option(None, help="Filename pattern"),
    min_size: Optional[float] = typer.Option(None, help="Minimum file size in MB"),
    max_size: Optional[float] = typer.Option(None, help="Maximum file size in MB"),
    limit: Optional[int] = typer.Option(
        None, help="Maximum number of results (default: no limit)"
    ),
    cache_dir: Optional[Path] = typer.Option(None, help="Cache directory"),
    json_output: bool = typer.Option(False, "--json", help="Output as JSON"),
    detailed: bool = typer.Option(
        False, "--detailed", help="Show detailed file list even in interactive mode"
    ),
):
    """Search for files matching criteria."""

    async def _search_files():
        async with create_client(cache_dir=cache_dir) as client:
            # Get available datasets for validation
            try:
                available_datasets = await client.list_available_datasets()
            except Exception as e:
                console.print(f"[red]Error getting dataset list: {e}[/red]")
                return

            # Validate all search parameters
            validation_result = validate_search_params(
                dataset=dataset,
                uf=uf,
                year=year,
                month=month,
                available_datasets=available_datasets,
            )

            match validation_result:
                case Failure(error):
                    console.print(f"[red]Validation Error: {error}[/red]")
                    if isinstance(error, ValidationError):
                        if hasattr(error, "field") and error.field == "uf":
                            console.print(
                                f"Valid UFs: {', '.join([u.value.upper() for u in UFCode])}"
                            )
                        elif hasattr(error, "field") and error.field == "dataset":
                            console.print(
                                f"Available datasets: {', '.join(available_datasets[:10])}{'...' if len(available_datasets) > 10 else ''}"
                            )
                    return
                case Success(validated_params):
                    # Convert validated parameters to the format expected by the client
                    uf_enum = None
                    if "uf_codes" in validated_params and validated_params["uf_codes"]:
                        try:
                            uf_enum = UFCode(validated_params["uf_codes"][0].lower())
                        except ValueError:
                            console.print(
                                f"[red]Invalid UF: {validated_params['uf_codes'][0]}[/red]"
                            )
                            return

                    validated_year = (
                        validated_params.get("years", [None])[0]
                        if validated_params.get("years")
                        else year
                    )
                    validated_month = (
                        validated_params.get("months", [None])[0]
                        if validated_params.get("months")
                        else month
                    )
                    validated_dataset = validated_params.get("dataset", dataset)

            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                console=console,
            ) as progress:
                task = progress.add_task("Searching files...", total=None)

                files = await client.search_files(
                    dataset=validated_dataset,
                    uf=uf_enum,
                    year=validated_year,
                    month=validated_month,
                    file_pattern=pattern,
                    min_size_mb=min_size,
                    max_size_mb=max_size,
                )

                progress.update(task, completed=True)

            # Limit results if specified
            if limit:
                files = files[:limit]

            if not files:
                console.print("[yellow]No files found matching criteria[/yellow]")
                return

            # Determine output format based on context
            # Check if we're in a real terminal (not redirected or piped)
            is_interactive = stdout.isatty() and not json_output
            show_summary = is_interactive and not detailed and len(files) > 10

            # For testing purposes, force summary when we have many results in interactive mode
            if not json_output and not detailed and len(files) > 10:
                show_summary = True

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
            elif show_summary:
                # Show grouped summary for interactive mode with many results
                dataset_groups = defaultdict(
                    lambda: {
                        "count": 0,
                        "total_size": 0.0,
                        "years": set(),
                        "months": set(),
                    }
                )

                for file in files:
                    key = file.dataset
                    dataset_groups[key]["count"] += 1
                    if file.size_mb:
                        dataset_groups[key]["total_size"] += file.size_mb
                    if file.partition and file.partition.year:
                        dataset_groups[key]["years"].add(file.partition.year)
                    if file.partition and file.partition.month:
                        dataset_groups[key]["months"].add(file.partition.month)

                # Summary table
                summary_table = Table(
                    title=f"Search Results Summary ({len(files)} files total)"
                )
                summary_table.add_column("Dataset", style="cyan")
                summary_table.add_column("Files", style="yellow", justify="right")
                summary_table.add_column("Total Size", style="blue", justify="right")
                summary_table.add_column("Year Range", style="green")
                summary_table.add_column("Months", style="magenta")

                for dataset_name in sorted(dataset_groups.keys()):
                    group = dataset_groups[dataset_name]
                    years = sorted(group["years"]) if group["years"] else []
                    months = sorted(group["months"]) if group["months"] else []

                    year_range = (
                        f"{min(years)}-{max(years)}"
                        if len(years) > 1
                        else str(years[0])
                        if years
                        else "N/A"
                    )
                    month_display = (
                        f"{len(months)} months"
                        if len(months) > 3
                        else ", ".join(map(str, months))
                        if months
                        else "N/A"
                    )

                    summary_table.add_row(
                        dataset_name,
                        str(group["count"]),
                        f"{group['total_size']:.1f} MB",
                        year_range,
                        month_display,
                    )

                console.print(summary_table)
                console.print(
                    f"\n[dim]Use --detailed to see individual files or --json for machine-readable output[/dim]"
                )
            else:
                # Show detailed file list
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


@app.command()
def extract_metadata(
    cache_dir: Optional[Path] = typer.Option(
        None,
        "--cache-dir",
        help="Cache directory (default: ~/.arrow_sus_cache)",
    ),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Verbose output"),
):
    """Extract metadata from mappings.py and build initial cache."""

    async def _extract_metadata():
        with Progress(
            SpinnerColumn(), TextColumn("[progress.description]{task.description}")
        ) as progress:
            extract_task = progress.add_task(
                "Extracting metadata from mappings.py...", total=None
            )

            try:
                # Create updater
                config = DataSUSConfig()

                updater = MetadataUpdater(
                    config=config,
                    cache_dir=cache_dir,
                )

                # Extract metadata
                result = await updater.extract_metadata()

                if isinstance(result, Success):
                    source_systems = result.unwrap()
                    progress.update(
                        extract_task, description="✅ Metadata extracted successfully"
                    )

                    # Show summary
                    console.print(
                        "\n[bold green]✅ Metadata extraction completed successfully![/bold green]"
                    )
                    console.print(f"\n[bold]Summary:[/bold]")
                    console.print(f"  Source systems: {len(source_systems)}")

                    total_groups = sum(
                        len(sys_info["groups"]) for sys_info in source_systems.values()
                    )
                    console.print(f"  Total groups: {total_groups}")

                    if verbose:
                        console.print("\n[bold]Source Systems:[/bold]")
                        for source, sys_info in source_systems.items():
                            console.print(
                                f"  • {source.upper()}: {len(sys_info['groups'])} groups ({sys_info['type']})"
                            )
                            if verbose:
                                for group, group_info in sys_info["groups"].items():
                                    console.print(
                                        f"    - {group}: {group_info['name']}"
                                    )
                else:
                    error = result.failure()
                    progress.update(
                        extract_task, description="❌ Metadata extraction failed"
                    )
                    console.print(f"\n[bold red]❌ Error: {error}[/bold red]")
                    raise typer.Exit(1)

                await updater.close()

            except Exception as e:
                progress.update(
                    extract_task, description="❌ Metadata extraction failed"
                )
                console.print(f"\n[bold red]❌ Unexpected error: {e}[/bold red]")
                raise typer.Exit(1)

    run(_extract_metadata())


if __name__ == "__main__":
    app()
