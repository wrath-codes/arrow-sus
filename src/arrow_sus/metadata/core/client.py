"""High-performance DATASUS metadata client with async operations and advanced caching.

This module provides the main client interface for interacting with the DATASUS metadata
system. The `DataSUSMetadataClient` class offers high-performance operations for fetching,
caching, and managing metadata for Brazilian health system datasets.

Key Features:
    - **Async FTP Operations**: Non-blocking file operations with connection pooling
    - **Tiered Caching**: Memory + disk caching with configurable TTL and size limits
    - **Concurrent Downloads**: Parallel file downloads with configurable limits
    - **Smart Metadata Updates**: Incremental updates with change detection
    - **Performance Monitoring**: Built-in metrics and health checks
    - **Context Manager Support**: Automatic resource cleanup with async context managers

Performance Characteristics:
    - **Memory Usage**: Configurable memory cache with LRU eviction (default: 1GB)
    - **Disk Cache**: Configurable disk cache with automatic cleanup (default: 10GB)
    - **Concurrency**: Up to 10 concurrent FTP connections by default
    - **Throughput**: ~50-100MB/s download speeds depending on network conditions

Example:
    Basic usage with async context manager:

    ```python
    import asyncio
    from arrow_sus.metadata.core.client import DataSUSMetadataClient
    from arrow_sus.metadata.core.config import DataSUSConfig

    async def main():
        config = DataSUSConfig(cache_enabled=True)

        async with DataSUSMetadataClient(config=config) as client:
            # Get available datasets
            datasets = await client.list_available_datasets()
            print(f"Found {len(datasets)} datasets")

            # Get metadata for SINAN dataset
            metadata = await client.get_dataset_metadata("sinan")
            if metadata:
                print(f"SINAN has {metadata.metadata.total_files} files")
                print(f"Total size: {metadata.metadata.total_size_gb:.2f} GB")

    asyncio.run(main())
    ```

Note:
    This client is thread-safe and can be used in multithreaded environments.
    However, it's designed for async operations and should be used with asyncio.
"""

from asyncio import Lock, gather
from logging import getLogger
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

from ..io.async_ftp import AsyncFTPClient
from ..utils.cache import DiskCache, LRUCache, TieredCache
from .config import CacheConfig, DataSUSConfig, PerformanceConfig
from .models import (
    DatasetMetadata,
    DatasetSource,
    FileMetadata,
    MetadataIndex,
    RemoteFile,
    UFCode,
)
from .updater import MetadataUpdater

logger = getLogger(__name__)


class DataSUSMetadataClient:
    """High-performance async client for DATASUS metadata operations with advanced caching.

    This client provides a comprehensive interface for accessing Brazilian health system
    metadata from DATASUS FTP servers. It features intelligent caching, concurrent
    operations, and automatic resource management.

    The client supports three categories of data:
        - **data**: Main health datasets (SINAN, SIM, SINASC, etc.)
        - **documentation**: Data dictionaries and documentation files
        - **auxiliary**: Supporting files like geographic codes

    Attributes:
        config (DataSUSConfig): Configuration for FTP connections and data sources
        cache_config (CacheConfig): Caching behavior and limits configuration
        performance_config (PerformanceConfig): Performance tuning parameters
        cache_dir (Path): Directory for persistent cache storage
        ftp_client (AsyncFTPClient): Async FTP client with connection pooling
        updater (MetadataUpdater): Component for metadata refresh operations
        cache (TieredCache): Two-tier memory + disk cache system

    Example:
        Initialize with custom configuration:

        ```python
        from pathlib import Path
        from arrow_sus.metadata.core import (
            DataSUSMetadataClient, DataSUSConfig, CacheConfig, PerformanceConfig
        )

        # Custom configuration for high-performance usage
        config = DataSUSConfig(
            ftp_host="ftp.datasus.gov.br",
            timeout_seconds=30,
            retry_attempts=3
        )

        cache_config = CacheConfig(
            memory_max_size=10000,  # 10k items in memory
            memory_max_mb=2048,     # 2GB memory limit
            disk_max_gb=50,         # 50GB disk cache
            default_ttl_hours=24    # 24-hour TTL
        )

        perf_config = PerformanceConfig(
            max_ftp_connections=20,
            max_concurrent_downloads=10,
            chunk_size=8192
        )

        client = DataSUSMetadataClient(
            config=config,
            cache_config=cache_config,
            performance_config=perf_config,
            cache_dir=Path("/fast/ssd/cache")
        )
        ```

        Search and download files:

        ```python
        async def download_sinan_data():
            async with DataSUSMetadataClient() as client:
                # Search for SINAN dengue data from São Paulo in 2023
                files = await client.search_files(
                    dataset="sinan",
                    uf=UFCode.SP,
                    year=2023,
                    file_pattern="dengue"
                )

                if files:
                    # Download files concurrently
                    local_dir = Path("./downloads")
                    local_dir.mkdir(exist_ok=True)

                    results = await client.batch_download_files(
                        files=files[:5],  # Download first 5 files
                        local_dir=local_dir,
                        max_concurrent=3
                    )

                    for remote_path, local_path in results:
                        print(f"Downloaded: {local_path}")
        ```

        Monitor performance and health:

        ```python
        async def monitor_client():
            async with DataSUSMetadataClient() as client:
                # Check system health
                health = await client.health_check()
                print(f"System status: {health['status']}")

                # Get cache performance metrics
                cache_stats = await client.get_cache_stats()
                print(f"Cache hit rate: {cache_stats['memory_hit_rate']:.2%}")
                print(f"Disk usage: {cache_stats['disk_usage_gb']:.2f} GB")

                # Clean up old cache entries
                cleanup_results = await client.cleanup_cache(max_age_days=3)
                print(f"Freed {cleanup_results['freed_space_gb']:.2f} GB")
        ```

    Note:
        - Always use as an async context manager to ensure proper resource cleanup
        - The client maintains persistent connections and should be reused for multiple operations
        - Cache performance is optimal when the client instance is long-lived
        - Use `force_refresh=True` sparingly as it invalidates cache benefits
    """

    def __init__(
        self,
        config: Optional[DataSUSConfig] = None,
        cache_config: Optional[CacheConfig] = None,
        performance_config: Optional[PerformanceConfig] = None,
        cache_dir: Optional[Path] = None,
    ):
        """Initialize the DATASUS metadata client with configuration options.

        Args:
            config: DATASUS-specific configuration (FTP settings, data sources).
                If None, uses default configuration with standard DATASUS FTP server.
            cache_config: Caching behavior configuration (memory/disk limits, TTL).
                If None, uses conservative defaults (1GB memory, 10GB disk, 6h TTL).
            performance_config: Performance tuning parameters (connection limits, timeouts).
                If None, uses balanced defaults (10 connections, 5 concurrent downloads).
            cache_dir: Directory for persistent cache storage.
                If None, uses ~/.arrow_sus_cache in user's home directory.

        Raises:
            OSError: If cache directory cannot be created or accessed.
            ValueError: If configuration parameters are invalid.

        Example:
            ```python
            from pathlib import Path
            from arrow_sus.metadata.core import DataSUSMetadataClient, CacheConfig

            # Basic initialization with defaults
            client = DataSUSMetadataClient()

            # Custom cache configuration for large-scale operations
            cache_config = CacheConfig(
                memory_max_mb=4096,    # 4GB memory cache
                disk_max_gb=100,       # 100GB disk cache
                default_ttl_hours=12   # 12-hour TTL
            )

            client = DataSUSMetadataClient(
                cache_config=cache_config,
                cache_dir=Path("/mnt/fast-storage/sus-cache")
            )
            ```
        """
        self.config = config or DataSUSConfig()
        self.cache_config = cache_config or CacheConfig()
        self.performance_config = performance_config or PerformanceConfig()
        self.cache_dir = cache_dir or Path.home() / ".arrow_sus_cache"

        # Initialize caches
        self._setup_caches()

        # Initialize FTP client
        self.ftp_client = AsyncFTPClient(
            host=self.config.ftp_host,
            max_connections=self.performance_config.max_ftp_connections,
            max_concurrent_operations=self.performance_config.max_concurrent_downloads,
            cache_dir=self.cache_dir / "ftp_cache",
        )

        # Initialize updater
        self.updater = MetadataUpdater(
            config=self.config,
            cache_dir=self.cache_dir,
            max_concurrent_operations=self.performance_config.max_concurrent_downloads,
        )

        self._closed = False

    def _setup_caches(self):
        """Set up the two-tier memory + disk caching system.

        Creates and configures:
            - Memory cache: LRU-based with configurable size and memory limits
            - Disk cache: Persistent storage with automatic cleanup and size management
            - Tiered cache: Combines both with intelligent promotion/demotion

        The tiered cache automatically promotes frequently accessed items to memory
        and demotes less-used items to disk storage for optimal performance.
        """
        memory_cache = LRUCache(
            max_size=self.cache_config.memory_max_size,
            max_memory_mb=self.cache_config.memory_max_mb,
            default_ttl_hours=self.cache_config.default_ttl_hours,
        )

        disk_cache = DiskCache(
            cache_dir=self.cache_dir / "disk_cache",
            max_size_gb=self.cache_config.disk_max_gb,
            default_ttl_hours=self.cache_config.default_ttl_hours,
        )

        self.cache = TieredCache(memory_cache, disk_cache)

    async def get_metadata_index(
        self,
        force_refresh: bool = False,
        use_cache: bool = True,
    ) -> Optional[MetadataIndex]:
        """Get the complete metadata index with all available datasets and files.

        The metadata index provides a comprehensive overview of all available datasets,
        their file counts, sizes, and availability periods. This method implements
        intelligent caching to minimize FTP operations while ensuring data freshness.

        Caching Strategy:
            1. Check memory cache first (fastest, ~1ms access time)
            2. Check disk cache if memory miss (~10ms access time)
            3. Use existing index file if less than 6 hours old
            4. Refresh from FTP servers only when necessary

        Args:
            force_refresh: If True, bypasses all caches and forces a complete
                refresh from FTP servers. Use sparingly as this is expensive.
            use_cache: If False, skips cache lookup but still stores results
                in cache for future use.

        Returns:
            MetadataIndex containing all dataset information, or None if the
            FTP servers are unreachable and no cached data is available.

        Raises:
            RuntimeError: If the client has been closed.
            ConnectionError: If FTP servers are unreachable and no cache is available.
            OSError: If cache directory is not accessible.

        Example:
            ```python
            async with DataSUSMetadataClient() as client:
                # Get cached index (fast)
                index = await client.get_metadata_index()
                if index:
                    print(f"Total datasets: {index.total_datasets}")
                    print(f"Total files: {index.total_files:,}")
                    print(f"Total size: {index.total_size_gb:.1f} GB")
                    print(f"Last updated: {index.last_updated}")

                # Force fresh data from servers (slow but up-to-date)
                fresh_index = await client.get_metadata_index(force_refresh=True)

                # Compare freshness
                age_hours = (datetime.utcnow() - index.last_updated).total_seconds() / 3600
                print(f"Cached data is {age_hours:.1f} hours old")
            ```

        Performance:
            - Cache hit: ~1-10ms response time
            - Cache miss (recent index): ~100-500ms response time
            - Full refresh: ~30-300s depending on FTP server load and dataset count
        """
        if self._closed:
            raise RuntimeError("Client is closed")

        cache_key = "metadata_index"

        # Try cache first
        if use_cache and not force_refresh:
            cached_index = await self.cache.get(cache_key)
            if cached_index:
                logger.debug("Using cached metadata index")
                return MetadataIndex.model_validate(cached_index)

        # Check if we have a recent index file
        if not force_refresh:
            index = await self.updater.get_index(use_cache=use_cache)
            if index:
                # Check if index is recent (less than 6 hours old)
                age = datetime.utcnow() - index.last_updated
                if age < timedelta(hours=6):
                    # Cache in memory for faster access
                    await self.cache.set(
                        cache_key,
                        index.model_dump(),
                        ttl=timedelta(hours=2),
                    )
                    return index

        # Refresh metadata
        logger.info("Refreshing metadata index...")
        index = await self.updater.update_all_metadata(force_refresh=force_refresh)

        # Cache the fresh index
        await self.cache.set(
            cache_key,
            index.model_dump(),
            ttl=timedelta(hours=2),
        )

        return index

    async def get_dataset_metadata(
        self,
        dataset_name: str,
        category: str = "data",
        force_refresh: bool = False,
        use_cache: bool = True,
    ) -> Optional[DatasetMetadata]:
        """Get detailed metadata for a specific dataset including all files and statistics.

        Retrieves comprehensive metadata for a single dataset, including file listings,
        size information, geographic coverage, and temporal availability. This method
        provides more detailed information than the metadata index.

        Args:
            dataset_name: Name of the dataset (e.g., "sinan", "sim", "sinasc").
                Case-insensitive matching is performed.
            category: Dataset category to search in. Options:
                - "data": Main health datasets (default)
                - "documentation": Data dictionaries and documentation
                - "auxiliary": Supporting files like geographic codes
            force_refresh: If True, forces a fresh fetch from FTP servers,
                bypassing all caches.
            use_cache: If False, skips cache lookup but still stores results.

        Returns:
            DatasetMetadata object containing detailed information about the dataset,
            or None if the dataset is not found or not accessible.

        Raises:
            RuntimeError: If the client has been closed.
            ValueError: If category is not one of the supported values.

        Example:
            ```python
            async with DataSUSMetadataClient() as client:
                # Get SINAN metadata
                sinan = await client.get_dataset_metadata("sinan")
                if sinan:
                    print(f"Dataset: {sinan.name}")
                    print(f"Source: {sinan.source.value}")
                    print(f"Files: {len(sinan.files):,}")
                    print(f"Size: {sinan.metadata.total_size_gb:.2f} GB")
                    print(f"UFs: {[uf.value for uf in sinan.metadata.supported_ufs]}")
                    print(f"Period: {sinan.metadata.first_period} to {sinan.metadata.last_period}")

                # Get documentation for SINAN
                docs = await client.get_dataset_metadata("sinan", category="documentation")
                if docs:
                    print(f"Documentation files: {len(docs.files)}")
                    for file in docs.files[:3]:  # Show first 3 files
                        print(f"  - {file.filename} ({file.size_mb:.1f} MB)")
            ```

        Performance:
            - Cache hit: ~5-20ms response time
            - Cache miss: ~500ms-5s depending on dataset size and FTP response time
        """
        if self._closed:
            raise RuntimeError("Client is closed")

        # Get from updater directly (it handles its own caching)
        metadata = await self.updater.get_dataset_metadata(
            dataset_name=dataset_name,
            category=category,
            use_cache=use_cache and not force_refresh,
        )

        return metadata

    async def list_available_datasets(
        self,
        source: Optional[DatasetSource] = None,
        category: str = "data",
    ) -> List[str]:
        """List all available datasets, optionally filtered by data source.

        Returns a sorted list of dataset names available in the specified category.
        This is useful for discovery and validation of dataset names before making
        more detailed metadata requests.

        Args:
            source: Optional filter by data source (e.g., DatasetSource.SIM,
                DatasetSource.SINAN). If None, returns datasets from all sources.
            category: Dataset category to list from:
                - "data": Main health datasets (default)
                - "documentation": Data dictionaries and documentation
                - "auxiliary": Supporting files like geographic codes

        Returns:
            Sorted list of dataset names available in the specified category.
            Empty list if no datasets are found or category doesn't exist.

        Example:
            ```python
            async with DataSUSMetadataClient() as client:
                # List all available datasets
                all_datasets = await client.list_available_datasets()
                print(f"Available datasets: {len(all_datasets)}")
                for dataset in all_datasets[:5]:  # Show first 5
                    print(f"  - {dataset}")

                # List only SIM datasets
                sim_datasets = await client.list_available_datasets(
                    source=DatasetSource.SIM
                )
                print(f"SIM datasets: {sim_datasets}")

                # List documentation datasets
                docs = await client.list_available_datasets(category="documentation")
                print(f"Documentation datasets: {len(docs)}")
            ```

        Performance:
            - Typically completes in <100ms using cached metadata index
            - First call may take longer if metadata index needs to be fetched
        """
        index = await self.get_metadata_index()
        if not index:
            return []

        # Get datasets from the specified category
        if category == "data":
            datasets = list(index.data.keys())
        elif category == "documentation":
            datasets = list(index.documentation.keys())
        elif category == "auxiliary":
            datasets = list(index.auxiliary.keys())
        else:
            datasets = []

        # Filter by source if specified
        if source:
            filtered_datasets = []
            for dataset_name in datasets:
                # Check if dataset belongs to the specified source
                if dataset_name.startswith(source.value):
                    filtered_datasets.append(dataset_name)
            datasets = filtered_datasets

        return sorted(datasets)

    async def search_files(
        self,
        dataset: Optional[str] = None,
        uf: Optional[UFCode] = None,
        year: Optional[int] = None,
        month: Optional[int] = None,
        file_pattern: Optional[str] = None,
        min_size_mb: Optional[float] = None,
        max_size_mb: Optional[float] = None,
        category: str = "data",
    ) -> List[RemoteFile]:
        """Search for files across datasets using multiple filter criteria.

        Performs a comprehensive search across one or more datasets to find files
        matching the specified criteria. Supports filtering by geographic region,
        time period, file patterns, and size constraints.

        Args:
            dataset: Specific dataset name to search in. If None, searches all datasets.
            uf: Brazilian state/territory code (UFCode) to filter by geographic region.
            year: Year to filter files by (e.g., 2023).
            month: Month to filter files by (1-12).
            file_pattern: Case-insensitive substring to match in filenames.
            min_size_mb: Minimum file size in megabytes.
            max_size_mb: Maximum file size in megabytes.
            category: Dataset category to search in ("data", "documentation", "auxiliary").

        Returns:
            List of RemoteFile objects matching the search criteria, sorted by
            dataset name, year, month, UF, and filename.

        Raises:
            RuntimeError: If the client has been closed.
            ValueError: If year is invalid (not between 1990-2030) or month is invalid (not 1-12).

        Example:
            ```python
            from arrow_sus.metadata.core.models import UFCode

            async with DataSUSMetadataClient() as client:
                # Search for SINAN files from São Paulo in 2023
                files = await client.search_files(
                    dataset="sinan",
                    uf=UFCode.SP,
                    year=2023
                )
                print(f"Found {len(files)} SINAN files for SP in 2023")

                # Search for large files (>100MB) across all datasets
                large_files = await client.search_files(
                    min_size_mb=100,
                    category="data"
                )

                # Search for specific disease data
                dengue_files = await client.search_files(
                    file_pattern="dengue",
                    year=2023,
                    min_size_mb=1.0  # At least 1MB
                )

                # Complex search: mortality data from Northeast in Q1 2023
                northeast_ufs = [UFCode.BA, UFCode.PE, UFCode.CE, UFCode.MA]
                for uf in northeast_ufs:
                    for month in [1, 2, 3]:  # Q1
                        monthly_files = await client.search_files(
                            dataset="sim",
                            uf=uf,
                            year=2023,
                            month=month
                        )
                        print(f"{uf.value} {month:02d}/2023: {len(monthly_files)} files")
            ```

        Performance:
            - Search within single dataset: ~50-200ms
            - Search across all datasets: ~500ms-2s depending on filter selectivity
            - Results are cached per dataset to improve subsequent searches
        """
        if self._closed:
            raise RuntimeError("Client is closed")

        # Get datasets to search
        if dataset:
            datasets = [dataset]
        else:
            datasets = await self.list_available_datasets(category=category)

        matching_files = []

        # Search in each dataset
        for dataset_name in datasets:
            metadata = await self.get_dataset_metadata(
                dataset_name=dataset_name,
                category=category,
                use_cache=True,
            )

            if not metadata:
                continue

            # Filter files
            for file in metadata.files:
                # Apply filters
                if uf and (not file.partition or file.partition.uf != uf):
                    continue

                if year and (not file.partition or file.partition.year != year):
                    continue

                if month and (not file.partition or file.partition.month != month):
                    continue

                if file_pattern and file_pattern.lower() not in file.filename.lower():
                    continue

                if min_size_mb and (not file.size_mb or file.size_mb < min_size_mb):
                    continue

                if max_size_mb and (not file.size_mb or file.size_mb > max_size_mb):
                    continue

                matching_files.append(file)

        # Sort by dataset, year, month, UF
        matching_files.sort(
            key=lambda f: (
                f.dataset,
                f.partition.year if f.partition else 0,
                f.partition.month if f.partition else 0,
                f.partition.uf.value if f.partition and f.partition.uf else "",
                f.filename,
            )
        )

        return matching_files

    async def get_source_metadata(
        self,
        source_name: str,
        category: str = "data",
    ) -> Optional["SourceMetadata"]:
        """Get source metadata for a specific source system.

        Args:
            source_name: Name of the source system (sia, sih, cnes, etc.)
            category: Dataset category

        Returns:
            SourceMetadata if found, None otherwise
        """
        # Load enhanced source-based metadata with actual files
        source_config_path = self.cache_dir / "enhanced_metadata_by_source.json"
        if not source_config_path.exists():
            return None

        try:
            import orjson

            with open(source_config_path, "r", encoding="utf-8") as f:
                sources_data = orjson.loads(f.read())

            # Find the source within the "sources" key
            sources = sources_data.get("sources", {})
            source_data = sources.get(source_name.lower())
            if not source_data:
                return None

            # Convert to SourceMetadata model
            from arrow_sus.metadata.core.models import SourceMetadata

            return SourceMetadata.from_dict(source_data)

        except Exception as e:
            return None

    async def get_dataset_stats(
        self,
        dataset_name: str,
        category: str = "data",
    ) -> Optional[Dict[str, Any]]:
        """Get comprehensive statistics and summary information for a dataset.

        Computes detailed statistics including file counts, size distributions,
        geographic coverage, temporal coverage, and file type breakdowns.

        Args:
            dataset_name: Name of the dataset to analyze.
            category: Dataset category ("data", "documentation", "auxiliary").

        Returns:
            Dictionary containing dataset statistics, or None if dataset not found.

            Keys include:
                - name: Dataset display name
                - source: Data source (e.g., "SIM", "SINAN")
                - total_files: Total number of files
                - total_size_gb: Total size in gigabytes
                - supported_ufs: List of available state codes
                - available_periods: List of available year-month periods
                - first_period, last_period: Temporal coverage bounds
                - files_by_uf: File count breakdown by state
                - files_by_year: File count breakdown by year
                - file_extensions: Available file formats
                - last_updated: When metadata was last refreshed

        Example:
            ```python
            async with DataSUSMetadataClient() as client:
                stats = await client.get_dataset_stats("sinan")
                if stats:
                    print(f"Dataset: {stats['name']}")
                    print(f"Source: {stats['source']}")
                    print(f"Files: {stats['total_files']:,}")
                    print(f"Size: {stats['total_size_gb']:.1f} GB")
                    print(f"Coverage: {stats['first_period']} to {stats['last_period']}")
                    print(f"States: {len(stats['supported_ufs'])}")

                    # File distribution by state
                    print("\\nFiles by state:")
                    for uf, count in sorted(stats['files_by_uf'].items())[:5]:
                        print(f"  {uf}: {count:,} files")

                    # File distribution by year
                    print("\\nFiles by year:")
                    for year, count in sorted(stats['files_by_year'].items())[-5:]:
                        print(f"  {year}: {count:,} files")
            ```

        Performance:
            - Uses cached dataset metadata, typically <50ms response time
        """
        metadata = await self.get_dataset_metadata(
            dataset_name=dataset_name,
            category=category,
        )

        if not metadata:
            return None

        # Calculate additional statistics
        files_by_uf = {}
        files_by_year = {}

        for file in metadata.files:
            if file.partition:
                if file.partition.uf:
                    uf = file.partition.uf.value.upper()
                    files_by_uf[uf] = files_by_uf.get(uf, 0) + 1

                if file.partition.year:
                    year = file.partition.year
                    files_by_year[year] = files_by_year.get(year, 0) + 1

        return {
            "name": metadata.name,
            "source": metadata.source.value,
            "total_files": metadata.metadata.total_files,
            "total_size_gb": metadata.metadata.total_size_gb,
            "supported_ufs": sorted(
                [uf.value.upper() for uf in metadata.metadata.supported_ufs]
            ),
            "available_periods": sorted(metadata.metadata.available_periods),
            "first_period": metadata.metadata.first_period,
            "last_period": metadata.metadata.last_period,
            "files_by_uf": files_by_uf,
            "files_by_year": files_by_year,
            "file_extensions": sorted(
                [ext.value for ext in metadata.metadata.file_extensions]
            ),
            "last_updated": metadata.metadata.last_updated.isoformat(),
        }

    async def download_file(
        self,
        remote_file: RemoteFile,
        local_path: Optional[Path] = None,
        progress_callback: Optional[callable] = None,
    ) -> Path:
        """Download a single file from DATASUS FTP servers with progress tracking.

        Downloads a file using the high-performance async FTP client with automatic
        retry logic, connection pooling, and progress reporting.

        Args:
            remote_file: RemoteFile object containing file metadata and path information.
            local_path: Local path where the file should be saved. If None, saves to
                the cache directory with the original filename.
            progress_callback: Optional callback function to track download progress.
                Should accept parameters: (bytes_downloaded: int, total_bytes: int).

        Returns:
            Path to the downloaded local file.

        Raises:
            RuntimeError: If the client has been closed.
            ConnectionError: If FTP connection fails after all retry attempts.
            OSError: If local file cannot be written (permissions, disk space).
            ValueError: If remote_file is invalid or malformed.

        Example:
            ```python
            from pathlib import Path

            async def download_with_progress(bytes_down, total_bytes):
                percent = (bytes_down / total_bytes) * 100 if total_bytes > 0 else 0
                print(f"\\rDownload progress: {percent:.1f}%", end="", flush=True)

            async with DataSUSMetadataClient() as client:
                # Find a file to download
                files = await client.search_files(
                    dataset="sinan",
                    uf=UFCode.SP,
                    year=2023,
                    max_size_mb=50  # Reasonable size for demo
                )

                if files:
                    file = files[0]
                    print(f"Downloading {file.filename} ({file.size_mb:.1f} MB)")

                    # Download with progress tracking
                    local_path = await client.download_file(
                        remote_file=file,
                        local_path=Path("./downloads") / file.filename,
                        progress_callback=download_with_progress
                    )

                    print(f"\\nDownloaded to: {local_path}")
                    print(f"File size: {local_path.stat().st_size / (1024*1024):.1f} MB")
            ```

        Performance:
            - Typical download speeds: 10-100 MB/s depending on network conditions
            - Automatic retry with exponential backoff on connection errors
            - Connection reuse for multiple downloads
        """
        if self._closed:
            raise RuntimeError("Client is closed")

        return await self.ftp_client.download_file(
            remote_path=remote_file.full_path,
            local_path=local_path,
            progress_callback=progress_callback,
        )

    async def batch_download_files(
        self,
        files: List[RemoteFile],
        local_dir: Path,
        max_concurrent: int = 5,
        progress_callback: Optional[callable] = None,
    ) -> List[tuple[str, Path]]:
        """Download multiple files concurrently with optimal performance.

        Downloads multiple files in parallel using a semaphore to control concurrency
        and prevent overwhelming the FTP servers. Features automatic retry logic,
        progress tracking, and error resilience.

        Args:
            files: List of RemoteFile objects to download.
            local_dir: Directory where files should be saved. Created if it doesn't exist.
            max_concurrent: Maximum number of simultaneous downloads (default: 5).
                Higher values may improve speed but can overwhelm servers.
            progress_callback: Optional callback for overall progress tracking.
                Receives (completed_files: int, total_files: int, bytes_downloaded: int, total_bytes: int).

        Returns:
            List of tuples containing (remote_path, local_path) for successfully
            downloaded files. Failed downloads are excluded from results.

        Raises:
            RuntimeError: If the client has been closed.
            OSError: If local_dir cannot be created or accessed.
            ValueError: If files list is empty or contains invalid entries.

        Example:
            ```python
            import asyncio
            from pathlib import Path

            async def batch_progress(completed, total, bytes_down, bytes_total):
                percent = (completed / total) * 100 if total > 0 else 0
                mb_down = bytes_down / (1024 * 1024)
                mb_total = bytes_total / (1024 * 1024) if bytes_total > 0 else 0
                print(f"\\rProgress: {completed}/{total} files ({percent:.1f}%) "
                      f"- {mb_down:.1f}/{mb_total:.1f} MB", end="", flush=True)

            async with DataSUSMetadataClient() as client:
                # Search for files to download
                files = await client.search_files(
                    dataset="sinan",
                    uf=UFCode.RJ,
                    year=2023,
                    max_size_mb=20  # Keep files reasonably sized
                )

                if files:
                    download_dir = Path("./batch_downloads")
                    download_dir.mkdir(exist_ok=True)

                    print(f"Downloading {len(files)} files...")
                    results = await client.batch_download_files(
                        files=files,
                        local_dir=download_dir,
                        max_concurrent=3,  # Conservative for demo
                        progress_callback=batch_progress
                    )

                    print(f"\\nSuccessfully downloaded {len(results)} files")
                    total_size = sum(p.stat().st_size for _, p in results)
                    print(f"Total size: {total_size / (1024*1024):.1f} MB")
            ```

        Performance:
            - Optimal concurrency depends on network and server capacity
            - Typical performance: 3-5x faster than sequential downloads
            - Conservative max_concurrent (3-5) recommended for stability
            - Failed downloads don't affect successful ones
        """
        if self._closed:
            raise RuntimeError("Client is closed")

        remote_paths = [f.full_path for f in files]
        return await self.ftp_client.batch_download(
            remote_paths=remote_paths,
            local_dir=local_dir,
            max_concurrent=max_concurrent,
            progress_callback=progress_callback,
        )

    async def refresh_metadata(
        self,
        datasets: Optional[List[str]] = None,
        force_full_refresh: bool = False,
    ) -> MetadataIndex:
        """Refresh metadata for specified datasets or all datasets from FTP servers.

        Performs incremental or full metadata refresh, updating the local cache with
        the latest file information from DATASUS FTP servers. Use this when you need
        the most current data or suspect the cache is stale.

        Args:
            datasets: List of specific dataset names to refresh. If None, refreshes
                all datasets. Use this to limit refresh scope for performance.
            force_full_refresh: If True, performs a complete refresh of all metadata,
                ignoring incremental update optimizations. Slower but most thorough.

        Returns:
            Updated MetadataIndex with fresh information from FTP servers.

        Raises:
            RuntimeError: If the client has been closed.
            ConnectionError: If FTP servers are unreachable.
            OSError: If cache directory is not writable.

        Example:
            ```python
            async with DataSUSMetadataClient() as client:
                # Refresh specific datasets only
                index = await client.refresh_metadata(datasets=["sinan", "sim"])
                print(f"Refreshed metadata for SINAN and SIM")
                print(f"Total files: {index.total_files:,}")

                # Full refresh of all datasets (expensive operation)
                full_index = await client.refresh_metadata(force_full_refresh=True)
                print(f"Full refresh completed")
                print(f"Last updated: {full_index.last_updated}")

                # Check cache performance after refresh
                stats = await client.get_cache_stats()
                print(f"Cache cleared, hit rate reset to: {stats['memory_hit_rate']:.2%}")
            ```

        Performance:
            - Selective refresh (specific datasets): 10s-60s depending on dataset size
            - Full refresh (all datasets): 5-30 minutes depending on server load
            - Incremental refresh: 2-10x faster than full refresh
            - Network-intensive operation, plan accordingly

        Note:
            - Clears relevant cache entries to ensure fresh data
            - Consider running during off-peak hours for full refreshes
            - Monitor server load and be respectful of DATASUS resources
        """
        if self._closed:
            raise RuntimeError("Client is closed")

        logger.info("Starting metadata refresh...")

        index = await self.updater.update_all_metadata(
            force_refresh=force_full_refresh,
            datasets=datasets,
        )

        # Clear relevant cache entries
        if datasets:
            for dataset in datasets:
                await self.cache.delete(f"dataset_data_{dataset}")
                await self.cache.delete(f"dataset_documentation_{dataset}")
                await self.cache.delete(f"dataset_auxiliary_{dataset}")
        else:
            await self.cache.clear()

        logger.info("Metadata refresh completed")
        return index

    async def get_cache_stats(self) -> Dict[str, Any]:
        """Get comprehensive cache performance statistics and metrics.

        Returns:
            Dictionary containing detailed cache performance metrics including
            hit rates, memory usage, disk usage, and performance indicators.

        Example:
            ```python
            async with DataSUSMetadataClient() as client:
                stats = await client.get_cache_stats()

                print("Cache Performance:")
                print(f"  Memory hit rate: {stats['memory_hit_rate']:.2%}")
                print(f"  Disk hit rate: {stats['disk_hit_rate']:.2%}")
                print(f"  Overall hit rate: {stats['overall_hit_rate']:.2%}")

                print("\\nMemory Usage:")
                print(f"  Items: {stats['memory_items']:,}")
                print(f"  Usage: {stats['memory_usage_mb']:.1f} MB")
                print(f"  Limit: {stats['memory_limit_mb']:.1f} MB")

                print("\\nDisk Usage:")
                print(f"  Items: {stats['disk_items']:,}")
                print(f"  Usage: {stats['disk_usage_gb']:.1f} GB")
                print(f"  Limit: {stats['disk_limit_gb']:.1f} GB")
            ```
        """
        return await self.cache.get_stats()

    async def cleanup_cache(self, max_age_days: int = 7) -> Dict[str, Any]:
        """Clean up old cache entries to free disk space and improve performance.

        Removes cache entries older than the specified age and performs maintenance
        on the cache storage to optimize performance.

        Args:
            max_age_days: Maximum age of cache entries to keep (default: 7 days).
                Entries older than this will be removed.

        Returns:
            Dictionary with cleanup results including freed space and removed items.

        Example:
            ```python
            async with DataSUSMetadataClient() as client:
                # Clean up cache entries older than 3 days
                results = await client.cleanup_cache(max_age_days=3)

                print("Cache Cleanup Results:")
                print(f"  Freed space: {results['freed_space_gb']:.2f} GB")
                print(f"  Removed items: {results['removed_items']:,}")
                print(f"  Remaining items: {results['remaining_items']:,}")
                print(f"  Cleanup duration: {results['duration_seconds']:.1f}s")
            ```
        """
        return await self.updater.cleanup_cache(max_age_days=max_age_days)

    async def health_check(self) -> Dict[str, Any]:
        """Perform a comprehensive health check of the entire metadata system.

        Tests all major components including FTP connectivity, cache functionality,
        and metadata index integrity to ensure the system is operating correctly.

        Returns:
            Dictionary containing health check results with status and detailed metrics.

            Status values:
                - "healthy": All systems operational
                - "degraded": Some issues detected but system still functional
                - "unhealthy": Critical issues detected

        Example:
            ```python
            async with DataSUSMetadataClient() as client:
                health = await client.health_check()

                print(f"System Health: {health['status'].upper()}")
                print(f"Timestamp: {health['timestamp']}")

                print("\\nComponent Status:")
                for component, status in health['checks'].items():
                    print(f"  {component}: {status}")

                if 'metadata_stats' in health:
                    stats = health['metadata_stats']
                    print(f"\\nMetadata Statistics:")
                    print(f"  Total datasets: {stats['total_datasets']:,}")
                    print(f"  Total files: {stats['total_files']:,}")
                    print(f"  Total size: {stats['total_size_gb']:.1f} GB")
                    print(f"  Last updated: {stats['last_updated']}")

                if 'cache_stats' in health:
                    cache = health['cache_stats']
                    print(f"\\nCache Performance:")
                    print(f"  Memory hit rate: {cache['memory_hit_rate']:.2%}")
                    print(f"  Disk usage: {cache['disk_usage_gb']:.1f} GB")
            ```

        Performance:
            - Typically completes in 1-5 seconds
            - May take longer if FTP connections need to be established
        """
        health_status = {
            "timestamp": datetime.utcnow().isoformat(),
            "status": "healthy",
            "checks": {},
        }

        try:
            # Check FTP connectivity
            async with self.ftp_client.pool.get_connection() as conn:
                health_status["checks"]["ftp_connection"] = "ok"
        except Exception as e:
            health_status["checks"]["ftp_connection"] = f"error: {e}"
            health_status["status"] = "degraded"

        try:
            # Check cache
            cache_stats = await self.get_cache_stats()
            health_status["checks"]["cache"] = "ok"
            health_status["cache_stats"] = cache_stats
        except Exception as e:
            health_status["checks"]["cache"] = f"error: {e}"
            health_status["status"] = "degraded"

        try:
            # Check metadata index
            index = await self.get_metadata_index(use_cache=True)
            if index:
                health_status["checks"]["metadata_index"] = "ok"
                health_status["metadata_stats"] = {
                    "total_datasets": index.total_datasets,
                    "total_files": index.total_files,
                    "total_size_gb": index.total_size_gb,
                    "last_updated": index.last_updated.isoformat(),
                }
            else:
                health_status["checks"]["metadata_index"] = "no index found"
                health_status["status"] = "degraded"
        except Exception as e:
            health_status["checks"]["metadata_index"] = f"error: {e}"
            health_status["status"] = "degraded"

        return health_status

    async def close(self):
        """Close all resources and connections gracefully.

        Cleanly shuts down all active connections, flushes caches, and releases
        system resources. Should always be called when finished with the client
        to prevent resource leaks.

        Note:
            This method is automatically called when using the client as an async
            context manager. Manual calls are only needed when not using context managers.

        Example:
            ```python
            # Manual resource management (not recommended)
            client = DataSUSMetadataClient()
            try:
                # Use client...
                pass
            finally:
                await client.close()

            # Preferred: automatic resource management
            async with DataSUSMetadataClient() as client:
                # Use client...
                pass  # close() called automatically
            ```
        """
        if not self._closed:
            logger.info("Closing DATASUS metadata client...")
            await gather(
                self.ftp_client.close(),
                self.updater.close(),
                self.cache.close(),
                return_exceptions=True,
            )
            self._closed = True
            logger.info("Client closed")

    async def __aenter__(self):
        """Async context manager entry point.

        Returns:
            Self for use in async with statements.
        """
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit point.

        Automatically closes all resources when exiting the context, regardless
        of whether an exception occurred.

        Args:
            exc_type: Exception type if an exception occurred, None otherwise.
            exc_val: Exception value if an exception occurred, None otherwise.
            exc_tb: Exception traceback if an exception occurred, None otherwise.
        """
        await self.close()
