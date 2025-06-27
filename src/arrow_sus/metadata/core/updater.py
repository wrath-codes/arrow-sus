"""High-performance metadata updater with async operations and intelligent caching."""

from asyncio import gather, create_task, sleep, Lock, Semaphore
from logging import getLogger
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

import aiofiles
import orjson
from returns.result import Result, Success, Failure
from returns.maybe import Nothing, Some

from ..io.async_ftp import AsyncFTPClient
from ..utils.cache import DiskCache, LRUCache, TieredCache
from .config import DataSUSConfig
from .errors import DatasusError
from .models import (
    DataPartition,
    DatasetMetadata,
    DatasetSource,
    FileMetadata,
    MetadataIndex,
    RemoteFile,
    UFCode,
)

logger = getLogger(__name__)


class MetadataUpdater:
    """High-performance async metadata updater for DATASUS."""

    def __init__(
        self,
        config: Optional[DataSUSConfig] = None,
        cache_dir: Optional[Path] = None,
        max_concurrent_operations: int = 20,
    ):
        self.config = config or DataSUSConfig()
        self.cache_dir = cache_dir or Path.home() / ".arrow_sus_cache"
        self.metadata_dir = self.cache_dir / "metadata"
        self.metadata_dir.mkdir(parents=True, exist_ok=True)

        # Initialize caches
        memory_cache = LRUCache(
            max_size=1000,
            max_memory_mb=500,
            default_ttl_hours=24,
        )
        disk_cache = DiskCache(
            cache_dir=self.cache_dir / "disk_cache",
            max_size_gb=10,
            default_ttl_hours=168,  # 1 week
        )
        self.cache = TieredCache(memory_cache, disk_cache)

        # Initialize FTP client
        self.ftp_client = AsyncFTPClient(
            host=self.config.ftp_host,
            max_connections=10,
            max_concurrent_operations=max_concurrent_operations,
            cache_dir=self.cache_dir / "ftp_cache",
        )

        self._update_lock = Lock()

    async def update_all_metadata(
        self,
        force_refresh: bool = False,
        datasets: Optional[List[str]] = None,
    ) -> MetadataIndex:
        """Update metadata for all or specified datasets."""
        async with self._update_lock:
            logger.info("Starting metadata update...")
            start_time = datetime.utcnow()

            # Get datasets to update
            target_datasets = datasets or list(self.config.datasets.keys())

            # Update data files
            data_metadata = await self._update_data_files(
                target_datasets, force_refresh
            )

            # Update documentation files
            doc_metadata = await self._update_documentation_files(
                target_datasets, force_refresh
            )

            # Update auxiliary files
            aux_metadata = await self._update_auxiliary_files(
                target_datasets, force_refresh
            )

            # Create index
            index = MetadataIndex(
                data=data_metadata,
                documentation=doc_metadata,
                auxiliary=aux_metadata,
                last_updated=datetime.utcnow(),
            )

            # Save index
            await self._save_index(index)

            duration = datetime.utcnow() - start_time
            logger.info(
                f"Metadata update completed in {duration.total_seconds():.2f}s. "
                f"Total datasets: {index.total_datasets}, "
                f"Total files: {index.total_files}, "
                f"Total size: {index.total_size_gb:.2f}GB"
            )

            return index

    async def _update_data_files(
        self,
        datasets: List[str],
        force_refresh: bool,
    ) -> Dict[str, FileMetadata]:
        """Update data file metadata for specified datasets."""
        logger.info(f"Updating data files for {len(datasets)} datasets...")

        # Process datasets concurrently
        semaphore = Semaphore(5)  # Limit concurrent dataset processing

        async def update_dataset(dataset_name: str) -> tuple[str, FileMetadata]:
            async with semaphore:
                return await self._update_single_dataset(
                    dataset_name, "data", force_refresh
                )

        tasks = [update_dataset(dataset) for dataset in datasets]
        results = await gather(*tasks, return_exceptions=True)

        metadata = {}
        for result in results:
            if isinstance(result, Exception):
                logger.error(f"Dataset update failed: {result}")
            else:
                dataset_name, file_metadata = result
                metadata[dataset_name] = file_metadata

        return metadata

    async def _update_documentation_files(
        self,
        datasets: List[str],
        force_refresh: bool,
    ) -> Dict[str, FileMetadata]:
        """Update documentation file metadata."""
        logger.info("Updating documentation files...")

        # Similar to data files but for documentation
        docs_config = getattr(self.config, "documentation", {})
        target_datasets = [d for d in datasets if d in docs_config]

        if not target_datasets:
            return {}

        semaphore = Semaphore(5)

        async def update_docs(dataset_name: str) -> tuple[str, FileMetadata]:
            async with semaphore:
                return await self._update_single_dataset(
                    dataset_name, "documentation", force_refresh
                )

        tasks = [update_docs(dataset) for dataset in target_datasets]
        results = await gather(*tasks, return_exceptions=True)

        metadata = {}
        for result in results:
            if isinstance(result, Exception):
                logger.error(f"Documentation update failed: {result}")
            else:
                dataset_name, file_metadata = result
                metadata[dataset_name] = file_metadata

        return metadata

    async def _update_auxiliary_files(
        self,
        datasets: List[str],
        force_refresh: bool,
    ) -> Dict[str, FileMetadata]:
        """Update auxiliary file metadata."""
        logger.info("Updating auxiliary files...")

        aux_config = getattr(self.config, "auxiliary_tables", {})
        target_datasets = [d for d in datasets if d in aux_config]

        if not target_datasets:
            return {}

        semaphore = Semaphore(5)

        async def update_aux(dataset_name: str) -> tuple[str, FileMetadata]:
            async with semaphore:
                return await self._update_single_dataset(
                    dataset_name, "auxiliary", force_refresh
                )

        tasks = [update_aux(dataset) for dataset in target_datasets]
        results = await gather(*tasks, return_exceptions=True)

        metadata = {}
        for result in results:
            if isinstance(result, Exception):
                logger.error(f"Auxiliary update failed: {result}")
            else:
                dataset_name, file_metadata = result
                metadata[dataset_name] = file_metadata

        return metadata

    async def _update_single_dataset(
        self,
        dataset_name: str,
        category: str,
        force_refresh: bool,
    ) -> tuple[str, FileMetadata]:
        """Update metadata for a single dataset."""
        cache_key = f"dataset_{category}_{dataset_name}"

        # Check cache first
        if not force_refresh:
            cached_metadata = await self.cache.get(cache_key)
            if cached_metadata:
                logger.debug(f"Using cached metadata for {dataset_name}")
                return dataset_name, FileMetadata.model_validate(cached_metadata)

        logger.info(f"Fetching fresh metadata for {dataset_name} ({category})")

        # Get dataset configuration
        if category == "data":
            dataset_config = self.config.datasets.get(dataset_name)
        elif category == "documentation":
            dataset_config = getattr(self.config, "documentation", {}).get(dataset_name)
        else:  # auxiliary
            dataset_config = getattr(self.config, "auxiliary_tables", {}).get(
                dataset_name
            )

        if not dataset_config:
            raise ValueError(f"No configuration found for {dataset_name}")

        # Fetch files
        files = await self._fetch_dataset_files(dataset_name, dataset_config)

        # Generate metadata
        file_metadata = self._generate_file_metadata(dataset_name, files)

        # Cache results
        await self.cache.set(
            cache_key,
            file_metadata.model_dump(),
            ttl=timedelta(hours=6),  # Cache for 6 hours
        )

        # Save detailed file list
        dataset_metadata = DatasetMetadata(
            name=dataset_config.get("name", dataset_name),
            source=DatasetSource(dataset_config.get("source", dataset_name)),
            metadata=file_metadata,
            files=files,
        )

        output_file = self.metadata_dir / category / f"{dataset_name}.json"
        output_file.parent.mkdir(parents=True, exist_ok=True)
        await dataset_metadata.save_to_file(output_file)

        return dataset_name, file_metadata

    async def _fetch_dataset_files(
        self,
        dataset_name: str,
        dataset_config: Dict[str, Any],
    ) -> List[RemoteFile]:
        """Fetch file list for a dataset."""
        all_files = []

        # Get directories from config
        periods = dataset_config.get("periods", [])
        if not periods:
            # Single directory config
            directories = [dataset_config.get("dir", "")]
        else:
            # Multiple period config
            directories = [period["dir"] for period in periods]

        # Filter empty directories
        directories = [d for d in directories if d]

        if not directories:
            logger.warning(f"No directories configured for {dataset_name}")
            return []

        # Fetch files from all directories
        files = await self.ftp_client.list_dataset_files(
            dataset=dataset_name,
            directories=directories,
            filename_prefix=dataset_config.get("filename_prefix"),
            use_cache=True,
        )

        # Apply additional filtering
        if "filename_pattern" in dataset_config:
            pattern = dataset_config["filename_pattern"]
            files = [f for f in files if self._matches_pattern(f.filename, pattern)]

        return files

    def _matches_pattern(self, filename: str, pattern: str) -> bool:
        """Check if filename matches the configured pattern."""
        # This is a simplified pattern matching
        # In a real implementation, you'd use proper regex matching
        import re

        try:
            return bool(re.search(pattern, filename.lower()))
        except re.error:
            return True  # If pattern is invalid, include all files

    def _generate_file_metadata(
        self,
        dataset_name: str,
        files: List[RemoteFile],
    ) -> FileMetadata:
        """Generate aggregated metadata from file list."""
        if not files:
            return FileMetadata(
                dataset=dataset_name,
                total_files=0,
                total_size_bytes=0,
                last_updated=datetime.utcnow(),
            )

        # Collect statistics
        total_size = sum(f.size or 0 for f in files)
        supported_ufs = {
            f.partition.uf for f in files if f.partition and f.partition.uf
        }
        available_periods = {f.partition.period_key for f in files if f.partition}
        file_extensions = {f.extension for f in files if f.extension}

        # Find period range
        period_list = sorted(available_periods) if available_periods else []
        first_period = period_list[0] if period_list else None
        last_period = period_list[-1] if period_list else None

        return FileMetadata(
            dataset=dataset_name,
            total_files=len(files),
            total_size_bytes=total_size,
            supported_ufs=supported_ufs,
            available_periods=available_periods,
            first_period=first_period,
            last_period=last_period,
            last_updated=datetime.utcnow(),
            file_extensions=file_extensions,
        )

    async def _save_index(self, index: MetadataIndex) -> None:
        """Save metadata index to file."""
        index_file = self.metadata_dir / "index.json"

        try:
            async with aiofiles.open(index_file, "wb") as f:
                await f.write(index.to_json_bytes())
            logger.info(f"Metadata index saved to {index_file}")
        except Exception as e:
            logger.error(f"Failed to save metadata index: {e}")
            raise

    async def get_dataset_metadata(
        self,
        dataset_name: str,
        category: str = "data",
        use_cache: bool = True,
    ) -> Optional[DatasetMetadata]:
        """Get metadata for a specific dataset."""
        if use_cache:
            cache_key = f"full_dataset_{category}_{dataset_name}"
            cached = await self.cache.get(cache_key)
            if cached:
                return DatasetMetadata.model_validate(cached)

        # Load from file
        metadata_file = self.metadata_dir / category / f"{dataset_name}.json"
        if metadata_file.exists():
            try:
                metadata = await DatasetMetadata.load_from_file(metadata_file)

                # Cache for future use
                if use_cache:
                    await self.cache.set(
                        cache_key,
                        metadata.model_dump(),
                        ttl=timedelta(hours=12),
                    )

                return metadata
            except Exception as e:
                logger.error(f"Failed to load metadata for {dataset_name}: {e}")

        return None

    async def get_index(self, use_cache: bool = True) -> Optional[MetadataIndex]:
        """Get the metadata index."""
        if use_cache:
            cached = await self.cache.get("metadata_index")
            if cached:
                return MetadataIndex.model_validate(cached)

        # Load from file
        index_file = self.metadata_dir / "index.json"
        if index_file.exists():
            try:
                import aiofiles

                async with aiofiles.open(index_file, "rb") as f:
                    data = await f.read()
                    index = MetadataIndex.from_json_bytes(data)

                # Cache for future use
                if use_cache:
                    await self.cache.set(
                        "metadata_index",
                        index.model_dump(),
                        ttl=timedelta(hours=6),
                    )

                return index
            except Exception as e:
                logger.error(f"Failed to load metadata index: {e}")

        return None

    async def cleanup_cache(self, max_age_days: int = 7) -> Dict[str, Any]:
        """Clean up old cache entries."""
        logger.info(f"Cleaning up cache entries older than {max_age_days} days")

        # Clean up disk cache
        await self.cache.disk_cache.cleanup_expired()

        # Get statistics
        stats = await self.cache.get_stats()

        logger.info("Cache cleanup completed")
        return stats

    async def get_cache_stats(self) -> Dict[str, Any]:
        """Get cache performance statistics."""
        return await self.cache.get_stats()

    async def extract_metadata(self) -> Result[dict, DatasusError]:
        """Extract metadata from mappings.py and build cache structure."""
        try:
            from arrow_sus.metadata.data.mappings import datasets

            # Convert datasets to our source system structure
            source_systems = {}

            for dataset_id, dataset_info in datasets.items():
                source = dataset_info["source"]

                if source not in source_systems:
                    source_systems[source] = {
                        "name": source.upper(),
                        "type": self._determine_system_type(source),
                        "groups": {},
                    }

                # Extract group from dataset_id (e.g., "sia-pa" -> "pa")
                if "-" in dataset_id:
                    _, group = dataset_id.split("-", 1)
                else:
                    group = dataset_id

                source_systems[source]["groups"][group] = {
                    "name": dataset_info["name"],
                    "periods": dataset_info["periods"],
                    "partition": dataset_info.get("partition", []),
                }

            # Save to cache
            cache_result = await self.cache.save_source_systems(source_systems)
            if isinstance(cache_result, Failure):
                return cache_result

            return Success(source_systems)

        except Exception as e:
            return Failure(DatasusError(f"Failed to extract metadata: {e}"))

    def _determine_system_type(self, source: str) -> str:
        """Determine system type based on source name."""
        monthly_systems = {"sia", "sih", "cnes", "cih", "ciha", "sisprenatal"}
        yearly_systems = {"sinasc", "sim", "sinan"}

        if source in monthly_systems:
            return "monthly"
        elif source in yearly_systems:
            return "yearly"
        else:
            return "other"

    async def close(self):
        """Close all resources."""
        await gather(
            self.ftp_client.close(),
            self.cache.close(),
            return_exceptions=True,
        )

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()
