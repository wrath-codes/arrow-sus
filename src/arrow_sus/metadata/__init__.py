"""High-performance DATASUS metadata management module."""

from .core import (
    DataSUSMetadataClient,
    DataPartition,
    DatasetMetadata,
    DatasetSource,
    FileExtension,
    FileMetadata,
    RemoteFile,
    SubsystemInfo,
    MetadataIndex,
    CacheEntry,
    UFCode,
    MetadataUpdater,
    DataSUSConfig,
    S3Config,
    CacheConfig,
    PerformanceConfig,
)
from .data import states, datasets
from .io import AsyncFTPClient
from .utils import CacheStats, LRUCache, DiskCache, TieredCache

__all__ = [
    "DataSUSMetadataClient",
    "DataPartition",
    "DatasetMetadata",
    "DatasetSource",
    "FileExtension",
    "FileMetadata",
    "RemoteFile",
    "SubsystemInfo",
    "MetadataIndex",
    "CacheEntry",
    "UFCode",
    "MetadataUpdater",
    "DataSUSConfig",
    "S3Config",
    "CacheConfig",
    "PerformanceConfig",
    "states",
    "datasets",
    "AsyncFTPClient",
    "CacheStats",
    "LRUCache",
    "DiskCache",
    "TieredCache",
]
