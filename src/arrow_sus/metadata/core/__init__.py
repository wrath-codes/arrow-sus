"""Core metadata functionality."""

from .client import DataSUSMetadataClient
from .config import DataSUSConfig, S3Config, CacheConfig, PerformanceConfig
from .models import (
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
)
from .updater import MetadataUpdater

__all__ = [
    "DataSUSMetadataClient",
    "DataSUSConfig",
    "S3Config",
    "CacheConfig",
    "PerformanceConfig",
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
]
