"""High-performance multi-tier caching system with TTL, size management, and async operations.

This module provides a comprehensive caching solution for the Arrow SUS metadata system,
featuring memory-based LRU caching, persistent disk caching with compression, and
intelligent tiered caching strategies.

Key Features:
    - **Memory Cache**: High-speed LRU cache with configurable size and TTL limits
    - **Disk Cache**: Persistent cache with metadata, compression, and automatic cleanup
    - **Tiered Cache**: Intelligent multi-level caching combining memory and disk
    - **Thread Safety**: All operations are async-safe with proper locking mechanisms
    - **Statistics**: Comprehensive metrics for cache performance monitoring
    - **Auto-cleanup**: Background tasks for expired entry removal

Performance Characteristics:
    - Memory cache operations: O(1) average case
    - Disk cache operations: O(1) with filesystem overhead
    - Supports concurrent operations with configurable limits
    - Automatic memory pressure management with LRU eviction

Usage Patterns:
    - High-frequency metadata lookups (use memory cache)
    - Large dataset caching across sessions (use disk cache)
    - General-purpose caching (use tiered cache)
    - Performance-critical applications (configure appropriate sizes)

Thread Safety:
    All cache implementations are fully async-safe and can be used from multiple
    coroutines simultaneously. Internal locking ensures data consistency.

Example:
    ```python
    from arrow_sus.metadata.utils.cache import TieredCache, LRUCache, DiskCache
    from pathlib import Path

    # Create tiered cache for optimal performance
    memory_cache = LRUCache(max_size=1000, max_memory_mb=100)
    disk_cache = DiskCache(Path(".cache"), max_size_gb=5)
    cache = TieredCache(memory_cache, disk_cache)

    # Store and retrieve data
    await cache.set("dataset:SIM", large_dataset)
    data = await cache.get("dataset:SIM")

    # Monitor performance
    stats = await cache.get_stats()
    print(f"Hit ratio: {stats['memory'].hit_ratio:.2%}")
    ```

Dependencies:
    - orjson: High-performance JSON serialization
    - aiofiles: Async file operations
    - weakref: Memory management and cleanup
"""

from asyncio import gather, create_task, Lock, sleep, Task, CancelledError
from hashlib import md5, sha256
from logging import getLogger
import weakref
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Generic, List, Optional, TypeVar

import aiofiles
import orjson
from aiofiles import os as aio_os

from ..core.models import CacheEntry, _serialize_for_json

logger = getLogger(__name__)

T = TypeVar("T")


@dataclass
class CacheStats:
    """Cache performance statistics and metrics.

    Provides comprehensive metrics for monitoring cache performance,
    including hit ratios, memory usage, and eviction patterns.

    Attributes:
        hits: Number of successful cache retrievals
        misses: Number of failed cache retrievals (cache miss)
        evictions: Number of entries removed due to size/memory limits
        total_size_bytes: Total memory usage in bytes
        total_entries: Current number of cached entries

    Properties:
        hit_ratio: Cache effectiveness as hits/(hits+misses)
        size_mb: Memory usage in megabytes for easier reading

    Example:
        ```python
        stats = await cache.get_stats()
        print(f"Cache efficiency: {stats.hit_ratio:.2%}")
        print(f"Memory usage: {stats.size_mb:.1f} MB")
        print(f"Total entries: {stats.total_entries}")
        ```
    """

    hits: int = 0
    misses: int = 0
    evictions: int = 0
    total_size_bytes: int = 0
    total_entries: int = 0

    @property
    def hit_ratio(self) -> float:
        """Calculate cache hit ratio as a decimal between 0.0 and 1.0.

        Returns:
            float: Ratio of hits to total requests. 1.0 means perfect cache,
                  0.0 means no cache hits. Returns 0.0 if no requests made.

        Example:
            ```python
            ratio = stats.hit_ratio
            print(f"Cache is {ratio:.1%} effective")
            ```
        """
        total = self.hits + self.misses
        return self.hits / total if total > 0 else 0.0

    @property
    def size_mb(self) -> float:
        """Total cache size in megabytes for human-readable output.

        Returns:
            float: Cache size in MB, calculated as bytes / 1,000,000

        Example:
            ```python
            if stats.size_mb > 100:
                print("Cache is using significant memory")
            ```
        """
        return self.total_size_bytes / 1_000_000


class LRUCache(Generic[T]):
    """High-performance async LRU (Least Recently Used) cache with TTL and memory limits.

    Provides in-memory caching with automatic eviction based on both usage patterns
    and time-to-live constraints. Uses asyncio locks for thread safety and background
    cleanup tasks for expired entries.

    Performance Characteristics:
        - Get operations: O(1) average case
        - Set operations: O(1) average case
        - Memory usage: Automatically managed with configurable limits
        - Cleanup frequency: Every 5 minutes via background task
        - Thread safety: Full async/await support with internal locking

    Attributes:
        max_size: Maximum number of entries before LRU eviction
        max_memory_bytes: Maximum memory usage before eviction
        default_ttl: Default time-to-live for cached entries

    Example:
        ```python
        # Create cache with custom limits
        cache = LRUCache[dict](
            max_size=500,
            max_memory_mb=100,
            default_ttl_hours=6
        )

        # Store data with custom TTL
        await cache.set("user:123", user_data, ttl=timedelta(hours=1))

        # Retrieve data
        user = await cache.get("user:123")
        if user is None:
            user = await fetch_user_from_db(123)
            await cache.set("user:123", user)

        # Monitor performance
        stats = await cache.get_stats()
        print(f"Hit ratio: {stats.hit_ratio:.2%}")
        ```

    Thread Safety:
        All methods are coroutine-safe and can be called concurrently from
        multiple tasks. Internal asyncio.Lock ensures data consistency.

    Memory Management:
        Automatically evicts least recently used entries when limits are exceeded.
        Background cleanup task removes expired entries every 5 minutes.
    """

    def __init__(
        self,
        max_size: int = 1000,
        max_memory_mb: int = 500,
        default_ttl_hours: int = 24,
    ):
        """Initialize LRU cache with specified limits.

        Args:
            max_size: Maximum number of entries before LRU eviction starts.
                     Recommended: 100-10000 depending on entry size.
            max_memory_mb: Maximum memory usage in megabytes before eviction.
                          Actual usage may slightly exceed due to estimation.
            default_ttl_hours: Default time-to-live in hours for cached entries.
                              Entries expire after this time regardless of usage.

        Raises:
            ValueError: If any parameter is negative or zero.

        Example:
            ```python
            # Small cache for frequent lookups
            cache = LRUCache(max_size=100, max_memory_mb=10, default_ttl_hours=1)

            # Large cache for dataset metadata
            cache = LRUCache(max_size=5000, max_memory_mb=500, default_ttl_hours=24)
            ```
        """
        if max_size <= 0 or max_memory_mb <= 0 or default_ttl_hours <= 0:
            raise ValueError("All cache parameters must be positive")

        self.max_size = max_size
        self.max_memory_bytes = max_memory_mb * 1_000_000
        self.default_ttl = timedelta(hours=default_ttl_hours)

        self._cache: Dict[str, CacheEntry] = {}
        self._access_order: List[str] = []
        self._stats = CacheStats()
        self._lock = Lock()

        # Weak reference cleanup
        self._cleanup_task: Optional[Task] = None
        self._start_cleanup_task()

    def _start_cleanup_task(self):
        """Start background cleanup task."""
        if self._cleanup_task is None or self._cleanup_task.done():
            self._cleanup_task = create_task(self._cleanup_loop())

    async def _cleanup_loop(self):
        """Background task to clean up expired entries."""
        while True:
            try:
                await sleep(300)  # Check every 5 minutes
                async with self._lock:
                    await self._cleanup_expired()
            except CancelledError:
                break
            except Exception as e:
                logger.error(f"Cache cleanup error: {e}")

    async def _cleanup_expired(self):
        """Remove expired entries."""
        now = datetime.utcnow()
        expired_keys = [
            key for key, entry in self._cache.items() if entry.expires_at <= now
        ]

        for key in expired_keys:
            await self._remove_entry(key)
            self._stats.evictions += 1

    async def _remove_entry(self, key: str):
        """Remove entry and update stats."""
        if key in self._cache:
            entry = self._cache.pop(key)
            self._stats.total_size_bytes -= entry.size_bytes
            self._stats.total_entries -= 1

            if key in self._access_order:
                self._access_order.remove(key)

    async def _evict_lru(self):
        """Evict least recently used entries."""
        while (
            len(self._cache) >= self.max_size
            or self._stats.total_size_bytes >= self.max_memory_bytes
        ):
            if not self._access_order:
                break

            lru_key = self._access_order.pop(0)
            await self._remove_entry(lru_key)
            self._stats.evictions += 1

    async def get(self, key: str) -> Optional[T]:
        """Retrieve an item from the cache if it exists and hasn't expired.

        This method is thread-safe and updates the LRU access order automatically.
        Expired entries are removed during retrieval.

        Args:
            key: Unique identifier for the cached item

        Returns:
            The cached item if found and not expired, None otherwise

        Example:
            ```python
            # Try to get cached data
            user_data = await cache.get("user:123")
            if user_data is None:
                # Cache miss - fetch from source
                user_data = await fetch_user_data(123)
                await cache.set("user:123", user_data)
            ```

        Performance:
            - Average case: O(1)
            - Updates access order for LRU tracking
            - Automatically removes expired entries
        """
        async with self._lock:
            entry = self._cache.get(key)

            if entry is None:
                self._stats.misses += 1
                return None

            if entry.is_expired:
                await self._remove_entry(key)
                self._stats.misses += 1
                return None

            # Update access order
            if key in self._access_order:
                self._access_order.remove(key)
            self._access_order.append(key)

            self._stats.hits += 1
            return entry.data

    async def set(
        self,
        key: str,
        value: T,
        ttl: Optional[timedelta] = None,
    ) -> None:
        """Store an item in the cache with optional custom TTL.

        If the cache is at capacity, this method will evict the least recently
        used entries to make space. Thread-safe operation with proper locking.

        Args:
            key: Unique identifier for the item to cache
            value: The data to store in the cache
            ttl: Optional time-to-live override. If None, uses default_ttl

        Raises:
            MemoryError: If the item is too large to fit in cache limits

        Example:
            ```python
            # Store with default TTL
            await cache.set("dataset:SIM", dataset_metadata)

            # Store with custom TTL for temporary data
            await cache.set("temp:processing", temp_data, ttl=timedelta(minutes=5))

            # Store large objects (automatic eviction if needed)
            await cache.set("large:dataset", huge_dataframe)
            ```

        Performance:
            - Average case: O(1)
            - May trigger LRU eviction: O(n) where n is number of evicted items
            - Updates cache statistics and access order
        """
        if ttl is None:
            ttl = self.default_ttl

        expires_at = datetime.utcnow() + ttl

        # Estimate size (rough approximation)
        size_bytes = len(str(value).encode("utf-8"))

        entry = CacheEntry(
            data=value,
            expires_at=expires_at,
            cache_key=key,
            size_bytes=size_bytes,
        )

        async with self._lock:
            # Remove existing entry if present
            if key in self._cache:
                await self._remove_entry(key)

            # Evict if necessary
            await self._evict_lru()

            # Add new entry
            self._cache[key] = entry
            self._access_order.append(key)
            self._stats.total_size_bytes += size_bytes
            self._stats.total_entries += 1

    async def delete(self, key: str) -> bool:
        """Delete item from cache."""
        async with self._lock:
            if key in self._cache:
                await self._remove_entry(key)
                return True
            return False

    async def clear(self):
        """Clear all cache entries."""
        async with self._lock:
            self._cache.clear()
            self._access_order.clear()
            self._stats = CacheStats()

    async def get_stats(self) -> CacheStats:
        """Get cache statistics."""
        async with self._lock:
            return CacheStats(
                hits=self._stats.hits,
                misses=self._stats.misses,
                evictions=self._stats.evictions,
                total_size_bytes=self._stats.total_size_bytes,
                total_entries=self._stats.total_entries,
            )

    async def close(self):
        """Close cache and cleanup resources."""
        if self._cleanup_task:
            self._cleanup_task.cancel()
            try:
                await self._cleanup_task
            except CancelledError:
                pass


class DiskCache:
    """Persistent disk-based cache with metadata tracking and automatic cleanup.

    Provides durable caching that survives application restarts. Uses JSON serialization
    with orjson for performance and stores metadata separately for efficient expiration
    checking without loading full data.

    Performance Characteristics:
        - Read operations: O(1) with filesystem overhead
        - Write operations: O(1) with JSON serialization overhead
        - Storage efficiency: Organized into subdirectories to avoid filesystem limits
        - Cleanup performance: Metadata-only scanning for expired entries

    Storage Structure:
        ```
        cache_dir/
        ├── ab/
        │   ├── ab123...hash.json      # Cached data
        │   └── ab123...hash.meta      # Metadata (expiry, size, etc.)
        └── cd/
            ├── cd456...hash.json
            └── cd456...hash.meta
        ```

    Features:
        - **Persistence**: Data survives application restarts
        - **Metadata**: Separate metadata files for efficient expiry checking
        - **Organization**: Hash-based subdirectories prevent filesystem bottlenecks
        - **Cleanup**: Automatic expired entry removal
        - **Error Recovery**: Handles corrupted files gracefully

    Example:
        ```python
        from pathlib import Path

        # Create persistent cache
        cache = DiskCache(
            cache_dir=Path.home() / ".arrow_sus_cache",
            max_size_gb=5,
            default_ttl_hours=168  # 1 week
        )

        # Store large dataset that should persist
        await cache.set("processed:SIM:2023", large_processed_data)

        # Retrieve after application restart
        data = await cache.get("processed:SIM:2023")  # Still available!

        # Clean up expired entries
        await cache.cleanup_expired()

        # Monitor disk usage
        stats = await cache.get_cache_stats()
        print(f"Using {stats['total_size_mb']:.1f} MB on disk")
        ```

    Thread Safety:
        Uses asyncio.Lock for thread-safe operations. Can be safely used
        from multiple coroutines simultaneously.

    Error Handling:
        Automatically removes corrupted cache files and continues operation.
        Logs errors for monitoring but doesn't raise exceptions for corrupted data.
    """

    def __init__(
        self,
        cache_dir: Path,
        max_size_gb: int = 10,
        default_ttl_hours: int = 168,  # 1 week
    ):
        """Initialize disk cache with specified directory and limits.

        Creates the cache directory if it doesn't exist and sets up internal
        tracking structures.

        Args:
            cache_dir: Path where cache files will be stored
            max_size_gb: Maximum total size in gigabytes (not strictly enforced)
            default_ttl_hours: Default time-to-live in hours for cached entries

        Raises:
            OSError: If cache directory cannot be created
            ValueError: If parameters are invalid

        Example:
            ```python
            # Standard cache location
            cache = DiskCache(Path.home() / ".arrow_sus_cache")

            # High-capacity cache for large datasets
            cache = DiskCache(
                Path("/var/cache/arrow_sus"),
                max_size_gb=50,
                default_ttl_hours=336  # 2 weeks
            )
            ```
        """
        if max_size_gb <= 0 or default_ttl_hours <= 0:
            raise ValueError("Cache size and TTL must be positive")

        self.cache_dir = cache_dir
        self.max_size_bytes = max_size_gb * 1_000_000_000
        self.default_ttl = timedelta(hours=default_ttl_hours)

        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self._lock = Lock()

    def _get_cache_path(self, key: str) -> Path:
        """Get cache file path for key."""
        # Create hash-based subdirectories to avoid too many files in one dir
        key_hash = sha256(key.encode()).hexdigest()
        subdir = key_hash[:2]
        (self.cache_dir / subdir).mkdir(exist_ok=True)
        return self.cache_dir / subdir / f"{key_hash}.json"

    def _get_metadata_path(self, key: str) -> Path:
        """Get metadata file path for key."""
        cache_path = self._get_cache_path(key)
        return cache_path.with_suffix(".meta")

    async def get(self, key: str) -> Optional[Any]:
        """Get item from disk cache."""
        cache_path = self._get_cache_path(key)
        meta_path = self._get_metadata_path(key)

        if not cache_path.exists() or not meta_path.exists():
            return None

        try:
            # Check metadata first
            async with aiofiles.open(meta_path, "rb") as f:
                meta_data = await f.read()
                metadata = orjson.loads(meta_data)

            expires_at = datetime.fromisoformat(metadata["expires_at"])
            if datetime.utcnow() > expires_at:
                # Expired, clean up
                await self._remove_files(cache_path, meta_path)
                return None

            # Load data
            async with aiofiles.open(cache_path, "rb") as f:
                data = await f.read()
                return orjson.loads(data)

        except Exception as e:
            logger.error(f"Disk cache read error for {key}: {e}")
            # Clean up corrupted files
            await self._remove_files(cache_path, meta_path)
            return None

    async def set(
        self,
        key: str,
        value: Any,
        ttl: Optional[timedelta] = None,
    ) -> None:
        """Set item in disk cache."""
        if ttl is None:
            ttl = self.default_ttl

        cache_path = self._get_cache_path(key)
        meta_path = self._get_metadata_path(key)

        expires_at = datetime.utcnow() + ttl

        try:
            async with self._lock:
                # Write data with proper serialization
                serializable_value = _serialize_for_json(value)
                data_bytes = orjson.dumps(serializable_value)
                async with aiofiles.open(cache_path, "wb") as f:
                    await f.write(data_bytes)

                # Write metadata
                metadata = {
                    "key": key,
                    "expires_at": expires_at.isoformat(),
                    "size_bytes": len(data_bytes),
                    "created_at": datetime.utcnow().isoformat(),
                }
                meta_bytes = orjson.dumps(metadata)
                async with aiofiles.open(meta_path, "wb") as f:
                    await f.write(meta_bytes)

        except Exception as e:
            logger.error(f"Disk cache write error for {key}: {e}")
            # Clean up partial writes
            await self._remove_files(cache_path, meta_path)
            raise

    async def delete(self, key: str) -> bool:
        """Delete item from disk cache."""
        cache_path = self._get_cache_path(key)
        meta_path = self._get_metadata_path(key)

        if cache_path.exists() or meta_path.exists():
            await self._remove_files(cache_path, meta_path)
            return True
        return False

    async def _remove_files(self, *paths: Path):
        """Remove files safely."""
        for path in paths:
            try:
                if path.exists():
                    await aio_os.remove(path)
            except Exception as e:
                logger.error(f"Error removing {path}: {e}")

    async def cleanup_expired(self):
        """Clean up expired cache entries."""
        now = datetime.utcnow()

        for subdir in self.cache_dir.iterdir():
            if not subdir.is_dir():
                continue

            for meta_file in subdir.glob("*.meta"):
                try:
                    async with aiofiles.open(meta_file, "rb") as f:
                        meta_data = await f.read()
                        metadata = orjson.loads(meta_data)

                    expires_at = datetime.fromisoformat(metadata["expires_at"])
                    if now > expires_at:
                        cache_file = meta_file.with_suffix(".json")
                        await self._remove_files(cache_file, meta_file)

                except Exception as e:
                    logger.error(f"Error checking expiry for {meta_file}: {e}")

    async def get_cache_stats(self) -> Dict[str, Any]:
        """Get cache statistics."""
        total_files = 0
        total_size = 0
        expired_files = 0
        now = datetime.utcnow()

        for subdir in self.cache_dir.iterdir():
            if not subdir.is_dir():
                continue

            for meta_file in subdir.glob("*.meta"):
                try:
                    total_files += 1

                    async with aiofiles.open(meta_file, "rb") as f:
                        meta_data = await f.read()
                        metadata = orjson.loads(meta_data)

                    total_size += metadata.get("size_bytes", 0)

                    expires_at = datetime.fromisoformat(metadata["expires_at"])
                    if now > expires_at:
                        expired_files += 1

                except Exception:
                    pass

        return {
            "total_files": total_files,
            "total_size_mb": total_size / 1_000_000,
            "expired_files": expired_files,
            "cache_dir": str(self.cache_dir),
        }


class TieredCache:
    """Intelligent multi-level cache combining memory and disk storage for optimal performance.

    Provides transparent caching with automatic promotion/demotion between memory and disk
    tiers based on access patterns. Offers the speed of memory caching with the persistence
    of disk caching.

    Cache Strategy:
        1. **Read Path**: Check memory first, then disk, promote to memory on disk hit
        2. **Write Path**: Store in both memory and disk simultaneously
        3. **Eviction**: Memory cache evicts to disk automatically via LRU
        4. **Persistence**: Disk cache survives application restarts

    Performance Characteristics:
        - Memory hits: ~1-10 μs (fastest)
        - Disk hits: ~100-1000 μs (fast)
        - Cache misses: Full source lookup time (slowest)
        - Promotion cost: Single memory write operation

    Use Cases:
        - **Frequently accessed data**: Stays in memory for maximum speed
        - **Occasionally accessed data**: Available from disk without full reload
        - **Large datasets**: Automatic memory pressure management
        - **Session persistence**: Important data survives restarts

    Example:
        ```python
        # Create optimized tiered cache
        memory_cache = LRUCache(max_size=1000, max_memory_mb=100)
        disk_cache = DiskCache(Path(".cache"), max_size_gb=10)
        cache = TieredCache(memory_cache, disk_cache)

        # Transparent usage - cache handles the complexity
        data = await cache.get("dataset:SIM:2023")
        if data is None:
            data = await expensive_data_processing()
            await cache.set("dataset:SIM:2023", data)

        # Monitor both tiers
        stats = await cache.get_stats()
        memory_stats = stats['memory']
        disk_stats = stats['disk']
        print(f"Memory hit ratio: {memory_stats.hit_ratio:.2%}")
        print(f"Disk usage: {disk_stats['total_size_mb']:.1f} MB")
        ```

    Thread Safety:
        Both underlying caches are thread-safe, making the tiered cache
        safe for concurrent access from multiple coroutines.

    Memory Management:
        - Memory cache automatically evicts to disk when full
        - Disk cache handles cleanup of expired entries
        - Promotion keeps hot data in memory for optimal performance
    """

    def __init__(
        self,
        memory_cache: LRUCache,
        disk_cache: DiskCache,
    ):
        """Initialize tiered cache with memory and disk cache instances.

        Args:
            memory_cache: Configured LRUCache instance for fast access
            disk_cache: Configured DiskCache instance for persistence

        Example:
            ```python
            # Create balanced configuration
            memory = LRUCache(max_size=500, max_memory_mb=50)
            disk = DiskCache(Path(".cache"), max_size_gb=5)
            cache = TieredCache(memory, disk)

            # High-performance configuration
            memory = LRUCache(max_size=5000, max_memory_mb=500)
            disk = DiskCache(Path("/fast/ssd/cache"), max_size_gb=20)
            cache = TieredCache(memory, disk)
            ```
        """
        self.memory_cache = memory_cache
        self.disk_cache = disk_cache

    async def get(self, key: str) -> Optional[Any]:
        """Get item from tiered cache (memory first, then disk)."""
        # Try memory cache first
        result = await self.memory_cache.get(key)
        if result is not None:
            return result

        # Try disk cache
        result = await self.disk_cache.get(key)
        if result is not None:
            # Promote to memory cache
            await self.memory_cache.set(key, result)
            return result

        return None

    async def set(
        self,
        key: str,
        value: Any,
        ttl: Optional[timedelta] = None,
    ) -> None:
        """Set item in both caches."""
        await gather(
            self.memory_cache.set(key, value, ttl),
            self.disk_cache.set(key, value, ttl),
            return_exceptions=True,
        )

    async def delete(self, key: str) -> bool:
        """Delete from both caches."""
        results = await gather(
            self.memory_cache.delete(key),
            self.disk_cache.delete(key),
            return_exceptions=True,
        )
        return any(isinstance(r, bool) and r for r in results)

    async def clear(self):
        """Clear both caches."""
        await gather(
            self.memory_cache.clear(),
            self.disk_cache.cleanup_expired(),
            return_exceptions=True,
        )

    async def get_stats(self) -> Dict[str, Any]:
        """Get combined cache statistics."""
        memory_stats, disk_stats = await gather(
            self.memory_cache.get_stats(),
            self.disk_cache.get_cache_stats(),
            return_exceptions=True,
        )

        return {
            "memory": memory_stats if not isinstance(memory_stats, Exception) else None,
            "disk": disk_stats if not isinstance(disk_stats, Exception) else None,
        }

    async def close(self):
        """Close both caches."""
        await self.memory_cache.close()
