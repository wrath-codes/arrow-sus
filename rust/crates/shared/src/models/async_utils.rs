use std::collections::HashMap;
use std::env;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH, Duration};
use tokio::sync::Mutex;
use lazy_static::lazy_static;
use serde::{Serialize, Deserialize};

/// Type aliases (same as sync version)
pub type PathLike = PathBuf;
// Note: FileContent will reference Directory when it's implemented
// pub type FileContent = HashMap<String, DirectoryOrFile>;

/// Constants (reuse from sync version)
pub use super::utils::DEFAULT_CACHE_DIR;

lazy_static! {
    /// Async cache path - equivalent to CACHEPATH in Python
    pub static ref ASYNC_CACHE_PATH: String = {
        env::var("PYSUS_CACHEPATH")
            .unwrap_or_else(|_| {
                dirs::home_dir()
                    .map(|home| home.join(DEFAULT_CACHE_DIR).to_string_lossy().to_string())
                    .unwrap_or_else(|| format!("./{}", DEFAULT_CACHE_DIR))
            })
    };

    /// Async cache path as PathBuf - equivalent to __cachepath__ in Python
    pub static ref ASYNC_CACHE_PATH_BUF: PathBuf = {
        PathBuf::from(ASYNC_CACHE_PATH.as_str())
    };

    /// Async directory cache - equivalent to DIRECTORY_CACHE in Python
    /// Note: This will store Directory objects when Directory is implemented
    pub static ref ASYNC_DIRECTORY_CACHE: Arc<Mutex<HashMap<String, String>>> = {
        Arc::new(Mutex::new(HashMap::new()))
    };
}

/// Parse any data type into a Vec - equivalent to to_list in Python (async version)
/// 
/// This function converts various input types into a Vec:
/// - None/Option::None -> empty Vec
/// - Single item -> Vec with one element
/// - Vec -> returns as-is
/// - Array/slice -> converts to Vec
/// 
/// # Examples
/// ```
/// use shared::models::async_utils::to_vec_async;
/// 
/// # tokio_test::block_on(async {
/// assert_eq!(to_vec_async(None::<i32>).await, Vec::<i32>::new());
/// assert_eq!(to_vec_async(Some(42)).await, vec![42]);
/// # });
/// ```
pub async fn to_vec_async<T>(item: Option<T>) -> Vec<T> {
    match item {
        None => Vec::new(),
        Some(value) => vec![value],
    }
}

/// Convert a single item to Vec (async version)
pub async fn item_to_vec_async<T>(item: T) -> Vec<T> {
    vec![item]
}

/// Convert a slice to Vec (async version)
pub async fn slice_to_vec_async<T: Clone>(items: &[T]) -> Vec<T> {
    items.to_vec()
}

/// Convert various collection types to Vec - additional utility functions (async versions)
pub async fn option_to_vec_async<T>(item: Option<T>) -> Vec<T> {
    to_vec_async(item).await
}

pub async fn vec_from_slice_async<T: Clone>(items: &[T]) -> Vec<T> {
    items.to_vec()
}

/// Async utility functions for working with paths
pub mod async_path_utils {
    use super::*;
    use tokio::fs;

    /// Convert various path-like types to PathBuf (async version)
    pub async fn to_path_buf_async<P: AsRef<Path>>(path: P) -> PathBuf {
        path.as_ref().to_path_buf()
    }

    /// Check if a path exists (async version)
    pub async fn path_exists_async<P: AsRef<Path>>(path: P) -> bool {
        fs::metadata(path).await.is_ok()
    }

    /// Create directory if it doesn't exist (async version)
    pub async fn ensure_dir_async<P: AsRef<Path>>(path: P) -> Result<(), std::io::Error> {
        fs::create_dir_all(path).await
    }

    /// Get the cache directory path (async version)
    pub async fn get_cache_dir_async() -> &'static PathBuf {
        &ASYNC_CACHE_PATH_BUF
    }

    /// Get a path relative to the cache directory (async version)
    pub async fn cache_path_async<P: AsRef<Path>>(relative_path: P) -> PathBuf {
        ASYNC_CACHE_PATH_BUF.join(relative_path)
    }

    /// Initialize cache directory (async version)
    pub async fn init_cache_dir_async() -> Result<(), std::io::Error> {
        ensure_dir_async(&*ASYNC_CACHE_PATH_BUF).await
    }

    /// Check if cache directory exists and create if necessary
    pub async fn ensure_cache_dir_async() -> Result<PathBuf, std::io::Error> {
        let cache_dir = &*ASYNC_CACHE_PATH_BUF;
        if !path_exists_async(cache_dir).await {
            ensure_dir_async(cache_dir).await?;
        }
        Ok(cache_dir.clone())
    }

    /// Get file size asynchronously
    pub async fn get_file_size_async<P: AsRef<Path>>(path: P) -> Result<u64, std::io::Error> {
        let metadata = fs::metadata(path).await?;
        Ok(metadata.len())
    }

    /// Check if path is a file
    pub async fn is_file_async<P: AsRef<Path>>(path: P) -> bool {
        match fs::metadata(path).await {
            Ok(metadata) => metadata.is_file(),
            Err(_) => false,
        }
    }

    /// Check if path is a directory
    pub async fn is_dir_async<P: AsRef<Path>>(path: P) -> bool {
        match fs::metadata(path).await {
            Ok(metadata) => metadata.is_dir(),
            Err(_) => false,
        }
    }

    /// List directory contents
    pub async fn list_dir_async<P: AsRef<Path>>(path: P) -> Result<Vec<PathBuf>, std::io::Error> {
        let mut entries = fs::read_dir(path).await?;
        let mut paths = Vec::new();
        
        while let Some(entry) = entries.next_entry().await? {
            paths.push(entry.path());
        }
        
        Ok(paths)
    }

    /// Copy file asynchronously
    pub async fn copy_file_async<P: AsRef<Path>, Q: AsRef<Path>>(
        from: P,
        to: Q,
    ) -> Result<u64, std::io::Error> {
        fs::copy(from, to).await
    }

    /// Move file asynchronously
    pub async fn move_file_async<P: AsRef<Path>, Q: AsRef<Path>>(
        from: P,
        to: Q,
    ) -> Result<(), std::io::Error> {
        fs::rename(from, to).await
    }

    /// Remove file asynchronously
    pub async fn remove_file_async<P: AsRef<Path>>(path: P) -> Result<(), std::io::Error> {
        fs::remove_file(path).await
    }

    /// Remove directory recursively asynchronously
    pub async fn remove_dir_async<P: AsRef<Path>>(path: P) -> Result<(), std::io::Error> {
        fs::remove_dir_all(path).await
    }
}

/// Async cache utilities
pub mod async_cache {
    use super::*;

    /// Add an item to the directory cache (async version)
    /// Note: This is a placeholder until Directory is implemented
    pub async fn cache_directory_async(key: String, value: String) {
        let mut cache = ASYNC_DIRECTORY_CACHE.lock().await;
        cache.insert(key, value);
    }

    /// Get an item from the directory cache (async version)
    pub async fn get_cached_directory_async(key: &str) -> Option<String> {
        let cache = ASYNC_DIRECTORY_CACHE.lock().await;
        cache.get(key).cloned()
    }

    /// Clear the directory cache (async version)
    pub async fn clear_cache_async() {
        let mut cache = ASYNC_DIRECTORY_CACHE.lock().await;
        cache.clear();
    }

    /// Get cache size (async version)
    pub async fn cache_size_async() -> usize {
        let cache = ASYNC_DIRECTORY_CACHE.lock().await;
        cache.len()
    }

    /// Check if key exists in cache
    pub async fn cache_contains_key_async(key: &str) -> bool {
        let cache = ASYNC_DIRECTORY_CACHE.lock().await;
        cache.contains_key(key)
    }

    /// Remove specific key from cache
    pub async fn remove_cached_directory_async(key: &str) -> Option<String> {
        let mut cache = ASYNC_DIRECTORY_CACHE.lock().await;
        cache.remove(key)
    }

    /// Get all cache keys
    pub async fn get_cache_keys_async() -> Vec<String> {
        let cache = ASYNC_DIRECTORY_CACHE.lock().await;
        cache.keys().cloned().collect()
    }

    /// Bulk insert into cache
    pub async fn cache_bulk_insert_async(items: HashMap<String, String>) {
        let mut cache = ASYNC_DIRECTORY_CACHE.lock().await;
        cache.extend(items);
    }

    /// Export cache to HashMap
    pub async fn export_cache_async() -> HashMap<String, String> {
        let cache = ASYNC_DIRECTORY_CACHE.lock().await;
        cache.clone()
    }

    /// Import cache from HashMap (replaces existing cache)
    pub async fn import_cache_async(new_cache: HashMap<String, String>) {
        let mut cache = ASYNC_DIRECTORY_CACHE.lock().await;
        cache.clear();
        cache.extend(new_cache);
    }
}

/// Stream-based utilities for processing large datasets
pub mod async_stream_utils {
    use futures::stream::{Stream, StreamExt};

    /// Process a stream of items and convert them to vectors in chunks
    pub async fn stream_to_chunks<T, S>(
        mut stream: S,
        chunk_size: usize,
    ) -> Vec<Vec<T>>
    where
        S: Stream<Item = T> + Unpin,
    {
        let mut chunks = Vec::new();
        let mut current_chunk = Vec::with_capacity(chunk_size);

        while let Some(item) = stream.next().await {
            current_chunk.push(item);
            if current_chunk.len() >= chunk_size {
                chunks.push(current_chunk);
                current_chunk = Vec::with_capacity(chunk_size);
            }
        }

        if !current_chunk.is_empty() {
            chunks.push(current_chunk);
        }

        chunks
    }

    /// Convert async iterator to Vec
    pub async fn collect_async<T, S>(stream: S) -> Vec<T>
    where
        S: Stream<Item = T>,
    {
        stream.collect().await
    }
}

/// Content cache entry with TTL support
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheEntry {
    pub content: String,
    pub timestamp: u64,
    pub ttl_seconds: u64,
}

impl CacheEntry {
    pub fn new(content: String, ttl_seconds: u64) -> Self {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        
        Self {
            content,
            timestamp,
            ttl_seconds,
        }
    }
    
    pub fn is_expired(&self) -> bool {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        
        now > self.timestamp + self.ttl_seconds
    }
    
    pub fn time_left(&self) -> Option<Duration> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        
        let expires_at = self.timestamp + self.ttl_seconds;
        if now < expires_at {
            Some(Duration::from_secs(expires_at - now))
        } else {
            None
        }
    }
}

lazy_static! {
    /// Content cache with TTL support for directory listings
    pub static ref CONTENT_CACHE: Mutex<HashMap<String, CacheEntry>> = {
        Mutex::new(HashMap::new())
    };
}

/// Content cache utilities with TTL support
pub mod content_cache {
    use super::*;
    use tokio::fs;
    
    /// Default TTL for FTP directory cache (5 minutes)
    pub const DEFAULT_FTP_TTL_SECONDS: u64 = 300;
    
    /// Generate cache key for FTP provider
    pub fn generate_ftp_cache_key(host: &str, path: &str) -> String {
        format!("ftp://{}:{}", host, path)
    }
    
    /// Add content to cache with TTL
    pub async fn cache_content(key: String, content: String, ttl_seconds: u64) {
        let entry = CacheEntry::new(content, ttl_seconds);
        let mut cache = CONTENT_CACHE.lock().await;
        cache.insert(key, entry);
    }
    
    /// Get content from cache (returns None if expired or not found)
    pub async fn get_cached_content(key: &str) -> Option<String> {
        let mut cache = CONTENT_CACHE.lock().await;
        
        if let Some(entry) = cache.get(key) {
            if entry.is_expired() {
                // Remove expired entry
                cache.remove(key);
                None
            } else {
                Some(entry.content.clone())
            }
        } else {
            None
        }
    }
    
    /// Check if content exists in cache and is not expired
    pub async fn is_cached(key: &str) -> bool {
        let cache = CONTENT_CACHE.lock().await;
        
        if let Some(entry) = cache.get(key) {
            !entry.is_expired()
        } else {
            false
        }
    }
    
    /// Get cache entry info (for debugging)
    pub async fn get_cache_info(key: &str) -> Option<(u64, Option<Duration>)> {
        let cache = CONTENT_CACHE.lock().await;
        
        cache.get(key).map(|entry| {
            (entry.timestamp, entry.time_left())
        })
    }
    
    /// Clear expired entries from cache
    pub async fn cleanup_expired() -> usize {
        let mut cache = CONTENT_CACHE.lock().await;
        let mut to_remove = Vec::new();
        
        for (key, entry) in cache.iter() {
            if entry.is_expired() {
                to_remove.push(key.clone());
            }
        }
        
        let removed_count = to_remove.len();
        for key in to_remove {
            cache.remove(&key);
        }
        
        removed_count
    }
    
    /// Get cache statistics
    pub async fn cache_stats() -> (usize, usize) {
        let cache = CONTENT_CACHE.lock().await;
        let total = cache.len();
        let expired = cache.values().filter(|entry| entry.is_expired()).count();
        (total, expired)
    }
    
    /// Clear all cache entries
    pub async fn clear_content_cache() {
        let mut cache = CONTENT_CACHE.lock().await;
        cache.clear();
    }
    
    /// Save cache to disk (for persistence)
    pub async fn save_cache_to_disk() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let cache = CONTENT_CACHE.lock().await;
        let cache_file = super::async_path_utils::cache_path_async("content_cache.json").await;
        
        let serialized = serde_json::to_string_pretty(&*cache)?;
        fs::write(cache_file, serialized).await?;
        
        Ok(())
    }
    
    /// Load cache from disk (for persistence)
    pub async fn load_cache_from_disk() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let cache_file = super::async_path_utils::cache_path_async("content_cache.json").await;
        
        if cache_file.exists() {
            let content = fs::read_to_string(cache_file).await?;
            let loaded_cache: HashMap<String, CacheEntry> = serde_json::from_str(&content)?;
            
            let mut cache = CONTENT_CACHE.lock().await;
            cache.clear();
            cache.extend(loaded_cache);
        }
        
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_to_vec_async_option() {
        assert_eq!(to_vec_async(None::<i32>).await, Vec::<i32>::new());
        assert_eq!(to_vec_async(Some(42)).await, vec![42]);
    }

    #[tokio::test]
    async fn test_async_utility_functions() {
        assert_eq!(option_to_vec_async(Some(42)).await, vec![42]);
        assert_eq!(option_to_vec_async(None::<i32>).await, Vec::<i32>::new());
        assert_eq!(item_to_vec_async(42).await, vec![42]);
        assert_eq!(vec_from_slice_async(&[1, 2, 3]).await, vec![1, 2, 3]);
    }

    #[tokio::test]
    async fn test_async_path_utils() {
        use async_path_utils::*;
        
        let path = to_path_buf_async("/tmp").await;
        assert_eq!(path, PathBuf::from("/tmp"));
        
        // Test cache path functions
        let cache_dir = get_cache_dir_async().await;
        let relative = cache_path_async("test.txt").await;
        assert!(relative.starts_with(cache_dir));

        // Test cache directory initialization
        let cache_result = ensure_cache_dir_async().await;
        assert!(cache_result.is_ok());
    }

    #[tokio::test]
    async fn test_async_cache_operations() {
        use async_cache::*;
        
        // Use unique keys for this test
        let test_key = "test_cache_ops_key";
        let test_value = "test_cache_ops_value";
        
        // Remove any existing test key
        remove_cached_directory_async(test_key).await;
        
        // Test that our specific key doesn't exist
        assert!(!cache_contains_key_async(test_key).await);
        
        cache_directory_async(test_key.to_string(), test_value.to_string()).await;
        assert_eq!(get_cached_directory_async(test_key).await, Some(test_value.to_string()));
        
        assert!(cache_contains_key_async(test_key).await);
        assert!(!cache_contains_key_async("nonexistent_key_for_test").await);
        
        let removed = remove_cached_directory_async(test_key).await;
        assert_eq!(removed, Some(test_value.to_string()));
        assert!(!cache_contains_key_async(test_key).await);
    }

    #[tokio::test]
    async fn test_async_cache_bulk_operations() {
        use async_cache::*;
        
        // Use unique keys for this test to avoid conflicts
        let test_prefix = "bulk_test_";
        let mut bulk_data = HashMap::new();
        bulk_data.insert(format!("{}key1", test_prefix), "value1".to_string());
        bulk_data.insert(format!("{}key2", test_prefix), "value2".to_string());
        bulk_data.insert(format!("{}key3", test_prefix), "value3".to_string());
        
        // Remove any existing test keys
        for key in bulk_data.keys() {
            remove_cached_directory_async(key).await;
        }
        
        cache_bulk_insert_async(bulk_data.clone()).await;
        
        // Check that our keys exist
        for key in bulk_data.keys() {
            assert!(cache_contains_key_async(key).await);
        }
        
        let keys = get_cache_keys_async().await;
        let test_keys: Vec<_> = keys.iter().filter(|k| k.starts_with(test_prefix)).collect();
        assert_eq!(test_keys.len(), 3);
        
        // Clean up our test keys
        for key in bulk_data.keys() {
            remove_cached_directory_async(key).await;
        }
    }

    #[tokio::test]
    async fn test_async_file_operations() {
        use async_path_utils::*;
        
        // Test with a known directory (current directory)
        let current_dir = PathBuf::from(".");
        assert!(path_exists_async(&current_dir).await);
        assert!(is_dir_async(&current_dir).await);
        
        // List directory should work for current directory
        let entries = list_dir_async(&current_dir).await;
        assert!(entries.is_ok());
        
        // Get size of Cargo.toml if it exists
        let cargo_toml = current_dir.join("Cargo.toml");
        if path_exists_async(&cargo_toml).await {
            let size = get_file_size_async(&cargo_toml).await;
            assert!(size.is_ok());
            assert!(size.unwrap() > 0);
            assert!(is_file_async(&cargo_toml).await);
        }
    }

    #[tokio::test]
    async fn test_async_stream_utils() {
        use async_stream_utils::*;
        use futures::stream;
        
        let data = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
        let stream = stream::iter(data.clone());
        
        let chunks = stream_to_chunks(stream, 3).await;
        assert_eq!(chunks.len(), 4); // 3 + 3 + 3 + 1
        assert_eq!(chunks[0], vec![1, 2, 3]);
        assert_eq!(chunks[1], vec![4, 5, 6]);
        assert_eq!(chunks[2], vec![7, 8, 9]);
        assert_eq!(chunks[3], vec![10]);
        
        let stream2 = stream::iter(data.clone());
        let collected = collect_async(stream2).await;
        assert_eq!(collected, data);
    }
}
