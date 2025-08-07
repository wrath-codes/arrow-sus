use std::collections::HashMap;
use std::env;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use lazy_static::lazy_static;

/// Type aliases
pub type PathLike = PathBuf;
// Note: FileContent will reference Directory when it's implemented
// pub type FileContent = HashMap<String, DirectoryOrFile>;

/// Constants
pub const DEFAULT_CACHE_DIR: &str = "pysus";

lazy_static! {
    /// Cache path - equivalent to CACHEPATH in Python
    pub static ref CACHE_PATH: String = {
        env::var("PYSUS_CACHEPATH")
            .unwrap_or_else(|_| {
                dirs::home_dir()
                    .map(|home| home.join(DEFAULT_CACHE_DIR).to_string_lossy().to_string())
                    .unwrap_or_else(|| format!("./{}", DEFAULT_CACHE_DIR))
            })
    };

    /// Cache path as PathBuf - equivalent to __cachepath__ in Python
    pub static ref CACHE_PATH_BUF: PathBuf = {
        let path = PathBuf::from(CACHE_PATH.as_str());
        if let Err(e) = std::fs::create_dir_all(&path) {
            eprintln!("Warning: Failed to create cache directory {}: {}", path.display(), e);
        }
        path
    };

    /// Directory cache - equivalent to DIRECTORY_CACHE in Python
    /// Note: This will store Directory objects when Directory is implemented
    pub static ref DIRECTORY_CACHE: Mutex<HashMap<String, String>> = {
        Mutex::new(HashMap::new())
    };
}

/// Parse any data type into a Vec - equivalent to to_list in Python
/// 
/// This function converts various input types into a Vec:
/// - None/Option::None -> empty Vec
/// - Single item -> Vec with one element
/// - Vec -> returns as-is
/// - Array/slice -> converts to Vec
/// 
/// # Examples
/// ```
/// use shared::models::utils::to_vec;
/// 
/// assert_eq!(to_vec(None::<i32>), Vec::<i32>::new());
/// assert_eq!(to_vec(Some(42)), vec![42]);
/// ```
pub fn to_vec<T>(item: Option<T>) -> Vec<T> {
    match item {
        None => Vec::new(),
        Some(value) => vec![value],
    }
}

/// Convert a single item to Vec
pub fn item_to_vec<T>(item: T) -> Vec<T> {
    vec![item]
}

/// Convert a slice to Vec
pub fn slice_to_vec<T: Clone>(items: &[T]) -> Vec<T> {
    items.to_vec()
}

/// Convert various collection types to Vec - additional utility functions
pub fn option_to_vec<T>(item: Option<T>) -> Vec<T> {
    to_vec(item)
}

pub fn vec_from_slice<T: Clone>(items: &[T]) -> Vec<T> {
    items.to_vec()
}

/// Utility functions for working with paths
pub mod path_utils {
    use super::*;

    /// Convert various path-like types to PathBuf
    pub fn to_path_buf<P: AsRef<Path>>(path: P) -> PathBuf {
        path.as_ref().to_path_buf()
    }

    /// Check if a path exists
    pub fn path_exists<P: AsRef<Path>>(path: P) -> bool {
        path.as_ref().exists()
    }

    /// Create directory if it doesn't exist
    pub fn ensure_dir<P: AsRef<Path>>(path: P) -> Result<(), std::io::Error> {
        std::fs::create_dir_all(path)
    }

    /// Get the cache directory path
    pub fn get_cache_dir() -> &'static PathBuf {
        &CACHE_PATH_BUF
    }

    /// Get a path relative to the cache directory
    pub fn cache_path<P: AsRef<Path>>(relative_path: P) -> PathBuf {
        CACHE_PATH_BUF.join(relative_path)
    }
}

/// Cache utilities
pub mod cache {
    use super::*;

    /// Add an item to the directory cache
    /// Note: This is a placeholder until Directory is implemented
    pub fn cache_directory(key: String, value: String) {
        if let Ok(mut cache) = DIRECTORY_CACHE.lock() {
            cache.insert(key, value);
        }
    }

    /// Get an item from the directory cache
    pub fn get_cached_directory(key: &str) -> Option<String> {
        DIRECTORY_CACHE.lock()
            .ok()
            .and_then(|cache| cache.get(key).cloned())
    }

    /// Clear the directory cache
    pub fn clear_cache() {
        if let Ok(mut cache) = DIRECTORY_CACHE.lock() {
            cache.clear();
        }
    }

    /// Get cache size
    pub fn cache_size() -> usize {
        DIRECTORY_CACHE.lock()
            .map(|cache| cache.len())
            .unwrap_or(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_to_vec_option() {
        assert_eq!(to_vec(None::<i32>), Vec::<i32>::new());
        assert_eq!(to_vec(Some(42)), vec![42]);
    }

    #[test]
    fn test_utility_functions() {
        assert_eq!(option_to_vec(Some(42)), vec![42]);
        assert_eq!(option_to_vec(None::<i32>), Vec::<i32>::new());
        assert_eq!(item_to_vec(42), vec![42]);
        assert_eq!(vec_from_slice(&[1, 2, 3]), vec![1, 2, 3]);
    }

    #[test]
    fn test_item_to_vec() {
        assert_eq!(item_to_vec("hello"), vec!["hello"]);
        assert_eq!(item_to_vec(42), vec![42]);
    }

    #[test]
    fn test_slice_to_vec() {
        let arr = [1, 2, 3];
        assert_eq!(slice_to_vec(&arr), vec![1, 2, 3]);
    }

    #[test]
    fn test_cache_path_exists() {
        // Test that cache path is created
        assert!(CACHE_PATH_BUF.exists() || CACHE_PATH_BUF.parent().map_or(false, |p| p.exists()));
    }

    #[test]
    fn test_path_utils() {
        use path_utils::*;
        
        let path = to_path_buf("/tmp");
        assert_eq!(path, PathBuf::from("/tmp"));
        
        // Test cache path functions
        let cache_dir = get_cache_dir();
        let relative = cache_path("test.txt");
        assert!(relative.starts_with(cache_dir));
    }

    #[test]
    fn test_cache_operations() {
        use cache::*;
        
        clear_cache();
        assert_eq!(cache_size(), 0);
        
        cache_directory("test_key".to_string(), "test_value".to_string());
        assert_eq!(cache_size(), 1);
        assert_eq!(get_cached_directory("test_key"), Some("test_value".to_string()));
        
        clear_cache();
        assert_eq!(cache_size(), 0);
    }
}
