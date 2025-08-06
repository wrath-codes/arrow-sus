use std::collections::HashMap;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::path::Path;
use std::sync::Arc;
use std::pin::Pin;
use std::future::Future;

use anyhow::Result;
use tokio::sync::RwLock;

use super::file::File;

/// Content of a directory - can contain files and subdirectories
pub type DirectoryContent = HashMap<String, DirectoryItem>;

/// Items that can exist in a directory
#[derive(Debug, Clone)]
pub enum DirectoryItem {
    File(File),
    Directory(Directory),
}

/// Strategy trait for listing directory contents from different sources
#[async_trait::async_trait]
pub trait DirectoryListingStrategy: Send + Sync {
    /// List the contents of a directory at the given path
    async fn list_directory(&self, path: &str) -> Result<DirectoryContent>;
    
    /// Verify if the connection/strategy is working
    async fn verify_connection(&self) -> bool;
    
    /// Get a human-readable name for this strategy
    fn strategy_name(&self) -> &str;
}

/// Thread-safe cache for directory instances
type DirectoryCache = Arc<RwLock<HashMap<String, Directory>>>;

lazy_static::lazy_static! {
    static ref DIRECTORY_CACHE: DirectoryCache = Arc::new(RwLock::new(HashMap::new()));
}

/// Directory representation with lazy loading and caching
/// 
/// This struct provides async methods for interacting with directories from
/// various sources (FTP, S3, local filesystem, etc.) using a strategy pattern.
/// It includes caching and lazy loading capabilities.
#[derive(Clone)]
pub struct Directory {
    /// The name of the directory
    pub name: String,
    /// The normalized path of the directory
    pub path: String,
    /// The parent directory (Arc for shared ownership)
    pub parent: Option<Arc<Directory>>,
    /// Whether the directory content has been loaded
    pub loaded: bool,
    /// Cached directory content
    content: Arc<RwLock<DirectoryContent>>,
    /// Strategy for listing directory contents
    strategy: Arc<dyn DirectoryListingStrategy>,
}

impl Directory {
    /// Create a new directory instance with the given strategy
    pub async fn new<S: DirectoryListingStrategy + 'static>(
        path: &str, 
        strategy: S
    ) -> Result<Self> {
        let strategy = Arc::new(strategy);
        Self::new_with_strategy(path, strategy).await
    }
    
    /// Create a new directory instance with an Arc-wrapped strategy
    pub async fn new_with_strategy(
        path: &str,
        strategy: Arc<dyn DirectoryListingStrategy>
    ) -> Result<Self> {
        let normalized_path = Self::normalize_path(path);
        
        // Check cache first
        {
            let cache = DIRECTORY_CACHE.read().await;
            if let Some(cached) = cache.get(&normalized_path) {
                return Ok(cached.clone());
            }
        }
        
        // Verify strategy connection
        if !strategy.verify_connection().await {
            return Err(anyhow::anyhow!(
                "Cannot connect to source using {} strategy for path: {}", 
                strategy.strategy_name(),
                normalized_path
            ));
        }
        
        // Handle root directory case
        if normalized_path == "/" {
            return Ok(Self::get_root_directory(strategy).await);
        }
        
        // Extract path information
        let path_obj = Path::new(&normalized_path);
        let name = path_obj.file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("")
            .to_string();
        
        let parent_path = path_obj.parent()
            .and_then(|p| p.to_str())
            .unwrap_or("/");
        
        // Create new instance
        let mut instance = Self {
            name: name.clone(),
            path: normalized_path.clone(),
            parent: None, // Will be set in init methods
            loaded: false,
            content: Arc::new(RwLock::new(HashMap::new())),
            strategy: strategy.clone(),
        };
        
        // Initialize based on directory type
        if parent_path == "/" {
            instance.init_root_child().await;
        } else {
            instance.init_regular(parent_path, strategy).await?;
        }
        
        // Cache and return
        {
            let mut cache = DIRECTORY_CACHE.write().await;
            cache.insert(normalized_path, instance.clone());
        }
        
        Ok(instance)
    }
    
    /// Get or create the root directory instance (following Python pattern)
    async fn get_root_directory(strategy: Arc<dyn DirectoryListingStrategy>) -> Self {
        // Check cache first
        {
            let cache = DIRECTORY_CACHE.read().await;
            if let Some(root) = cache.get("/") {
                return root.clone();
            }
        }
        
        // Create root directory
        let root = Self {
            name: "/".to_string(),
            path: "/".to_string(),
            parent: None, // Root has no parent (could be self-referential like Python)
            loaded: false,
            content: Arc::new(RwLock::new(HashMap::new())),
            strategy,
        };
        
        // Cache it
        {
            let mut cache = DIRECTORY_CACHE.write().await;
            cache.insert("/".to_string(), root.clone());
        }
        
        root
    }
    
    /// Initialize a root child directory (following Python pattern)
    async fn init_root_child(&mut self) {
        // Get root from cache (it should exist by now)
        let root = Self::get_root_directory(self.strategy.clone()).await;
        self.parent = Some(Arc::new(root));
    }
    
    /// Initialize a regular directory (following Python pattern)
    fn init_regular(
        &mut self, 
        parent_path: &str, 
        strategy: Arc<dyn DirectoryListingStrategy>
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + '_>> {
        let parent_path = parent_path.to_string();
        Box::pin(async move {
            // Create parent directory (this is where the recursion resolves)
            let parent = Self::new_with_strategy(&parent_path, strategy).await?;
            self.parent = Some(Arc::new(parent));
            Ok(())
        })
    }
    
    /// Load the directory content asynchronously
    pub async fn load(&mut self) -> Result<()> {
        if self.loaded {
            return Ok(());
        }
        
        match self.strategy.list_directory(&self.path).await {
            Ok(content) => {
                let mut dir_content = self.content.write().await;
                dir_content.extend(content);
                drop(dir_content);
                
                self.loaded = true;
                log::info!(
                    "Successfully loaded directory: {} ({} items)", 
                    self.path, 
                    self.content.read().await.len()
                );
            }
            Err(e) => {
                log::warn!("Failed to load directory {}: {}", self.path, e);
                return Err(e);
            }
        }
        
        Ok(())
    }
    
    /// Reload the directory content (force refresh)
    pub async fn reload(&mut self) -> Result<()> {
        self.loaded = false;
        {
            let mut content = self.content.write().await;
            content.clear();
        }
        self.load().await
    }
    
    /// Get the directory content, loading it if necessary
    pub async fn content(&mut self) -> Result<Vec<DirectoryItem>> {
        if !self.loaded {
            self.load().await?;
        }
        
        let content = self.content.read().await;
        Ok(content.values().cloned().collect())
    }
    
    /// Get only files from the directory content
    pub async fn files(&mut self) -> Result<Vec<File>> {
        let content = self.content().await?;
        Ok(content.into_iter()
            .filter_map(|item| match item {
                DirectoryItem::File(file) => Some(file),
                _ => None,
            })
            .collect())
    }
    
    /// Get only subdirectories from the directory content
    pub async fn directories(&mut self) -> Result<Vec<Directory>> {
        let content = self.content().await?;
        Ok(content.into_iter()
            .filter_map(|item| match item {
                DirectoryItem::Directory(dir) => Some(dir),
                _ => None,
            })
            .collect())
    }
    
    /// Find a specific item by name
    pub async fn find_item(&mut self, name: &str) -> Result<Option<DirectoryItem>> {
        if !self.loaded {
            self.load().await?;
        }
        
        let content = self.content.read().await;
        Ok(content.get(name).cloned())
    }
    
    /// Check if the directory contains an item with the given name
    pub async fn contains(&mut self, name: &str) -> Result<bool> {
        Ok(self.find_item(name).await?.is_some())
    }
    
    /// Get the directory size (number of items)
    pub async fn size(&mut self) -> Result<usize> {
        if !self.loaded {
            self.load().await?;
        }
        
        Ok(self.content.read().await.len())
    }
    
    /// Check if the directory is empty
    pub async fn is_empty(&mut self) -> Result<bool> {
        Ok(self.size().await? == 0)
    }
    
    /// Get the strategy name being used
    pub fn strategy_name(&self) -> &str {
        self.strategy.strategy_name()
    }
    
    /// Get the parent directory 
    pub fn parent(&self) -> Option<&Directory> {
        self.parent.as_ref().map(|p| p.as_ref())
    }
    
    /// Normalize a path string
    fn normalize_path(path: &str) -> String {
        if path == "/" {
            return "/".to_string();
        }
        
        let path = if !path.starts_with('/') {
            format!("/{}", path)
        } else {
            path.to_string()
        };
        
        // Remove trailing slash except for root
        path.trim_end_matches('/').to_string()
    }
    
    /// Clear the global directory cache
    pub async fn clear_cache() {
        let mut cache = DIRECTORY_CACHE.write().await;
        cache.clear();
    }
    
    /// Get cache statistics
    pub async fn cache_stats() -> (usize, Vec<String>) {
        let cache = DIRECTORY_CACHE.read().await;
        let size = cache.len();
        let paths = cache.keys().cloned().collect();
        (size, paths)
    }
}

impl fmt::Display for Directory {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.path)
    }
}

impl fmt::Debug for Directory {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Directory")
            .field("name", &self.name)
            .field("path", &self.path)
            .field("loaded", &self.loaded)
            .field("strategy", &self.strategy.strategy_name())
            .finish()
    }
}

impl Hash for Directory {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.path.hash(state);
    }
}

impl PartialEq for Directory {
    fn eq(&self, other: &Self) -> bool {
        self.path == other.path
    }
}

impl Eq for Directory {}

// Implement Display for DirectoryItem
impl fmt::Display for DirectoryItem {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DirectoryItem::File(file) => write!(f, "File({})", file.basename),
            DirectoryItem::Directory(dir) => write!(f, "Directory({})", dir.path),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::file_info::{FileInfo, FileSize};
    use chrono::Utc;
    
    // Mock strategy for testing
    struct MockStrategy {
        should_fail: bool,
    }
    
    #[async_trait::async_trait]
    impl DirectoryListingStrategy for MockStrategy {
        async fn list_directory(&self, path: &str) -> Result<DirectoryContent> {
            if self.should_fail {
                return Err(anyhow::anyhow!("Mock failure"));
            }
            
            let mut content = HashMap::new();
            
            // Add some mock files
            let file_info = FileInfo::new(
                FileSize::from_bytes(1024),
                ".txt".to_string(),
                Utc::now(),
            );
            let file = File::new(path, "test.txt", file_info);
            content.insert("test.txt".to_string(), DirectoryItem::File(file));
            
            // Add a mock subdirectory
            if path == "/" {
                let subdir = Directory::get_root_directory(Arc::new(MockStrategy { should_fail: false })).await;
                content.insert("subdir".to_string(), DirectoryItem::Directory(subdir));
            }
            
            Ok(content)
        }
        
        async fn verify_connection(&self) -> bool {
            !self.should_fail
        }
        
        fn strategy_name(&self) -> &str {
            "Mock"
        }
    }
    
    #[tokio::test]
    async fn test_directory_creation() {
        let strategy = MockStrategy { should_fail: false };
        let dir = Directory::new("/test", strategy).await.unwrap();
        
        assert_eq!(dir.path, "/test");
        assert_eq!(dir.name, "test");
        assert!(!dir.loaded);
    }
    
    #[tokio::test]
    async fn test_root_directory() {
        let strategy = MockStrategy { should_fail: false };
        let root = Directory::new("/", strategy).await.unwrap();
        
        assert_eq!(root.path, "/");
        assert_eq!(root.name, "/");
        assert!(root.parent.is_none());
    }
    
    #[tokio::test]
    async fn test_directory_loading() {
        let strategy = MockStrategy { should_fail: false };
        let mut dir = Directory::new("/test", strategy).await.unwrap();
        
        dir.load().await.unwrap();
        assert!(dir.loaded);
        
        let content = dir.content().await.unwrap();
        assert_eq!(content.len(), 1); // Should have the mock file
    }
    
    #[tokio::test]
    async fn test_directory_caching() {
        Directory::clear_cache().await;
        
        let strategy1 = MockStrategy { should_fail: false };
        let strategy2 = MockStrategy { should_fail: false };
        
        let dir1 = Directory::new("/test", strategy1).await.unwrap();
        let dir2 = Directory::new("/test", strategy2).await.unwrap();
        
        // Should be the same instance due to caching
        assert_eq!(dir1.path, dir2.path);
        
        let (cache_size, _) = Directory::cache_stats().await;
        assert!(cache_size > 0);
    }
    
    #[tokio::test]
    async fn test_failed_connection() {
        let strategy = MockStrategy { should_fail: true };
        let result = Directory::new("/test", strategy).await;
        
        assert!(result.is_err());
    }
    
    #[tokio::test]
    #[ignore] // Run with: cargo test test_directory_with_ftp -- --ignored
    async fn test_directory_with_ftp() -> Result<()> {
        use crate::models::FtpDirectoryStrategy;
        
        // Test with real DataSUS FTP server using known path
        let strategy = FtpDirectoryStrategy::new_datasus();
        
        // Test directory creation and caching (note: path is relative to base_path)
        let mut dir = Directory::new("/SIHSUS/200801_/Dados", strategy).await?;
        
        println!("🔍 Created directory: {}", dir.path);
        println!("📂 Strategy: {}", dir.strategy_name());
        
        // Test directory loading with timing
        let start = std::time::Instant::now();
        dir.load().await?;
        let load_duration = start.elapsed();
        println!("✅ Directory loaded successfully in {:?}", load_duration);
        
        // Test content access
        let content = dir.content().await?;
        println!("📄 Found {} items", content.len());
        
        // Test file filtering
        let files = dir.files().await?;
        println!("📁 Found {} files", files.len());
        
        // Display first few items
        for (i, item) in content.iter().take(5).enumerate() {
            println!("  {}: {}", i + 1, item);
        }
        
        // Test caching works
        let (cache_size, cached_paths) = Directory::cache_stats().await;
        println!("💾 Cache size: {} directories", cache_size);
        for path in cached_paths.iter().take(3) {
            println!("  Cached: {}", path);
        }
        
        assert!(dir.loaded);
        assert!(!content.is_empty());
        
        // Performance summary
        println!("🚀 Performance Summary:");
        println!("   • Total time: {:?}", load_duration);
        println!("   • Files processed: {} files", files.len());
        println!("   • Rate: {:.1} files/second", files.len() as f64 / load_duration.as_secs_f64());
        println!("   • Directory hierarchy cached: {} levels", cache_size);
        
        Ok(())
    }
}
