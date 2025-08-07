use crate::models::file::File;
use crate::models::async_utils::async_cache;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::fmt;

/// Type alias for directory content
pub type DirectoryContent = HashMap<String, DirectoryEntry>;

/// Enum representing either a File or Directory entry
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DirectoryEntry {
    File(File),
    Directory(Directory),
}

/// Trait for different file system providers
#[async_trait]
pub trait FileSystemProvider: Send + Sync {
    /// List the contents of a directory
    async fn list_directory(&self, path: &str) -> Result<DirectoryContent, Box<dyn std::error::Error + Send + Sync>>;
    
    /// Check if a path exists
    async fn exists(&self, path: &str) -> Result<bool, Box<dyn std::error::Error + Send + Sync>>;
    
    /// Check if a path is a directory
    async fn is_directory(&self, path: &str) -> Result<bool, Box<dyn std::error::Error + Send + Sync>>;
    
    /// Get the name of the file system provider
    fn provider_name(&self) -> &'static str;
}

/// Local file system provider
#[derive(Debug, Clone)]
pub struct LocalFileSystemProvider;

#[async_trait]
impl FileSystemProvider for LocalFileSystemProvider {
    async fn list_directory(&self, path: &str) -> Result<DirectoryContent, Box<dyn std::error::Error + Send + Sync>> {
        use crate::models::async_utils::async_path_utils;
        use crate::models::file_info::{FileInfo, FileSize};
        use chrono::{DateTime, Utc};
        
        let mut content = DirectoryContent::new();
        let path_buf = PathBuf::from(path);
        
        if !async_path_utils::path_exists_async(&path_buf).await {
            return Ok(content);
        }
        
        let entries = async_path_utils::list_dir_async(&path_buf).await?;
        
        for entry in entries {
            let name = entry.file_name()
                .and_then(|n| n.to_str())
                .unwrap_or("unknown")
                .to_string();
                
            if async_path_utils::is_dir_async(&entry).await {
                let dir = Directory::new(entry.to_string_lossy().to_string()).await?;
                content.insert(name, DirectoryEntry::Directory(dir));
            } else {
                // Get file metadata
                let metadata = tokio::fs::metadata(&entry).await?;
                let size = FileSize::from_bytes(metadata.len());
                let modified: DateTime<Utc> = metadata.modified()?.into();
                
                let extension = entry.extension()
                    .and_then(|ext| ext.to_str())
                    .map(|ext| format!(".{}", ext))
                    .unwrap_or_default();
                
                let file_info = FileInfo::new(size, extension, modified);
                let file = File::new(path, &name, file_info);
                content.insert(name, DirectoryEntry::File(file));
            }
        }
        
        Ok(content)
    }
    
    async fn exists(&self, path: &str) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
        use crate::models::async_utils::async_path_utils;
        Ok(async_path_utils::path_exists_async(path).await)
    }
    
    async fn is_directory(&self, path: &str) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
        use crate::models::async_utils::async_path_utils;
        Ok(async_path_utils::is_dir_async(path).await)
    }
    
    fn provider_name(&self) -> &'static str {
        "local"
    }
}

/// FTP file system provider for DATASUS
#[derive(Debug, Clone)]
pub struct FtpFileSystemProvider {
    /// FTP server hostname
    pub host: String,
    /// Base path on the FTP server
    pub base_path: String,
    /// FTP port (default 21)
    pub port: u16,
}

impl FtpFileSystemProvider {
    /// Create a new FTP provider for DATASUS
    pub fn new_datasus() -> Self {
        Self {
            host: "ftp.datasus.gov.br".to_string(),
            base_path: "/dissemin/publicos".to_string(),
            port: 21,
        }
    }
    
    /// Create a new FTP provider with custom settings
    pub fn new(host: String, base_path: String, port: Option<u16>) -> Self {
        Self {
            host,
            base_path,
            port: port.unwrap_or(21),
        }
    }
    
    /// Parse FTP directory listing line
    /// Format: "MM-DD-YY HH:MMxm <DIR> name" or "MM-DD-YY HH:MMxm size name"
    pub fn parse_ftp_line(&self, line: &str, current_path: &str) -> Option<(String, DirectoryEntry)> {
        use crate::models::file_info::{FileInfo, FileSize};
        use chrono::{DateTime, Utc, NaiveDateTime};
        
        let parts: Vec<&str> = line.trim().split_whitespace().collect();
        if parts.len() < 4 {
            return None;
        }
        
        let date = parts[0];
        let time = parts[1];
        
        // Parse date and time
        let datetime_str = format!("{} {}", date, time);
        let naive_dt = NaiveDateTime::parse_from_str(&datetime_str, "%m-%d-%y %I:%M%p").ok()?;
        let modify: DateTime<Utc> = DateTime::from_naive_utc_and_offset(naive_dt, Utc);
        
        if parts[2] == "<DIR>" {
            // Directory entry
            let name = parts[3..].join(" ");
            let dir_path = if current_path.ends_with('/') {
                format!("{}{}", current_path, name)
            } else {
                format!("{}/{}", current_path, name)
            };
            
            // Create directory (this is a simplified version - we'll need to handle this properly)
            // For now, we'll create a basic directory structure
            let directory = Directory {
                path: dir_path,
                name: name.clone(),
                loaded: false,
                provider_type: "ftp".to_string(),
            };
            
            Some((name, DirectoryEntry::Directory(directory)))
        } else {
            // File entry
            let size_str = parts[2];
            let name = parts[3..].join(" ");
            
            // Parse file size
            let size = if let Ok(bytes) = size_str.parse::<u64>() {
                FileSize::from_bytes(bytes)
            } else {
                FileSize::from_string(size_str)
            };
            
            // Get file extension
            let extension = std::path::Path::new(&name)
                .extension()
                .and_then(|ext| ext.to_str())
                .map(|ext| format!(".{}", ext))
                .unwrap_or_default();
            
            let file_info = FileInfo::new(size, extension, modify);
            let file = File::new(current_path, &name, file_info);
            
            Some((name, DirectoryEntry::File(file)))
        }
    }
    
    /// Create FTP connection
    async fn create_connection(&self) -> Result<suppaftp::AsyncRustlsFtpStream, Box<dyn std::error::Error + Send + Sync>> {
        use suppaftp::{AsyncRustlsFtpStream, Mode};
        
        // Connect to FTP server
        let mut ftp_stream = AsyncRustlsFtpStream::connect(&format!("{}:{}", self.host, self.port)).await?;
        
        // Login as anonymous (DATASUS is public)
        ftp_stream.login("anonymous", "").await?;
        
        // Set passive mode (not async)
        ftp_stream.set_mode(Mode::Passive);
        
        Ok(ftp_stream)
    }
}

#[async_trait]
impl FileSystemProvider for FtpFileSystemProvider {
    async fn list_directory(&self, path: &str) -> Result<DirectoryContent, Box<dyn std::error::Error + Send + Sync>> {
        let mut content = DirectoryContent::new();
        let full_path = if path.starts_with('/') {
            format!("{}{}", self.base_path, path)
        } else {
            format!("{}/{}", self.base_path, path)
        };
        
        // Create connection
        let mut ftp_stream = self.create_connection().await?;
        
        // Change to target directory
        ftp_stream.cwd(&full_path).await?;
        
        // Get directory listing
        let lines = ftp_stream.list(None).await?;
        
        // Parse each line
        for line in lines {
            if let Some((name, entry)) = self.parse_ftp_line(&line, path) {
                // Filter out .DBF files if .DBC exists (as in Python version)
                if name.to_uppercase().ends_with(".DBF") {
                    let dbc_name = name.to_uppercase().replace(".DBF", ".DBC");
                    if content.contains_key(&dbc_name) {
                        continue; // Skip .DBF file
                    }
                }
                content.insert(name, entry);
            }
        }
        
        // Remove .DBF files if corresponding .DBC exists (post-processing)
        let to_remove: Vec<String> = content.keys()
            .filter(|name| {
                name.to_uppercase().ends_with(".DBF") && 
                content.contains_key(&name.to_uppercase().replace(".DBF", ".DBC"))
            })
            .cloned()
            .collect();
        
        for name in to_remove {
            content.remove(&name);
        }
        
        // Close connection
        let _ = ftp_stream.quit().await;
        
        Ok(content)
    }
    
    async fn exists(&self, path: &str) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
        let full_path = if path.starts_with('/') {
            format!("{}{}", self.base_path, path)
        } else {
            format!("{}/{}", self.base_path, path)
        };
        
        match self.create_connection().await {
            Ok(mut ftp_stream) => {
                match ftp_stream.cwd(&full_path).await {
                    Ok(_) => {
                        let _ = ftp_stream.quit().await;
                        Ok(true)
                    }
                    Err(_) => {
                        let _ = ftp_stream.quit().await;
                        Ok(false)
                    }
                }
            }
            Err(_) => Ok(false),
        }
    }
    
    async fn is_directory(&self, path: &str) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
        // For FTP, if we can cwd into it, it's a directory
        self.exists(path).await
    }
    
    fn provider_name(&self) -> &'static str {
        "ftp"
    }
}

/// S3 file system provider (placeholder for now)
#[derive(Debug, Clone)]
pub struct S3FileSystemProvider {
    // Will contain S3 connection details
}

#[async_trait]
impl FileSystemProvider for S3FileSystemProvider {
    async fn list_directory(&self, _path: &str) -> Result<DirectoryContent, Box<dyn std::error::Error + Send + Sync>> {
        // TODO: Implement S3 directory listing
        Ok(DirectoryContent::new())
    }
    
    async fn exists(&self, _path: &str) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
        // TODO: Implement S3 exists check
        Ok(false)
    }
    
    async fn is_directory(&self, _path: &str) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
        // TODO: Implement S3 directory check
        Ok(false)
    }
    
    fn provider_name(&self) -> &'static str {
        "s3"
    }
}

/// Directory class with async caching and lazy loading
/// 
/// The Directory struct represents a directory in a file system and includes
/// mechanisms for caching instances and lazy loading of directory content.
/// When a Directory instance is created, it normalizes the provided path
/// and caches the instance. The content of the directory is not loaded
/// immediately; instead, it is loaded when the `content` method is called.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Directory {
    /// The normalized path of the directory
    pub path: String,
    /// The name of the directory
    pub name: String,
    /// Indicates whether the directory content has been loaded
    pub loaded: bool,
    /// The file system provider type
    pub provider_type: String,
}

impl Directory {
    /// Creates a new Directory instance with default (local) provider
    pub async fn new(path: String) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        Self::new_with_provider(path, Arc::new(LocalFileSystemProvider)).await
    }
    
    /// Creates a new Directory instance with a specific provider
    pub async fn new_with_provider(
        path: String, 
        provider: Arc<dyn FileSystemProvider>
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let normalized_path = Self::normalize_path(&path);
        
        // Check cache first
        if let Some(cached) = async_cache::get_cached_directory_async(&normalized_path).await {
            // Try to deserialize from cache (simplified for now)
            if let Ok(dir) = serde_json::from_str::<Directory>(&cached) {
                return Ok(dir);
            }
        }
        
        // Parse path components
        let (_parent_path, name) = Self::split_path(&normalized_path);
        
        let directory = Directory {
            path: normalized_path.clone(),
            name,
            loaded: false,
            provider_type: provider.provider_name().to_string(),
        };
        
        // Cache the directory
        if let Ok(serialized) = serde_json::to_string(&directory) {
            async_cache::cache_directory_async(normalized_path, serialized).await;
        }
        
        Ok(directory)
    }
    
    /// Normalizes the given path
    pub fn normalize_path(path: &str) -> String {
        let path = if path.starts_with('/') {
            path.to_string()
        } else {
            format!("/{}", path)
        };
        
        // Remove trailing slash unless it's root
        if path.len() > 1 && path.ends_with('/') {
            path.trim_end_matches('/').to_string()
        } else {
            path
        }
    }
    
    /// Splits a path into parent path and name
    fn split_path(path: &str) -> (String, String) {
        if path == "/" {
            return ("/".to_string(), "/".to_string());
        }
        
        let path_buf = PathBuf::from(path);
        let name = path_buf.file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("")
            .to_string();
            
        let parent = path_buf.parent()
            .map(|p| p.to_string_lossy().to_string())
            .unwrap_or_else(|| "/".to_string());
            
        (parent, name)
    }
    
    /// Returns the content of the directory, loading it if necessary
    pub async fn content(&self) -> Result<Vec<DirectoryEntry>, Box<dyn std::error::Error + Send + Sync>> {
        self.content_with_provider(Arc::new(LocalFileSystemProvider)).await
    }
    
    /// Returns the content of the directory with a specific provider
    pub async fn content_with_provider(
        &self, 
        provider: Arc<dyn FileSystemProvider>
    ) -> Result<Vec<DirectoryEntry>, Box<dyn std::error::Error + Send + Sync>> {
        let content_map = provider.list_directory(&self.path).await?;
        Ok(content_map.into_values().collect())
    }
    
    /// Loads the content of the directory and returns a map
    pub async fn load(&self) -> Result<DirectoryContent, Box<dyn std::error::Error + Send + Sync>> {
        self.load_with_provider(Arc::new(LocalFileSystemProvider)).await
    }
    
    /// Loads the content of the directory with a specific provider
    pub async fn load_with_provider(
        &self, 
        provider: Arc<dyn FileSystemProvider>
    ) -> Result<DirectoryContent, Box<dyn std::error::Error + Send + Sync>> {
        provider.list_directory(&self.path).await
    }
    
    /// Reloads the content of the directory (clears cache and loads again)
    pub async fn reload(&self) -> Result<DirectoryContent, Box<dyn std::error::Error + Send + Sync>> {
        // Remove from cache
        async_cache::remove_cached_directory_async(&self.path).await;
        self.load().await
    }
    
    /// Get the parent directory
    pub async fn parent(&self) -> Result<Option<Directory>, Box<dyn std::error::Error + Send + Sync>> {
        if self.path == "/" {
            return Ok(None);
        }
        
        let (parent_path, _) = Self::split_path(&self.path);
        if parent_path == self.path {
            return Ok(None);
        }
        
        Ok(Some(Directory::new(parent_path).await?))
    }
    
    /// Check if this directory exists
    pub async fn exists(&self) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
        self.exists_with_provider(Arc::new(LocalFileSystemProvider)).await
    }
    
    /// Check if this directory exists with a specific provider
    pub async fn exists_with_provider(
        &self, 
        provider: Arc<dyn FileSystemProvider>
    ) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
        provider.exists(&self.path).await
    }
    
    /// Filter directory content by file extension
    pub async fn files_with_extension(&self, extension: &str) -> Result<Vec<File>, Box<dyn std::error::Error + Send + Sync>> {
        let content = self.content().await?;
        let mut files = Vec::new();
        
        for entry in content {
            if let DirectoryEntry::File(file) = entry {
                if file.has_extension(extension) {
                    files.push(file);
                }
            }
        }
        
        Ok(files)
    }
    
    /// Get all subdirectories
    pub async fn subdirectories(&self) -> Result<Vec<Directory>, Box<dyn std::error::Error + Send + Sync>> {
        let content = self.content().await?;
        let mut dirs = Vec::new();
        
        for entry in content {
            if let DirectoryEntry::Directory(dir) = entry {
                dirs.push(dir);
            }
        }
        
        Ok(dirs)
    }
    
    /// Get all files
    pub async fn files(&self) -> Result<Vec<File>, Box<dyn std::error::Error + Send + Sync>> {
        let content = self.content().await?;
        let mut files = Vec::new();
        
        for entry in content {
            if let DirectoryEntry::File(file) = entry {
                files.push(file);
            }
        }
        
        Ok(files)
    }
}

impl fmt::Display for Directory {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.path)
    }
}

impl std::hash::Hash for Directory {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.path.hash(state);
    }
}

impl PartialEq for Directory {
    fn eq(&self, other: &Self) -> bool {
        self.path == other.path
    }
}

impl Eq for Directory {}

/// Utility functions for working with directories
pub mod directory_utils {
    use super::*;

    /// Create a directory tree from a list of paths
    pub async fn create_directory_tree(paths: Vec<String>) -> Result<Vec<Directory>, Box<dyn std::error::Error + Send + Sync>> {
        let mut directories = Vec::new();
        
        for path in paths {
            let dir = Directory::new(path).await?;
            directories.push(dir);
        }
        
        Ok(directories)
    }
    
    /// Find all directories that match a pattern
    pub fn find_directories_matching(
        root: Directory, 
        pattern: String
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Vec<Directory>, Box<dyn std::error::Error + Send + Sync>>> + Send + 'static>> {
        Box::pin(async move {
            let mut matching = Vec::new();
            let subdirs = root.subdirectories().await?;
            
            for dir in subdirs {
                if dir.name.contains(&pattern) {
                    matching.push(dir.clone());
                }
                // Recursively search subdirectories
                let sub_matches = find_directories_matching(dir, pattern.clone()).await?;
                matching.extend(sub_matches);
            }
            
            Ok(matching)
        })
    }
    
    /// Get directory size (sum of all file sizes)
    pub fn get_directory_size(
        dir: Directory
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<u64, Box<dyn std::error::Error + Send + Sync>>> + Send + 'static>> {
        Box::pin(async move {
            let mut total_size = 0u64;
            let files = dir.files().await?;
            
            for file in files {
                if let Some(size) = file.size_bytes() {
                    total_size += size;
                }
            }
            
            // Recursively add subdirectory sizes
            let subdirs = dir.subdirectories().await?;
            for subdir in subdirs {
                total_size += get_directory_size(subdir).await?;
            }
            
            Ok(total_size)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;
    use tokio::fs;

    #[tokio::test]
    async fn test_normalize_path() {
        assert_eq!(Directory::normalize_path("/"), "/");
        assert_eq!(Directory::normalize_path("/home/user"), "/home/user");
        assert_eq!(Directory::normalize_path("/home/user/"), "/home/user");
        assert_eq!(Directory::normalize_path("home/user"), "/home/user");
        assert_eq!(Directory::normalize_path("user"), "/user");
    }

    #[tokio::test]
    async fn test_split_path() {
        let (parent, name) = Directory::split_path("/");
        assert_eq!(parent, "/");
        assert_eq!(name, "/");
        
        let (parent, name) = Directory::split_path("/home/user");
        assert_eq!(parent, "/home");
        assert_eq!(name, "user");
        
        let (parent, name) = Directory::split_path("/home");
        assert_eq!(parent, "/");
        assert_eq!(name, "home");
    }

    #[tokio::test]
    async fn test_directory_creation() {
        let dir = Directory::new("/tmp".to_string()).await;
        assert!(dir.is_ok());
        
        let dir = dir.unwrap();
        assert_eq!(dir.path, "/tmp");
        assert_eq!(dir.name, "tmp");
        assert!(!dir.loaded);
    }

    #[tokio::test]
    async fn test_root_directory() {
        let dir = Directory::new("/".to_string()).await;
        assert!(dir.is_ok());
        
        let dir = dir.unwrap();
        assert_eq!(dir.path, "/");
        assert_eq!(dir.name, "/");
    }

    #[tokio::test]
    async fn test_directory_equality() {
        let dir1 = Directory::new("/tmp".to_string()).await.unwrap();
        let dir2 = Directory::new("/tmp".to_string()).await.unwrap();
        let dir3 = Directory::new("/home".to_string()).await.unwrap();
        
        assert_eq!(dir1, dir2);
        assert_ne!(dir1, dir3);
    }

    #[tokio::test]
    async fn test_directory_display() {
        let dir = Directory::new("/home/user".to_string()).await.unwrap();
        assert_eq!(format!("{}", dir), "/home/user");
    }

    #[tokio::test]
    async fn test_parent_directory() {
        let dir = Directory::new("/home/user".to_string()).await.unwrap();
        let parent = dir.parent().await.unwrap();
        
        assert!(parent.is_some());
        let parent = parent.unwrap();
        assert_eq!(parent.path, "/home");
        assert_eq!(parent.name, "home");
    }

    #[tokio::test]
    async fn test_root_parent() {
        let root = Directory::new("/".to_string()).await.unwrap();
        let parent = root.parent().await.unwrap();
        assert!(parent.is_none());
    }

    #[tokio::test]
    async fn test_local_provider_name() {
        let provider = LocalFileSystemProvider;
        assert_eq!(provider.provider_name(), "local");
    }

    #[tokio::test]
    async fn test_directory_utils() {
        use directory_utils::*;
        
        let paths = vec!["/tmp".to_string(), "/home".to_string()];
        let dirs = create_directory_tree(paths).await.unwrap();
        
        assert_eq!(dirs.len(), 2);
        assert_eq!(dirs[0].path, "/tmp");
        assert_eq!(dirs[1].path, "/home");
    }

    #[tokio::test]
    async fn test_real_directory_operations() {
        // Create a temporary directory for testing
        let temp_dir = TempDir::new().unwrap();
        let temp_path = temp_dir.path().to_string_lossy().to_string();
        
        // Create some test files and subdirectories
        let sub_dir_path = temp_dir.path().join("subdir");
        fs::create_dir(&sub_dir_path).await.unwrap();
        
        let test_file_path = temp_dir.path().join("test.txt");
        fs::write(&test_file_path, "Hello, World!").await.unwrap();
        
        let sub_file_path = sub_dir_path.join("sub_test.csv");
        fs::write(&sub_file_path, "col1,col2\n1,2").await.unwrap();
        
        // Test directory creation and listing
        let dir = Directory::new(temp_path.clone()).await.unwrap();
        assert_eq!(dir.name, temp_dir.path().file_name().unwrap().to_string_lossy());
        
        // Test directory content loading
        let content = dir.content().await.unwrap();
        assert_eq!(content.len(), 2); // Should contain subdir and test.txt
        
        // Check that we have both a file and a directory
        let mut has_file = false;
        let mut has_dir = false;
        
        for entry in &content {
            match entry {
                DirectoryEntry::File(file) => {
                    if file.basename == "test.txt" {
                        has_file = true;
                        assert!(file.has_extension("txt"));
                    }
                }
                DirectoryEntry::Directory(subdir) => {
                    if subdir.name == "subdir" {
                        has_dir = true;
                    }
                }
            }
        }
        
        assert!(has_file, "Should have found test.txt file");
        assert!(has_dir, "Should have found subdir directory");
        
        // Test getting files only
        let files = dir.files().await.unwrap();
        assert_eq!(files.len(), 1);
        assert_eq!(files[0].basename, "test.txt");
        
        // Test getting subdirectories only
        let subdirs = dir.subdirectories().await.unwrap();
        assert_eq!(subdirs.len(), 1);
        assert_eq!(subdirs[0].name, "subdir");
        
        // Test files with extension
        let txt_files = dir.files_with_extension("txt").await.unwrap();
        assert_eq!(txt_files.len(), 1);
        assert_eq!(txt_files[0].basename, "test.txt");
        
        let csv_files = dir.files_with_extension("csv").await.unwrap();
        assert_eq!(csv_files.len(), 0); // CSV file is in subdirectory
        
        // Test subdirectory operations
        let subdir = &subdirs[0];
        let subdir_files = subdir.files().await.unwrap();
        assert_eq!(subdir_files.len(), 1);
        assert_eq!(subdir_files[0].basename, "sub_test.csv");
        
        let csv_files_in_subdir = subdir.files_with_extension("csv").await.unwrap();
        assert_eq!(csv_files_in_subdir.len(), 1);
    }

    #[tokio::test]
    async fn test_directory_exists() {
        // Test with a real directory
        let temp_dir = TempDir::new().unwrap();
        let temp_path = temp_dir.path().to_string_lossy().to_string();
        
        let dir = Directory::new(temp_path).await.unwrap();
        let exists = dir.exists().await.unwrap();
        assert!(exists, "Temporary directory should exist");
        
        // Test with a non-existent directory
        let fake_dir = Directory::new("/this/path/should/not/exist".to_string()).await.unwrap();
        let exists = fake_dir.exists().await.unwrap();
        assert!(!exists, "Fake directory should not exist");
    }

    #[tokio::test]
    async fn test_directory_parent() {
        // Create a temporary directory for testing
        let temp_dir = TempDir::new().unwrap();
        let temp_path = temp_dir.path().to_string_lossy().to_string();
        
        // Create a subdirectory
        let sub_dir_path = temp_dir.path().join("subdir");
        fs::create_dir(&sub_dir_path).await.unwrap();
        
        let subdir = Directory::new(sub_dir_path.to_string_lossy().to_string()).await.unwrap();
        let parent = subdir.parent().await.unwrap();
        
        assert!(parent.is_some());
        let parent = parent.unwrap();
        assert_eq!(parent.path, temp_path);
    }

    #[tokio::test]
    async fn test_directory_reload() {
        // Create a temporary directory for testing
        let temp_dir = TempDir::new().unwrap();
        let temp_path = temp_dir.path().to_string_lossy().to_string();
        
        let dir = Directory::new(temp_path).await.unwrap();
        
        // Initial load
        let content1 = dir.load().await.unwrap();
        let initial_count = content1.len();
        
        // Add a new file
        let new_file_path = temp_dir.path().join("new_file.txt");
        fs::write(&new_file_path, "New content").await.unwrap();
        
        // Reload and check if new file is detected
        let content2 = dir.reload().await.unwrap();
        assert_eq!(content2.len(), initial_count + 1);
        
        let has_new_file = content2.values().any(|entry| {
            matches!(entry, DirectoryEntry::File(file) if file.basename == "new_file.txt")
        });
        assert!(has_new_file, "New file should be detected after reload");
    }

    #[tokio::test]
    async fn test_directory_size_calculation() {
        use directory_utils::*;
        
        // Create a temporary directory for testing
        let temp_dir = TempDir::new().unwrap();
        let temp_path = temp_dir.path().to_string_lossy().to_string();
        
        // Create test files with known sizes
        let file1_content = "Hello, World!"; // 13 bytes
        let file2_content = "This is a test file with more content."; // 38 bytes
        
        fs::write(temp_dir.path().join("file1.txt"), file1_content).await.unwrap();
        fs::write(temp_dir.path().join("file2.txt"), file2_content).await.unwrap();
        
        // Create subdirectory with a file
        let sub_dir_path = temp_dir.path().join("subdir");
        fs::create_dir(&sub_dir_path).await.unwrap();
        let file3_content = "Sub file"; // 8 bytes
        fs::write(sub_dir_path.join("file3.txt"), file3_content).await.unwrap();
        
        let dir = Directory::new(temp_path).await.unwrap();
        let total_size = get_directory_size(dir).await.unwrap();
        
        // Should be 13 + 38 + 8 = 59 bytes
        assert_eq!(total_size, 59);
    }

    #[tokio::test]
    async fn test_local_provider() {
        let provider = LocalFileSystemProvider;
        assert_eq!(provider.provider_name(), "local");
        
        // Test with current directory
        let current_dir = std::env::current_dir().unwrap().to_string_lossy().to_string();
        let exists = provider.exists(&current_dir).await.unwrap();
        assert!(exists);
        
        let is_dir = provider.is_directory(&current_dir).await.unwrap();
        assert!(is_dir);
    }

    #[tokio::test]
    async fn test_ftp_provider_creation() {
        let ftp_provider = FtpFileSystemProvider::new_datasus();
        assert_eq!(ftp_provider.provider_name(), "ftp");
        assert_eq!(ftp_provider.host, "ftp.datasus.gov.br");
        assert_eq!(ftp_provider.base_path, "/dissemin/publicos");
        assert_eq!(ftp_provider.port, 21);
        
        let custom_provider = FtpFileSystemProvider::new(
            "custom.ftp.com".to_string(),
            "/custom/path".to_string(),
            Some(2121)
        );
        assert_eq!(custom_provider.host, "custom.ftp.com");
        assert_eq!(custom_provider.base_path, "/custom/path");
        assert_eq!(custom_provider.port, 2121);
    }

    #[tokio::test]
    async fn test_ftp_line_parsing() {
        let ftp_provider = FtpFileSystemProvider::new_datasus();
        
        // Test directory parsing
        let dir_line = "12-01-23 02:30PM    <DIR>          SIASUS";
        let result = ftp_provider.parse_ftp_line(dir_line, "/test");
        assert!(result.is_some());
        let (name, entry) = result.unwrap();
        assert_eq!(name, "SIASUS");
        assert!(matches!(entry, DirectoryEntry::Directory(_)));
        
        // Test file parsing
        let file_line = "12-01-23 02:30PM              1024 test.txt";
        let result = ftp_provider.parse_ftp_line(file_line, "/test");
        assert!(result.is_some());
        let (name, entry) = result.unwrap();
        assert_eq!(name, "test.txt");
        assert!(matches!(entry, DirectoryEntry::File(_)));
        
        // Test invalid line
        let invalid_line = "invalid line";
        let result = ftp_provider.parse_ftp_line(invalid_line, "/test");
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_ftp_provider_with_directory() {
        // Test creating directory with FTP provider
        let ftp_provider = Arc::new(FtpFileSystemProvider::new_datasus());
        
        // This would test against a real FTP server - for now we just test the provider setup
        let result = Directory::new_with_provider(
            "/SIASUS".to_string(),
            ftp_provider.clone()
        ).await;
        
        assert!(result.is_ok());
        let dir = result.unwrap();
        assert_eq!(dir.path, "/SIASUS");
        assert_eq!(dir.provider_type, "ftp");
        
        // Note: Actual FTP operations would require network access
        // In a real scenario, we'd test with a mock FTP server
    }
}
