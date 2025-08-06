use std::collections::HashMap;
use std::sync::Arc;

use anyhow::Result;
use chrono::{NaiveDateTime, Utc};
use regex::Regex;
use lazy_static::lazy_static;

use super::directory::{DirectoryContent, DirectoryItem, DirectoryListingStrategy};
use super::file::File;
use super::file_info::{FileInfo, FileSize};
use super::ftp_connection_manager::FtpConnectionManager;

// Pre-compile regex patterns for better performance
lazy_static! {
    static ref WINDOWS_FTP_REGEX: Regex = Regex::new(
        r"^(\d{2}-\d{2}-\d{2})\s+(\d{1,2}:\d{2}[AP]M)\s+(?:(<DIR>)|(\d+))\s+(.+)$"
    ).unwrap();
}

/// FTP-based strategy for listing directory contents
/// 
/// This strategy connects to DataSUS FTP servers to list directory contents.
/// It parses FTP LIST command output and creates File and Directory instances.
/// Uses connection manager for efficient connection reuse.
#[derive(Clone)]
pub struct FtpDirectoryStrategy {
    /// Connection manager for FTP operations
    connection_manager: Arc<FtpConnectionManager>,
    /// Base path for DataSUS FTP (e.g., "/dissemin/publicos")
    base_path: String,
}

impl FtpDirectoryStrategy {
    /// Create a new FTP strategy for DataSUS
    pub fn new_datasus() -> Self {
        Self {
            connection_manager: Arc::new(FtpConnectionManager::new_datasus()),
            base_path: "/dissemin/publicos".to_string(),
        }
    }
    
    /// Create a new FTP strategy with custom parameters
    pub fn new(host: String, port: u16, username: String, password: String, base_path: String) -> Self {
        Self {
            connection_manager: Arc::new(FtpConnectionManager::new(host, port, username, password)),
            base_path,
        }
    }
    
    /// Create a new FTP strategy with existing connection manager
    pub fn with_connection_manager(manager: Arc<FtpConnectionManager>, base_path: String) -> Self {
        Self {
            connection_manager: manager,
            base_path,
        }
    }
    
    /// Parse a line from FTP LIST command output
    fn parse_list_line(&self, line: &str, directory_path: &str) -> Option<(String, DirectoryItem)> {
        // Remove carriage return and extra whitespace
        let line = line.trim();
        if line.is_empty() {
            return None;
        }
        
        // Try to parse as Windows-style FTP listing (used by DataSUS)
        if let Some((name, item)) = self.parse_windows_list_line(line, directory_path) {
            return Some((name, item));
        }
        
        // Try to parse as Unix-style FTP listing
        if let Some((name, item)) = self.parse_unix_list_line(line, directory_path) {
            return Some((name, item));
        }
        
        None
    }
    
    /// Parse Windows-style FTP LIST output (MM-dd-yy HH:MMam/pm <DIR> filename)
    fn parse_windows_list_line(&self, line: &str, directory_path: &str) -> Option<(String, DirectoryItem)> {
        // Use pre-compiled regex for better performance
        if let Some(captures) = WINDOWS_FTP_REGEX.captures(line) {
            let date_str = captures.get(1)?.as_str();
            let time_str = captures.get(2)?.as_str();
            let is_dir = captures.get(3).is_some();
            let size_str = captures.get(4).map(|m| m.as_str()).unwrap_or("0");
            let name = captures.get(5)?.as_str().trim().to_string();
            
            // Skip current and parent directory entries
            if name == "." || name == ".." {
                return None;
            }
            
            // Parse datetime
            let datetime_str = format!("{} {}", date_str, time_str);
            let datetime = NaiveDateTime::parse_from_str(&datetime_str, "%m-%d-%y %I:%M%p")
                .ok()?
                .and_utc();
            
            if is_dir {
                // Create subdirectory - Note: This would need the strategy to avoid infinite recursion
                // For now, we'll create a minimal directory representation
                let subdir_path = if directory_path.ends_with('/') {
                    format!("{}{}", directory_path, name)
                } else {
                    format!("{}/{}", directory_path, name)
                };
                
                // We can't easily create a Directory here due to async trait constraints
                // This is a limitation we'll need to address in the architecture
                log::debug!("Found directory: {} at {}", name, subdir_path);
                return None; // Skip directories for now
            } else {
                // Create file
                let size = size_str.parse::<u64>().unwrap_or(0);
                let file_extension = std::path::Path::new(&name)
                    .extension()
                    .and_then(|ext| ext.to_str())
                    .map(|ext| format!(".{}", ext))
                    .unwrap_or_default();
                
                let file_info = FileInfo::new(
                    FileSize::from_bytes(size),
                    file_extension,
                    datetime,
                );
                
                let file = File::new(directory_path, &name, file_info);
                return Some((name, DirectoryItem::File(file)));
            }
        }
        
        None
    }
    
    /// Parse Unix-style FTP LIST output (permissions size date time filename)
    fn parse_unix_list_line(&self, line: &str, directory_path: &str) -> Option<(String, DirectoryItem)> {
        let parts: Vec<&str> = line.split_whitespace().collect();
        if parts.len() < 9 {
            return None;
        }
        
        let permissions = parts[0];
        let size_str = parts[4];
        let name = parts[8..].join(" ");
        
        // Skip current and parent directory entries
        if name == "." || name == ".." {
            return None;
        }
        
        let is_dir = permissions.starts_with('d');
        
        // Use current time as fallback for datetime
        let datetime = Utc::now();
        
        if is_dir {
            log::debug!("Found directory: {} at {}", name, directory_path);
            return None; // Skip directories for now
        } else {
            let size = size_str.parse::<u64>().unwrap_or(0);
            let file_extension = std::path::Path::new(&name)
                .extension()
                .and_then(|ext| ext.to_str())
                .map(|ext| format!(".{}", ext))
                .unwrap_or_default();
            
            let file_info = FileInfo::new(
                FileSize::from_bytes(size),
                file_extension,
                datetime,
            );
            
            let file = File::new(directory_path, &name, file_info);
            return Some((name, DirectoryItem::File(file)));
        }
    }
    
    /// Filter out .DBF files if corresponding .DBC file exists
    fn filter_dbf_files(&self, content: &mut DirectoryContent) {
        let dbf_to_remove: Vec<String> = content
            .keys()
            .filter(|name| {
                name.to_uppercase().ends_with(".DBF") && {
                    let base_name = name.to_uppercase().replace(".DBF", "");
                    // Check if any key in content matches the .DBC pattern (case insensitive)
                    content.keys().any(|key| key.to_uppercase() == format!("{}.DBC", base_name))
                }
            })
            .cloned()
            .collect();
        
        for name in dbf_to_remove {
            content.remove(&name);
            log::debug!("Removed .DBF file {} (corresponding .DBC exists)", name);
        }
    }
}

#[async_trait::async_trait]
impl DirectoryListingStrategy for FtpDirectoryStrategy {
    async fn list_directory(&self, path: &str) -> Result<DirectoryContent> {
        let start = std::time::Instant::now();
        
        // Normalize path like Python version - remove trailing slash
        let normalized_path = if path.ends_with('/') && path != "/" {
            &path[..path.len() - 1]
        } else {
            path
        };
        
        let full_path = format!("{}{}", self.base_path, normalized_path);
        let path_for_parsing = normalized_path.to_string();
        let strategy_clone = self.clone(); // Need to clone self to move into closure
        
        let content = self.connection_manager.managed_connection(move |ftp| {
            Box::pin(async move {
                // Change to the target directory (like Python's ftp.cwd(path))
                ftp.cwd(&full_path).await.map_err(|e| {
                    anyhow::anyhow!("Failed to change to directory {}: {}", full_path, e)
                })?;
                
                // Use LIST command to get detailed file information
                // Python uses ftp.retrlines("LIST", callback) - we use list() which is similar
                let lines = ftp.list(None).await.map_err(|e| {
                    anyhow::anyhow!("Failed to list directory {}: {}", full_path, e)
                })?;
                
                let mut content = HashMap::new();
                let mut file_count = 0;
                let total_lines = lines.len();
                let parsing_start = std::time::Instant::now();
                
                // Parse each line from LIST command (streaming-like parsing)
                for line in lines {
                    if let Some((name, item)) = strategy_clone.parse_list_line(&line, &path_for_parsing) {
                        content.insert(name, item);
                        file_count += 1;
                    }
                }
                
                let parsing_duration = parsing_start.elapsed();
                log::info!("Parsed {} lines into {} items in {:?}", total_lines, file_count, parsing_duration);
                
                // Apply DataSUS-specific filtering (like Python's to_remove logic)
                let filter_start = std::time::Instant::now();
                strategy_clone.filter_dbf_files(&mut content);
                let filter_duration = filter_start.elapsed();
                
                let duration = start.elapsed();
                log::info!("Listed directory {} with {} items in {:?} (parsing: {:?}, filtering: {:?})", 
                    full_path, content.len(), duration, parsing_duration, filter_duration);
                Ok::<DirectoryContent, anyhow::Error>(content)
            })
        }).await?;
        
        Ok(content)
    }
    
    async fn verify_connection(&self) -> bool {
        self.connection_manager.verify_connection().await
    }
    
    fn strategy_name(&self) -> &str {
        "FTP"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_parse_windows_list_line() {
        let strategy = FtpDirectoryStrategy::new_datasus();
        
        // Test directory line
        let dir_line = "12-31-23  03:45PM       <DIR>          SIASUS";
        let result = strategy.parse_windows_list_line(dir_line, "/test");
        assert!(result.is_none()); // Directories are skipped for now
        
        // Test file line
        let file_line = "01-15-24  09:30AM              1024 test.dbc";
        let result = strategy.parse_windows_list_line(file_line, "/test");
        assert!(result.is_some());
        
        if let Some((name, item)) = result {
            assert_eq!(name, "test.dbc");
            match item {
                DirectoryItem::File(file) => {
                    assert_eq!(file.basename, "test.dbc");
                    assert_eq!(file.size_bytes(), Some(1024));
                }
                _ => panic!("Expected file item"),
            }
        }
    }
    
    #[test]
    fn test_filter_dbf_files() {
        let strategy = FtpDirectoryStrategy::new_datasus();
        let mut content = HashMap::new();
        
        // Add both .DBF and .DBC files
        let file_info = FileInfo::new(
            FileSize::from_bytes(1024),
            ".dbf".to_string(),
            Utc::now(),
        );
        let dbf_file = File::new("/test", "data.DBF", file_info.clone());
        content.insert("data.DBF".to_string(), DirectoryItem::File(dbf_file));
        
        let file_info = FileInfo::new(
            FileSize::from_bytes(2048),
            ".dbc".to_string(),
            Utc::now(),
        );
        let dbc_file = File::new("/test", "data.DBC", file_info);
        content.insert("data.DBC".to_string(), DirectoryItem::File(dbc_file));
        
        assert_eq!(content.len(), 2);
        
        strategy.filter_dbf_files(&mut content);
        
        // .DBF should be removed, .DBC should remain
        assert_eq!(content.len(), 1);
        assert!(content.contains_key("data.DBC"));
        assert!(!content.contains_key("data.DBF"));
    }
    
    #[tokio::test]
    #[ignore] // Requires real FTP connection
    async fn test_real_ftp_connection() {
        let strategy = FtpDirectoryStrategy::new_datasus();
        
        let is_connected = strategy.verify_connection().await;
        println!("FTP connection test: {}", if is_connected { "SUCCESS" } else { "FAILED" });
        
        if is_connected {
            let result = strategy.list_directory("/dissemin/publicos/SIHSUS/200801_/Dados").await;
            match result {
                Ok(content) => {
                    println!("Found {} items in directory", content.len());
                    for (name, item) in content.iter().take(5) {
                        println!("  {}: {}", name, item);
                    }
                }
                Err(e) => {
                    println!("Failed to list directory: {}", e);
                }
            }
        }
    }
}
