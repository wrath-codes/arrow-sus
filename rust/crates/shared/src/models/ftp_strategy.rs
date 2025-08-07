use crate::models::{
    strategy::{ConnectionError, DirectoryItem, SourceStrategy},
    ftp_manager::FtpConnectionManager,
    file_info::{FileInfo, FileSize},
    file::File,
};
use async_trait::async_trait;
use chrono::{DateTime, NaiveDateTime, Utc};
use regex::Regex;
use std::collections::HashMap;

/// FTP strategy implementation for DATASUS
/// 
/// This strategy connects to the DATASUS FTP server and parses directory listings
/// in the specific format used by the DATASUS infrastructure.
#[derive(Debug, Clone)]
pub struct FtpStrategy {
    /// Host for the FTP server
    host: String,
    /// Base path for DATASUS data
    base_path: String,
    /// FTP connection manager
    connection_manager: FtpConnectionManager,
    /// Regex for parsing directory entries
    dir_regex: Regex,
    /// Regex for parsing file entries
    file_regex: Regex,
}

impl FtpStrategy {
    /// Default DATASUS FTP host
    pub const DEFAULT_HOST: &'static str = "ftp.datasus.gov.br";
    /// Default DATASUS base path
    pub const DEFAULT_BASE_PATH: &'static str = "/dissemin/publicos";

    /// Creates a new FTP strategy with default DATASUS settings
    pub fn new() -> Result<Self, ConnectionError> {
        Self::with_host(Self::DEFAULT_HOST.to_string())
    }

    /// Creates a new FTP strategy with custom host
    pub fn with_host(host: String) -> Result<Self, ConnectionError> {
        Self::with_host_and_base_path(host, Self::DEFAULT_BASE_PATH.to_string())
    }

    /// Creates a new FTP strategy with custom host and base path
    pub fn with_host_and_base_path(host: String, base_path: String) -> Result<Self, ConnectionError> {
        let connection_manager = FtpConnectionManager::new(host.clone());

        // Regex patterns for parsing DATASUS FTP listings
        // Directory format: "MM-DD-YY  HH:MMxM       <DIR>          name"
        let dir_regex = Regex::new(r"^(\d{2}-\d{2}-\d{2})\s+(\d{1,2}:\d{2}[AP]M)\s+<DIR>\s+(.+)$")
            .map_err(|e| ConnectionError::Connection {
                message: format!("Failed to compile directory regex: {}", e),
            })?;

        // File format: "MM-DD-YY  HH:MMxM            size name"
        let file_regex = Regex::new(r"^(\d{2}-\d{2}-\d{2})\s+(\d{1,2}:\d{2}[AP]M)\s+(\d+)\s+(.+)$")
            .map_err(|e| ConnectionError::Connection {
                message: format!("Failed to compile file regex: {}", e),
            })?;

        Ok(Self {
            host,
            base_path,
            connection_manager,
            dir_regex,
            file_regex,
        })
    }

    /// Parses a datetime string from DATASUS FTP format
    fn parse_datetime(&self, date_str: &str, time_str: &str) -> Result<DateTime<Utc>, ConnectionError> {
        Self::parse_datetime_static(date_str, time_str)
    }

    /// Static version of datetime parsing for use in closures
    fn parse_datetime_static(date_str: &str, time_str: &str) -> Result<DateTime<Utc>, ConnectionError> {
        let datetime_str = format!("{} {}", date_str, time_str);
        
        // Try parsing with different year formats (DATASUS uses 2-digit years)
        let parsed = NaiveDateTime::parse_from_str(&datetime_str, "%m-%d-%y %I:%M%p")
            .map_err(|e| ConnectionError::Connection {
                message: format!("Failed to parse datetime '{}': {}", datetime_str, e),
            })?;

        Ok(DateTime::from_naive_utc_and_offset(parsed, Utc))
    }

    /// Parses a single line from FTP LIST output (instance method)
    fn parse_listing_line(
        &self,
        line: &str,
        current_path: &str,
    ) -> Result<Option<(String, DirectoryItem)>, ConnectionError> {
        Self::parse_listing_line_static(line, current_path, &self.dir_regex, &self.file_regex)
    }

    /// Parses a single line from FTP LIST output (static version for closures)
    fn parse_listing_line_static(
        line: &str,
        current_path: &str,
        dir_regex: &Regex,
        file_regex: &Regex,
    ) -> Result<Option<(String, DirectoryItem)>, ConnectionError> {
        let line = line.trim();
        
        // Skip empty lines and common FTP status lines
        if line.is_empty() || line.starts_with("total ") || line.starts_with("d") {
            return Ok(None);
        }

        // Try to match directory pattern first
        if let Some(captures) = dir_regex.captures(line) {
            let date_str = captures.get(1).unwrap().as_str();
            let time_str = captures.get(2).unwrap().as_str();
            let name = captures.get(3).unwrap().as_str().trim();

            let modify = Self::parse_datetime_static(date_str, time_str)?;
            
            let info = FileInfo::new(
                FileSize::from_bytes(0), // Directories have no size
                "dir".to_string(),
                modify,
            );

            let directory_path = if current_path.ends_with('/') {
                format!("{}{}", current_path, name)
            } else {
                format!("{}/{}", current_path, name)
            };

            let directory = crate::models::strategy::Directory::new(directory_path, name.to_string());

            return Ok(Some((name.to_string(), DirectoryItem::Directory(directory))));
        }

        // Try to match file pattern
        if let Some(captures) = file_regex.captures(line) {
            let date_str = captures.get(1).unwrap().as_str();
            let time_str = captures.get(2).unwrap().as_str();
            let size_str = captures.get(3).unwrap().as_str();
            let name = captures.get(4).unwrap().as_str().trim();

            let modify = Self::parse_datetime_static(date_str, time_str)?;
            
            let size = size_str.parse::<u64>()
                .map_err(|e| ConnectionError::Connection {
                    message: format!("Failed to parse file size '{}': {}", size_str, e),
                })?;

            // Determine file type from extension
            let file_type = std::path::Path::new(name)
                .extension()
                .and_then(|ext| ext.to_str())
                .unwrap_or("file")
                .to_string();

            let info = FileInfo::new(
                FileSize::from_bytes(size),
                file_type,
                modify,
            );

            let file = File::new(current_path, name, info);

            return Ok(Some((name.to_string(), DirectoryItem::File(file))));
        }

        // If we can't parse the line, log it but don't fail
        log::debug!("Could not parse FTP listing line: '{}'", line);
        Ok(None)
    }

    /// Constructs the full path for FTP operations
    fn build_full_path(&self, path: &str) -> String {
        if path.starts_with('/') {
            // Absolute path - use as-is if it already includes base_path
            if path.starts_with(&self.base_path) {
                path.to_string()
            } else {
                format!("{}{}", self.base_path, path)
            }
        } else {
            // Relative path - always prepend base_path
            if path.is_empty() {
                self.base_path.clone()
            } else {
                format!("{}/{}", self.base_path, path)
            }
        }
    }
}

impl Default for FtpStrategy {
    fn default() -> Self {
        Self::new().expect("Failed to create default FTP strategy")
    }
}

#[async_trait]
impl SourceStrategy for FtpStrategy {
    async fn verify_connection(&self) -> bool {
        self.connection_manager.test_connection().await
    }

    async fn list_directory(
        &self,
        path: &str,
    ) -> Result<HashMap<String, DirectoryItem>, ConnectionError> {
        let full_path = self.build_full_path(path);
        let path_owned = path.to_string();
        
        // Clone the regex patterns and self for use in the closure
        let dir_regex = self.dir_regex.clone();
        let file_regex = self.file_regex.clone();

        let result = self.connection_manager.with_connection(move |ftp| {
            Box::pin(async move {
                let mut content = HashMap::new();
                
                // Navigate to the target directory
                if let Err(e) = ftp.cwd(&full_path).await {
                    return Err(ConnectionError::FtpConnection {
                        message: format!("Failed to change to directory '{}': {}", full_path, e),
                    });
                }

                // Get listing using NLST command (simpler, works better with DATASUS)
                match ftp.nlst(None).await {
                    Ok(listing) => {
                        for name in listing {
                            let name = name.trim();
                            if !name.is_empty() {
                                // For NLST, we don't get metadata, so we create basic items
                                // We'll need to make additional requests to get full metadata
                                let item = if name.contains('.') {
                                    // Likely a file (has extension)
                                    let info = FileInfo::new(
                                        FileSize::from_bytes(0), // Size unknown from NLST
                                        std::path::Path::new(name)
                                            .extension()
                                            .and_then(|ext| ext.to_str())
                                            .unwrap_or("file")
                                            .to_string(),
                                        Utc::now(), // Timestamp unknown from NLST
                                    );
                                    DirectoryItem::File(File::new(&path_owned, name, info))
                                } else {
                                    // Likely a directory (no extension)
                                    let directory_path = if path_owned.ends_with('/') {
                                        format!("{}{}", path_owned, name)
                                    } else {
                                        format!("{}/{}", path_owned, name)
                                    };
                                    DirectoryItem::Directory(
                                        crate::models::strategy::Directory::new(directory_path, name.to_string())
                                    )
                                };
                                content.insert(name.to_string(), item);
                            }
                        }
                        Ok(content)
                    }
                    Err(e) => Err(ConnectionError::FtpConnection {
                        message: format!("Failed to list directory '{}': {}", full_path, e),
                    }),
                }
            })
        }).await?;

        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ftp_strategy_creation() {
        let strategy = FtpStrategy::new().unwrap();
        assert_eq!(strategy.host, FtpStrategy::DEFAULT_HOST);
        assert_eq!(strategy.base_path, FtpStrategy::DEFAULT_BASE_PATH);
    }

    #[test]
    fn test_custom_host() {
        let custom_host = "custom.ftp.server".to_string();
        let strategy = FtpStrategy::with_host(custom_host.clone()).unwrap();
        assert_eq!(strategy.host, custom_host);
        assert_eq!(strategy.base_path, FtpStrategy::DEFAULT_BASE_PATH);
    }

    #[test]
    fn test_path_building() {
        let strategy = FtpStrategy::new().unwrap();
        
        // Test relative paths
        assert_eq!(strategy.build_full_path("SIASUS"), "/dissemin/publicos/SIASUS");
        assert_eq!(strategy.build_full_path("SIASUS/200801"), "/dissemin/publicos/SIASUS/200801");
        
        // Test absolute paths
        assert_eq!(strategy.build_full_path("/SIASUS"), "/dissemin/publicos/SIASUS");
        assert_eq!(strategy.build_full_path("/dissemin/publicos/SIASUS"), "/dissemin/publicos/SIASUS");
        
        // Test empty path
        assert_eq!(strategy.build_full_path(""), "/dissemin/publicos");
    }

    #[test]
    fn test_datetime_parsing() {
        let strategy = FtpStrategy::new().unwrap();
        
        // Test typical DATASUS format
        let result = strategy.parse_datetime("02-24-18", "07:38AM");
        assert!(result.is_ok());
        
        let datetime = result.unwrap();
        assert_eq!(datetime.format("%Y-%m-%d").to_string(), "2018-02-24");
        assert_eq!(datetime.format("%H:%M").to_string(), "07:38");
    }

    #[test]
    fn test_directory_line_parsing() {
        let strategy = FtpStrategy::new().unwrap();
        
        let line = "02-24-18  07:38AM       <DIR>          199407_200712";
        let result = strategy.parse_listing_line(line, "/SIASUS").unwrap();
        
        assert!(result.is_some());
        let (name, item) = result.unwrap();
        assert_eq!(name, "199407_200712");
        
        match item {
            DirectoryItem::Directory(dir) => {
                assert_eq!(dir.name, "199407_200712");
                assert_eq!(dir.path, "/SIASUS/199407_200712");
            }
            _ => panic!("Expected directory item"),
        }
    }

    #[test]
    fn test_file_line_parsing() {
        let strategy = FtpStrategy::new().unwrap();
        
        let line = "12-01-20  10:30AM         1234567 SIAPA0001.dbc";
        let result = strategy.parse_listing_line(line, "/SIASUS/200801").unwrap();
        
        assert!(result.is_some());
        let (name, item) = result.unwrap();
        assert_eq!(name, "SIAPA0001.dbc");
        
        match item {
            DirectoryItem::File(file) => {
                assert_eq!(file.get_basename(), "SIAPA0001.dbc");
                assert_eq!(file.size_bytes(), Some(1234567));
                assert_eq!(file.get_extension(), ".dbc");
            }
            _ => panic!("Expected file item"),
        }
    }

    #[test]
    fn test_invalid_line_handling() {
        let strategy = FtpStrategy::new().unwrap();
        
        // Test various invalid/unparseable lines
        let invalid_lines = vec![
            "",
            "total 1234",
            "invalid format line",
            "drwxr-xr-x  2 user group 4096 Jan 01 12:00 dirname", // Unix format
        ];
        
        for line in invalid_lines {
            let result = strategy.parse_listing_line(line, "/test").unwrap();
            assert!(result.is_none(), "Should not parse invalid line: '{}'", line);
        }
    }

    #[tokio::test]
    #[ignore] // Integration test - requires network
    async fn test_real_ftp_connection() {
        let strategy = FtpStrategy::new().unwrap();
        assert!(strategy.verify_connection().await);
    }

    #[tokio::test]
    #[ignore] // Integration test - requires network
    async fn test_real_directory_listing() {
        let strategy = FtpStrategy::new().unwrap();
        
        let result = strategy.list_directory("").await;
        assert!(result.is_ok());
        
        let content = result.unwrap();
        assert!(!content.is_empty());
        
        println!("Found {} entries:", content.len());
        for (name, item) in &content {
            match item {
                DirectoryItem::Directory(_) => println!("  DIR:  {}", name),
                DirectoryItem::File(_) => println!("  FILE: {}", name),
            }
        }
        
        // Should contain common DATASUS directories
        assert!(content.contains_key("SIASUS"), "Should contain SIASUS directory");
        assert!(content.contains_key("SIHSUS"), "Should contain SIHSUS directory");
    }
}
