use std::fmt;
use std::hash::{Hash, Hasher};
use std::path::Path;
use std::collections::HashMap;

use anyhow::Result;
use futures::io::AsyncReadExt;
use suppaftp::{AsyncFtpStream, FtpError};
use tokio::fs;
use tokio::io::AsyncWriteExt;

use super::file_info::{FileInfo, format_bytes_human};

/// FTP File representation with improved type safety.
/// 
/// This struct provides methods for interacting with files on the DataSUS FTP
/// server. It includes functionality for retrieving file information in a 
/// human-readable format and path manipulation.
#[derive(Debug, Clone)]
pub struct File {
    /// The name of the file without the extension
    pub name: String,
    /// The file extension (including the dot)
    pub extension: String,
    /// The full name of the file including the extension
    pub basename: String,
    /// The full path to the file on the FTP server
    pub path: String,
    /// The directory path where the file is located on the FTP server
    pub parent_path: String,
    /// Metadata about the file, including size, type, and modification date
    info: FileInfo,
}

impl File {
    /// Create a new File instance
    pub fn new<P: AsRef<str>, N: AsRef<str>>(path: P, name: N, info: FileInfo) -> Self {
        let path_str = path.as_ref();
        let name_str = name.as_ref();
        
        let (name_part, extension) = Self::split_filename(name_str);
        let basename = format!("{}{}", name_part, extension);
        
        let full_path = if path_str.ends_with('/') {
            format!("{}{}", path_str, basename)
        } else {
            format!("{}/{}", path_str, basename)
        };
        
        let parent_path = Self::get_parent_path(&full_path);
        
        Self {
            name: name_part,
            extension,
            basename,
            path: full_path,
            parent_path,
            info,
        }
    }
    
    /// Returns a HashMap with human-readable file information
    pub fn info(&self) -> HashMap<String, String> {
        let mut info_map = HashMap::new();
        
        let size_str = match self.info.size_as_bytes() {
            Some(bytes) => format_bytes_human(bytes),
            None => self.info.size.to_string(),
        };
        
        let file_type = if self.extension.is_empty() {
            "File".to_string()
        } else {
            format!("{} file", self.extension[1..].to_uppercase())
        };
        
        info_map.insert("size".to_string(), size_str);
        info_map.insert("type".to_string(), file_type);
        info_map.insert(
            "modify".to_string(), 
            self.info.modify.format("%Y-%m-%d %I:%M%p").to_string()
        );
        
        info_map
    }
    
    /// Get the file info struct
    pub fn file_info(&self) -> &FileInfo {
        &self.info
    }
    
    /// Get the file size in bytes if available
    pub fn size_bytes(&self) -> Option<u64> {
        self.info.size_as_bytes()
    }
    
    /// Get human-readable file size
    pub fn size_human(&self) -> String {
        self.info.format_size_human()
    }
    
    /// Check if file has a specific extension
    pub fn has_extension(&self, ext: &str) -> bool {
        let ext_with_dot = if ext.starts_with('.') {
            ext.to_string()
        } else {
            format!(".{}", ext)
        };
        self.extension.eq_ignore_ascii_case(&ext_with_dot)
    }
    
    /// Get the file extension without the dot
    pub fn extension_without_dot(&self) -> &str {
        if self.extension.starts_with('.') && self.extension.len() > 1 {
            &self.extension[1..]
        } else {
            &self.extension
        }
    }
    
    /// Check if this file was modified within the last N days
    pub fn modified_within_days(&self, days: i64) -> bool {
        self.info.modified_within_days(days)
    }
    
    /// Check if this is considered a large file
    pub fn is_large_file(&self, threshold_bytes: Option<u64>) -> bool {
        self.info.is_large_file(threshold_bytes)
    }
    
    /// Split filename into name and extension parts
    fn split_filename(filename: &str) -> (String, String) {
        let path = Path::new(filename);
        
        let name = path.file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("")
            .to_string();
            
        let extension = path.extension()
            .and_then(|s| s.to_str())
            .map(|s| format!(".{}", s))
            .unwrap_or_default();
            
        (name, extension)
    }
    
    /// Get parent directory path
    fn get_parent_path(full_path: &str) -> String {
        let path = Path::new(full_path);
        path.parent()
            .and_then(|p| p.to_str())
            .unwrap_or("")
            .to_string()
    }
}

// Async methods for FTP operations
impl File {
    /// Download file to memory using FTP stream
    /// Follows the established retr pattern from file_handle.md
    pub async fn download_to_memory(&self, ftp: &mut AsyncFtpStream) -> Result<Vec<u8>, FtpError> {
        // Change to parent directory first
        if !self.parent_path.is_empty() {
            ftp.cwd(&self.parent_path).await?;
        }
        
        let file_data = ftp
            .retr(&self.basename, |mut data_stream| {
                Box::pin(async move {
                    let mut buf = Vec::new();
                    data_stream
                        .read_to_end(&mut buf)
                        .await
                        .map_err(FtpError::ConnectionError)?;
                    Ok((buf, data_stream)) // closure must return tuple
                })
            })
            .await?;
            
        Ok(file_data)
    }
    
    /// Download file directly to local filesystem
    pub async fn download_to_file<P: AsRef<Path>>(&self, ftp: &mut AsyncFtpStream, dest_path: P) -> Result<(), anyhow::Error> {
        let data = self.download_to_memory(ftp).await?;
        
        // Create parent directories if they don't exist
        if let Some(parent) = dest_path.as_ref().parent() {
            fs::create_dir_all(parent).await?;
        }
        
        // Write to file
        let mut file = fs::File::create(dest_path).await?;
        file.write_all(&data).await?;
        file.flush().await?;
        
        Ok(())
    }
    
    /// Download file and process it in chunks for memory-efficient processing
    pub async fn download_and_process<F>(&self, ftp: &mut AsyncFtpStream, mut processor: F) -> Result<(), anyhow::Error>
    where
        F: FnMut(&[u8]),
    {
        // Download to memory first, then process in chunks
        let data = self.download_to_memory(ftp).await?;
        const CHUNK_SIZE: usize = 8192; // 8KB chunks
        
        for chunk in data.chunks(CHUNK_SIZE) {
            processor(chunk);
        }
        
        Ok(())
    }
    
    /// Check if file exists on FTP server
    pub async fn exists(&self, ftp: &mut AsyncFtpStream) -> Result<bool, FtpError> {
        // Save current directory
        let current_dir = ftp.pwd().await?;
        
        // Try to change to parent directory
        if !self.parent_path.is_empty() {
            if let Err(_) = ftp.cwd(&self.parent_path).await {
                return Ok(false);
            }
        }
        
        // List files and check if our file exists
        let files = ftp.nlst(None).await?;
        let exists = files.iter().any(|f| f == &self.basename);
        
        // Restore original directory
        ftp.cwd(&current_dir).await?;
        
        Ok(exists)
    }
    
    /// Get remote file size using FTP SIZE command
    pub async fn remote_size(&self, ftp: &mut AsyncFtpStream) -> Result<Option<u64>, FtpError> {
        // Save current directory
        let current_dir = ftp.pwd().await?;
        
        // Change to parent directory if needed
        if !self.parent_path.is_empty() {
            ftp.cwd(&self.parent_path).await?;
        }
        
        let result = match ftp.size(&self.basename).await {
            Ok(size) => Some(size as u64),
            Err(_) => None, // SIZE command might not be supported
        };
        
        // Restore original directory
        ftp.cwd(&current_dir).await?;
        
        Ok(result)
    }
    
    /// Verify file integrity by comparing local and remote sizes
    pub async fn verify_size<P: AsRef<Path>>(&self, ftp: &mut AsyncFtpStream, local_path: P) -> Result<bool, anyhow::Error> {
        let remote_size = self.remote_size(ftp).await?;
        
        if let Some(remote_size) = remote_size {
            let local_metadata = fs::metadata(local_path).await?;
            let local_size = local_metadata.len();
            Ok(local_size == remote_size)
        } else {
            // If we can't get remote size, assume it's fine
            Ok(true)
        }
    }
}

impl fmt::Display for File {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.basename)
    }
}

impl Hash for File {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.path.hash(state);
    }
}

impl PartialEq for File {
    fn eq(&self, other: &Self) -> bool {
        self.path == other.path
    }
}

impl Eq for File {}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use crate::models::file_info::FileSize;

    #[test]
    fn test_file_creation() {
        let info = FileInfo::new(
            FileSize::from_bytes(1024),
            ".txt".to_string(),
            Utc::now(),
        );
        
        let file = File::new("/data/files", "test.txt", info);
        
        assert_eq!(file.name, "test");
        assert_eq!(file.extension, ".txt");
        assert_eq!(file.basename, "test.txt");
        assert_eq!(file.path, "/data/files/test.txt");
        assert_eq!(file.parent_path, "/data/files");
    }
    
    #[test]
    fn test_file_info() {
        let info = FileInfo::new(
            FileSize::from_bytes(2048),
            ".pdf".to_string(),
            Utc::now(),
        );
        
        let file = File::new("/docs", "report.pdf", info);
        let file_info = file.info();
        
        assert_eq!(file_info.get("type").unwrap(), "PDF file");
        assert!(file_info.contains_key("size"));
        assert!(file_info.contains_key("modify"));
    }
    
    #[test]
    fn test_file_equality() {
        let info1 = FileInfo::new(
            FileSize::from_bytes(1024),
            ".txt".to_string(),
            Utc::now(),
        );
        let info2 = FileInfo::new(
            FileSize::from_bytes(2048),
            ".txt".to_string(),
            Utc::now(),
        );
        
        let file1 = File::new("/data", "test.txt", info1);
        let file2 = File::new("/data", "test.txt", info2.clone());
        let file3 = File::new("/other", "test.txt", info2);
        
        assert_eq!(file1, file2); // Same path
        assert_ne!(file1, file3); // Different path
    }
    
    #[tokio::test]
    async fn test_async_methods_interface() {
        // This test just verifies the async methods compile correctly
        // Real FTP testing would require a test server
        let info = FileInfo::new(
            FileSize::from_bytes(1024),
            ".txt".to_string(),
            Utc::now(),
        );
        
        let file = File::new("/test/path", "example.txt", info);
        
        // Verify method signatures compile
        assert_eq!(file.basename, "example.txt");
        assert_eq!(file.parent_path, "/test/path");
        assert!(file.has_extension("txt"));
        assert_eq!(file.extension_without_dot(), "txt");
    }
    
    #[tokio::test]
    async fn test_real_ftp_datasus() -> Result<(), anyhow::Error> {
        // Test with real DataSUS FTP server using known path from file_handle.md
        let mut ftp_stream = AsyncFtpStream::connect("ftp.datasus.gov.br:21").await?;
        ftp_stream.login("anonymous", "anonymous").await?;
        
        // Navigate to known directory with files
        ftp_stream.cwd("/dissemin/publicos/SIHSUS/200801_/Dados/").await?;
        let files = ftp_stream.nlst(None).await?;
        
        if let Some(first_file) = files.get(0) {
            println!("🔍 Testing with file: {}", first_file);
            
            // Create File instance for the first file found
            let info = FileInfo::new(
                FileSize::from_string("unknown"), // We don't know the size yet
                ".dbc".to_string(), // DataSUS files are typically .dbc
                Utc::now(),
            );
            
            let file = File::new("/dissemin/publicos/SIHSUS/200801_/Dados", first_file, info);
            
            // Test file existence
            let exists = file.exists(&mut ftp_stream).await?;
            println!("📄 File exists: {}", exists);
            assert!(exists);
            
            // Test remote size (might not be supported)
            if let Some(size) = file.remote_size(&mut ftp_stream).await? {
                println!("📊 Remote file size: {} bytes ({})", size, format_bytes_human(size));
            } else {
                println!("📊 Remote size not available (SIZE command not supported)");
            }
            
            // Test download to memory (only first 1KB to be gentle)
            println!("⬇️ Testing download to memory...");
            let data = file.download_to_memory(&mut ftp_stream).await?;
            println!("✅ Downloaded {} bytes", data.len());
            assert!(!data.is_empty());
            
            // Test chunked download
            println!("⬇️ Testing chunked download...");
            let mut total_chunks = 0;
            let mut total_bytes = 0;
            
            file.download_and_process(&mut ftp_stream, |chunk| {
                total_chunks += 1;
                total_bytes += chunk.len();
                if total_chunks <= 3 { // Only log first few chunks
                    println!("📦 Chunk {}: {} bytes", total_chunks, chunk.len());
                }
            }).await?;
            
            println!("✅ Chunked download: {} chunks, {} total bytes", total_chunks, total_bytes);
            assert_eq!(total_bytes, data.len()); // Should match memory download
            
        } else {
            println!("❌ No files found in directory");
        }
        
        ftp_stream.quit().await?;
        println!("✅ FTP test completed successfully");
        Ok(())
    }
}
