use crate::models::file_info::{FileInfo, format_bytes_human};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::{fmt, hash};

/// FTP File representation with improved type safety.
/// 
/// This struct provides methods for interacting with files on the DataSUS FTP
/// server. It includes functionality for retrieving file information in a 
/// human-readable format.
/// 
/// Fields:
/// - `name`: The name of the file without the extension.
/// - `extension`: The file extension.
/// - `basename`: The full name of the file including the extension.
/// - `path`: The full path to the file on the FTP server.
/// - `parent_path`: The directory path where the file is located on the FTP server.
/// - `info`: Metadata about the file, including size, type, and modification date.
#[derive(Debug, Clone, Serialize, Deserialize)]
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
    /// Metadata about the file
    info: FileInfo,
}

impl File {
    /// Creates a new File instance
    /// 
    /// # Arguments
    /// * `path` - The directory path where the file is located
    /// * `name` - The full filename including extension
    /// * `info` - File metadata
    pub fn new(path: &str, name: &str, info: FileInfo) -> Self {
        let path_obj = Path::new(name);
        let (file_name, extension) = match path_obj.extension() {
            Some(ext) => (
                path_obj.file_stem()
                    .and_then(|s| s.to_str())
                    .unwrap_or(name)
                    .to_string(),
                format!(".{}", ext.to_str().unwrap_or("")),
            ),
            None => (name.to_string(), String::new()),
        };

        let basename = name.to_string();
        let full_path = if path.ends_with('/') {
            format!("{}{}", path, basename)
        } else {
            format!("{}/{}", path, basename)
        };

        let parent_path = {
            let path_buf = PathBuf::from(&full_path);
            path_buf.parent()
                .map(|p| p.to_string_lossy().to_string())
                .unwrap_or_else(|| path.to_string())
        };

        Self {
            name: file_name,
            extension,
            basename,
            path: full_path,
            parent_path,
            info,
        }
    }

    /// Returns a HashMap with human-readable file information
    /// 
    /// Includes size, type, and modification date formatted for display
    pub fn info(&self) -> HashMap<String, String> {
        let mut info_map = HashMap::new();
        
        // Format size as human-readable
        let size = match self.info.size_as_bytes() {
            Some(bytes) => format_bytes_human(bytes),
            None => self.info.size.to_string(),
        };
        info_map.insert("size".to_string(), size);

        // Format file type
        let file_type = if self.extension.is_empty() {
            "File".to_string()
        } else {
            format!("{} file", self.extension[1..].to_uppercase())
        };
        info_map.insert("type".to_string(), file_type);

        // Format modification date
        let modify = self.info.modify.format("%Y-%m-%d %I:%M%p").to_string();
        info_map.insert("modify".to_string(), modify);

        info_map
    }

    /// Get the raw FileInfo
    pub fn file_info(&self) -> &FileInfo {
        &self.info
    }

    /// Get the file size in bytes if available
    pub fn size_bytes(&self) -> Option<u64> {
        self.info.size_as_bytes()
    }

    /// Get the modification date
    pub fn modification_date(&self) -> DateTime<Utc> {
        self.info.modify
    }

    /// Check if the file has a specific extension
    pub fn has_extension(&self, ext: &str) -> bool {
        let ext_with_dot = if ext.starts_with('.') {
            ext.to_string()
        } else {
            format!(".{}", ext)
        };
        self.extension.eq_ignore_ascii_case(&ext_with_dot)
    }

    /// Check if this is a large file (> 100MB by default)
    pub fn is_large(&self, threshold_bytes: Option<u64>) -> bool {
        self.info.is_large_file(threshold_bytes)
    }

    /// Check if the file was modified within the last N days
    pub fn modified_within_days(&self, days: i64) -> bool {
        self.info.modified_within_days(days)
    }
}

impl fmt::Display for File {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.basename)
    }
}

impl hash::Hash for File {
    fn hash<H: hash::Hasher>(&self, state: &mut H) {
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
    use crate::models::file_info::{FileSize, FileInfo};
    use chrono::Utc;

    #[test]
    fn test_file_creation() {
        let info = FileInfo::new(
            FileSize::from_bytes(1024),
            ".txt".to_string(),
            Utc::now(),
        );
        
        let file = File::new("/home/user", "test.txt", info);
        
        assert_eq!(file.name, "test");
        assert_eq!(file.extension, ".txt");
        assert_eq!(file.basename, "test.txt");
        assert_eq!(file.path, "/home/user/test.txt");
    }

    #[test]
    fn test_file_without_extension() {
        let info = FileInfo::new(
            FileSize::from_bytes(512),
            "".to_string(),
            Utc::now(),
        );
        
        let file = File::new("/tmp", "README", info);
        
        assert_eq!(file.name, "README");
        assert_eq!(file.extension, "");
        assert_eq!(file.basename, "README");
    }

    #[test]
    fn test_file_info_display() {
        let info = FileInfo::new(
            FileSize::from_bytes(2048),
            ".csv".to_string(),
            Utc::now(),
        );
        
        let file = File::new("/data", "report.csv", info);
        let info_map = file.info();
        
        assert!(info_map.contains_key("size"));
        assert!(info_map.contains_key("type"));
        assert!(info_map.contains_key("modify"));
        assert_eq!(info_map.get("type").unwrap(), "CSV file");
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
        
        let file1 = File::new("/home", "test.txt", info1);
        let file2 = File::new("/home", "test.txt", info2.clone());
        let file3 = File::new("/tmp", "test.txt", info2);
        
        assert_eq!(file1, file2); // Same path
        assert_ne!(file1, file3); // Different path
    }

    #[test]
    fn test_has_extension() {
        let info = FileInfo::new(
            FileSize::from_bytes(1024),
            ".txt".to_string(),
            Utc::now(),
        );
        
        let file = File::new("/home", "test.txt", info);
        
        assert!(file.has_extension("txt"));
        assert!(file.has_extension(".txt"));
        assert!(file.has_extension("TXT"));
        assert!(!file.has_extension("csv"));
    }
}
