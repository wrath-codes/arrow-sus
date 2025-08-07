use crate::models::file_info::{FileInfo, format_bytes_human};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::path::Path;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct File {
    /// The name of the file without the extension
    pub name: String,
    /// The file extension
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
    pub fn new(path: &str, name: &str, info: FileInfo) -> Self {
        let path_obj = Path::new(name);
        let (file_name, extension) = match path_obj.extension() {
            Some(ext) => (
                path_obj.file_stem().unwrap_or_default().to_string_lossy().to_string(),
                format!(".{}", ext.to_string_lossy())
            ),
            None => (name.to_string(), String::new()),
        };

        let basename = name.to_string();
        let full_path = if path.ends_with('/') {
            format!("{}{}", path, basename)
        } else {
            format!("{}/{}", path, basename)
        };

        let parent_path = Path::new(&full_path)
            .parent()
            .map(|p| p.to_string_lossy().to_string())
            .unwrap_or_else(|| path.to_string());

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
    pub fn info(&self) -> HashMap<String, String> {
        let mut info_map = HashMap::new();
        
        // Format size as human-readable
        let size_str = match self.info.size_as_bytes() {
            Some(bytes) => format_bytes_human(bytes),
            None => self.info.size.to_string(),
        };
        
        // Format file type
        let file_type = if self.extension.is_empty() {
            "File".to_string()
        } else {
            format!("{} file", self.extension[1..].to_uppercase())
        };
        
        // Format modification date
        let modify_str = self.info.modify.format("%Y-%m-%d %I:%M%p").to_string();
        
        info_map.insert("size".to_string(), size_str);
        info_map.insert("type".to_string(), file_type);
        info_map.insert("modify".to_string(), modify_str);
        
        info_map
    }

    /// Get the underlying FileInfo
    pub fn file_info(&self) -> &FileInfo {
        &self.info
    }

    /// Get the file size in bytes if available
    pub fn size_bytes(&self) -> Option<u64> {
        self.info.size_as_bytes()
    }

    /// Check if this file has a specific extension
    pub fn has_extension(&self, ext: &str) -> bool {
        let target_ext = if ext.starts_with('.') {
            ext.to_string()
        } else {
            format!(".{}", ext)
        };
        self.extension.eq_ignore_ascii_case(&target_ext)
    }

    /// Check if this file is of a specific type
    pub fn is_type(&self, file_type: &str) -> bool {
        self.info.is_type(file_type)
    }

    /// Check if the file was modified within the last N days
    pub fn modified_within_days(&self, days: i64) -> bool {
        self.info.modified_within_days(days)
    }

    /// Check if this is considered a large file
    pub fn is_large_file(&self, threshold_bytes: Option<u64>) -> bool {
        self.info.is_large_file(threshold_bytes)
    }

    /// Get just the file name without extension
    pub fn name_without_extension(&self) -> &str {
        &self.name
    }

    /// Get the file extension
    pub fn get_extension(&self) -> &str {
        &self.extension
    }

    /// Get the full basename (name + extension)
    pub fn get_basename(&self) -> &str {
        &self.basename
    }

    /// Get the full path
    pub fn get_path(&self) -> &str {
        &self.path
    }

    /// Get the parent directory path
    pub fn get_parent_path(&self) -> &str {
        &self.parent_path
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
    use crate::models::file_info::FileSize;
    use chrono::Utc;

    #[test]
    fn test_file_creation() {
        let info = FileInfo::new(
            FileSize::from_bytes(1024),
            "txt".to_string(),
            Utc::now(),
        );
        
        let file = File::new("/path/to", "document.txt", info);
        
        assert_eq!(file.name, "document");
        assert_eq!(file.extension, ".txt");
        assert_eq!(file.basename, "document.txt");
        assert_eq!(file.path, "/path/to/document.txt");
    }

    #[test]
    fn test_file_info() {
        let info = FileInfo::new(
            FileSize::from_bytes(2048),
            "pdf".to_string(),
            Utc::now(),
        );
        
        let file = File::new("/docs", "report.pdf", info);
        let file_info = file.info();
        
        assert!(file_info.contains_key("size"));
        assert!(file_info.contains_key("type"));
        assert!(file_info.contains_key("modify"));
        assert_eq!(file_info["type"], "PDF file");
    }

    #[test]
    fn test_file_equality() {
        let info1 = FileInfo::new(
            FileSize::from_bytes(1024),
            "txt".to_string(),
            Utc::now(),
        );
        let info2 = FileInfo::new(
            FileSize::from_bytes(2048),
            "txt".to_string(),
            Utc::now(),
        );
        let info3 = FileInfo::new(
            FileSize::from_bytes(2048),
            "txt".to_string(),
            Utc::now(),
        );
        
        let file1 = File::new("/path", "test.txt", info1);
        let file2 = File::new("/path", "test.txt", info2);
        let file3 = File::new("/other", "test.txt", info3);
        
        assert_eq!(file1, file2); // Same path
        assert_ne!(file1, file3); // Different path
    }

    #[test]
    fn test_has_extension() {
        let info = FileInfo::new(
            FileSize::from_bytes(1024),
            "jpg".to_string(),
            Utc::now(),
        );
        
        let file = File::new("/images", "photo.jpg", info);
        
        assert!(file.has_extension("jpg"));
        assert!(file.has_extension(".jpg"));
        assert!(file.has_extension("JPG"));
        assert!(!file.has_extension("png"));
    }
}
