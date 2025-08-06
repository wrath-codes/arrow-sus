use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileInfo {
    /// File size - can be numeric or string representation
    pub size: FileSize,
    /// File type/extension
    #[serde(rename = "type")]
    pub file_type: String,
    /// Last modification timestamp
    pub modify: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum FileSize {
    Numeric(u64),
    Text(String),
}

impl FileInfo {
    pub fn new(size: FileSize, file_type: String, modify: DateTime<Utc>) -> Self {
        Self {
            size,
            file_type,
            modify,
        }
    }

    pub fn size_as_bytes(&self) -> Option<u64> {
        match &self.size {
            FileSize::Numeric(bytes) => Some(*bytes),
            FileSize::Text(text) => text.parse().ok(),
        }
    }

    pub fn is_type(&self, file_type: &str) -> bool {
        self.file_type.eq_ignore_ascii_case(file_type)
    }
}

impl FileSize {
    pub fn from_bytes(bytes: u64) -> Self {
        Self::Numeric(bytes)
    }

    pub fn from_string<S: Into<String>>(size: S) -> Self {
        Self::Text(size.into())
    }

    pub fn to_bytes(&self) -> Option<u64> {
        match self {
            Self::Numeric(bytes) => Some(*bytes),
            Self::Text(text) => text.parse().ok(),
        }
    }
}

impl fmt::Display for FileSize {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Numeric(bytes) => write!(f, "{}", bytes),
            Self::Text(text) => write!(f, "{}", text),
        }
    }
}

impl fmt::Display for FileInfo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "FileInfo {{ type: {}, size: {}, modified: {} }}",
            self.file_type,
            self.size,
            self.modify.format("%Y-%m-%d %H:%M:%S")
        )
    }
}

impl From<u64> for FileSize {
    fn from(bytes: u64) -> Self {
        Self::Numeric(bytes)
    }
}

impl From<String> for FileSize {
    fn from(text: String) -> Self {
        Self::Text(text)
    }
}

impl From<&str> for FileSize {
    fn from(text: &str) -> Self {
        Self::Text(text.to_string())
    }
}

// Helper functions
impl FileInfo {
    /// Parse file size from human-readable format (e.g., "1.5MB", "2GB")
    pub fn parse_human_size(size_str: &str) -> Option<u64> {
        let size_str = size_str.trim().to_uppercase();
        
        if let Ok(bytes) = size_str.parse::<u64>() {
            return Some(bytes);
        }
        
        let (number_part, unit_part) = if let Some(pos) = size_str.find(|c: char| c.is_alphabetic()) {
            (&size_str[..pos], &size_str[pos..])
        } else {
            return size_str.parse().ok();
        };
        
        let number: f64 = number_part.parse().ok()?;
        
        let multiplier = match unit_part {
            "B" | "BYTES" => 1,
            "KB" | "K" => 1_024,
            "MB" | "M" => 1_024_u64.pow(2),
            "GB" | "G" => 1_024_u64.pow(3),
            "TB" | "T" => 1_024_u64.pow(4),
            _ => return None,
        };
        
        Some((number * multiplier as f64) as u64)
    }
    
    /// Format size as human-readable string
    pub fn format_size_human(&self) -> String {
        match self.size_as_bytes() {
            Some(bytes) => format_bytes_human(bytes),
            None => self.size.to_string(),
        }
    }
    
    /// Check if file was modified within the last N days
    pub fn modified_within_days(&self, days: i64) -> bool {
        let now = Utc::now();
        let duration = chrono::Duration::days(days);
        self.modify > now - duration
    }
    
    /// Get file extension from type
    pub fn extension(&self) -> Option<&str> {
        if self.file_type.starts_with('.') {
            Some(&self.file_type[1..])
        } else {
            Some(&self.file_type)
        }
    }
    
    /// Check if file is considered "large" (default: > 100MB)
    pub fn is_large_file(&self, threshold_bytes: Option<u64>) -> bool {
        let threshold = threshold_bytes.unwrap_or(100 * 1024 * 1024); // 100MB
        self.size_as_bytes().map(|size| size > threshold).unwrap_or(false)
    }
    
    /// Create FileInfo from common file metadata
    pub fn from_metadata(
        size: u64,
        file_type: &str,
        modified: DateTime<Utc>,
    ) -> Self {
        Self::new(
            FileSize::from_bytes(size),
            file_type.to_string(),
            modified,
        )
    }
}

/// Format bytes as human-readable string
pub fn format_bytes_human(bytes: u64) -> String {
    const UNITS: &[&str] = &["B", "KB", "MB", "GB", "TB"];
    const THRESHOLD: f64 = 1024.0;
    
    if bytes == 0 {
        return "0 B".to_string();
    }
    
    let mut size = bytes as f64;
    let mut unit_index = 0;
    
    while size >= THRESHOLD && unit_index < UNITS.len() - 1 {
        size /= THRESHOLD;
        unit_index += 1;
    }
    
    if unit_index == 0 {
        format!("{} {}", bytes, UNITS[unit_index])
    } else {
        format!("{:.1} {}", size, UNITS[unit_index])
    }
}

/// Parse file info from JSON string
pub fn parse_file_info_json(json_str: &str) -> Result<FileInfo, serde_json::Error> {
    serde_json::from_str(json_str)
}

/// Convert FileInfo to JSON string
pub fn file_info_to_json(file_info: &FileInfo) -> Result<String, serde_json::Error> {
    serde_json::to_string(file_info)
}

/// Convert FileInfo to pretty JSON string
pub fn file_info_to_json_pretty(file_info: &FileInfo) -> Result<String, serde_json::Error> {
    serde_json::to_string_pretty(file_info)
}
