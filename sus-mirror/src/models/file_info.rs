use serde::{Deserialize, Serialize};
use std::path::Path;

use crate::models::{
    file_extensions::{FileExtension, find_by_extension},
    months::Month,
    states::{StateInfo, find_by_uf},
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileInfo {
    pub filename: String,
    pub full_path: String,
    pub datetime: String, // ISO format: "2014-06-04T18:59:00"
    pub extension: String,
    pub size: u64,
    pub dataset: String,
    pub partition: Partition,
    pub preliminary: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Partition {
    pub uf: Option<String>,
    pub year: i32,
    pub month: Option<i8>,
    pub subpartition: Option<String>,
}

impl FileInfo {
    pub fn new(
        filename: String,
        full_path: String,
        datetime: String,
        extension: String,
        size: u64,
        dataset: String,
        partition: Partition,
        preliminary: bool,
    ) -> Self {
        Self {
            filename,
            full_path,
            datetime,
            extension,
            size,
            dataset,
            partition,
            preliminary,
        }
    }

    pub fn from_path(path: &Path) -> Option<Self> {
        let filename = path.file_name()?.to_str()?.to_string();
        let full_path = path.to_str()?.to_string();
        let extension = path.extension()?.to_str()?.to_string();

        // Basic file info - you'll need to fill in the rest based on your logic
        Some(Self {
            filename,
            full_path,
            datetime: String::new(), // You'll need to get this from file metadata
            extension,
            size: 0,                // You'll need to get this from file metadata
            dataset: String::new(), // You'll need to parse this from filename/path
            partition: Partition::default(),
            preliminary: false,
        })
    }

    // Get file extension info
    pub fn get_extension_info(&self) -> Option<&'static FileExtension> {
        find_by_extension(&format!(".{}", self.extension))
    }

    // Get MIME type
    pub fn get_mime_type(&self) -> Option<&'static str> {
        self.get_extension_info()?.mime_type
    }

    // Get file description
    pub fn get_file_description(&self) -> Option<&'static str> {
        self.get_extension_info().map(|ext| ext.description)
    }

    // Check if file extension is supported
    pub fn is_supported_extension(&self) -> bool {
        self.get_extension_info().is_some()
    }

    // Get state info if UF is present
    pub fn get_state_info(&self) -> Option<&'static StateInfo> {
        self.partition.uf.as_ref().and_then(|uf| find_by_uf(uf))
    }

    // Get state name
    pub fn get_state_name(&self) -> Option<&'static str> {
        self.get_state_info().map(|state| state.name)
    }

    // Get month info if month is present
    pub fn get_month_info(&self) -> Option<Month> {
        self.partition.month.and_then(Month::from_number)
    }

    // Get month name
    pub fn get_month_name(&self) -> Option<&'static str> {
        self.partition.month.and_then(Month::get_name)
    }

    // Get formatted month string (zero-padded)
    pub fn get_month_padded(&self) -> Option<String> {
        self.partition.month.map(|m| format!("{:02}", m))
    }

    // Check if partition has UF
    pub fn has_uf(&self) -> bool {
        self.partition.uf.is_some()
    }

    // Check if partition has month
    pub fn has_month(&self) -> bool {
        self.partition.month.is_some()
    }

    // Check if partition has subpartition
    pub fn has_subpartition(&self) -> bool {
        self.partition.subpartition.is_some()
    }

    // Get file size in human-readable format
    pub fn get_human_readable_size(&self) -> String {
        let size = self.size as f64;
        let units = ["B", "KB", "MB", "GB", "TB"];
        let mut size_f = size;
        let mut unit_index = 0;

        while size_f >= 1024.0 && unit_index < units.len() - 1 {
            size_f /= 1024.0;
            unit_index += 1;
        }

        if unit_index == 0 {
            format!("{} {}", size as u64, units[unit_index])
        } else {
            format!("{:.1} {}", size_f, units[unit_index])
        }
    }
}

impl Partition {
    pub fn new(
        uf: Option<String>,
        year: i32,
        month: Option<i8>,
        subpartition: Option<String>,
    ) -> Self {
        Self {
            uf,
            year,
            month,
            subpartition,
        }
    }

    pub fn with_uf_and_year(uf: String, year: i32) -> Self {
        Self {
            uf: Some(uf),
            year,
            month: None,
            subpartition: None,
        }
    }

    pub fn with_year_only(year: i32) -> Self {
        Self {
            uf: None,
            year,
            month: None,
            subpartition: None,
        }
    }

    pub fn full(uf: String, year: i32, month: i8, subpartition: Option<String>) -> Self {
        Self {
            uf: Some(uf),
            year,
            month: Some(month),
            subpartition,
        }
    }

    // Validate the partition data
    pub fn is_valid(&self) -> bool {
        // Check UF if present
        if let Some(ref uf) = self.uf {
            if !crate::models::states::is_valid_uf(uf) {
                return false;
            }
        }

        // Check month if present
        if let Some(month) = self.month {
            if !(1..=12).contains(&month) {
                return false;
            }
        }

        // Year should be reasonable
        if self.year < 1900 || self.year > 2100 {
            return false;
        }

        true
    }

    // Get a string representation of the partition
    pub fn to_string(&self) -> String {
        let mut parts = vec![self.year.to_string()];

        if let Some(ref uf) = self.uf {
            parts.push(uf.clone());
        }

        if let Some(month) = self.month {
            parts.push(format!("{:02}", month));
        }

        if let Some(ref subpartition) = self.subpartition {
            parts.push(subpartition.clone());
        }

        parts.join("-")
    }
}

impl Default for Partition {
    fn default() -> Self {
        Self {
            uf: None,
            year: 2000,
            month: None,
            subpartition: None,
        }
    }
}

// Utility functions
pub fn create_file_info_from_example() -> FileInfo {
    FileInfo {
        filename: "LTAC0510.dbc".to_string(),
        full_path: "/dissemin/publicos/CNES/200508_/Dados/LT/LTAC0510.dbc".to_string(),
        datetime: "2014-06-04T18:59:00".to_string(),
        extension: "dbc".to_string(),
        size: 2756,
        dataset: "cnes-lt".to_string(),
        partition: Partition {
            uf: Some("ac".to_string()),
            year: 2005,
            month: Some(10),
            subpartition: None,
        },
        preliminary: false,
    }
}

pub fn validate_file_info(file_info: &FileInfo) -> Vec<String> {
    let mut errors = Vec::new();

    // Check if extension is supported
    if !file_info.is_supported_extension() {
        errors.push(format!(
            "Unsupported file extension: {}",
            file_info.extension
        ));
    }

    // Validate partition
    if !file_info.partition.is_valid() {
        errors.push("Invalid partition data".to_string());
    }

    // Check filename is not empty
    if file_info.filename.is_empty() {
        errors.push("Filename cannot be empty".to_string());
    }

    // Check path is not empty
    if file_info.full_path.is_empty() {
        errors.push("Full path cannot be empty".to_string());
    }

    errors
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    #[test]
    fn test_file_info_new() {
        let file_info = FileInfo::new(
            "test.dbc".to_string(),
            "/path/to/test.dbc".to_string(),
            "2023-01-01T12:00:00".to_string(),
            "dbc".to_string(),
            1024,
            "test-dataset".to_string(),
            Partition::with_year_only(2023),
            false,
        );

        assert_eq!(file_info.filename, "test.dbc");
        assert_eq!(file_info.full_path, "/path/to/test.dbc");
        assert_eq!(file_info.datetime, "2023-01-01T12:00:00");
        assert_eq!(file_info.extension, "dbc");
        assert_eq!(file_info.size, 1024);
        assert_eq!(file_info.dataset, "test-dataset");
        assert_eq!(file_info.partition.year, 2023);
        assert_eq!(file_info.preliminary, false);
    }

    #[test]
    fn test_file_info_from_path() {
        let path = PathBuf::from("/path/to/test.dbc");
        let file_info = FileInfo::from_path(&path).unwrap();

        assert_eq!(file_info.filename, "test.dbc");
        assert_eq!(file_info.full_path, "/path/to/test.dbc");
        assert_eq!(file_info.extension, "dbc");
        assert_eq!(file_info.datetime, "");
        assert_eq!(file_info.size, 0);
        assert_eq!(file_info.dataset, "");
        assert_eq!(file_info.partition.year, 2000); // default
        assert_eq!(file_info.preliminary, false);
    }

    #[test]
    fn test_file_info_from_path_invalid() {
        let path = PathBuf::from("/path/to/");
        let result = FileInfo::from_path(&path);
        assert!(result.is_none());
    }

    #[test]
    fn test_file_info_has_methods() {
        let file_info = FileInfo::new(
            "test.dbc".to_string(),
            "/path/to/test.dbc".to_string(),
            "2023-01-01T12:00:00".to_string(),
            "dbc".to_string(),
            1024,
            "test-dataset".to_string(),
            Partition::full("sp".to_string(), 2023, 5, Some("sub".to_string())),
            false,
        );

        assert!(file_info.has_uf());
        assert!(file_info.has_month());
        assert!(file_info.has_subpartition());
    }

    #[test]
    fn test_file_info_has_methods_empty() {
        let file_info = FileInfo::new(
            "test.dbc".to_string(),
            "/path/to/test.dbc".to_string(),
            "2023-01-01T12:00:00".to_string(),
            "dbc".to_string(),
            1024,
            "test-dataset".to_string(),
            Partition::with_year_only(2023),
            false,
        );

        assert!(!file_info.has_uf());
        assert!(!file_info.has_month());
        assert!(!file_info.has_subpartition());
    }

    #[test]
    fn test_file_info_get_month_padded() {
        let file_info = FileInfo::new(
            "test.dbc".to_string(),
            "/path/to/test.dbc".to_string(),
            "2023-01-01T12:00:00".to_string(),
            "dbc".to_string(),
            1024,
            "test-dataset".to_string(),
            Partition::full("sp".to_string(), 2023, 5, None),
            false,
        );

        assert_eq!(file_info.get_month_padded(), Some("05".to_string()));
    }

    #[test]
    fn test_file_info_get_month_padded_none() {
        let file_info = FileInfo::new(
            "test.dbc".to_string(),
            "/path/to/test.dbc".to_string(),
            "2023-01-01T12:00:00".to_string(),
            "dbc".to_string(),
            1024,
            "test-dataset".to_string(),
            Partition::with_year_only(2023),
            false,
        );

        assert_eq!(file_info.get_month_padded(), None);
    }

    #[test]
    fn test_file_info_get_human_readable_size() {
        let test_cases = vec![
            (512, "512 B"),
            (1024, "1.0 KB"),
            (1536, "1.5 KB"),
            (1048576, "1.0 MB"),
            (1073741824, "1.0 GB"),
            (1099511627776, "1.0 TB"),
        ];

        for (size, expected) in test_cases {
            let file_info = FileInfo::new(
                "test.dbc".to_string(),
                "/path/to/test.dbc".to_string(),
                "2023-01-01T12:00:00".to_string(),
                "dbc".to_string(),
                size,
                "test-dataset".to_string(),
                Partition::with_year_only(2023),
                false,
            );

            assert_eq!(file_info.get_human_readable_size(), expected);
        }
    }

    #[test]
    fn test_partition_new() {
        let partition = Partition::new(
            Some("sp".to_string()),
            2023,
            Some(5),
            Some("sub".to_string()),
        );

        assert_eq!(partition.uf, Some("sp".to_string()));
        assert_eq!(partition.year, 2023);
        assert_eq!(partition.month, Some(5));
        assert_eq!(partition.subpartition, Some("sub".to_string()));
    }

    #[test]
    fn test_partition_with_uf_and_year() {
        let partition = Partition::with_uf_and_year("rj".to_string(), 2023);

        assert_eq!(partition.uf, Some("rj".to_string()));
        assert_eq!(partition.year, 2023);
        assert_eq!(partition.month, None);
        assert_eq!(partition.subpartition, None);
    }

    #[test]
    fn test_partition_with_year_only() {
        let partition = Partition::with_year_only(2023);

        assert_eq!(partition.uf, None);
        assert_eq!(partition.year, 2023);
        assert_eq!(partition.month, None);
        assert_eq!(partition.subpartition, None);
    }

    #[test]
    fn test_partition_full() {
        let partition = Partition::full("mg".to_string(), 2023, 8, Some("test".to_string()));

        assert_eq!(partition.uf, Some("mg".to_string()));
        assert_eq!(partition.year, 2023);
        assert_eq!(partition.month, Some(8));
        assert_eq!(partition.subpartition, Some("test".to_string()));
    }

    #[test]
    fn test_partition_full_no_subpartition() {
        let partition = Partition::full("mg".to_string(), 2023, 8, None);

        assert_eq!(partition.uf, Some("mg".to_string()));
        assert_eq!(partition.year, 2023);
        assert_eq!(partition.month, Some(8));
        assert_eq!(partition.subpartition, None);
    }

    #[test]
    fn test_partition_is_valid() {
        let valid_partition = Partition::full("sp".to_string(), 2023, 5, None);
        assert!(valid_partition.is_valid());

        let invalid_uf = Partition::full("invalid".to_string(), 2023, 5, None);
        assert!(!invalid_uf.is_valid());

        let invalid_month = Partition::full("sp".to_string(), 2023, 13, None);
        assert!(!invalid_month.is_valid());

        let invalid_year_low = Partition::full("sp".to_string(), 1800, 5, None);
        assert!(!invalid_year_low.is_valid());

        let invalid_year_high = Partition::full("sp".to_string(), 2200, 5, None);
        assert!(!invalid_year_high.is_valid());
    }

    #[test]
    fn test_partition_is_valid_edge_cases() {
        let valid_month_1 = Partition::full("sp".to_string(), 2023, 1, None);
        assert!(valid_month_1.is_valid());

        let valid_month_12 = Partition::full("sp".to_string(), 2023, 12, None);
        assert!(valid_month_12.is_valid());

        let invalid_month_0 = Partition::full("sp".to_string(), 2023, 0, None);
        assert!(!invalid_month_0.is_valid());

        let valid_year_1900 = Partition::full("sp".to_string(), 1900, 5, None);
        assert!(valid_year_1900.is_valid());

        let valid_year_2100 = Partition::full("sp".to_string(), 2100, 5, None);
        assert!(valid_year_2100.is_valid());
    }

    #[test]
    fn test_partition_to_string() {
        let partition1 = Partition::with_year_only(2023);
        assert_eq!(partition1.to_string(), "2023");

        let partition2 = Partition::with_uf_and_year("sp".to_string(), 2023);
        assert_eq!(partition2.to_string(), "2023-sp");

        let partition3 = Partition::full("rj".to_string(), 2023, 5, None);
        assert_eq!(partition3.to_string(), "2023-rj-05");

        let partition4 = Partition::full("mg".to_string(), 2023, 8, Some("sub".to_string()));
        assert_eq!(partition4.to_string(), "2023-mg-08-sub");
    }

    #[test]
    fn test_partition_default() {
        let partition = Partition::default();

        assert_eq!(partition.uf, None);
        assert_eq!(partition.year, 2000);
        assert_eq!(partition.month, None);
        assert_eq!(partition.subpartition, None);
    }

    #[test]
    fn test_create_file_info_from_example() {
        let file_info = create_file_info_from_example();

        assert_eq!(file_info.filename, "LTAC0510.dbc");
        assert_eq!(
            file_info.full_path,
            "/dissemin/publicos/CNES/200508_/Dados/LT/LTAC0510.dbc"
        );
        assert_eq!(file_info.datetime, "2014-06-04T18:59:00");
        assert_eq!(file_info.extension, "dbc");
        assert_eq!(file_info.size, 2756);
        assert_eq!(file_info.dataset, "cnes-lt");
        assert_eq!(file_info.partition.uf, Some("ac".to_string()));
        assert_eq!(file_info.partition.year, 2005);
        assert_eq!(file_info.partition.month, Some(10));
        assert_eq!(file_info.partition.subpartition, None);
        assert_eq!(file_info.preliminary, false);
    }

    #[test]
    fn test_validate_file_info_valid() {
        let file_info = create_file_info_from_example();
        let errors = validate_file_info(&file_info);
        assert!(errors.is_empty());
    }

    #[test]
    fn test_validate_file_info_empty_filename() {
        let mut file_info = create_file_info_from_example();
        file_info.filename = String::new();

        let errors = validate_file_info(&file_info);
        assert!(errors.contains(&"Filename cannot be empty".to_string()));
    }

    #[test]
    fn test_validate_file_info_empty_path() {
        let mut file_info = create_file_info_from_example();
        file_info.full_path = String::new();

        let errors = validate_file_info(&file_info);
        assert!(errors.contains(&"Full path cannot be empty".to_string()));
    }

    #[test]
    fn test_validate_file_info_unsupported_extension() {
        let mut file_info = create_file_info_from_example();
        file_info.extension = "unsupported".to_string();

        let errors = validate_file_info(&file_info);
        assert!(errors.contains(&"Unsupported file extension: unsupported".to_string()));
    }

    #[test]
    fn test_validate_file_info_invalid_partition() {
        let mut file_info = create_file_info_from_example();
        file_info.partition.year = 1800; // Invalid year

        let errors = validate_file_info(&file_info);
        assert!(errors.contains(&"Invalid partition data".to_string()));
    }

    #[test]
    fn test_validate_file_info_multiple_errors() {
        let file_info = FileInfo::new(
            String::new(), // Empty filename
            String::new(), // Empty path
            "2023-01-01T12:00:00".to_string(),
            "unsupported".to_string(), // Unsupported extension
            1024,
            "test-dataset".to_string(),
            Partition::full("invalid".to_string(), 1800, 13, None), // Invalid partition
            false,
        );

        let errors = validate_file_info(&file_info);
        assert!(errors.len() >= 3); // Should have multiple errors
        assert!(errors.contains(&"Filename cannot be empty".to_string()));
        assert!(errors.contains(&"Full path cannot be empty".to_string()));
        assert!(errors.contains(&"Unsupported file extension: unsupported".to_string()));
        assert!(errors.contains(&"Invalid partition data".to_string()));
    }

    #[test]
    fn test_file_info_serialization() {
        let file_info = create_file_info_from_example();

        // Test serialization
        let serialized = serde_json::to_string(&file_info).unwrap();
        assert!(serialized.contains("LTAC0510.dbc"));
        assert!(serialized.contains("cnes-lt"));

        // Test deserialization
        let deserialized: FileInfo = serde_json::from_str(&serialized).unwrap();
        assert_eq!(deserialized.filename, file_info.filename);
        assert_eq!(deserialized.dataset, file_info.dataset);
        assert_eq!(deserialized.partition.uf, file_info.partition.uf);
    }

    #[test]
    fn test_partition_serialization() {
        let partition = Partition::full("sp".to_string(), 2023, 5, Some("sub".to_string()));

        // Test serialization
        let serialized = serde_json::to_string(&partition).unwrap();
        assert!(serialized.contains("sp"));
        assert!(serialized.contains("2023"));

        // Test deserialization
        let deserialized: Partition = serde_json::from_str(&serialized).unwrap();
        assert_eq!(deserialized.uf, partition.uf);
        assert_eq!(deserialized.year, partition.year);
        assert_eq!(deserialized.month, partition.month);
        assert_eq!(deserialized.subpartition, partition.subpartition);
    }

    #[test]
    fn test_file_info_clone() {
        let file_info = create_file_info_from_example();
        let cloned = file_info.clone();

        assert_eq!(file_info.filename, cloned.filename);
        assert_eq!(file_info.full_path, cloned.full_path);
        assert_eq!(file_info.datetime, cloned.datetime);
        assert_eq!(file_info.extension, cloned.extension);
        assert_eq!(file_info.size, cloned.size);
        assert_eq!(file_info.dataset, cloned.dataset);
        assert_eq!(file_info.partition.uf, cloned.partition.uf);
        assert_eq!(file_info.preliminary, cloned.preliminary);
    }

    #[test]
    fn test_partition_clone() {
        let partition = Partition::full("rj".to_string(), 2023, 8, Some("test".to_string()));
        let cloned = partition.clone();

        assert_eq!(partition.uf, cloned.uf);
        assert_eq!(partition.year, cloned.year);
        assert_eq!(partition.month, cloned.month);
        assert_eq!(partition.subpartition, cloned.subpartition);
    }
}
