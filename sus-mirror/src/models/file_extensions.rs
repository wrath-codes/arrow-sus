use serde::{Deserialize, Serialize};
use std::path::Path;
use tokio::fs;

#[derive(Debug, Serialize, Deserialize)]
pub struct FileExtension {
    pub extension: &'static str,         // ".dbc"
    pub description: &'static str,       // "DBC file (Data Base Compressed)"
    pub mime_type: Option<&'static str>, // e.g., "application/octet-stream"
}

impl FileExtension {
    pub fn matches_extension(&self, ext: &str) -> bool {
        self.extension.eq_ignore_ascii_case(ext)
    }

    pub fn matches_filename(&self, filename: &str) -> bool {
        filename
            .to_lowercase()
            .ends_with(&self.extension.to_lowercase())
    }

    pub fn matches_path(&self, path: &Path) -> bool {
        path.extension()
            .and_then(|ext| ext.to_str())
            .map(|ext| self.matches_extension(&format!(".{}", ext)))
            .unwrap_or(false)
    }

    pub async fn matches_file(&self, file_path: &Path) -> tokio::io::Result<bool> {
        if !fs::try_exists(file_path).await? {
            return Ok(false);
        }
        Ok(self.matches_path(file_path))
    }
}

pub const FILE_EXTENSIONS: &[FileExtension] = &[
    FileExtension {
        extension: ".dbc",
        description: "DBC file (Data Base Compressed)",
        mime_type: Some("application/octet-stream"),
    },
    FileExtension {
        extension: ".zip",
        description: "ZIP archive",
        mime_type: Some("application/zip"),
    },
    FileExtension {
        extension: ".gzip",
        description: "GZIP compressed archive",
        mime_type: Some("application/gzip"),
    },
    FileExtension {
        extension: ".parquet",
        description: "Apache Parquet columnar storage",
        mime_type: Some("application/octet-stream"),
    },
    FileExtension {
        extension: ".dbf",
        description: "DBF (dBase) file",
        mime_type: Some("application/octet-stream"),
    },
    FileExtension {
        extension: ".csv",
        description: "Comma-separated values file",
        mime_type: Some("text/csv"),
    },
    FileExtension {
        extension: ".json",
        description: "JSON (JavaScript Object Notation) file",
        mime_type: Some("application/json"),
    },
];

// Synchronous utility functions
pub fn find_by_extension(ext: &str) -> Option<&'static FileExtension> {
    FILE_EXTENSIONS
        .iter()
        .find(|file_ext| file_ext.matches_extension(ext))
}

pub fn find_by_filename(filename: &str) -> Option<&'static FileExtension> {
    FILE_EXTENSIONS
        .iter()
        .find(|file_ext| file_ext.matches_filename(filename))
}

pub fn find_by_path(path: &Path) -> Option<&'static FileExtension> {
    FILE_EXTENSIONS
        .iter()
        .find(|file_ext| file_ext.matches_path(path))
}

pub fn get_mime_type(ext: &str) -> Option<&'static str> {
    find_by_extension(ext)?.mime_type
}

pub fn get_description(ext: &str) -> Option<&'static str> {
    find_by_extension(ext).map(|fe| fe.description)
}

pub fn is_supported_extension(ext: &str) -> bool {
    find_by_extension(ext).is_some()
}

pub fn is_supported_filename(filename: &str) -> bool {
    find_by_filename(filename).is_some()
}

pub fn extract_extension_from_filename(filename: &str) -> Option<String> {
    Path::new(filename)
        .extension()
        .and_then(|ext| ext.to_str())
        .filter(|ext| !ext.is_empty()) // Add this line to filter out empty extensions
        .map(|ext| format!(".{}", ext.to_lowercase()))
}

// Async utility functions
pub async fn find_by_file_path(
    file_path: &Path,
) -> tokio::io::Result<Option<&'static FileExtension>> {
    if !fs::try_exists(file_path).await? {
        return Ok(None);
    }
    Ok(find_by_path(file_path))
}

pub async fn is_supported_file(file_path: &Path) -> tokio::io::Result<bool> {
    if !fs::try_exists(file_path).await? {
        return Ok(false);
    }
    Ok(find_by_path(file_path).is_some())
}

pub async fn get_file_info(
    file_path: &Path,
) -> tokio::io::Result<Option<(&'static str, Option<&'static str>)>> {
    if let Some(ext_info) = find_by_file_path(file_path).await? {
        Ok(Some((ext_info.description, ext_info.mime_type)))
    } else {
        Ok(None)
    }
}

pub async fn validate_file_extension(
    file_path: &Path,
    expected_ext: &str,
) -> tokio::io::Result<bool> {
    if !fs::try_exists(file_path).await? {
        return Ok(false);
    }

    if let Some(ext_info) = find_by_path(file_path) {
        Ok(ext_info.matches_extension(expected_ext))
    } else {
        Ok(false)
    }
}

pub async fn filter_supported_files(file_paths: Vec<&Path>) -> tokio::io::Result<Vec<&Path>> {
    let mut supported_files = Vec::new();

    for path in file_paths {
        if is_supported_file(path).await? {
            supported_files.push(path);
        }
    }

    Ok(supported_files)
}

pub async fn scan_directory_for_supported_files(
    dir_path: &Path,
) -> tokio::io::Result<Vec<std::path::PathBuf>> {
    let mut supported_files = Vec::new();
    let mut entries = fs::read_dir(dir_path).await?;

    while let Some(entry) = entries.next_entry().await? {
        let path = entry.path();
        if path.is_file() && is_supported_file(&path).await? {
            supported_files.push(path);
        }
    }

    Ok(supported_files)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;
    use tempfile::{NamedTempFile, TempDir};
    use tokio::fs;

    #[test]
    fn test_file_extension_matches_extension() {
        let dbc_ext = &FILE_EXTENSIONS[0]; // .dbc

        assert!(dbc_ext.matches_extension(".dbc"));
        assert!(dbc_ext.matches_extension(".DBC")); // Case insensitive
        assert!(dbc_ext.matches_extension(".DbC"));
        assert!(!dbc_ext.matches_extension(".zip"));
        assert!(!dbc_ext.matches_extension("dbc")); // Missing dot
        assert!(!dbc_ext.matches_extension(""));
    }

    #[test]
    fn test_file_extension_matches_filename() {
        let zip_ext = &FILE_EXTENSIONS[1]; // .zip

        assert!(zip_ext.matches_filename("test.zip"));
        assert!(zip_ext.matches_filename("test.ZIP")); // Case insensitive
        assert!(zip_ext.matches_filename("archive.zip"));
        assert!(zip_ext.matches_filename("path/to/file.zip"));
        assert!(!zip_ext.matches_filename("test.dbc"));
        assert!(!zip_ext.matches_filename("zipfile"));
        assert!(!zip_ext.matches_filename(""));
    }

    #[test]
    fn test_file_extension_matches_path() {
        let csv_ext = &FILE_EXTENSIONS[5]; // .csv

        assert!(csv_ext.matches_path(Path::new("data.csv")));
        assert!(csv_ext.matches_path(Path::new("data.CSV")));
        assert!(csv_ext.matches_path(Path::new("/path/to/data.csv")));
        assert!(!csv_ext.matches_path(Path::new("data.json")));
        assert!(!csv_ext.matches_path(Path::new("csvfile")));
        assert!(!csv_ext.matches_path(Path::new("")));
        assert!(!csv_ext.matches_path(Path::new("file_without_extension")));
    }

    #[test]
    fn test_find_by_extension() {
        assert!(find_by_extension(".dbc").is_some());
        assert!(find_by_extension(".DBC").is_some()); // Case insensitive
        assert!(find_by_extension(".zip").is_some());
        assert!(find_by_extension(".parquet").is_some());
        assert!(find_by_extension(".unknown").is_none());
        assert!(find_by_extension("").is_none());

        let dbc_info = find_by_extension(".dbc").unwrap();
        assert_eq!(dbc_info.extension, ".dbc");
        assert_eq!(dbc_info.description, "DBC file (Data Base Compressed)");
        assert_eq!(dbc_info.mime_type, Some("application/octet-stream"));
    }

    #[test]
    fn test_find_by_filename() {
        assert!(find_by_filename("test.dbc").is_some());
        assert!(find_by_filename("test.DBC").is_some());
        assert!(find_by_filename("data.csv").is_some());
        assert!(find_by_filename("archive.zip").is_some());
        assert!(find_by_filename("test.unknown").is_none());
        assert!(find_by_filename("").is_none());
        assert!(find_by_filename("no_extension").is_none());

        let json_info = find_by_filename("config.json").unwrap();
        assert_eq!(json_info.extension, ".json");
        assert_eq!(
            json_info.description,
            "JSON (JavaScript Object Notation) file"
        );
        assert_eq!(json_info.mime_type, Some("application/json"));
    }

    #[test]
    fn test_find_by_path() {
        assert!(find_by_path(Path::new("test.dbc")).is_some());
        assert!(find_by_path(Path::new("/path/to/test.DBC")).is_some());
        assert!(find_by_path(Path::new("data.csv")).is_some());
        assert!(find_by_path(Path::new("test.unknown")).is_none());
        assert!(find_by_path(Path::new("")).is_none());
        assert!(find_by_path(Path::new("no_extension")).is_none());

        let dbf_info = find_by_path(Path::new("database.dbf")).unwrap();
        assert_eq!(dbf_info.extension, ".dbf");
        assert_eq!(dbf_info.description, "DBF (dBase) file");
    }

    #[test]
    fn test_get_mime_type() {
        assert_eq!(get_mime_type(".zip"), Some("application/zip"));
        assert_eq!(get_mime_type(".csv"), Some("text/csv"));
        assert_eq!(get_mime_type(".json"), Some("application/json"));
        assert_eq!(get_mime_type(".gzip"), Some("application/gzip"));
        assert_eq!(get_mime_type(".unknown"), None);
        assert_eq!(get_mime_type(""), None);
    }

    #[test]
    fn test_get_description() {
        assert_eq!(
            get_description(".dbc"),
            Some("DBC file (Data Base Compressed)")
        );
        assert_eq!(get_description(".zip"), Some("ZIP archive"));
        assert_eq!(get_description(".csv"), Some("Comma-separated values file"));
        assert_eq!(
            get_description(".parquet"),
            Some("Apache Parquet columnar storage")
        );
        assert_eq!(get_description(".unknown"), None);
        assert_eq!(get_description(""), None);
    }

    #[test]
    fn test_is_supported_extension() {
        assert!(is_supported_extension(".dbc"));
        assert!(is_supported_extension(".DBC")); // Case insensitive
        assert!(is_supported_extension(".zip"));
        assert!(is_supported_extension(".csv"));
        assert!(is_supported_extension(".json"));
        assert!(is_supported_extension(".parquet"));
        assert!(is_supported_extension(".dbf"));
        assert!(is_supported_extension(".gzip"));
        assert!(!is_supported_extension(".unknown"));
        assert!(!is_supported_extension(""));
        assert!(!is_supported_extension("dbc")); // Missing dot
    }

    #[test]
    fn test_is_supported_filename() {
        assert!(is_supported_filename("test.dbc"));
        assert!(is_supported_filename("test.DBC"));
        assert!(is_supported_filename("archive.zip"));
        assert!(is_supported_filename("data.csv"));
        assert!(is_supported_filename("config.json"));
        assert!(is_supported_filename("/path/to/file.parquet"));
        assert!(!is_supported_filename("test.unknown"));
        assert!(!is_supported_filename(""));
        assert!(!is_supported_filename("no_extension"));
    }

    #[test]
    fn test_extract_extension_from_filename() {
        assert_eq!(
            extract_extension_from_filename("test.dbc"),
            Some(".dbc".to_string())
        );
        assert_eq!(
            extract_extension_from_filename("test.DBC"),
            Some(".dbc".to_string())
        ); // Lowercase
        assert_eq!(
            extract_extension_from_filename("archive.ZIP"),
            Some(".zip".to_string())
        );
        assert_eq!(
            extract_extension_from_filename("data.CSV"),
            Some(".csv".to_string())
        );
        assert_eq!(
            extract_extension_from_filename("/path/to/file.JSON"),
            Some(".json".to_string())
        );
        assert_eq!(extract_extension_from_filename("no_extension"), None);
        assert_eq!(extract_extension_from_filename(""), None);
        assert_eq!(extract_extension_from_filename(".hidden"), None); // Fixed: .hidden is a hidden file with no extension

        // Add more test cases for clarity
        assert_eq!(extract_extension_from_filename(".gitignore"), None); // Hidden file, no extension
        assert_eq!(
            extract_extension_from_filename(".hidden.txt"),
            Some(".txt".to_string())
        ); // Hidden file with extension
        assert_eq!(extract_extension_from_filename("file."), None); // File ending with dot has no extension
    }

    #[test]
    fn test_file_extensions_constant() {
        // Test that all expected extensions are present
        let extensions: Vec<&str> = FILE_EXTENSIONS.iter().map(|fe| fe.extension).collect();

        assert!(extensions.contains(&".dbc"));
        assert!(extensions.contains(&".zip"));
        assert!(extensions.contains(&".gzip"));
        assert!(extensions.contains(&".parquet"));
        assert!(extensions.contains(&".dbf"));
        assert!(extensions.contains(&".csv"));
        assert!(extensions.contains(&".json"));

        // Test that all have descriptions
        for ext in FILE_EXTENSIONS {
            assert!(!ext.description.is_empty());
            assert!(ext.mime_type.is_some());
        }
    }

    // Async tests
    #[tokio::test]
    async fn test_file_extension_matches_file() {
        let temp_file = NamedTempFile::new().unwrap();
        let temp_path = temp_file.path();

        // Create a file with .dbc extension
        let dbc_path = temp_path.with_extension("dbc");
        fs::write(&dbc_path, b"test content").await.unwrap();

        let dbc_ext = &FILE_EXTENSIONS[0];
        assert!(dbc_ext.matches_file(&dbc_path).await.unwrap());

        // Test with non-existent file
        let non_existent = Path::new("non_existent.dbc");
        assert!(!dbc_ext.matches_file(non_existent).await.unwrap());

        // Clean up
        fs::remove_file(&dbc_path).await.unwrap();
    }

    #[tokio::test]
    async fn test_find_by_file_path() {
        let temp_file = NamedTempFile::new().unwrap();
        let temp_path = temp_file.path();

        // Create files with different extensions
        let csv_path = temp_path.with_extension("csv");
        fs::write(&csv_path, b"col1,col2\nval1,val2").await.unwrap();

        let result = find_by_file_path(&csv_path).await.unwrap();
        assert!(result.is_some());
        let ext_info = result.unwrap();
        assert_eq!(ext_info.extension, ".csv");

        // Test with non-existent file
        let non_existent = Path::new("non_existent.csv");
        let result = find_by_file_path(non_existent).await.unwrap();
        assert!(result.is_none());

        // Clean up
        fs::remove_file(&csv_path).await.unwrap();
    }

    #[tokio::test]
    async fn test_is_supported_file() {
        let temp_file = NamedTempFile::new().unwrap();
        let temp_path = temp_file.path();

        // Create a supported file
        let json_path = temp_path.with_extension("json");
        fs::write(&json_path, br#"{"key": "value"}"#).await.unwrap();

        assert!(is_supported_file(&json_path).await.unwrap());

        // Create an unsupported file
        let unknown_path = temp_path.with_extension("unknown");
        fs::write(&unknown_path, b"unknown content").await.unwrap();

        assert!(!is_supported_file(&unknown_path).await.unwrap());

        // Test with non-existent file
        let non_existent = Path::new("non_existent.json");
        assert!(!is_supported_file(non_existent).await.unwrap());

        // Clean up
        fs::remove_file(&json_path).await.unwrap();
        fs::remove_file(&unknown_path).await.unwrap();
    }

    #[tokio::test]
    async fn test_get_file_info() {
        let temp_file = NamedTempFile::new().unwrap();
        let temp_path = temp_file.path();

        // Create a ZIP file
        let zip_path = temp_path.with_extension("zip");
        fs::write(&zip_path, b"PK\x03\x04").await.unwrap(); // ZIP file signature

        let info = get_file_info(&zip_path).await.unwrap();
        assert!(info.is_some());
        let (description, mime_type) = info.unwrap();
        assert_eq!(description, "ZIP archive");
        assert_eq!(mime_type, Some("application/zip"));

        // Test with unsupported file
        let unknown_path = temp_path.with_extension("unknown");
        fs::write(&unknown_path, b"unknown").await.unwrap();

        let info = get_file_info(&unknown_path).await.unwrap();
        assert!(info.is_none());

        // Clean up
        fs::remove_file(&zip_path).await.unwrap();
        fs::remove_file(&unknown_path).await.unwrap();
    }

    #[tokio::test]
    async fn test_validate_file_extension() {
        let temp_file = NamedTempFile::new().unwrap();
        let temp_path = temp_file.path();

        // Create a DBC file
        let dbc_path = temp_path.with_extension("dbc");
        fs::write(&dbc_path, b"DBC content").await.unwrap();

        // Test with matching extension
        assert!(validate_file_extension(&dbc_path, ".dbc").await.unwrap());
        assert!(validate_file_extension(&dbc_path, ".DBC").await.unwrap()); // Case insensitive

        // Test with non-matching extension
        assert!(!validate_file_extension(&dbc_path, ".zip").await.unwrap());

        // Test with non-existent file
        let non_existent = Path::new("non_existent.dbc");
        assert!(!validate_file_extension(non_existent, ".dbc").await.unwrap());

        // Clean up
        fs::remove_file(&dbc_path).await.unwrap();
    }

    #[tokio::test]
    async fn test_filter_supported_files() {
        let temp_dir = TempDir::new().unwrap();
        let temp_path = temp_dir.path();

        // Create various files
        let csv_path = temp_path.join("data.csv");
        let json_path = temp_path.join("config.json");
        let unknown_path = temp_path.join("readme.txt");
        let dbc_path = temp_path.join("database.dbc");

        fs::write(&csv_path, b"col1,col2\nval1,val2").await.unwrap();
        fs::write(&json_path, br#"{"key": "value"}"#).await.unwrap();
        fs::write(&unknown_path, b"This is a text file")
            .await
            .unwrap();
        fs::write(&dbc_path, b"DBC content").await.unwrap();

        let all_paths = vec![
            csv_path.as_path(),
            json_path.as_path(),
            unknown_path.as_path(),
            dbc_path.as_path(),
        ];

        let supported_files = filter_supported_files(all_paths).await.unwrap();

        assert_eq!(supported_files.len(), 3); // csv, json, dbc
        assert!(supported_files.contains(&csv_path.as_path()));
        assert!(supported_files.contains(&json_path.as_path()));
        assert!(supported_files.contains(&dbc_path.as_path()));
        assert!(!supported_files.contains(&unknown_path.as_path()));
    }

    #[tokio::test]
    async fn test_scan_directory_for_supported_files() {
        let temp_dir = TempDir::new().unwrap();
        let temp_path = temp_dir.path();

        // Create various files in the directory
        let csv_path = temp_path.join("data.csv");
        let json_path = temp_path.join("config.json");
        let zip_path = temp_path.join("archive.zip");
        let unknown_path = temp_path.join("readme.txt");
        let parquet_path = temp_path.join("dataset.parquet");

        fs::write(&csv_path, b"col1,col2\nval1,val2").await.unwrap();
        fs::write(&json_path, br#"{"key": "value"}"#).await.unwrap();
        fs::write(&zip_path, b"PK\x03\x04").await.unwrap();
        fs::write(&unknown_path, b"This is a text file")
            .await
            .unwrap();
        fs::write(&parquet_path, b"PAR1").await.unwrap();

        let supported_files = scan_directory_for_supported_files(temp_path).await.unwrap();

        assert_eq!(supported_files.len(), 4); // csv, json, zip, parquet

        let file_names: Vec<String> = supported_files
            .iter()
            .map(|p| p.file_name().unwrap().to_string_lossy().to_string())
            .collect();

        assert!(file_names.contains(&"data.csv".to_string()));
        assert!(file_names.contains(&"config.json".to_string()));
        assert!(file_names.contains(&"archive.zip".to_string()));
        assert!(file_names.contains(&"dataset.parquet".to_string()));
        assert!(!file_names.contains(&"readme.txt".to_string()));
    }

    #[tokio::test]
    async fn test_scan_empty_directory() {
        let temp_dir = TempDir::new().unwrap();
        let temp_path = temp_dir.path();

        let supported_files = scan_directory_for_supported_files(temp_path).await.unwrap();
        assert_eq!(supported_files.len(), 0);
    }

    #[tokio::test]
    async fn test_scan_nonexistent_directory() {
        let non_existent = Path::new("non_existent_directory");
        let result = scan_directory_for_supported_files(non_existent).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_filter_supported_files_empty_list() {
        let empty_paths: Vec<&Path> = vec![];
        let result = filter_supported_files(empty_paths).await.unwrap();
        assert_eq!(result.len(), 0);
    }

    #[tokio::test]
    async fn test_filter_supported_files_with_nonexistent() {
        let temp_file = NamedTempFile::new().unwrap();
        let temp_path = temp_file.path();

        let csv_path = temp_path.with_extension("csv");
        fs::write(&csv_path, b"col1,col2").await.unwrap();

        let non_existent = Path::new("non_existent.json");
        let paths = vec![csv_path.as_path(), non_existent];

        let result = filter_supported_files(paths).await.unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], csv_path.as_path());

        // Clean up
        fs::remove_file(&csv_path).await.unwrap();
    }

    #[test]
    fn test_case_sensitivity() {
        // Test that extension matching is case insensitive
        assert!(is_supported_extension(".DBC"));
        assert!(is_supported_extension(".dbc"));
        assert!(is_supported_extension(".DbC"));
        assert!(is_supported_extension(".ZIP"));
        assert!(is_supported_extension(".zip"));
        assert!(is_supported_extension(".Zip"));

        // Test filename matching is case insensitive
        assert!(is_supported_filename("TEST.DBC"));
        assert!(is_supported_filename("test.dbc"));
        assert!(is_supported_filename("Test.DbC"));
        assert!(is_supported_filename("ARCHIVE.ZIP"));
        assert!(is_supported_filename("archive.zip"));
        assert!(is_supported_filename("Archive.Zip"));
    }

    #[test]
    fn test_edge_cases() {
        // Test with empty strings
        assert!(!is_supported_extension(""));
        assert!(!is_supported_filename(""));
        assert!(find_by_extension("").is_none());
        assert!(find_by_filename("").is_none());

        // Test with just dot
        assert!(!is_supported_extension("."));
        assert!(!is_supported_filename("."));

        // Test with multiple dots
        assert!(is_supported_filename("file.name.csv"));
        assert!(is_supported_filename("archive.backup.zip"));

        // Test with no extension
        assert!(!is_supported_filename("filename"));
        assert!(!is_supported_filename("path/to/filename"));
    }

    #[test]
    fn test_all_supported_extensions() {
        let expected_extensions =
            vec![".dbc", ".zip", ".gzip", ".parquet", ".dbf", ".csv", ".json"];

        for ext in expected_extensions {
            assert!(
                is_supported_extension(ext),
                "Extension {} should be supported",
                ext
            );
            assert!(
                find_by_extension(ext).is_some(),
                "Extension {} should be found",
                ext
            );
            assert!(
                get_mime_type(ext).is_some(),
                "Extension {} should have mime type",
                ext
            );
            assert!(
                get_description(ext).is_some(),
                "Extension {} should have description",
                ext
            );
        }
    }

    #[test]
    fn test_mime_types() {
        assert_eq!(get_mime_type(".dbc"), Some("application/octet-stream"));
        assert_eq!(get_mime_type(".zip"), Some("application/zip"));
        assert_eq!(get_mime_type(".gzip"), Some("application/gzip"));
        assert_eq!(get_mime_type(".parquet"), Some("application/octet-stream"));
        assert_eq!(get_mime_type(".dbf"), Some("application/octet-stream"));
        assert_eq!(get_mime_type(".csv"), Some("text/csv"));
        assert_eq!(get_mime_type(".json"), Some("application/json"));
    }

    #[test]
    fn test_descriptions() {
        assert_eq!(
            get_description(".dbc"),
            Some("DBC file (Data Base Compressed)")
        );
        assert_eq!(get_description(".zip"), Some("ZIP archive"));
        assert_eq!(get_description(".gzip"), Some("GZIP compressed archive"));
        assert_eq!(
            get_description(".parquet"),
            Some("Apache Parquet columnar storage")
        );
        assert_eq!(get_description(".dbf"), Some("DBF (dBase) file"));
        assert_eq!(get_description(".csv"), Some("Comma-separated values file"));
        assert_eq!(
            get_description(".json"),
            Some("JSON (JavaScript Object Notation) file")
        );
    }

    #[test]
    fn test_path_with_special_characters() {
        // Test paths with special characters
        assert!(is_supported_filename("file with spaces.csv"));
        assert!(is_supported_filename("file-with-dashes.json"));
        assert!(is_supported_filename("file_with_underscores.zip"));
        assert!(is_supported_filename("file.with.dots.parquet"));
        assert!(is_supported_filename("файл.dbc")); // Cyrillic characters
        assert!(is_supported_filename("文件.csv")); // Chinese characters
    }

    #[tokio::test]
    async fn test_async_operations_with_subdirectories() {
        let temp_dir = TempDir::new().unwrap();
        let temp_path = temp_dir.path();

        // Create subdirectory
        let sub_dir = temp_path.join("subdir");
        fs::create_dir(&sub_dir).await.unwrap();

        // Create files in main directory
        let csv_path = temp_path.join("data.csv");
        fs::write(&csv_path, b"col1,col2").await.unwrap();

        // Create files in subdirectory (should not be found by scan_directory_for_supported_files)
        let sub_json_path = sub_dir.join("config.json");
        fs::write(&sub_json_path, br#"{"key": "value"}"#)
            .await
            .unwrap();

        let supported_files = scan_directory_for_supported_files(temp_path).await.unwrap();

        // Should only find files in the main directory, not subdirectories
        assert_eq!(supported_files.len(), 1);
        assert_eq!(supported_files[0].file_name().unwrap(), "data.csv");
    }

    #[tokio::test]
    async fn test_concurrent_file_operations() {
        let temp_dir = TempDir::new().unwrap();
        let temp_path = temp_dir.path();

        // Create multiple files concurrently
        let csv_path = temp_path.join("data.csv");
        let json_path = temp_path.join("config.json");
        let zip_path = temp_path.join("archive.zip");

        let (csv_result, json_result, zip_result) = tokio::join!(
            fs::write(&csv_path, b"col1,col2"),
            fs::write(&json_path, br#"{"key": "value"}"#),
            fs::write(&zip_path, b"PK\x03\x04")
        );

        csv_result.unwrap();
        json_result.unwrap();
        zip_result.unwrap();

        // Test concurrent validation
        let (csv_valid, json_valid, zip_valid) = tokio::join!(
            is_supported_file(&csv_path),
            is_supported_file(&json_path),
            is_supported_file(&zip_path)
        );

        assert!(csv_valid.unwrap());
        assert!(json_valid.unwrap());
        assert!(zip_valid.unwrap());
    }

    #[test]
    fn test_file_extension_struct_debug() {
        let dbc_ext = &FILE_EXTENSIONS[0];
        let debug_str = format!("{:?}", dbc_ext);
        assert!(debug_str.contains("FileExtension"));
        assert!(debug_str.contains(".dbc"));
        assert!(debug_str.contains("DBC file"));
    }
}
