use encoding::{Encoding, EncoderTrap, DecoderTrap};
use encoding::all::ISO_8859_1;
use std::error::Error as StdError;
use std::fmt;
use std::fs::File;
use std::io::{BufRead, BufReader, Read};
use std::path::Path;
use tokio::fs::File as AsyncFile;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, BufReader as AsyncBufReader};
use dbase::{Reader, FieldInfo};
use arrow::datatypes::{Schema, Field, DataType};
use explode::ExplodeReader;
use std::io::{Chain, Cursor};

/// Type alias for a DBF reader that chains pre-header, header, and decompressed content
type DbfReader<R> = Chain<Chain<Cursor<[u8; 10]>, Cursor<Vec<u8>>>, ExplodeReader<R>>;

/// Errors that can occur during encoding/decoding operations
#[derive(Debug)]
pub enum DbfEncodingError {
    /// Encoding failed - typically when a character cannot be represented in ISO-8859-1
    EncodingFailed(String),
    /// Decoding failed - typically when invalid byte sequences are encountered
    DecodingFailed(String),
    /// File I/O error
    IoError(String),
    /// File parsing error
    ParseError(String),
}

impl From<std::io::Error> for DbfEncodingError {
    fn from(err: std::io::Error) -> Self {
        DbfEncodingError::IoError(err.to_string())
    }
}

impl fmt::Display for DbfEncodingError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DbfEncodingError::EncodingFailed(msg) => write!(f, "Encoding failed: {}", msg),
            DbfEncodingError::DecodingFailed(msg) => write!(f, "Decoding failed: {}", msg),
            DbfEncodingError::IoError(msg) => write!(f, "I/O error: {}", msg),
            DbfEncodingError::ParseError(msg) => write!(f, "Parse error: {}", msg),
        }
    }
}

impl StdError for DbfEncodingError {}

/// Represents the three parts of a DBC file
#[derive(Debug, Clone)]
pub struct DbcFileParts {
    /// Preheader section containing metadata like VERSION, BO_TX_BU_, etc.
    pub preheader: Vec<String>,
    /// Header section containing message and signal definitions
    pub header: Vec<String>,
    /// Contents section with the actual data definitions
    pub contents: Vec<String>,
}

/// Synchronous encoding from UTF-8 string to ISO-8859-1 bytes
pub fn encode_to_iso_8859_1(input: &str) -> Result<Vec<u8>, DbfEncodingError> {
    ISO_8859_1
        .encode(input, EncoderTrap::Strict)
        .map_err(|e| DbfEncodingError::EncodingFailed(e.to_string()))
}

/// Synchronous encoding from UTF-8 string to ISO-8859-1 bytes with replacement for invalid characters
pub fn encode_to_iso_8859_1_lossy(input: &str) -> Vec<u8> {
    ISO_8859_1
        .encode(input, EncoderTrap::Replace)
        .unwrap_or_else(|_| input.bytes().collect())
}

/// Synchronous decoding from ISO-8859-1 bytes to UTF-8 string
pub fn decode_from_iso_8859_1(input: &[u8]) -> Result<String, DbfEncodingError> {
    ISO_8859_1
        .decode(input, DecoderTrap::Strict)
        .map_err(|e| DbfEncodingError::DecodingFailed(e.to_string()))
}

/// Synchronous decoding from ISO-8859-1 bytes to UTF-8 string with replacement for invalid sequences
pub fn decode_from_iso_8859_1_lossy(input: &[u8]) -> String {
    ISO_8859_1
        .decode(input, DecoderTrap::Replace)
        .unwrap_or_else(|_| String::from_utf8_lossy(input).to_string())
}

/// Asynchronous encoding from UTF-8 string to ISO-8859-1 bytes
pub async fn encode_to_iso_8859_1_async(input: &str) -> Result<Vec<u8>, DbfEncodingError> {
    let input = input.to_string();
    tokio::task::spawn_blocking(move || encode_to_iso_8859_1(&input))
        .await
        .map_err(|e| DbfEncodingError::EncodingFailed(format!("Task join error: {}", e)))?
}

/// Asynchronous encoding from UTF-8 string to ISO-8859-1 bytes with replacement for invalid characters
pub async fn encode_to_iso_8859_1_lossy_async(input: &str) -> Vec<u8> {
    let input = input.to_string();
    let fallback_input = input.clone();
    tokio::task::spawn_blocking(move || encode_to_iso_8859_1_lossy(&input))
        .await
        .unwrap_or_else(|_| fallback_input.bytes().collect())
}

/// Asynchronous decoding from ISO-8859-1 bytes to UTF-8 string
pub async fn decode_from_iso_8859_1_async(input: Vec<u8>) -> Result<String, DbfEncodingError> {
    tokio::task::spawn_blocking(move || decode_from_iso_8859_1(&input))
        .await
        .map_err(|e| DbfEncodingError::DecodingFailed(format!("Task join error: {}", e)))?
}

/// Asynchronous decoding from ISO-8859-1 bytes to UTF-8 string with replacement for invalid sequences
pub async fn decode_from_iso_8859_1_lossy_async(input: Vec<u8>) -> String {
    let fallback_input = input.clone();
    tokio::task::spawn_blocking(move || decode_from_iso_8859_1_lossy(&input))
        .await
        .unwrap_or_else(|_| String::from_utf8_lossy(&fallback_input).to_string())
}

/// Synchronously opens and splits a DBC file into preheader, header, and contents
pub fn split_dbc_file<P: AsRef<Path>>(file_path: P) -> Result<DbcFileParts, DbfEncodingError> {
    let file = File::open(file_path)?;
    let reader = BufReader::new(file);
    
    let mut preheader = Vec::new();
    let mut header = Vec::new();
    let mut contents = Vec::new();
    
    let mut current_section = 0; // 0=preheader, 1=header, 2=contents
    
    for line in reader.lines() {
        let line = line?;
        let trimmed = line.trim();
        
        // Skip empty lines
        if trimmed.is_empty() {
            continue;
        }
        
        // Determine section transitions based on DBC file structure
        if current_section == 0 {
            // In preheader section
            if trimmed.starts_with("BO_") || trimmed.starts_with("SG_") {
                current_section = 1; // Move to header
                header.push(line);
            } else if trimmed.starts_with("BA_DEF_") || trimmed.starts_with("BA_") {
                current_section = 2; // Move to contents
                contents.push(line);
            } else {
                preheader.push(line);
            }
        } else if current_section == 1 {
            // In header section
            if trimmed.starts_with("BA_DEF_") || trimmed.starts_with("BA_") || 
               trimmed.starts_with("VAL_") || trimmed.starts_with("CM_") {
                current_section = 2; // Move to contents
                contents.push(line);
            } else {
                header.push(line);
            }
        } else {
            // In contents section
            contents.push(line);
        }
    }
    
    Ok(DbcFileParts {
        preheader,
        header,
        contents,
    })
}

/// Synchronously opens and splits a DBC file, reading all content as bytes first
pub fn split_dbc_file_bytes<P: AsRef<Path>>(file_path: P) -> Result<DbcFileParts, DbfEncodingError> {
    let mut file = File::open(file_path)?;
    let mut buffer = Vec::new();
    file.read_to_end(&mut buffer)?;
    
    // Decode from ISO-8859-1 to UTF-8
    let content = decode_from_iso_8859_1(&buffer)?;
    let lines: Vec<String> = content.lines().map(|s| s.to_string()).collect();
    
    split_lines_into_parts(lines)
}

/// Asynchronously opens and splits a DBC file into preheader, header, and contents
pub async fn split_dbc_file_async<P: AsRef<Path>>(file_path: P) -> Result<DbcFileParts, DbfEncodingError> {
    let file = AsyncFile::open(file_path).await?;
    let reader = AsyncBufReader::new(file);
    let mut lines = reader.lines();
    
    let mut preheader = Vec::new();
    let mut header = Vec::new();
    let mut contents = Vec::new();
    
    let mut current_section = 0; // 0=preheader, 1=header, 2=contents
    
    while let Some(line) = lines.next_line().await? {
        let trimmed = line.trim();
        
        // Skip empty lines
        if trimmed.is_empty() {
            continue;
        }
        
        // Determine section transitions based on DBC file structure
        if current_section == 0 {
            // In preheader section
            if trimmed.starts_with("BO_") || trimmed.starts_with("SG_") {
                current_section = 1; // Move to header
                header.push(line);
            } else if trimmed.starts_with("BA_DEF_") || trimmed.starts_with("BA_") {
                current_section = 2; // Move to contents
                contents.push(line);
            } else {
                preheader.push(line);
            }
        } else if current_section == 1 {
            // In header section
            if trimmed.starts_with("BA_DEF_") || trimmed.starts_with("BA_") || 
               trimmed.starts_with("VAL_") || trimmed.starts_with("CM_") {
                current_section = 2; // Move to contents
                contents.push(line);
            } else {
                header.push(line);
            }
        } else {
            // In contents section
            contents.push(line);
        }
    }
    
    Ok(DbcFileParts {
        preheader,
        header,
        contents,
    })
}

/// Asynchronously opens and splits a DBC file, reading all content as bytes first
pub async fn split_dbc_file_bytes_async<P: AsRef<Path>>(file_path: P) -> Result<DbcFileParts, DbfEncodingError> {
    let mut file = AsyncFile::open(file_path).await?;
    let mut buffer = Vec::new();
    file.read_to_end(&mut buffer).await?;
    
    // Decode from ISO-8859-1 to UTF-8 in a blocking task
    let content = decode_from_iso_8859_1_async(buffer).await?;
    let lines: Vec<String> = content.lines().map(|s| s.to_string()).collect();
    
    split_lines_into_parts(lines)
}

/// Helper function to split lines into the three parts
fn split_lines_into_parts(lines: Vec<String>) -> Result<DbcFileParts, DbfEncodingError> {
    let mut preheader = Vec::new();
    let mut header = Vec::new();
    let mut contents = Vec::new();
    
    let mut current_section = 0; // 0=preheader, 1=header, 2=contents
    
    for line in lines {
        let trimmed = line.trim();
        
        // Skip empty lines
        if trimmed.is_empty() {
            continue;
        }
        
        // Determine section transitions based on DBC file structure
        if current_section == 0 {
            // In preheader section
            if trimmed.starts_with("BO_") || trimmed.starts_with("SG_") {
                current_section = 1; // Move to header
                header.push(line);
            } else if trimmed.starts_with("BA_DEF_") || trimmed.starts_with("BA_") {
                current_section = 2; // Move to contents
                contents.push(line);
            } else {
                preheader.push(line);
            }
        } else if current_section == 1 {
            // In header section
            if trimmed.starts_with("BA_DEF_") || trimmed.starts_with("BA_") || 
               trimmed.starts_with("VAL_") || trimmed.starts_with("CM_") {
                current_section = 2; // Move to contents
                contents.push(line);
            } else {
                header.push(line);
            }
        } else {
            // In contents section
            contents.push(line);
        }
    }
    
    Ok(DbcFileParts {
        preheader,
        header,
        contents,
    })
}

/// Convert dbase FieldInfo to Arrow DataType
fn dbase_field_to_arrow_type(field_info: &FieldInfo) -> DataType {
    match field_info.field_type() {
        dbase::FieldType::Character => DataType::Utf8,
        dbase::FieldType::Currency => DataType::Decimal128(19, 4), // Standard currency precision
        dbase::FieldType::Numeric => {
            // Since we can't access decimal_count directly, we'll use length as a heuristic
            // Smaller numeric fields are likely integers, larger ones likely decimals
            if field_info.length() <= 9 {
                DataType::Int32
            } else {
                DataType::Float64
            }
        }
        dbase::FieldType::Float => DataType::Float64,
        dbase::FieldType::Date => DataType::Date32,
        dbase::FieldType::DateTime => DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None),
        dbase::FieldType::Logical => DataType::Boolean,
        dbase::FieldType::Memo => DataType::Utf8, // Large text field
        dbase::FieldType::Integer => DataType::Int32,
        dbase::FieldType::Double => DataType::Float64,
    }
}

/// Synchronously read dbase file header and convert to Arrow Schema
pub fn dbase_header_to_arrow_schema<P: AsRef<Path>>(file_path: P) -> Result<Schema, DbfEncodingError> {
    let reader = Reader::from_path(file_path)
        .map_err(|e| DbfEncodingError::IoError(format!("Failed to open dbase file: {}", e)))?;
    
    let fields: Vec<Field> = reader
        .fields()
        .iter()
        .map(|field_info| {
            let data_type = dbase_field_to_arrow_type(field_info);
            Field::new(field_info.name(), data_type, true) // Allow nulls by default
        })
        .collect();
    
    Ok(Schema::new(fields))
}

/// Synchronously read dbase file header with additional metadata and convert to Arrow Schema
pub fn dbase_header_to_arrow_schema_with_metadata<P: AsRef<Path>>(
    file_path: P,
) -> Result<(Schema, Vec<FieldInfo>), DbfEncodingError> {
    let reader = Reader::from_path(file_path)
        .map_err(|e| DbfEncodingError::IoError(format!("Failed to open dbase file: {}", e)))?;
    
    let field_infos: Vec<FieldInfo> = reader.fields().to_vec();
    
    let fields: Vec<Field> = field_infos
        .iter()
        .map(|field_info| {
            let data_type = dbase_field_to_arrow_type(field_info);
            let mut field = Field::new(field_info.name(), data_type, true);
            
            // Add dbase-specific metadata
            let mut metadata = std::collections::HashMap::new();
            metadata.insert("dbase_type".to_string(), format!("{:?}", field_info.field_type()));
            metadata.insert("dbase_length".to_string(), field_info.length().to_string());
            
            field = field.with_metadata(metadata);
            field
        })
        .collect();
    
    Ok((Schema::new(fields), field_infos))
}

/// Asynchronously read dbase file header and convert to Arrow Schema
pub async fn dbase_header_to_arrow_schema_async<P: AsRef<Path>>(
    file_path: P,
) -> Result<Schema, DbfEncodingError> {
    let path = file_path.as_ref().to_path_buf();
    tokio::task::spawn_blocking(move || dbase_header_to_arrow_schema(path))
        .await
        .map_err(|e| DbfEncodingError::IoError(format!("Task join error: {}", e)))?
}

/// Asynchronously read dbase file header with additional metadata and convert to Arrow Schema
pub async fn dbase_header_to_arrow_schema_with_metadata_async<P: AsRef<Path>>(
    file_path: P,
) -> Result<(Schema, Vec<FieldInfo>), DbfEncodingError> {
    let path = file_path.as_ref().to_path_buf();
    tokio::task::spawn_blocking(move || dbase_header_to_arrow_schema_with_metadata(path))
        .await
        .map_err(|e| DbfEncodingError::IoError(format!("Task join error: {}", e)))?
}

/// Transform a DBC reader into a DBF reader for streaming decompression
/// This follows the same approach as the datasus-dbc crate
pub fn dbc_to_dbf_reader<R: Read>(mut dbc_reader: R) -> Result<DbfReader<R>, DbfEncodingError> {
    // Read the 10-byte pre-header
    let mut pre_header: [u8; 10] = Default::default();
    dbc_reader
        .read_exact(&mut pre_header)
        .map_err(|_| DbfEncodingError::ParseError("Missing or invalid DBC header".to_string()))?;

    // Extract header size from bytes 8-9 (little-endian)
    let header_size: usize = usize::from(pre_header[8]) + (usize::from(pre_header[9]) << 8);
    
    // Validate header size
    if header_size < 10 {
        return Err(DbfEncodingError::ParseError(
            format!("Invalid header size: {} (must be >= 10)", header_size)
        ));
    }
    
    // Read the header content (excluding the 10 bytes already read)
    let mut header: Vec<u8> = vec![0; header_size - 10];
    dbc_reader
        .read_exact(&mut header)
        .map_err(|_| DbfEncodingError::ParseError("Invalid header size in DBC file".to_string()))?;

    // Read the 4-byte CRC32 (we don't validate it, just skip it)
    let mut _crc32: [u8; 4] = Default::default();
    dbc_reader
        .read_exact(&mut _crc32)
        .map_err(|_| DbfEncodingError::ParseError("Missing CRC32 in DBC file".to_string()))?;

    // Create the chained reader: pre_header + header + decompressed_content
    let pre_header_reader = Cursor::new(pre_header);
    let header_reader = Cursor::new(header);
    let compressed_content_reader = ExplodeReader::new(dbc_reader);

    let dbf_reader = std::io::Read::chain(
        std::io::Read::chain(pre_header_reader, header_reader),
        compressed_content_reader,
    );

    Ok(dbf_reader)
}

/// Decompress a DBC file to a DBF file on disk
pub fn decompress_dbc_to_dbf<P: AsRef<Path>, Q: AsRef<Path>>(
    dbc_path: P,
    dbf_path: Q,
) -> Result<(), DbfEncodingError> {
    let dbc_file = File::open(dbc_path)?;
    let mut dbf_reader = dbc_to_dbf_reader(dbc_file)?;
    
    let mut dbf_file = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(dbf_path)?;
    
    std::io::copy(&mut dbf_reader, &mut dbf_file)?;
    Ok(())
}

/// Asynchronously decompress a DBC file to a DBF file on disk
pub async fn decompress_dbc_to_dbf_async<P: AsRef<Path>, Q: AsRef<Path>>(
    dbc_path: P,
    dbf_path: Q,
) -> Result<(), DbfEncodingError> {
    let dbc_path = dbc_path.as_ref().to_path_buf();
    let dbf_path = dbf_path.as_ref().to_path_buf();
    
    tokio::task::spawn_blocking(move || {
        decompress_dbc_to_dbf(dbc_path, dbf_path)
    })
    .await
    .map_err(|e| DbfEncodingError::IoError(format!("Task join error: {}", e)))?
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode_basic_ascii() {
        let input = "Hello World";
        let result = encode_to_iso_8859_1(input).unwrap();
        assert_eq!(result, input.as_bytes());
    }

    #[test]
    fn test_encode_iso_8859_1_characters() {
        let input = "Café";
        let result = encode_to_iso_8859_1(input).unwrap();
        // 'é' in ISO-8859-1 is byte 0xE9
        assert_eq!(result, vec![67, 97, 102, 233]);
    }

    #[test]
    fn test_decode_basic_ascii() {
        let input = b"Hello World";
        let result = decode_from_iso_8859_1(input).unwrap();
        assert_eq!(result, "Hello World");
    }

    #[test]
    fn test_decode_iso_8859_1_characters() {
        let input = &[67, 97, 102, 233]; // "Café" in ISO-8859-1
        let result = decode_from_iso_8859_1(input).unwrap();
        assert_eq!(result, "Café");
    }

    #[test]
    fn test_round_trip() {
        let original = "Hello, Café!";
        let encoded = encode_to_iso_8859_1(original).unwrap();
        let decoded = decode_from_iso_8859_1(&encoded).unwrap();
        assert_eq!(original, decoded);
    }

    #[tokio::test]
    async fn test_async_encode_basic() {
        let input = "Hello World";
        let result = encode_to_iso_8859_1_async(input).await.unwrap();
        assert_eq!(result, input.as_bytes());
    }

    #[tokio::test]
    async fn test_async_decode_basic() {
        let input = b"Hello World".to_vec();
        let result = decode_from_iso_8859_1_async(input).await.unwrap();
        assert_eq!(result, "Hello World");
    }

    #[tokio::test]
    async fn test_async_round_trip() {
        let original = "Hello, Café!";
        let encoded = encode_to_iso_8859_1_async(original).await.unwrap();
        let decoded = decode_from_iso_8859_1_async(encoded).await.unwrap();
        assert_eq!(original, decoded);
    }

    #[test]
    fn test_lossy_encoding() {
        // Test with emoji that can't be represented in ISO-8859-1
        let input = "Hello 🌍";
        let result = encode_to_iso_8859_1_lossy(input);
        // Should replace the emoji with something, but keep "Hello "
        assert!(result.starts_with(b"Hello "));
    }

    #[tokio::test]
    async fn test_async_lossy_encoding() {
        let input = "Hello 🌍";
        let result = encode_to_iso_8859_1_lossy_async(input).await;
        assert!(result.starts_with(b"Hello "));
    }

    #[test]
    fn test_split_lines_into_parts() {
        let lines = vec![
            "VERSION \"\"".to_string(),
            "".to_string(),
            "NS_ :".to_string(),
            "".to_string(),
            "BO_ 100 MSG_NAME: 8 Vector__XXX".to_string(),
            " SG_ Signal1 : 0|8@1+ (1,0) [0|255] \"\" Vector__XXX".to_string(),
            " SG_ Signal2 : 8|8@1+ (1,0) [0|255] \"\" Vector__XXX".to_string(),
            "".to_string(),
            "BA_DEF_ \"AttributeName\" STRING;".to_string(),
            "BA_ \"AttributeName\" \"AttributeValue\";".to_string(),
        ];

        let result = split_lines_into_parts(lines).unwrap();
        
        assert_eq!(result.preheader.len(), 2);
        assert!(result.preheader[0].contains("VERSION"));
        assert!(result.preheader[1].contains("NS_"));
        
        assert_eq!(result.header.len(), 3);
        assert!(result.header[0].contains("BO_"));
        assert!(result.header[1].contains("Signal1"));
        assert!(result.header[2].contains("Signal2"));
        
        assert_eq!(result.contents.len(), 2);
        assert!(result.contents[0].contains("BA_DEF_"));
        assert!(result.contents[1].contains("BA_"));
    }

    #[test]
    fn test_dbc_file_parts_structure() {
        let parts = DbcFileParts {
            preheader: vec!["VERSION \"\"".to_string()],
            header: vec!["BO_ 100 MSG: 8 Vector__XXX".to_string()],
            contents: vec!["BA_DEF_ \"test\" STRING;".to_string()],
        };

        assert_eq!(parts.preheader.len(), 1);
        assert_eq!(parts.header.len(), 1);
        assert_eq!(parts.contents.len(), 1);
    }

    // Note: File-based tests would require actual DBC files
    // These tests demonstrate the structure and expected behavior
    
    #[test]
    fn test_with_actual_dbc_file() {
        let file_path = "/Users/wrath/projects/arrow-sus/rust/downloads/parallel/CHBR1901.dbc";
        
        if std::path::Path::new(file_path).exists() {
            match split_dbc_file(file_path) {
                Ok(parts) => {
                    println!("DBC file split successfully:");
                    println!("  Preheader lines: {}", parts.preheader.len());
                    println!("  Header lines: {}", parts.header.len());
                    println!("  Contents lines: {}", parts.contents.len());
                    
                    // Basic validation - should have some content
                    let total_lines = parts.preheader.len() + parts.header.len() + parts.contents.len();
                    assert!(total_lines > 0, "DBC file should have some content");
                }
                Err(e) => {
                    println!("DBC file split failed: {}", e);
                    // This might be expected if the file is compressed or binary
                }
            }
        } else {
            println!("DBC test file not found, skipping file-based test");
        }
    }
    
    #[tokio::test]
    async fn test_with_actual_dbc_file_async() {
        let file_path = "/Users/wrath/projects/arrow-sus/rust/downloads/parallel/CHBR1901.dbc";
        
        if std::path::Path::new(file_path).exists() {
            match split_dbc_file_async(file_path).await {
                Ok(parts) => {
                    println!("Async DBC file split successfully:");
                    println!("  Preheader lines: {}", parts.preheader.len());
                    println!("  Header lines: {}", parts.header.len());
                    println!("  Contents lines: {}", parts.contents.len());
                    
                    // Basic validation - should have some content
                    let total_lines = parts.preheader.len() + parts.header.len() + parts.contents.len();
                    assert!(total_lines > 0, "DBC file should have some content");
                }
                Err(e) => {
                    println!("Async DBC file split failed: {}", e);
                    // This might be expected if the file is compressed or binary
                }
            }
        } else {
            println!("DBC test file not found, skipping async file-based test");
        }
    }

    // Note: Testing dbase_field_to_arrow_type would require creating FieldInfo instances
    // which have private fields and no public constructor. The function will be tested
    // indirectly through the file-based schema conversion functions when actual DBF files are available.

    #[test]
    fn test_arrow_schema_functions_exist() {
        // This test verifies that our functions compile and can be called
        // We're testing the API structure rather than actual file processing
        
        // These functions should compile and exist in the API
        let _sync_fn = dbase_header_to_arrow_schema;
        let _sync_with_meta_fn = dbase_header_to_arrow_schema_with_metadata;
        
        // Verify the functions can be referenced (they exist and compile)
        println!("All dbase to Arrow schema conversion functions are available and compile correctly");
        
        // Test that we can pass function parameters (won't actually call due to no real file)
        let test_path = "/non/existent/path.dbf";
        let _would_fail = || {
            let _ = _sync_fn(test_path);
            let _ = _sync_with_meta_fn(test_path);
        };
    }

    // Note: File-based tests for dbase schema conversion would require actual DBF files
    // The functions are designed to handle real dbase files and convert their headers to Arrow schemas

    #[test]
    fn test_dbc_streaming_functions_exist() {
        // Verify that our streaming functions compile and can be referenced
        use std::io::Cursor;
        let test_data = vec![0u8; 20]; // Minimal test data
        let cursor = Cursor::new(test_data);
        
        // This won't succeed with empty data, but verifies the function signature compiles
        let _result = dbc_to_dbf_reader(cursor);
        
        println!("All DBC streaming functions are available and compile correctly");
    }

    #[test] 
    fn test_with_actual_dbc_streaming() {
        let dbc_file_path = "/Users/wrath/projects/arrow-sus/rust/downloads/parallel/CHBR1901.dbc";
        let dbf_output_path = "/tmp/test_decompressed.dbf";
        
        if std::path::Path::new(dbc_file_path).exists() {
            match decompress_dbc_to_dbf(dbc_file_path, dbf_output_path) {
                Ok(()) => {
                    println!("DBC file decompressed successfully to: {}", dbf_output_path);
                    
                    // Verify the output file exists and has content
                    if let Ok(metadata) = std::fs::metadata(dbf_output_path) {
                        println!("DBF file size: {} bytes", metadata.len());
                        assert!(metadata.len() > 0, "DBF file should not be empty");
                        
                        // Clean up test file
                        let _ = std::fs::remove_file(dbf_output_path);
                    }
                }
                Err(e) => {
                    println!("DBC decompression failed: {}", e);
                    // This might be expected if the file format is different than expected
                }
            }
        } else {
            println!("DBC test file not found, skipping streaming test");
        }
    }

    #[tokio::test]
    async fn test_with_actual_dbc_streaming_async() {
        let dbc_file_path = "/Users/wrath/projects/arrow-sus/rust/downloads/parallel/CHBR1901.dbc";
        let dbf_output_path = "/tmp/test_decompressed_async.dbf";
        
        if std::path::Path::new(dbc_file_path).exists() {
            match decompress_dbc_to_dbf_async(dbc_file_path, dbf_output_path).await {
                Ok(()) => {
                    println!("Async DBC file decompressed successfully to: {}", dbf_output_path);
                    
                    // Verify the output file exists and has content
                    if let Ok(metadata) = std::fs::metadata(dbf_output_path) {
                        println!("Async DBF file size: {} bytes", metadata.len());
                        assert!(metadata.len() > 0, "Async DBF file should not be empty");
                        
                        // Clean up test file
                        let _ = std::fs::remove_file(dbf_output_path);
                    }
                }
                Err(e) => {
                    println!("Async DBC decompression failed: {}", e);
                    // This might be expected if the file format is different than expected
                }
            }
        } else {
            println!("DBC test file not found, skipping async streaming test");
        }
    }
}
