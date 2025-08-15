//! Error types for DBC IO plugin

use std::error::Error as StdError;
use std::fmt::{Display, Formatter, Result as FmtResult};
use std::io;

use dbase::FieldInfo;
use polars::error::PolarsError;
use polars::prelude::{ArrowDataType, DataType};
use arrow::datatypes::Schema as ArrowSchema;

use crate::models::dbase_utils::DbfEncodingError;

/// Any error raised by the DBC IO plugin
#[non_exhaustive]
#[derive(Debug)]
pub enum DbcError {
    /// An error from polars
    Polars(PolarsError),
    /// An error from the underlying dbase library
    Dbase(dbase::Error),
    /// An error from DBC/DBF encoding/decoding operations
    Encoding(DbfEncodingError),
    /// Cannot scan empty sources
    EmptySources,
    /// DBC file format is invalid or corrupted
    InvalidDbcFormat(String),
    /// DBF schema couldn't be converted to Polars schema
    SchemaConversion(String),
    /// Unsupported DBF field type
    UnsupportedDbfType(dbase::FieldType),
    /// Unsupported Polars type for DBC conversion
    UnsupportedPolarsType(DataType),
    /// If not all schemas in a batch were identical
    NonMatchingSchemas,
    /// If arrow type doesn't match expected schema type during conversion
    InvalidArrowType(DataType, ArrowDataType),
    /// I/O related errors with context
    IO(io::Error, String),
    /// Missing required DBC header or metadata
    MissingHeader(String),
    /// Invalid or corrupted DBC compression
    CompressionError(String),
    /// DBF record parsing failed
    RecordParsingError(String),
}

impl Display for DbcError {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        match self {
            DbcError::Polars(e) => write!(f, "Polars error: {e}"),
            DbcError::Dbase(e) => write!(f, "Dbase error: {e}"),
            DbcError::Encoding(e) => write!(f, "Encoding error: {e}"),
            DbcError::EmptySources => write!(f, "Cannot scan empty sources"),
            DbcError::InvalidDbcFormat(msg) => {
                write!(f, "Invalid DBC file format: {msg}")
            }
            DbcError::SchemaConversion(msg) => {
                write!(f, "Schema conversion failed: {msg}")
            }
            DbcError::UnsupportedDbfType(field_type) => {
                write!(f, "Unsupported DBF field type: {field_type:?}")
            }
            DbcError::UnsupportedPolarsType(dtype) => {
                write!(f, "Unsupported Polars type for DBC conversion: {dtype}")
            }
            DbcError::NonMatchingSchemas => {
                write!(f, "All batches must share the same schema")
            }
            DbcError::InvalidArrowType(dtype, arrow_dtype) => {
                write!(
                    f,
                    "Column dtypes must match their arrow types during conversion: {dtype} != {arrow_dtype:?}"
                )
            }
            DbcError::IO(err, path) => {
                write!(f, "I/O error with {path}: {err}")
            }
            DbcError::MissingHeader(msg) => {
                write!(f, "Missing required DBC header: {msg}")
            }
            DbcError::CompressionError(msg) => {
                write!(f, "DBC compression error: {msg}")
            }
            DbcError::RecordParsingError(msg) => {
                write!(f, "DBF record parsing failed: {msg}")
            }
        }
    }
}

impl StdError for DbcError {}

impl From<PolarsError> for DbcError {
    fn from(value: PolarsError) -> Self {
        Self::Polars(value)
    }
}

impl From<dbase::Error> for DbcError {
    fn from(value: dbase::Error) -> Self {
        Self::Dbase(value)
    }
}

impl From<DbfEncodingError> for DbcError {
    fn from(value: DbfEncodingError) -> Self {
        Self::Encoding(value)
    }
}

impl From<io::Error> for DbcError {
    fn from(value: io::Error) -> Self {
        Self::IO(value, "unknown source".to_string())
    }
}

/// Helper function to create IO error with context
impl DbcError {
    pub fn io_error<P: AsRef<str>>(err: io::Error, path: P) -> Self {
        Self::IO(err, path.as_ref().to_string())
    }

    pub fn invalid_format<S: Into<String>>(msg: S) -> Self {
        Self::InvalidDbcFormat(msg.into())
    }

    pub fn schema_conversion<S: Into<String>>(msg: S) -> Self {
        Self::SchemaConversion(msg.into())
    }

    pub fn compression_error<S: Into<String>>(msg: S) -> Self {
        Self::CompressionError(msg.into())
    }

    pub fn record_parsing<S: Into<String>>(msg: S) -> Self {
        Self::RecordParsingError(msg.into())
    }

    pub fn missing_header<S: Into<String>>(msg: S) -> Self {
        Self::MissingHeader(msg.into())
    }
}

/// Type alias for Results using DbcError
pub type DbcResult<T> = Result<T, DbcError>;

#[cfg(test)]
mod tests {
    use super::*;
    use polars::prelude::*;

    #[test]
    fn test_error_display() {
        let errors = vec![
            DbcError::Polars(PolarsError::NoData("test".into())),
            DbcError::EmptySources,
            DbcError::InvalidDbcFormat("bad format".to_string()),
            DbcError::SchemaConversion("conversion failed".to_string()),
            DbcError::UnsupportedDbfType(dbase::FieldType::Character),
            DbcError::UnsupportedPolarsType(DataType::Null),
            DbcError::NonMatchingSchemas,
            DbcError::InvalidArrowType(DataType::Null, ArrowDataType::Null),
            DbcError::MissingHeader("header missing".to_string()),
            DbcError::CompressionError("lzw failed".to_string()),
            DbcError::RecordParsingError("bad record".to_string()),
        ];

        for err in errors {
            let display_str = format!("{err}");
            assert!(!display_str.is_empty(), "Error display should not be empty");
            println!("Error: {}", display_str);
        }
    }

    #[test]
    fn test_error_conversions() {
        // Test PolarsError conversion
        let polars_err = PolarsError::NoData("test".into());
        let dbc_err: DbcError = polars_err.into();
        assert!(matches!(dbc_err, DbcError::Polars(_)));

        // Test io::Error conversion
        let io_err = io::Error::new(io::ErrorKind::NotFound, "file not found");
        let dbc_err: DbcError = io_err.into();
        assert!(matches!(dbc_err, DbcError::IO(_, _)));
    }

    #[test]
    fn test_error_helper_functions() {
        let err = DbcError::io_error(
            io::Error::new(io::ErrorKind::NotFound, "not found"),
            "/path/to/file.dbc"
        );
        assert!(matches!(err, DbcError::IO(_, path) if path == "/path/to/file.dbc"));

        let err = DbcError::invalid_format("bad header");
        assert!(matches!(err, DbcError::InvalidDbcFormat(msg) if msg == "bad header"));

        let err = DbcError::schema_conversion("type mismatch");
        assert!(matches!(err, DbcError::SchemaConversion(msg) if msg == "type mismatch"));

        let err = DbcError::compression_error("lzw decompression failed");
        assert!(matches!(err, DbcError::CompressionError(msg) if msg == "lzw decompression failed"));

        let err = DbcError::record_parsing("malformed record");
        assert!(matches!(err, DbcError::RecordParsingError(msg) if msg == "malformed record"));

        let err = DbcError::missing_header("pre-header not found");
        assert!(matches!(err, DbcError::MissingHeader(msg) if msg == "pre-header not found"));
    }
}
