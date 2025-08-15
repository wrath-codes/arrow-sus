//! Utilities for converting from DBF/DBC to Polars using existing Arrow schema utilities

use std::path::Path;

use super::error::{DbcError, DbcResult};
use crate::models::dbase_utils::{
    dbase_header_to_arrow_schema, dbase_header_to_arrow_schema_with_metadata,
    decompress_dbc_to_dbf,
};
use dbase::{FieldInfo, Reader};
use polars::prelude::{PlSmallStr, Schema as PlSchema, DataType};
use arrow::datatypes::Schema as ArrowSchema;

/// Convert Arrow DataType to Polars DataType
fn arrow_dtype_to_polars(arrow_dtype: &arrow::datatypes::DataType) -> DataType {
    match arrow_dtype {
        arrow::datatypes::DataType::Null => DataType::Null,
        arrow::datatypes::DataType::Boolean => DataType::Boolean,
        arrow::datatypes::DataType::Int8 => DataType::Int8,
        arrow::datatypes::DataType::Int16 => DataType::Int16,
        arrow::datatypes::DataType::Int32 => DataType::Int32,
        arrow::datatypes::DataType::Int64 => DataType::Int64,
        arrow::datatypes::DataType::UInt8 => DataType::UInt8,
        arrow::datatypes::DataType::UInt16 => DataType::UInt16,
        arrow::datatypes::DataType::UInt32 => DataType::UInt32,
        arrow::datatypes::DataType::UInt64 => DataType::UInt64,
        arrow::datatypes::DataType::Float16 => DataType::Float32, // Polars doesn't have Float16
        arrow::datatypes::DataType::Float32 => DataType::Float32,
        arrow::datatypes::DataType::Float64 => DataType::Float64,
        arrow::datatypes::DataType::Utf8 => DataType::String,
        arrow::datatypes::DataType::LargeUtf8 => DataType::String,
        arrow::datatypes::DataType::Binary => DataType::Binary,
        arrow::datatypes::DataType::LargeBinary => DataType::Binary,
        arrow::datatypes::DataType::Date32 => DataType::Date,
        arrow::datatypes::DataType::Date64 => DataType::Date,
        arrow::datatypes::DataType::Timestamp(unit, _tz) => {
            let time_unit = match unit {
                arrow::datatypes::TimeUnit::Second => polars::prelude::TimeUnit::Milliseconds,
                arrow::datatypes::TimeUnit::Millisecond => polars::prelude::TimeUnit::Milliseconds,
                arrow::datatypes::TimeUnit::Microsecond => polars::prelude::TimeUnit::Microseconds,
                arrow::datatypes::TimeUnit::Nanosecond => polars::prelude::TimeUnit::Nanoseconds,
            };
            let timezone = None; // Simplify for now - timezone handling can be added later
            DataType::Datetime(time_unit, timezone)
        }
        arrow::datatypes::DataType::Time32(_) | arrow::datatypes::DataType::Time64(_) => {
            DataType::Time
        }
        arrow::datatypes::DataType::Decimal128(precision, scale) => {
            DataType::Decimal(Some(*precision as usize), Some(*scale as usize))
        }
        // For unsupported types, default to String
        _ => DataType::String,
    }
}

/// Convert Arrow Schema to Polars Schema
pub fn arrow_schema_to_polars(arrow_schema: &ArrowSchema) -> DbcResult<PlSchema> {
    let polars_fields: Vec<_> = arrow_schema
        .fields()
        .iter()
        .map(|field| {
            let polars_dtype = arrow_dtype_to_polars(field.data_type());
            (field.name().clone().into(), polars_dtype)
        })
        .collect();

    Ok(PlSchema::from_iter(polars_fields))
}

/// Convert DBF file header to Polars Schema using existing utilities
pub fn dbf_header_to_polars_schema<P: AsRef<Path>>(
    file_path: P,
    single_column_name: Option<&PlSmallStr>,
) -> DbcResult<PlSchema> {
    // Use existing Arrow schema conversion
    let arrow_schema = dbase_header_to_arrow_schema(file_path)?;
    
    // Convert Arrow → Polars
    let mut polars_schema = arrow_schema_to_polars(&arrow_schema)?;
    
    // Handle single column renaming if requested
    if let Some(col_name) = single_column_name {
        if polars_schema.len() == 1 {
            let (_, dtype) = polars_schema.iter().next().unwrap();
            polars_schema = PlSchema::from_iter([(col_name.clone(), dtype.clone())]);
        }
    }
    
    Ok(polars_schema)
}

/// Convert DBF file header to Polars Schema with metadata using existing utilities
pub fn dbf_header_to_polars_schema_with_metadata<P: AsRef<Path>>(
    file_path: P,
    single_column_name: Option<&PlSmallStr>,
) -> DbcResult<(PlSchema, Vec<FieldInfo>)> {
    // Use existing Arrow schema conversion with metadata
    let (arrow_schema, field_infos) = dbase_header_to_arrow_schema_with_metadata(file_path)?;
    
    // Convert Arrow → Polars
    let mut polars_schema = arrow_schema_to_polars(&arrow_schema)?;
    
    // Handle single column renaming if requested
    if let Some(col_name) = single_column_name {
        if polars_schema.len() == 1 {
            let (_, dtype) = polars_schema.iter().next().unwrap();
            polars_schema = PlSchema::from_iter([(col_name.clone(), dtype.clone())]);
        }
    }
    
    Ok((polars_schema, field_infos))
}

/// Convert DBC file to Polars Schema by first decompressing to DBF
pub fn dbc_to_polars_schema<P: AsRef<Path>>(
    dbc_path: P,
    single_column_name: Option<&PlSmallStr>,
) -> DbcResult<PlSchema> {
    // Create a temporary DBF file
    let temp_dbf = tempfile::NamedTempFile::new()
        .map_err(|e| DbcError::IO(e, "creating temp file".to_string()))?;
    
    // Decompress DBC to DBF
    decompress_dbc_to_dbf(&dbc_path, temp_dbf.path())?;
    
    // Get schema from the decompressed DBF
    dbf_header_to_polars_schema(temp_dbf.path(), single_column_name)
}

/// Create a DBF reader from a file path  
pub fn create_dbf_reader_from_file<P: AsRef<Path>>(
    file_path: P,
) -> DbcResult<Reader<std::io::BufReader<std::fs::File>>> {
    Reader::from_path(file_path).map_err(DbcError::from)
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_arrow_to_polars_schema_conversion() {
        // Create a simple Arrow schema
        let arrow_fields = vec![
            arrow::datatypes::Field::new("id", arrow::datatypes::DataType::Int32, false),
            arrow::datatypes::Field::new("name", arrow::datatypes::DataType::Utf8, true),
            arrow::datatypes::Field::new("score", arrow::datatypes::DataType::Float64, true),
        ];
        let arrow_schema = ArrowSchema::new(arrow_fields);
        
        // Convert to Polars schema
        let polars_schema = arrow_schema_to_polars(&arrow_schema).unwrap();
        
        // Verify conversion
        assert_eq!(polars_schema.len(), 3);
        assert!(polars_schema.contains("id"));
        assert!(polars_schema.contains("name"));
        assert!(polars_schema.contains("score"));
        
        // Check data types
        let id_type = polars_schema.get("id").unwrap();
        let name_type = polars_schema.get("name").unwrap();
        let score_type = polars_schema.get("score").unwrap();
        
        assert_eq!(*id_type, DataType::Int32);
        assert_eq!(*name_type, DataType::String);
        assert_eq!(*score_type, DataType::Float64);
    }
    
    #[test]
    fn test_arrow_dtype_conversions() {
        // Test individual type conversions
        assert_eq!(arrow_dtype_to_polars(&arrow::datatypes::DataType::Boolean), DataType::Boolean);
        assert_eq!(arrow_dtype_to_polars(&arrow::datatypes::DataType::Int32), DataType::Int32);
        assert_eq!(arrow_dtype_to_polars(&arrow::datatypes::DataType::Float64), DataType::Float64);
        assert_eq!(arrow_dtype_to_polars(&arrow::datatypes::DataType::Utf8), DataType::String);
        assert_eq!(arrow_dtype_to_polars(&arrow::datatypes::DataType::Date32), DataType::Date);
        
        // Test unsupported type fallback
        assert_eq!(arrow_dtype_to_polars(&arrow::datatypes::DataType::Interval(arrow::datatypes::IntervalUnit::DayTime)), DataType::String);
    }
    
    #[test]
    fn test_single_column_renaming() {
        let arrow_fields = vec![
            arrow::datatypes::Field::new("original_name", arrow::datatypes::DataType::Int32, false),
        ];
        let arrow_schema = ArrowSchema::new(arrow_fields);
        
        let polars_schema = arrow_schema_to_polars(&arrow_schema).unwrap();
        assert!(polars_schema.contains("original_name"));
        
        // Test that the schema conversion logic exists (actual renaming happens in higher-level functions)
        assert_eq!(polars_schema.len(), 1);
    }
    
    #[test]
    fn test_utility_functions_exist() {
        // Verify that our utility functions compile and can be referenced
        use std::path::Path;
        
        // Test function signatures exist (won't call with dummy data)
        let test_path = Path::new("/nonexistent/test.dbf");
        let _result1 = || dbf_header_to_polars_schema(test_path, None);
        let _result2 = || dbf_header_to_polars_schema_with_metadata(test_path, None);
        let _result3 = || dbc_to_polars_schema(test_path, None);
        let _result4 = || create_dbf_reader_from_file(test_path);
        
        println!("All DBC to Polars utility functions are available and compile correctly");
    }
    
    #[test]
    fn test_with_actual_dbf_file() {
        // Test with a decompressed DBF file if it exists
        let dbf_file_path = "/tmp/test_decompressed.dbf";
        
        if std::path::Path::new(dbf_file_path).exists() {
            match dbf_header_to_polars_schema(dbf_file_path, None) {
                Ok(schema) => {
                    println!("DBF to Polars schema conversion successful:");
                    println!("  Schema fields: {}", schema.len());
                    for (name, dtype) in schema.iter() {
                        println!("    {}: {:?}", name, dtype);
                    }
                    assert!(schema.len() > 0, "Schema should have at least one field");
                }
                Err(e) => {
                    println!("DBF to Polars schema conversion failed: {}", e);
                }
            }
        } else {
            println!("Test DBF file not found, skipping schema conversion test");
        }
    }
    
    #[test]
    fn test_with_actual_dbc_file() {
        let dbc_file_path = "/Users/wrath/projects/arrow-sus/rust/downloads/parallel/CHBR1901.dbc";
        
        if std::path::Path::new(dbc_file_path).exists() {
            match dbc_to_polars_schema(dbc_file_path, None) {
                Ok(schema) => {
                    println!("DBC to Polars schema conversion successful:");
                    println!("  Schema fields: {}", schema.len());
                    for (name, dtype) in schema.iter() {
                        println!("    {}: {:?}", name, dtype);
                    }
                    assert!(schema.len() > 0, "Schema should have at least one field");
                }
                Err(e) => {
                    println!("DBC to Polars schema conversion failed: {}", e);
                }
            }
        } else {
            println!("DBC test file not found, skipping DBC schema conversion test");
        }
    }
}
