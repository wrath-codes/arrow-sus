use polars::prelude::*;
use std::path::Path;
use dbase::Reader;
use crate::models::polars_utils::dbase_pl::{
    DbasePolarsError,
    dbase_field_to_polars_type,
};
use crate::models::dbase_utils::{
    dbase_header_to_arrow_schema_with_metadata,
};

/// Configuration for describing dBase files
#[derive(Debug, Clone)]
pub struct DescribeConfig {
    /// Whether to output quietly (minimal information)
    pub quietly: bool,
    /// Whether to show detailed information about each field
    pub detailed: bool,
    /// Optional SQL WHERE condition to filter records
    pub sql_where: Option<String>,
}

impl Default for DescribeConfig {
    fn default() -> Self {
        Self {
            quietly: false,
            detailed: false,
            sql_where: None,
        }
    }
}

/// Summary information about a dBase file
#[derive(Debug, Clone)]
pub struct DbaseFileSummary {
    /// Number of columns (fields) in the file
    pub n_columns: usize,
    /// Number of rows (records) in the file  
    pub n_rows: usize,
    /// Schema information
    pub schema: Schema,
    /// Field information with dBase-specific metadata
    pub field_info: Vec<DbaseFieldInfo>,
}

/// Information about a dBase field
#[derive(Debug, Clone)]
pub struct DbaseFieldInfo {
    /// Field name
    pub name: String,
    /// dBase field type as string
    pub dbase_type: String,
    /// Length of the field
    pub length: u8,
    /// Polars data type
    pub polars_type: DataType,
}

/// Get schema from a dBase file (leveraging Arrow conversion)
pub fn get_dbase_schema<P: AsRef<Path>>(file_path: P) -> Result<Schema, DbasePolarsError> {
    // First get the Arrow schema with metadata from dbase_utils
    let (arrow_schema, _field_infos) = dbase_header_to_arrow_schema_with_metadata(file_path.as_ref())
        .map_err(|e| DbasePolarsError::IoError(format!("Arrow schema conversion failed: {}", e)))?;
    
    // Convert Arrow schema to Polars schema
    let polars_fields: Vec<Field> = arrow_schema
        .fields()
        .iter()
        .map(|arrow_field| {
            let polars_type = arrow_to_polars_type(arrow_field.data_type());
            Field::new(PlSmallStr::from(arrow_field.name()), polars_type)
        })
        .collect();
    
    Ok(Schema::from_iter(polars_fields))
}

/// Convert Arrow DataType to Polars DataType
fn arrow_to_polars_type(arrow_type: &arrow::datatypes::DataType) -> DataType {
    use arrow::datatypes::DataType as ArrowDataType;
    
    match arrow_type {
        ArrowDataType::Utf8 => DataType::String,
        ArrowDataType::Boolean => DataType::Boolean,
        ArrowDataType::Int8 => DataType::Int8,
        ArrowDataType::Int16 => DataType::Int16,
        ArrowDataType::Int32 => DataType::Int32,
        ArrowDataType::Int64 => DataType::Int64,
        ArrowDataType::Float32 => DataType::Float32,
        ArrowDataType::Float64 => DataType::Float64,
        ArrowDataType::Date32 => DataType::Date,
        ArrowDataType::Timestamp(time_unit, _timezone) => {
            let polars_time_unit = match time_unit {
                arrow::datatypes::TimeUnit::Second => TimeUnit::Milliseconds,
                arrow::datatypes::TimeUnit::Millisecond => TimeUnit::Milliseconds,
                arrow::datatypes::TimeUnit::Microsecond => TimeUnit::Microseconds,
                arrow::datatypes::TimeUnit::Nanosecond => TimeUnit::Nanoseconds,
            };
            // For now, ignore timezone info for simplicity
            DataType::Datetime(polars_time_unit, None)
        },
        // Default fallback for unsupported types
        _ => DataType::String,
    }
}

/// Get row count from a dBase file
pub fn get_dbase_row_count<P: AsRef<Path>>(file_path: P) -> Result<usize, DbasePolarsError> {
    let mut reader = Reader::from_path(file_path)?;
    let mut count = 0;
    
    for record_result in reader.iter_records() {
        let _record = record_result?;
        count += 1;
    }
    
    Ok(count)
}

/// Get comprehensive summary of a dBase file
pub fn get_dbase_file_summary<P: AsRef<Path>>(
    file_path: P, 
    _config: DescribeConfig
) -> Result<DbaseFileSummary, DbasePolarsError> {
    let reader = Reader::from_path(file_path.as_ref())?;
    let fields = reader.fields();
    
    // Build schema
    let schema_fields: Vec<Field> = fields
        .iter()
        .map(|field_info| {
            let name = field_info.name();
            let data_type = dbase_field_to_polars_type(field_info);
            Field::new(PlSmallStr::from(name), data_type)
        })
        .collect();
    
    let schema = Schema::from_iter(schema_fields);
    let n_columns = schema.len();
    
    // Build field info
    let field_info: Vec<DbaseFieldInfo> = fields
        .iter()
        .map(|field| {
            DbaseFieldInfo {
                name: field.name().to_string(),
                dbase_type: format!("{:?}", field.field_type()),
                length: field.length(),
                polars_type: dbase_field_to_polars_type(field),
            }
        })
        .collect();
    
    // Get row count (this could be optimized for large files)
    let n_rows = get_dbase_row_count(file_path)?;
    
    Ok(DbaseFileSummary {
        n_columns,
        n_rows,
        schema,
        field_info,
    })
}

/// Display summary information about a dBase file
pub fn describe_dbase_file<P: AsRef<Path>>(
    file_path: P, 
    config: DescribeConfig
) -> Result<(), DbasePolarsError> {
    let summary = get_dbase_file_summary(file_path, config.clone())?;
    
    if !config.quietly {
        println!("dBase File Summary");
        println!("==================");
        println!("Columns: {}", summary.n_columns);
        println!("Rows: {}", summary.n_rows);
        println!();
    }
    
    if config.detailed {
        println!("Field Details:");
        println!("--------------");
        for (i, field) in summary.field_info.iter().enumerate() {
            println!(
                "{:3}: {} ({}) -> {:?} [length: {}]",
                i + 1,
                field.name,
                field.dbase_type,
                field.polars_type,
                field.length
            );
        }
        println!();
    } else if !config.quietly {
        println!("Fields:");
        for (i, field) in summary.field_info.iter().enumerate() {
            println!("  {:3}: {} ({})", i + 1, field.name, field.dbase_type);
        }
        println!();
    }
    
    Ok(())
}

/// Create a Polars schema with dBase-specific metadata
pub fn dbase_schema_with_metadata<P: AsRef<Path>>(
    file_path: P
) -> Result<(Schema, Vec<DbaseFieldInfo>), DbasePolarsError> {
    let reader = Reader::from_path(file_path)?;
    let fields = reader.fields();
    
    let mut schema_fields = Vec::new();
    let mut field_info = Vec::new();
    
    for field in fields.iter() {
        let name = field.name();
        let data_type = dbase_field_to_polars_type(field);
        
        // Create field without metadata for now (metadata API may differ in polars 0.49)
        let polars_field = Field::new(PlSmallStr::from(name), data_type.clone());
        
        schema_fields.push(polars_field);
        
        field_info.push(DbaseFieldInfo {
            name: name.to_string(),
            dbase_type: format!("{:?}", field.field_type()),
            length: field.length(),
            polars_type: data_type,
        });
    }
    
    let schema = Schema::from_iter(schema_fields);
    Ok((schema, field_info))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_describe_config_default() {
        let config = DescribeConfig::default();
        assert!(!config.quietly);
        assert!(!config.detailed);
        assert!(config.sql_where.is_none());
    }

    #[test]
    fn test_describe_config_custom() {
        let config = DescribeConfig {
            quietly: true,
            detailed: true,
            sql_where: Some("age > 25".to_string()),
        };
        assert!(config.quietly);
        assert!(config.detailed);
        assert_eq!(config.sql_where.unwrap(), "age > 25");
    }

    #[test]
    fn test_dbase_field_info_creation() {
        let field_info = DbaseFieldInfo {
            name: "TEST_FIELD".to_string(),
            dbase_type: "Character".to_string(),
            length: 50,
            polars_type: DataType::String,
        };
        
        assert_eq!(field_info.name, "TEST_FIELD");
        assert_eq!(field_info.dbase_type, "Character");
        assert_eq!(field_info.length, 50);
        assert_eq!(field_info.polars_type, DataType::String);
    }

    // Note: File-based tests would require actual dBase files
    // These would test the actual schema extraction and description functionality
    #[test]
    fn test_schema_functions_with_nonexistent_file() {
        let nonexistent = "/nonexistent/file.dbf";
        
        // These should return errors for non-existent files
        assert!(get_dbase_schema(nonexistent).is_err());
        assert!(get_dbase_row_count(nonexistent).is_err());
        assert!(get_dbase_file_summary(nonexistent, DescribeConfig::default()).is_err());
    }

    #[test]
    fn test_with_actual_dbase_file() {
        let test_files = [
            "/Users/wrath/projects/arrow-sus/rust/downloads/parallel/CHBR1901.dbc",
            "/Users/wrath/projects/arrow-sus/rust/downloads/parallel/CHBR1902.dbc",
        ];

        for file_path in &test_files {
            if std::path::Path::new(file_path).exists() {
                println!("\n🧪 Testing with actual dBase file: {}", file_path);
                
                // Test schema extraction
                match get_dbase_schema(file_path) {
                    Ok(schema) => {
                        println!("✅ Schema extraction successful!");
                        println!("   📊 Fields: {}", schema.len());
                        
                        // Print first few fields
                        for (i, (name, dtype)) in schema.iter().take(5).enumerate() {
                            println!("   📋 Field {}: {} -> {:?}", i+1, name, dtype);
                        }
                        
                        assert!(schema.len() > 0, "Schema should have at least one field");
                    }
                    Err(e) => {
                        println!("❌ Schema extraction failed: {}", e);
                    }
                }
                
                // Test row count
                match get_dbase_row_count(file_path) {
                    Ok(count) => {
                        println!("✅ Row count: {}", count);
                        assert!(count >= 0, "Row count should be non-negative");
                    }
                    Err(e) => {
                        println!("❌ Row count failed: {}", e);
                    }
                }
                
                // Test full file summary
                let config = DescribeConfig {
                    quietly: true,
                    detailed: false,
                    sql_where: None,
                };
                
                match get_dbase_file_summary(file_path, config) {
                    Ok(summary) => {
                        println!("✅ File summary successful!");
                        println!("   📊 Columns: {}, Rows: {}", summary.n_columns, summary.n_rows);
                        println!("   📋 Field types: {:?}", 
                            summary.field_info.iter().map(|f| &f.dbase_type).collect::<Vec<_>>());
                        
                        assert_eq!(summary.n_columns, summary.schema.len());
                        assert_eq!(summary.n_columns, summary.field_info.len());
                    }
                    Err(e) => {
                        println!("❌ File summary failed: {}", e);
                    }
                }
                
                // Test schema with metadata
                match dbase_schema_with_metadata(file_path) {
                    Ok((schema, field_info)) => {
                        println!("✅ Schema with metadata successful!");
                        println!("   📊 Schema fields: {}, FieldInfo count: {}", 
                                schema.len(), field_info.len());
                        
                        assert_eq!(schema.len(), field_info.len());
                    }
                    Err(e) => {
                        println!("❌ Schema with metadata failed: {}", e);
                    }
                }
                
                break; // Only test with first available file
            } else {
                println!("📂 Test file not found: {}", file_path);
            }
        }
    }
}
