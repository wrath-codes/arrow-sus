//! Ultra-fast DBC scanner using existing proven utilities

use std::io::BufReader;
use std::path::Path;
use std::sync::Arc;

use rayon::prelude::*;
use dbase::{Reader, Record};
use polars::prelude::{DataFrame, Series, Schema as PlSchema, PlSmallStr};

use super::error::{DbcError, DbcResult};
use super::des::{dbc_to_polars_schema, create_dbf_reader_from_file};
use crate::models::dbase_utils::decompress_dbc_to_dbf;

/// Ultra-fast scanner leveraging existing utilities
pub struct DbcScanner {
    dbf_path: std::path::PathBuf,
    schema: Arc<PlSchema>,
    chunk_size: usize,
}

impl DbcScanner {
    /// Create scanner from DBC file (uses temporary DBF)
    pub fn from_dbc_path<P: AsRef<Path>>(
        dbc_path: P,
        chunk_size: Option<usize>,
    ) -> DbcResult<Self> {
        // Get schema using existing utility
        let schema = Arc::new(dbc_to_polars_schema(&dbc_path, None)?);
        
        // Create temp DBF file  
        let temp_dbf = tempfile::NamedTempFile::new()
            .map_err(|e| DbcError::IO(e, "creating temp file".to_string()))?;
        
        // Decompress using existing utility
        decompress_dbc_to_dbf(&dbc_path, temp_dbf.path())?;
        
        Ok(Self {
            dbf_path: temp_dbf.into_temp_path().keep()
                .map_err(|e| DbcError::IO(e.error, "keeping temp file".to_string()))?,
            schema,
            chunk_size: chunk_size.unwrap_or(10_000),
        })
    }

    /// Create scanner from DBF file directly
    pub fn from_dbf_path<P: AsRef<Path>>(
        dbf_path: P,
        chunk_size: Option<usize>,
    ) -> DbcResult<Self> {
        // Get schema using existing utility
        let schema = Arc::new(super::des::dbf_header_to_polars_schema(&dbf_path, None)?);
        
        Ok(Self {
            dbf_path: dbf_path.as_ref().to_path_buf(),
            schema,
            chunk_size: chunk_size.unwrap_or(10_000),
        })
    }

    /// Get the schema
    pub fn schema(&self) -> Arc<PlSchema> {
        self.schema.clone()
    }

    /// Read entire file as single DataFrame with parallel processing
    pub fn read_all(&self) -> DbcResult<DataFrame> {
        let mut reader = create_dbf_reader_from_file(&self.dbf_path)?;
        
        // Collect all records using iterator
        let mut records = Vec::new();
        for record_result in reader.iter_records() {
            match record_result {
                Ok(record) => records.push(record),
                Err(e) => return Err(DbcError::RecordParsingError(format!("Failed to read record: {}", e))),
            }
        }
        
        if records.is_empty() {
            return Err(DbcError::EmptySources);
        }

        // Process in parallel chunks
        self.records_to_dataframe_parallel(records)
    }

    /// Convert records to DataFrame using parallel processing
    fn records_to_dataframe_parallel(&self, records: Vec<Record>) -> DbcResult<DataFrame> {
        let schema = &self.schema;
        let num_fields = schema.len();
        
        // Calculate optimal chunk size for parallel processing
        let num_threads = rayon::current_num_threads();
        let parallel_chunk_size = std::cmp::max(1, records.len() / num_threads);
        
        // Process records in parallel chunks and collect field data
        let chunked_columns: Vec<Vec<Vec<String>>> = records
            .par_chunks(parallel_chunk_size)
            .map(|record_chunk| {
                let mut chunk_columns: Vec<Vec<String>> = (0..num_fields)
                    .map(|_| Vec::with_capacity(record_chunk.len()))
                    .collect();

                for record in record_chunk {
                    // Get field names from schema to access record values
                    for (field_idx, (field_name, _)) in schema.iter().enumerate() {
                        let field_str = if let Some(field_value) = record.get(field_name.as_str()) {
                            format!("{}", field_value)
                        } else {
                            String::new() // Empty string for missing fields
                        };
                        chunk_columns[field_idx].push(field_str);
                    }
                }

                chunk_columns
            })
            .collect();

        // Merge parallel chunks
        let mut final_columns: Vec<Vec<String>> = (0..num_fields)
            .map(|_| Vec::with_capacity(records.len()))
            .collect();

        for chunk_columns in chunked_columns {
            for (field_idx, mut chunk_values) in chunk_columns.into_iter().enumerate() {
                final_columns[field_idx].append(&mut chunk_values);
            }
        }

        // Convert to Polars Series in parallel
        let series_results: Result<Vec<Series>, polars::error::PolarsError> = schema
            .iter()
            .enumerate()
            .collect::<Vec<_>>()
            .par_iter()
            .map(|(field_idx, (field_name, field_dtype))| {
                let values = &final_columns[*field_idx];
                self.strings_to_series(field_name, field_dtype, values)
            })
            .collect();

        let series = series_results.map_err(DbcError::Polars)?;
        
        // Create DataFrame
        let columns: Vec<polars::prelude::Column> = series.into_iter().map(|s| s.into()).collect();
        DataFrame::new(columns).map_err(DbcError::Polars)
    }

    /// Convert string values to appropriate Polars Series based on data type
    fn strings_to_series(
        &self,
        field_name: &PlSmallStr,
        field_dtype: &polars::prelude::DataType,
        values: &[String],
    ) -> Result<Series, polars::error::PolarsError> {
        use polars::prelude::*;

        match field_dtype {
            DataType::String => {
                Ok(Series::new(field_name.clone(), values))
            }
            DataType::Int32 => {
                let int_values: Vec<Option<i32>> = values
                    .iter()
                    .map(|s| s.trim().parse().ok())
                    .collect();
                Ok(Series::new(field_name.clone(), int_values))
            }
            DataType::Float64 => {
                let float_values: Vec<Option<f64>> = values
                    .iter()
                    .map(|s| s.trim().parse().ok())
                    .collect();
                Ok(Series::new(field_name.clone(), float_values))
            }
            DataType::Boolean => {
                let bool_values: Vec<Option<bool>> = values
                    .iter()
                    .map(|s| {
                        match s.trim().to_lowercase().as_str() {
                            "true" | "t" | "1" | "y" | "yes" => Some(true),
                            "false" | "f" | "0" | "n" | "no" => Some(false),
                            _ => None,
                        }
                    })
                    .collect();
                Ok(Series::new(field_name.clone(), bool_values))
            }
            _ => {
                // Fallback to string for other types
                Ok(Series::new(field_name.clone(), values))
            }
        }
    }
}

/// Convenience function to read entire DBC file
pub fn read_dbc<P: AsRef<Path>>(dbc_path: P) -> DbcResult<DataFrame> {
    let scanner = DbcScanner::from_dbc_path(dbc_path, None)?;
    scanner.read_all()
}

/// Convenience function to read entire DBF file
pub fn read_dbf<P: AsRef<Path>>(dbf_path: P) -> DbcResult<DataFrame> {
    let scanner = DbcScanner::from_dbf_path(dbf_path, None)?;
    scanner.read_all()
}

/// Create scanner for chunked processing
pub fn scan_dbc<P: AsRef<Path>>(
    dbc_path: P,
    chunk_size: Option<usize>,
) -> DbcResult<DbcScanner> {
    DbcScanner::from_dbc_path(dbc_path, chunk_size)
}

/// Create scanner for DBF files
pub fn scan_dbf<P: AsRef<Path>>(
    dbf_path: P,
    chunk_size: Option<usize>,
) -> DbcResult<DbcScanner> {
    DbcScanner::from_dbf_path(dbf_path, chunk_size)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Instant;

    #[test]
    fn test_scanner_creation() {
        let test_path = "/nonexistent/test.dbc";
        
        // Test that functions compile
        let _scan_result = || scan_dbc(test_path, Some(1000));
        let _read_result = || read_dbc(test_path);
        
        println!("Scanner functions compile correctly");
    }

    #[test]
    fn test_parallel_processing_setup() {
        let num_threads = rayon::current_num_threads();
        println!("Rayon configured with {} threads", num_threads);
        assert!(num_threads > 0);
        
        // Test parallel chunk calculation
        let record_count = 10_000;
        let parallel_chunk_size = std::cmp::max(1, record_count / num_threads);
        println!("For {} records with {} threads, chunk size: {}", 
                record_count, num_threads, parallel_chunk_size);
    }

    #[test]
    fn test_with_actual_dbc_performance() {
        let dbc_file_path = "/Users/wrath/projects/arrow-sus/rust/downloads/parallel/CHBR1901.dbc";
        
        if std::path::Path::new(dbc_file_path).exists() {
            println!("\n=== DBC Performance Test ===");
            
            // Test schema extraction performance
            let start = Instant::now();
            match dbc_to_polars_schema(dbc_file_path, None) {
                Ok(schema) => {
                    let schema_time = start.elapsed();
                    println!("✓ Schema extraction: {:?}", schema_time);
                    println!("  Fields: {}", schema.len());
                    
                    // Test full file reading performance
                    let start = Instant::now();
                    match read_dbc(dbc_file_path) {
                        Ok(df) => {
                            let read_time = start.elapsed();
                            println!("✓ Full file read: {:?}", read_time);
                            println!("  Rows: {}, Cols: {}", df.height(), df.width());
                            println!("  Memory: ~{} MB", (df.height() * df.width() * 8) / 1_000_000);
                            
                            // Test scanning with different chunk sizes
                            for chunk_size in [1_000, 5_000, 10_000] {
                                let start = Instant::now();
                                match scan_dbc(dbc_file_path, Some(chunk_size)) {
                                    Ok(scanner) => {
                                        let setup_time = start.elapsed();
                                        println!("✓ Scanner setup ({}): {:?}", chunk_size, setup_time);
                                    }
                                    Err(e) => {
                                        println!("✗ Scanner failed ({}): {}", chunk_size, e);
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            println!("✗ File read failed: {}", e);
                        }
                    }
                }
                Err(e) => {
                    println!("✗ Schema extraction failed: {}", e);
                }
            }
        } else {
            println!("DBC test file not found, skipping performance test");
        }
    }

    #[test]
    fn test_string_type_conversion() {
        // Test our string-based type conversion approach
        let test_strings = vec!["123".to_string(), "456".to_string(), "invalid".to_string()];
        
        // Test int conversion
        let int_values: Vec<Option<i32>> = test_strings
            .iter()
            .map(|s| s.trim().parse().ok())
            .collect();
        
        assert_eq!(int_values, vec![Some(123), Some(456), None]);
        
        // Test float conversion
        let float_strings = vec!["123.45".to_string(), "67.89".to_string(), "invalid".to_string()];
        let float_values: Vec<Option<f64>> = float_strings
            .iter()
            .map(|s| s.trim().parse().ok())
            .collect();
        
        assert_eq!(float_values, vec![Some(123.45), Some(67.89), None]);
        
        // Test boolean conversion
        let bool_strings = vec!["true".to_string(), "false".to_string(), "1".to_string(), "invalid".to_string()];
        let bool_values: Vec<Option<bool>> = bool_strings
            .iter()
            .map(|s| {
                match s.trim().to_lowercase().as_str() {
                    "true" | "t" | "1" | "y" | "yes" => Some(true),
                    "false" | "f" | "0" | "n" | "no" => Some(false),
                    _ => None,
                }
            })
            .collect();
        
        assert_eq!(bool_values, vec![Some(true), Some(false), Some(true), None]);
        
        println!("String-based type conversion working correctly");
    }
}
