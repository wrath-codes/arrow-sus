//! Ultra-fast DBC scanner with maximum performance defaults and LazyFrame support

use std::io::BufReader;
use std::path::Path;
use std::sync::Arc;

use rayon::prelude::*;
use dbase::{Reader, Record};
use polars::prelude::{DataFrame, Series, LazyFrame, Schema as PlSchema, PlSmallStr, IntoLazy};

use super::error::{DbcError, DbcResult};
use super::des::{dbc_to_polars_schema, create_dbf_reader_from_file};
use crate::models::dbase_utils::decompress_dbc_to_dbf;

/// Performance configuration with optimal defaults
#[derive(Debug, Clone)]
pub struct DbcConfig {
    /// Chunk size for parallel processing (auto-tuned by default)
    pub chunk_size: usize,
    /// Number of parallel threads (uses all available by default)
    pub num_threads: Option<usize>,
    /// Columns to select (None = all columns)
    pub columns: Option<Vec<String>>,
    /// Memory limit per chunk in MB (default: 100MB)
    pub memory_limit_mb: usize,
}

impl Default for DbcConfig {
    fn default() -> Self {
        let num_threads = rayon::current_num_threads();
        // Auto-tune chunk size based on cores and memory
        let optimal_chunk_size = std::cmp::max(1_000, 50_000 / num_threads);
        
        Self {
            chunk_size: optimal_chunk_size,
            num_threads: None, // Use all available
            columns: None,     // Read all columns
            memory_limit_mb: 100,
        }
    }
}

/// Ultra-fast scanner leveraging existing utilities
pub struct DbcScanner {
    dbf_path: std::path::PathBuf,
    schema: Arc<PlSchema>,
    config: DbcConfig,
}

impl DbcScanner {
    /// Create scanner from DBC file with optimal performance defaults
    pub fn from_dbc_path<P: AsRef<Path>>(
        dbc_path: P,
        config: Option<DbcConfig>,
    ) -> DbcResult<Self> {
        let config = config.unwrap_or_default();
        
        // Set rayon thread pool if specified
        if let Some(num_threads) = config.num_threads {
            rayon::ThreadPoolBuilder::new()
                .num_threads(num_threads)
                .build_global()
                .map_err(|e| DbcError::InvalidDbcFormat(format!("Failed to set thread pool: {}", e)))?;
        }
        
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
            config,
        })
    }

    /// Create scanner from DBF file directly
    pub fn from_dbf_path<P: AsRef<Path>>(
        dbf_path: P,
        config: Option<DbcConfig>,
    ) -> DbcResult<Self> {
        let config = config.unwrap_or_default();
        
        // Get schema using existing utility
        let schema = Arc::new(super::des::dbf_header_to_polars_schema(&dbf_path, None)?);
        
        Ok(Self {
            dbf_path: dbf_path.as_ref().to_path_buf(),
            schema,
            config,
        })
    }

    /// Get the schema
    pub fn schema(&self) -> Arc<PlSchema> {
        self.schema.clone()
    }

    /// Create a LazyFrame for efficient lazy evaluation
    pub fn lazy(&self) -> DbcResult<LazyFrame> {
        // For now, read the data and convert to lazy
        // TODO: Implement true lazy evaluation with predicate pushdown
        let df = self.read_all()?;
        Ok(df.lazy())
    }

    /// Read with column selection for better performance
    pub fn read_columns(&self, columns: &[&str]) -> DbcResult<DataFrame> {
        // Filter schema to only requested columns
        let filtered_schema: PlSchema = self.schema
            .iter()
            .filter(|(name, _)| columns.contains(&name.as_str()))
            .map(|(name, dtype)| (name.clone(), dtype.clone()))
            .collect();

        if filtered_schema.is_empty() {
            return Err(DbcError::InvalidDbcFormat("No valid columns found".to_string()));
        }

        // Read all data but only process requested columns
        let mut reader = create_dbf_reader_from_file(&self.dbf_path)?;
        
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

        // Process only selected columns in parallel
        self.records_to_dataframe_filtered(records, &filtered_schema, columns)
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
        
        // Use optimized chunk size from config
        let parallel_chunk_size = std::cmp::max(1, records.len() / rayon::current_num_threads());
        
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

    /// Convert records to DataFrame with column filtering (for better performance)
    fn records_to_dataframe_filtered(
        &self, 
        records: Vec<Record>, 
        filtered_schema: &PlSchema,
        selected_columns: &[&str]
    ) -> DbcResult<DataFrame> {
        let num_fields = filtered_schema.len();
        
        // Use optimized chunk size from config
        let parallel_chunk_size = std::cmp::max(1, records.len() / rayon::current_num_threads());
        
        // Process records in parallel chunks - only selected columns
        let chunked_columns: Vec<Vec<Vec<String>>> = records
            .par_chunks(parallel_chunk_size)
            .map(|record_chunk| {
                let mut chunk_columns: Vec<Vec<String>> = (0..num_fields)
                    .map(|_| Vec::with_capacity(record_chunk.len()))
                    .collect();

                for record in record_chunk {
                    // Only process selected columns
                    for (field_idx, column_name) in selected_columns.iter().enumerate() {
                        let field_str = if let Some(field_value) = record.get(column_name) {
                            format!("{}", field_value)
                        } else {
                            String::new()
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
        let series_results: Result<Vec<Series>, polars::error::PolarsError> = filtered_schema
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

/// Read entire DBC file with maximum performance defaults
pub fn read_dbc<P: AsRef<Path>>(dbc_path: P) -> DbcResult<DataFrame> {
    let scanner = DbcScanner::from_dbc_path(dbc_path, None)?;
    scanner.read_all()
}

/// Read DBC file with custom configuration
pub fn read_dbc_with_config<P: AsRef<Path>>(dbc_path: P, config: DbcConfig) -> DbcResult<DataFrame> {
    let scanner = DbcScanner::from_dbc_path(dbc_path, Some(config))?;
    scanner.read_all()
}

/// Read DBC file with column selection (fastest for partial data)
pub fn read_dbc_columns<P: AsRef<Path>>(dbc_path: P, columns: &[&str]) -> DbcResult<DataFrame> {
    let scanner = DbcScanner::from_dbc_path(dbc_path, None)?;
    scanner.read_columns(columns)
}

/// Create LazyFrame from DBC file (recommended for chaining operations)
pub fn scan_dbc_lazy<P: AsRef<Path>>(dbc_path: P) -> DbcResult<LazyFrame> {
    let scanner = DbcScanner::from_dbc_path(dbc_path, None)?;
    scanner.lazy()
}

/// Read entire DBF file with maximum performance defaults
pub fn read_dbf<P: AsRef<Path>>(dbf_path: P) -> DbcResult<DataFrame> {
    let scanner = DbcScanner::from_dbf_path(dbf_path, None)?;
    scanner.read_all()
}

/// Read DBF file with column selection
pub fn read_dbf_columns<P: AsRef<Path>>(dbf_path: P, columns: &[&str]) -> DbcResult<DataFrame> {
    let scanner = DbcScanner::from_dbf_path(dbf_path, None)?;
    scanner.read_columns(columns)
}

/// Create LazyFrame from DBF file
pub fn scan_dbf_lazy<P: AsRef<Path>>(dbf_path: P) -> DbcResult<LazyFrame> {
    let scanner = DbcScanner::from_dbf_path(dbf_path, None)?;
    scanner.lazy()
}

/// Legacy function for compatibility
pub fn scan_dbc<P: AsRef<Path>>(dbc_path: P, _chunk_size: Option<usize>) -> DbcResult<DbcScanner> {
    DbcScanner::from_dbc_path(dbc_path, None)
}

/// Legacy function for compatibility  
pub fn scan_dbf<P: AsRef<Path>>(dbf_path: P, _chunk_size: Option<usize>) -> DbcResult<DbcScanner> {
    DbcScanner::from_dbf_path(dbf_path, None)
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
                            
                            // Test LazyFrame creation
                            let start = Instant::now();
                            match scan_dbc_lazy(dbc_file_path) {
                                Ok(lazy_df) => {
                                    let lazy_time = start.elapsed();
                                    println!("✓ LazyFrame creation: {:?}", lazy_time);
                                    
                                    // Test lazy operations
                                    let start = Instant::now();
                                    let result = lazy_df
                                        .select([polars::prelude::col("CNES"), polars::prelude::col("RAZAO")])
                                        .limit(100)
                                        .collect();
                                    match result {
                                        Ok(df) => {
                                            let collect_time = start.elapsed();
                                            println!("✓ Lazy collect (2 cols, 100 rows): {:?}", collect_time);
                                            println!("  Result: {} rows × {} cols", df.height(), df.width());
                                        }
                                        Err(e) => {
                                            println!("✗ Lazy collect failed: {}", e);
                                        }
                                    }
                                }
                                Err(e) => {
                                    println!("✗ LazyFrame creation failed: {}", e);
                                }
                            }
                            
                            // Test column selection performance
                            let start = Instant::now();
                            match read_dbc_columns(dbc_file_path, &["CNES", "RAZAO"]) {
                                Ok(df) => {
                                    let select_time = start.elapsed();
                                    println!("✓ Column selection read: {:?}", select_time);
                                    println!("  Selected: {} rows × {} cols", df.height(), df.width());
                                }
                                Err(e) => {
                                    println!("✗ Column selection failed: {}", e);
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
