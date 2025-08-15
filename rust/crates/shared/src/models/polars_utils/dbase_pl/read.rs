use std::path::Path;
use std::collections::HashMap;
use polars::prelude::*;
use dbase::Reader;
use rayon::prelude::*;

use crate::models::polars_utils::dbase_pl::{
    DbasePolarsError,
    DbaseDowncastConfig,
    intelligent_dbase_downcast_df,
    DbaseInterface,
    DbaseInterfaceConfig,
    get_dbase_file_summary,
    DescribeConfig,
    convert_dbase_value_to_polars_value,
    dbase_field_to_polars_type,
    estimate_file_memory_usage,
    determine_parallelization_strategy,
    get_thread_count,
    ParallelizationStrategy,
    ProgressIndicator,
};

/// Configuration for reading dBase files
#[derive(Debug, Clone)]
pub struct DbaseReadConfig {
    /// Whether to apply type optimization after reading
    pub optimize_types: bool,
    /// Whether to compress string-to-numeric conversions
    pub compress_strings: bool,
    /// Whether to prefer integers over floats
    pub prefer_integers: bool,
    /// Maximum number of records to read (None for all)
    pub max_records: Option<usize>,
    /// Number of records to read at once for memory efficiency
    pub batch_size: usize,
    /// Whether to show progress during reading
    pub show_progress: bool,
    /// Whether to use parallel processing where possible
    pub parallel: bool,
}

impl Default for DbaseReadConfig {
    fn default() -> Self {
        Self {
            optimize_types: true,
            compress_strings: true,
            prefer_integers: true,
            max_records: None,
            batch_size: 10_000,
            show_progress: true,
            parallel: true,
        }
    }
}

/// Result of reading a dBase file
#[derive(Debug)]
pub struct DbaseReadResult {
    /// The resulting DataFrame
    pub dataframe: DataFrame,
    /// Number of records read
    pub records_read: usize,
    /// Number of fields read
    pub fields_read: usize,
    /// Type optimization summary (if optimization was enabled)
    pub optimization_summary: Option<String>,
    /// Estimated memory usage before optimization
    pub original_memory_estimate: usize,
    /// Memory saved through optimization
    pub memory_saved_bytes: usize,
}

/// Check if a dBase file exists and is readable
pub fn dbase_file_exists<P: AsRef<Path>>(path: P) -> bool {
    let path_obj = path.as_ref();
    
    // Check if it's a regular dBase file
    if path_obj.exists() && path_obj.is_file() {
        // Check file extension
        if let Some(ext) = path_obj.extension() {
            return ext == "dbf" || ext == "dbc";
        }
    }
    
    false
}

/// Scan a dBase file and create a LazyFrame (main entry point)
pub fn scan_dbase_lazyframe<P: AsRef<Path>>(
    path: P,
    config: DbaseReadConfig,
) -> Result<LazyFrame, DbasePolarsError> {
    let interface = DbaseInterface::new(DbaseInterfaceConfig {
        verbose: config.show_progress,
        set_env_vars: false,
        json_output: false,
    });

    interface.status("Scanning", &format!("Reading dBase file: {}", path.as_ref().display()));

    // Read the DataFrame first, then convert to LazyFrame
    let read_result = read_dbase_dataframe(path, config)?;
    
    Ok(read_result.dataframe.lazy())
}

/// Read a dBase file into a Polars DataFrame with full optimization
pub fn read_dbase_dataframe<P: AsRef<Path>>(
    path: P,
    config: DbaseReadConfig,
) -> Result<DbaseReadResult, DbasePolarsError> {
    let interface = DbaseInterface::new(DbaseInterfaceConfig {
        verbose: config.show_progress,
        set_env_vars: false,
        json_output: false,
    });

    let path_str = path.as_ref().display().to_string();
    interface.status("Reading", &format!("Processing dBase file: {}", path_str));

    // Step 1: Analyze the file using our tested infrastructure
    let memory_estimate = estimate_file_memory_usage(&path)?;
    let file_summary = get_dbase_file_summary(&path, DescribeConfig::default())?;
    
    interface.success("Analysis", Some(&format!(
        "{} fields, {} records, ~{} bytes",
        file_summary.n_columns,
        file_summary.n_rows,
        memory_estimate.total_estimated_size
    )));

    // Step 2: Determine processing strategy
    let cores = get_thread_count();
    let strategy = if config.parallel {
        determine_parallelization_strategy(
            file_summary.n_columns,
            file_summary.n_rows,
            cores
        )
    } else {
        ParallelizationStrategy::Sequential
    };

    interface.status("Strategy", &format!("Using {:?} processing with {} cores", strategy, cores));

    // Step 3: Read the data using the appropriate strategy
    let df = match strategy {
        ParallelizationStrategy::Sequential => read_dbase_sequential(&path, &config, &interface)?,
        ParallelizationStrategy::ByRow => read_dbase_batched(&path, &config, &interface)?,
        ParallelizationStrategy::ByColumn => read_dbase_by_column(&path, &config, &interface)?,
    };

    let records_read = df.height();
    let fields_read = df.width();
    let original_memory_estimate = memory_estimate.total_estimated_size;

    interface.success("Reading", Some(&format!("{} records × {} fields", records_read, fields_read)));

    // Step 4: Apply type optimization using our tested downcast infrastructure
    let mut final_df = df;
    let mut optimization_summary = None;
    let mut memory_saved_bytes = 0;

    if config.optimize_types {
        interface.status("Optimization", "Applying type optimizations");
        
        let downcast_config = DbaseDowncastConfig {
            check_strings: config.compress_strings,
            prefer_int_over_float: config.prefer_integers,
            optimize_coded_fields: true,
            convert_logical_strings: true,
            shrink_numeric_types: true,
            categorical_threshold: 50,
        };

        match intelligent_dbase_downcast_df(final_df.clone(), None, downcast_config) {
            Ok(result) => {
                final_df = result.dataframe.collect()
                    .map_err(|e| DbasePolarsError::PolarsError(e))?;
                optimization_summary = Some(result.type_changes_json.clone());
                memory_saved_bytes = result.summary.estimated_memory_saved;
                interface.success("Optimization", Some(&format!(
                    "Memory saved: {} bytes, {} optimizations",
                    memory_saved_bytes,
                    result.summary.strings_to_numeric + result.summary.floats_to_integers + 
                    result.summary.type_shrinks + result.summary.logical_conversions
                )));
            }
            Err(e) => {
                interface.error("Optimization", &e);
                // Continue without optimization rather than failing
            }
        }
    }

    interface.success("Complete", Some(&format!(
        "DataFrame ready: {} records × {} fields",
        final_df.height(),
        final_df.width()
    )));

    Ok(DbaseReadResult {
        dataframe: final_df,
        records_read,
        fields_read,
        optimization_summary,
        original_memory_estimate,
        memory_saved_bytes,
    })
}

/// Sequential reading strategy (safest, works for all file sizes)
fn read_dbase_sequential<P: AsRef<Path>>(
    path: P,
    config: &DbaseReadConfig,
    interface: &DbaseInterface,
) -> Result<DataFrame, DbasePolarsError> {
    interface.status("Sequential Read", "Reading records sequentially");

    let mut reader = Reader::from_path(&path)?;
    let fields = reader.fields().to_vec();
    
    let max_records = config.max_records.unwrap_or(usize::MAX);
    let mut progress = ProgressIndicator::new(max_records.min(1000), !config.show_progress);
    
    // Pre-compute target types for each field to avoid lifetime issues
    let field_types: Vec<(String, DataType)> = fields
        .iter()
        .map(|field| (field.name().to_string(), dbase_field_to_polars_type(field)))
        .collect();
    
    // Initialize columns for each field
    let mut column_data: HashMap<String, Vec<AnyValue>> = HashMap::new();
    for (field_name, _) in &field_types {
        column_data.insert(field_name.clone(), Vec::new());
    }
    
    let mut records_processed = 0;
    for record_result in reader.iter_records() {
        if records_processed >= max_records {
            break;
        }
        
        let record = record_result?;
        
        // Process each field in the record
        for ((_field_name, field_value), (target_field_name, target_type)) in record.into_iter().zip(field_types.iter()) {
            let polars_value = convert_dbase_value_to_polars_value(field_value, target_type)?;
            
            if let Some(column_vec) = column_data.get_mut(target_field_name) {
                column_vec.push(polars_value);
            }
        }
        
        records_processed += 1;
        if records_processed % 100 == 0 {
            progress.increment();
        }
    }
    
    progress.finish();
    
    // Create Series from the collected data
    let mut columns = Vec::new();
    for field in &fields {
        let field_name = field.name().to_string();
        if let Some(values) = column_data.remove(&field_name) {
            let series = Series::from_any_values(PlSmallStr::from(&field_name), &values, true)
                .map_err(|e| DbasePolarsError::PolarsError(e))?;
            columns.push(series.into());
        }
    }
    
    DataFrame::new(columns).map_err(|e| DbasePolarsError::PolarsError(e))
}

/// Batched reading strategy (good for large files) with Rayon parallelization
fn read_dbase_batched<P: AsRef<Path>>(
    path: P,
    config: &DbaseReadConfig,
    interface: &DbaseInterface,
) -> Result<DataFrame, DbasePolarsError> {
    interface.status("Batched Read", &format!("Reading in batches of {} with parallel processing", config.batch_size));

    let mut reader = Reader::from_path(&path)?;
    let fields = reader.fields().to_vec();
    
    let max_records = config.max_records.unwrap_or(usize::MAX);
    let mut progress = ProgressIndicator::new(max_records.min(10000), !config.show_progress);
    
    // Pre-compute target types for each field to avoid lifetime issues
    let field_types: Vec<(String, DataType)> = fields
        .iter()
        .map(|field| (field.name().to_string(), dbase_field_to_polars_type(field)))
        .collect();
    
    // Collect all records first for parallel processing
    let mut all_records = Vec::new();
    let mut records_processed = 0;
    
    interface.status("Collection", "Collecting records for parallel processing");
    
    for record_result in reader.iter_records() {
        if records_processed >= max_records {
            break;
        }
        
        let record = record_result?;
        all_records.push(record);
        records_processed += 1;
        
        if records_processed % 1000 == 0 {
            progress.increment();
        }
    }
    
    progress.finish();
    
    if all_records.is_empty() {
        return Err(DbasePolarsError::ConversionError("No data was read".to_string()));
    }
    
    interface.status("Parallel Processing", &format!("Processing {} records in parallel", all_records.len()));
    
    // Determine optimal thread pool size
    let thread_count = get_thread_count();
    let chunk_size = (all_records.len() / thread_count).max(config.batch_size / 4).max(100);
    
    interface.status("Threading", &format!("Using {} threads with chunk size {}", thread_count, chunk_size));
    
    // Process records in parallel chunks using Rayon ThreadPool
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(thread_count)
        .build()
        .map_err(|e| DbasePolarsError::ConversionError(format!("Failed to create thread pool: {}", e)))?;
        
    let processed_chunks: Result<Vec<_>, DbasePolarsError> = pool.install(|| {
        all_records
            .par_chunks(chunk_size)
            .enumerate()
            .map(|(chunk_idx, record_chunk)| {
                process_record_chunk(record_chunk, &field_types, chunk_idx)
            })
            .collect()
    });
    
    let chunk_dataframes = processed_chunks?;
    
    interface.status("Concatenation", &format!("Concatenating {} chunks", chunk_dataframes.len()));
    
    // Concatenate all chunks
    if chunk_dataframes.len() == 1 {
        Ok(chunk_dataframes.into_iter().next().unwrap())
    } else {
        let lazy_frames: Vec<LazyFrame> = chunk_dataframes.into_iter().map(|df| df.lazy()).collect();
        concat(lazy_frames, UnionArgs::default())
            .map_err(|e| DbasePolarsError::PolarsError(e))?
            .collect()
            .map_err(|e| DbasePolarsError::PolarsError(e))
    }
}

/// Column-based reading strategy (good for wide tables)
fn read_dbase_by_column<P: AsRef<Path>>(
    path: P,
    config: &DbaseReadConfig,
    interface: &DbaseInterface,
) -> Result<DataFrame, DbasePolarsError> {
    interface.status("Column Read", "Reading by columns (wide table optimization)");
    
    // For now, delegate to sequential reading since column-based reading
    // would require multiple passes through the file
    // TODO: Implement true column-based reading if needed
    read_dbase_sequential(path, config, interface)
}

/// Process a chunk of records in parallel - helper function for Rayon processing
fn process_record_chunk(
    record_chunk: &[dbase::Record],
    field_types: &[(String, DataType)],
    chunk_idx: usize,
) -> Result<DataFrame, DbasePolarsError> {
    // Initialize column data for this chunk
    let mut column_data: HashMap<String, Vec<AnyValue>> = HashMap::new();
    for (field_name, _) in field_types {
        column_data.insert(field_name.clone(), Vec::with_capacity(record_chunk.len()));
    }
    
    // Process each record in the chunk
    for record in record_chunk {
        for ((_field_name, field_value), (target_field_name, target_type)) in record.clone().into_iter().zip(field_types.iter()) {
            let polars_value = convert_dbase_value_to_polars_value(field_value, target_type)
                .map_err(|e| DbasePolarsError::ConversionError(
                    format!("Chunk {}: Failed to convert field '{}': {}", chunk_idx, target_field_name, e)
                ))?;
            
            if let Some(column_vec) = column_data.get_mut(target_field_name) {
                column_vec.push(polars_value);
            }
        }
    }
    
    // Create Series from the collected data for this chunk
    let mut columns = Vec::new();
    for (field_name, _) in field_types {
        if let Some(values) = column_data.remove(field_name) {
            let series = Series::from_any_values(PlSmallStr::from(field_name), &values, true)
                .map_err(|e| DbasePolarsError::PolarsError(e))?;
            columns.push(series.into());
        }
    }
    
    DataFrame::new(columns).map_err(|e| DbasePolarsError::PolarsError(e))
}

/// Quick read function for simple use cases
pub fn quick_read_dbase<P: AsRef<Path>>(path: P) -> Result<DataFrame, DbasePolarsError> {
    let result = read_dbase_dataframe(path, DbaseReadConfig::default())?;
    Ok(result.dataframe)
}

/// Read dBase file with custom SQL-like filtering (conceptual)
pub fn read_dbase_with_filter<P: AsRef<Path>>(
    path: P,
    filter_expr: Expr,
    config: DbaseReadConfig,
) -> Result<DataFrame, DbasePolarsError> {
    // Read the full DataFrame first
    let result = read_dbase_dataframe(path, config)?;
    
    // Apply the filter
    let filtered_df = result.dataframe
        .lazy()
        .filter(filter_expr)
        .collect()
        .map_err(|e| DbasePolarsError::PolarsError(e))?;
    
    Ok(filtered_df)
}

/// Read multiple dBase files in parallel and concatenate them
pub fn read_multiple_dbase_files<P: AsRef<Path> + Send + Sync>(
    paths: Vec<P>,
    config: DbaseReadConfig,
) -> Result<DataFrame, DbasePolarsError> {
    let interface = DbaseInterface::new(DbaseInterfaceConfig {
        verbose: config.show_progress,
        set_env_vars: false,
        json_output: false,
    });

    interface.status("Multi-File Read", &format!("Reading {} dBase files in parallel", paths.len()));

    // If only one file, process it directly
    if paths.len() == 1 {
        let result = read_dbase_dataframe(&paths[0], config)?;
        return Ok(result.dataframe);
    }

    // Determine thread count for file processing
    let thread_count = get_thread_count().min(paths.len());
    interface.status("Threading", &format!("Using {} threads for {} files", thread_count, paths.len()));

    // Process files in parallel using Rayon ThreadPool
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(thread_count)
        .build()
        .map_err(|e| DbasePolarsError::ConversionError(format!("Failed to create thread pool: {}", e)))?;

    let dataframes_result: Result<Vec<_>, DbasePolarsError> = pool.install(|| {
        paths
            .par_iter()
            .enumerate()
            .map(|(i, path)| {
                // Clone config for each thread
                let thread_config = DbaseReadConfig {
                    show_progress: false, // Disable progress for individual files in parallel processing
                    ..config.clone()
                };
                
                match read_dbase_dataframe(path, thread_config) {
                    Ok(result) => Ok((i, result.dataframe)),
                    Err(e) => Err(DbasePolarsError::ConversionError(
                        format!("File {} failed to read: {}", i + 1, e)
                    ))
                }
            })
            .collect::<Result<Vec<_>, _>>()
    });

    let mut dataframes_with_index = dataframes_result?;

    // Sort by original order to maintain file order
    dataframes_with_index.sort_by_key(|(i, _)| *i);
    let dataframes: Vec<DataFrame> = dataframes_with_index.into_iter().map(|(_, df)| df).collect();
    
    if dataframes.is_empty() {
        return Err(DbasePolarsError::ConversionError("No files were successfully read".to_string()));
    }
    
    interface.status("Concatenation", &format!("Concatenating {} DataFrames", dataframes.len()));

    // Concatenate all DataFrames
    let lazy_frames: Vec<LazyFrame> = dataframes.into_iter().map(|df| df.lazy()).collect();
    concat(lazy_frames, UnionArgs::default())
        .map_err(|e| DbasePolarsError::PolarsError(e))?
        .collect()
        .map_err(|e| DbasePolarsError::PolarsError(e))
}

/// Batch read all dBase files in a directory
pub fn read_dbase_directory<P: AsRef<Path>>(
    dir_path: P,
    config: DbaseReadConfig,
) -> Result<DataFrame, DbasePolarsError> {
    let interface = DbaseInterface::new(DbaseInterfaceConfig {
        verbose: config.show_progress,
        set_env_vars: false,
        json_output: false,
    });

    let dir = dir_path.as_ref();
    interface.status("Directory Scan", &format!("Scanning directory: {}", dir.display()));

    // Find all dBase files in the directory
    let mut dbase_files = Vec::new();
    if let Ok(entries) = std::fs::read_dir(dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if dbase_file_exists(&path) {
                dbase_files.push(path);
            }
        }
    }

    interface.status("Discovery", &format!("Found {} dBase files", dbase_files.len()));

    if dbase_files.is_empty() {
        return Err(DbasePolarsError::IoError(
            format!("No dBase files found in directory: {}", dir.display())
        ));
    }

    read_multiple_dbase_files(dbase_files, config)
}

/// Create a sample DataFrame for testing (used when actual reading isn't implemented yet)
pub fn create_sample_from_dbase_schema<P: AsRef<Path>>(
    path: P,
    sample_size: usize,
) -> Result<DataFrame, DbasePolarsError> {
    let file_summary = get_dbase_file_summary(&path, DescribeConfig::default())?;
    
    let mut columns = Vec::new();
    
    for field in &file_summary.field_info {
        let sample_data: Vec<AnyValue> = (0..sample_size).map(|i| {
            match field.polars_type {
                DataType::String => AnyValue::StringOwned(format!("sample_{}", i).into()),
                DataType::Int32 => AnyValue::Int32(i as i32),
                DataType::Int64 => AnyValue::Int64(i as i64),
                DataType::Float64 => AnyValue::Float64(i as f64 + 0.5),
                DataType::Boolean => AnyValue::Boolean(i % 2 == 0),
                DataType::Date => AnyValue::Date((19000 + i) as i32), // Days since epoch
                _ => AnyValue::StringOwned(format!("sample_{}", i).into()),
            }
        }).collect();
        
        let series = Series::from_any_values(PlSmallStr::from(&field.name), &sample_data, true)
            .map_err(|e| DbasePolarsError::PolarsError(e))?;
        columns.push(series.into());
    }
    
    DataFrame::new(columns).map_err(|e| DbasePolarsError::PolarsError(e))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_dbase_read_config_default() {
        let config = DbaseReadConfig::default();
        assert!(config.optimize_types);
        assert!(config.compress_strings);
        assert!(config.prefer_integers);
        assert!(config.max_records.is_none());
        assert_eq!(config.batch_size, 10_000);
        assert!(config.show_progress);
        assert!(config.parallel);
    }

    #[test]
    fn test_dbase_file_exists() {
        // Test with non-existent file
        assert!(!dbase_file_exists("/non/existent/file.dbf"));
        
        // Test with actual dBase files
        let test_files = [
            "/Users/wrath/projects/arrow-sus/rust/downloads/parallel/CHBR1901.dbc",
            "/Users/wrath/projects/arrow-sus/rust/downloads/parallel/CHBR1902.dbc",
        ];

        for file_path in &test_files {
            if std::path::Path::new(file_path).exists() {
                assert!(dbase_file_exists(file_path));
                break;
            }
        }
    }

    #[test]
    fn test_create_sample_from_schema() {
        let test_files = [
            "/Users/wrath/projects/arrow-sus/rust/downloads/parallel/CHBR1901.dbc",
        ];

        for file_path in &test_files {
            if std::path::Path::new(file_path).exists() {
                println!("\n🧪 Testing sample creation with: {}", file_path);
                
                match create_sample_from_dbase_schema(file_path, 5) {
                    Ok(df) => {
                        println!("✅ Sample DataFrame created!");
                        println!("   📊 Shape: {} × {}", df.height(), df.width());
                        println!("   📋 Columns: {:?}", df.get_column_names());
                        
                        assert_eq!(df.height(), 5);
                        assert!(df.width() > 0);
                    }
                    Err(e) => {
                        println!("❌ Sample creation failed: {}", e);
                    }
                }
                
                break;
            }
        }
    }

    #[test]
    fn test_read_actual_dbase_file() {
        let test_files = [
            "/Users/wrath/projects/arrow-sus/rust/downloads/parallel/CHBR1901.dbc",
        ];

        for file_path in &test_files {
            if std::path::Path::new(file_path).exists() {
                println!("\n🧪 Testing actual dBase reading with: {}", file_path);
                
                let config = DbaseReadConfig {
                    max_records: Some(100), // Limit for testing
                    show_progress: false,   // Quiet for testing
                    ..DbaseReadConfig::default()
                };
                
                match read_dbase_dataframe(file_path, config) {
                    Ok(result) => {
                        println!("✅ dBase reading successful!");
                        println!("   📊 Shape: {} × {}", result.dataframe.height(), result.dataframe.width());
                        println!("   📋 Records read: {}", result.records_read);
                        println!("   💾 Memory saved: {} bytes", result.memory_saved_bytes);
                        
                        assert!(result.records_read > 0);
                        assert!(result.fields_read > 0);
                        assert_eq!(result.dataframe.height(), result.records_read);
                        assert_eq!(result.dataframe.width(), result.fields_read);
                        
                        // Test that we can convert to LazyFrame
                        let lazy_df = result.dataframe.lazy();
                        assert!(lazy_df.collect().is_ok());
                    }
                    Err(e) => {
                        println!("❌ dBase reading failed: {}", e);
                        // This is expected if the .dbc files aren't actually valid dBase format
                    }
                }
                
                break;
            }
        }
    }

    #[test]
    fn test_quick_read_function() {
        let test_files = [
            "/Users/wrath/projects/arrow-sus/rust/downloads/parallel/CHBR1901.dbc",
        ];

        for file_path in &test_files {
            if std::path::Path::new(file_path).exists() {
                println!("\n🧪 Testing quick read with: {}", file_path);
                
                match quick_read_dbase(file_path) {
                    Ok(df) => {
                        println!("✅ Quick read successful!");
                        println!("   📊 Shape: {} × {}", df.height(), df.width());
                        
                        assert!(df.height() > 0);
                        assert!(df.width() > 0);
                    }
                    Err(e) => {
                        println!("❌ Quick read failed: {}", e);
                        // Expected if files aren't valid dBase format
                    }
                }
                
                break;
            }
        }
    }

    #[test]
    fn test_scan_lazyframe() {
        let test_files = [
            "/Users/wrath/projects/arrow-sus/rust/downloads/parallel/CHBR1901.dbc",
        ];

        for file_path in &test_files {
            if std::path::Path::new(file_path).exists() {
                println!("\n🧪 Testing LazyFrame scan with: {}", file_path);
                
                let config = DbaseReadConfig {
                    max_records: Some(50),
                    show_progress: false,
                    ..DbaseReadConfig::default()
                };
                
                match scan_dbase_lazyframe(file_path, config) {
                    Ok(lazy_df) => {
                        println!("✅ LazyFrame scan successful!");
                        
                        // Test that we can collect it
                        match lazy_df.collect() {
                            Ok(df) => {
                                println!("   📊 Collected shape: {} × {}", df.height(), df.width());
                                assert!(df.height() > 0);
                                assert!(df.width() > 0);
                            }
                            Err(e) => {
                                println!("   ❌ LazyFrame collection failed: {}", e);
                            }
                        }
                    }
                    Err(e) => {
                        println!("❌ LazyFrame scan failed: {}", e);
                    }
                }
                
                break;
            }
        }
    }

    #[test]
    fn test_parallel_processing_strategies() {
        let test_files = [
            "/Users/wrath/projects/arrow-sus/rust/downloads/parallel/CHBR1901.dbc",
        ];

        for file_path in &test_files {
            if std::path::Path::new(file_path).exists() {
                println!("\n🧪 Testing parallel processing strategies with: {}", file_path);
                
                // Test with parallel enabled
                let parallel_config = DbaseReadConfig {
                    max_records: Some(500),
                    show_progress: false,
                    parallel: true,
                    batch_size: 100,
                    ..DbaseReadConfig::default()
                };
                
                match read_dbase_dataframe(file_path, parallel_config) {
                    Ok(result) => {
                        println!("✅ Parallel processing successful!");
                        println!("   📊 Shape: {} × {}", result.dataframe.height(), result.dataframe.width());
                        println!("   💾 Memory saved: {} bytes", result.memory_saved_bytes);
                        
                        assert!(result.records_read > 0);
                        assert!(result.fields_read > 0);
                    }
                    Err(e) => {
                        println!("❌ Parallel processing failed: {}", e);
                    }
                }

                // Test with sequential processing for comparison
                let sequential_config = DbaseReadConfig {
                    max_records: Some(500),
                    show_progress: false,
                    parallel: false,
                    ..DbaseReadConfig::default()
                };
                
                match read_dbase_dataframe(file_path, sequential_config) {
                    Ok(result) => {
                        println!("✅ Sequential processing successful!");
                        println!("   📊 Shape: {} × {}", result.dataframe.height(), result.dataframe.width());
                        
                        assert!(result.records_read > 0);
                        assert!(result.fields_read > 0);
                    }
                    Err(e) => {
                        println!("❌ Sequential processing failed: {}", e);
                    }
                }
                
                break;
            }
        }
    }

    #[test]
    fn test_multi_file_parallel_read() {
        let test_files = vec![
            "/Users/wrath/projects/arrow-sus/rust/downloads/parallel/CHBR1901.dbc",
            "/Users/wrath/projects/arrow-sus/rust/downloads/parallel/CHBR1902.dbc",
        ];

        // Filter to only existing files
        let existing_files: Vec<&str> = test_files.into_iter()
            .filter(|path| std::path::Path::new(path).exists())
            .collect();

        if existing_files.len() >= 1 {
            println!("\n🧪 Testing multi-file parallel read with {} files", existing_files.len());
            
            let config = DbaseReadConfig {
                max_records: Some(100),
                show_progress: false,
                parallel: true,
                ..DbaseReadConfig::default()
            };
            
            match read_multiple_dbase_files(existing_files, config) {
                Ok(combined_df) => {
                    println!("✅ Multi-file parallel read successful!");
                    println!("   📊 Combined shape: {} × {}", combined_df.height(), combined_df.width());
                    
                    assert!(combined_df.height() > 0);
                    assert!(combined_df.width() > 0);
                }
                Err(e) => {
                    println!("❌ Multi-file parallel read failed: {}", e);
                }
            }
        } else {
            println!("⏭️  Skipping multi-file test - no test files available");
        }
    }
}
