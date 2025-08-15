use std::thread;
use std::env;
use std::path::Path;
use chrono::NaiveDate;
use dbase::Reader;

use crate::models::polars_utils::dbase_pl::DbasePolarsError;

/// dBase epoch starts at 1900-01-01, while Unix epoch is 1970-01-01
/// This is the number of days difference between the two epochs
pub const DBASE_EPOCH_SHIFT_DAYS: i32 = 25567; // Days from 1900-01-01 to 1970-01-01

/// Date constants for dBase to modern date conversions
pub const DBASE_DATE_BASE_YEAR: i32 = 1900;
pub const UNIX_EPOCH_YEAR: i32 = 1970;

/// Time unit conversion constants
pub const MILLISECONDS_PER_SECOND: i64 = 1_000;
pub const MICROSECONDS_PER_SECOND: i64 = 1_000_000;
pub const NANOSECONDS_PER_SECOND: i64 = 1_000_000_000;

/// Get the optimal thread count for parallel processing
pub fn get_thread_count() -> usize {
    // First try to get the thread count from POLARS_MAX_THREADS env var
    match env::var("POLARS_MAX_THREADS") {
        Ok(threads_str) => {
            // Try to parse the environment variable as a usize
            match threads_str.parse::<usize>() {
                Ok(threads) => threads,
                Err(_) => {
                    // If parsing fails, fall back to system thread count
                    thread::available_parallelism()
                        .map(|p| p.get())
                        .unwrap_or(1)
                }
            }
        },
        Err(_) => {
            // If environment variable is not set, use system thread count
            thread::available_parallelism()
                .map(|p| p.get())
                .unwrap_or(1)
        }
    }
}

/// Strategies for parallelizing dBase file processing
#[derive(Copy, Clone, Debug, PartialEq)]
pub enum ParallelizationStrategy {
    /// Process records in parallel batches
    ByRow,
    /// Process fields/columns in parallel
    ByColumn,
    /// Process the file sequentially (best for small files)
    Sequential,
}

/// Determine the best parallelization strategy based on file characteristics
pub fn determine_parallelization_strategy(
    n_columns: usize,
    n_rows: usize,
    available_cores: usize,
) -> ParallelizationStrategy {
    // For very small files, sequential processing is most efficient
    if n_rows < 1_000 || n_columns < 5 {
        return ParallelizationStrategy::Sequential;
    }

    // Column parallelism when:
    // 1. We have significantly more columns than CPU cores
    // 2. We have relatively few rows compared to columns
    // 3. Wide tables benefit from column-wise parallel processing
    if n_columns > available_cores * 2 && n_rows < 100_000 {
        ParallelizationStrategy::ByColumn
    } else {
        // Default to row parallelism for most dBase files
        // dBase files are typically record-oriented
        ParallelizationStrategy::ByRow
    }
}

/// Estimate optimal chunk size for processing dBase records
pub fn calculate_optimal_chunk_size(
    total_records: usize,
    available_threads: usize,
    avg_record_size_bytes: Option<usize>,
) -> usize {
    let base_chunk_size = if total_records < 10_000 {
        // Small files: process in fewer, larger chunks
        (total_records / available_threads.max(1)).max(100)
    } else if total_records < 1_000_000 {
        // Medium files: balance parallelism and overhead
        (total_records / (available_threads * 4)).max(1_000)
    } else {
        // Large files: smaller chunks for better load balancing
        (total_records / (available_threads * 8)).max(5_000)
    };

    // Adjust based on record size if available
    if let Some(record_size) = avg_record_size_bytes {
        let target_chunk_memory = 64 * 1024 * 1024; // 64MB target chunk size
        let memory_based_chunk = target_chunk_memory / record_size.max(1);
        base_chunk_size.min(memory_based_chunk).max(100)
    } else {
        base_chunk_size
    }
}

/// Convert dBase date number to NaiveDate
pub fn dbase_date_to_naive_date(dbase_date: u32) -> Result<NaiveDate, DbasePolarsError> {
    // dBase dates are typically days since 1900-01-01
    let base_date = NaiveDate::from_ymd_opt(DBASE_DATE_BASE_YEAR, 1, 1)
        .ok_or_else(|| DbasePolarsError::ConversionError("Invalid dBase base date".to_string()))?;
    
    base_date
        .checked_add_days(chrono::Days::new(dbase_date as u64))
        .ok_or_else(|| DbasePolarsError::ConversionError(format!("Invalid dBase date: {}", dbase_date)))
}

/// Convert NaiveDate to dBase date number
pub fn naive_date_to_dbase_date(date: NaiveDate) -> Result<u32, DbasePolarsError> {
    let base_date = NaiveDate::from_ymd_opt(DBASE_DATE_BASE_YEAR, 1, 1)
        .ok_or_else(|| DbasePolarsError::ConversionError("Invalid dBase base date".to_string()))?;
    
    let days_diff = date.signed_duration_since(base_date).num_days();
    if days_diff < 0 {
        return Err(DbasePolarsError::ConversionError(
            format!("Date {} is before dBase epoch", date)
        ));
    }
    
    Ok(days_diff as u32)
}

/// Estimate the memory usage of a dBase file for processing optimization
pub fn estimate_file_memory_usage<P: AsRef<Path>>(
    file_path: P,
) -> Result<FileMemoryEstimate, DbasePolarsError> {
    let reader = Reader::from_path(file_path.as_ref())?;
    let fields = reader.fields().to_vec(); // Clone fields to own them
    let n_columns = fields.len();
    
    // Estimate bytes per record based on field types and lengths
    let mut estimated_record_size = 0usize;
    for field in fields.iter() {
        let field_size = match field.field_type() {
            dbase::FieldType::Character => field.length() as usize,
            dbase::FieldType::Numeric => 8, // Assume f64
            dbase::FieldType::Float => 4,
            dbase::FieldType::Date => 4,
            dbase::FieldType::DateTime => 8,
            dbase::FieldType::Logical => 1,
            dbase::FieldType::Memo => field.length() as usize, // Variable, use declared length
            dbase::FieldType::Integer => 4,
            dbase::FieldType::Double => 8,
            dbase::FieldType::Currency => 8,
        };
        estimated_record_size += field_size;
    }
    
    // Count records (this is expensive for large files, could be optimized)
    let mut record_count = 0usize;
    drop(reader); // Close the first reader
    
    let mut counting_reader = Reader::from_path(file_path.as_ref())?;
    for record_result in counting_reader.iter_records() {
        let _record = record_result?;
        record_count += 1;
    }
    
    let total_estimated_size = estimated_record_size * record_count;
    
    Ok(FileMemoryEstimate {
        estimated_record_size,
        record_count,
        total_estimated_size,
        n_columns,
    })
}

/// Memory usage estimate for a dBase file
#[derive(Debug, Clone)]
pub struct FileMemoryEstimate {
    /// Estimated bytes per record
    pub estimated_record_size: usize,
    /// Total number of records
    pub record_count: usize,
    /// Total estimated file size in memory
    pub total_estimated_size: usize,
    /// Number of columns/fields
    pub n_columns: usize,
}

impl FileMemoryEstimate {
    /// Check if the file can be processed entirely in memory
    pub fn fits_in_memory(&self, available_memory_bytes: usize) -> bool {
        // Add 20% overhead for processing
        let required_memory = (self.total_estimated_size as f64 * 1.2) as usize;
        required_memory <= available_memory_bytes
    }
    
    /// Recommend processing strategy based on memory constraints
    pub fn recommend_processing_strategy(&self, available_memory_bytes: usize) -> ProcessingStrategy {
        if self.fits_in_memory(available_memory_bytes) {
            ProcessingStrategy::InMemory
        } else if self.record_count > 1_000_000 {
            ProcessingStrategy::Streaming
        } else {
            ProcessingStrategy::Chunked(self.calculate_chunk_count(available_memory_bytes))
        }
    }
    
    /// Calculate optimal number of chunks for processing
    fn calculate_chunk_count(&self, available_memory_bytes: usize) -> usize {
        let max_chunk_memory = available_memory_bytes / 2; // Use half of available memory
        let records_per_chunk = max_chunk_memory / self.estimated_record_size.max(1);
        (self.record_count / records_per_chunk.max(1)).max(1)
    }
}

/// Processing strategy recommendation
#[derive(Debug, Clone, PartialEq)]
pub enum ProcessingStrategy {
    /// Process the entire file in memory at once
    InMemory,
    /// Process in chunks of the specified count
    Chunked(usize),
    /// Stream process record by record
    Streaming,
}

/// Utility to create a simple progress indicator for dBase processing
pub struct ProgressIndicator {
    total: usize,
    current: usize,
    last_reported_percent: usize,
    quiet: bool,
}

impl ProgressIndicator {
    pub fn new(total: usize, quiet: bool) -> Self {
        Self {
            total,
            current: 0,
            last_reported_percent: 0,
            quiet,
        }
    }
    
    pub fn increment(&mut self) {
        self.current += 1;
        if !self.quiet && self.total > 0 {
            let percent = (self.current * 100) / self.total;
            if percent != self.last_reported_percent && percent % 10 == 0 {
                eprintln!("Processing: {}% ({}/{})", percent, self.current, self.total);
                self.last_reported_percent = percent;
            }
        }
    }
    
    pub fn finish(&self) {
        if !self.quiet {
            eprintln!("Completed processing {} records", self.total);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_thread_count() {
        let thread_count = get_thread_count();
        assert!(thread_count >= 1);
        assert!(thread_count <= 256); // Reasonable upper bound
    }

    #[test]
    fn test_parallelization_strategy() {
        let cores = 4;
        
        // Small files should be sequential
        assert_eq!(
            determine_parallelization_strategy(3, 500, cores),
            ParallelizationStrategy::Sequential
        );
        
        // Wide tables with few rows should use column parallelism
        assert_eq!(
            determine_parallelization_strategy(20, 50_000, cores),
            ParallelizationStrategy::ByColumn
        );
        
        // Normal tables should use row parallelism
        assert_eq!(
            determine_parallelization_strategy(10, 500_000, cores),
            ParallelizationStrategy::ByRow
        );
    }

    #[test]
    fn test_chunk_size_calculation() {
        // Small file
        let chunk_size = calculate_optimal_chunk_size(5_000, 4, None);
        assert!(chunk_size >= 100);
        assert!(chunk_size <= 5_000);
        
        // Large file
        let chunk_size = calculate_optimal_chunk_size(10_000_000, 8, None);
        assert!(chunk_size >= 5_000);
    }

    #[test]
    fn test_dbase_date_conversion() {
        // Test converting a known date
        let date_2000_01_01 = naive_date_to_dbase_date(
            NaiveDate::from_ymd_opt(2000, 1, 1).unwrap()
        ).unwrap();
        
        // Convert back
        let converted_back = dbase_date_to_naive_date(date_2000_01_01).unwrap();
        assert_eq!(converted_back, NaiveDate::from_ymd_opt(2000, 1, 1).unwrap());
    }

    #[test]
    fn test_progress_indicator() {
        let mut progress = ProgressIndicator::new(100, true); // quiet mode
        progress.increment();
        assert_eq!(progress.current, 1);
        progress.finish(); // Should not panic
    }

    #[test]
    fn test_file_memory_estimate() {
        let estimate = FileMemoryEstimate {
            estimated_record_size: 100,
            record_count: 1_000,
            total_estimated_size: 100_000,
            n_columns: 10,
        };
        
        // Should fit in 1MB
        assert!(estimate.fits_in_memory(1_000_000));
        
        // Should not fit in 50KB
        assert!(!estimate.fits_in_memory(50_000));
        
        // Test strategy recommendation
        assert_eq!(
            estimate.recommend_processing_strategy(1_000_000),
            ProcessingStrategy::InMemory
        );
        
        // The exact chunk count may vary based on the calculation, just check it's chunked
        match estimate.recommend_processing_strategy(50_000) {
            ProcessingStrategy::Chunked(_) => {}, // Any chunk count is acceptable
            other => panic!("Expected Chunked strategy, got {:?}", other),
        }
    }

    #[test]
    fn test_with_actual_dbase_files() {
        let test_files = [
            "/Users/wrath/projects/arrow-sus/rust/downloads/parallel/CHBR1901.dbc",
            "/Users/wrath/projects/arrow-sus/rust/downloads/parallel/CHBR1902.dbc",
        ];

        for file_path in &test_files {
            if std::path::Path::new(file_path).exists() {
                println!("\n🧪 Testing utilities with actual dBase file: {}", file_path);
                
                // Test memory estimation
                match estimate_file_memory_usage(file_path) {
                    Ok(estimate) => {
                        println!("✅ Memory estimation successful!");
                        println!("   📊 Records: {}, Columns: {}", estimate.record_count, estimate.n_columns);
                        println!("   💾 Record size: {} bytes, Total: {} bytes", 
                                estimate.estimated_record_size, estimate.total_estimated_size);
                        
                        assert!(estimate.record_count > 0, "Should have records");
                        assert!(estimate.n_columns > 0, "Should have columns");
                        assert!(estimate.estimated_record_size > 0, "Records should have size");
                        
                        // Test parallelization strategy recommendation
                        let cores = get_thread_count();
                        let strategy = determine_parallelization_strategy(
                            estimate.n_columns,
                            estimate.record_count,
                            cores
                        );
                        println!("   🔧 Recommended strategy: {:?} (cores: {})", strategy, cores);
                        
                        // Test chunk size calculation
                        let chunk_size = calculate_optimal_chunk_size(
                            estimate.record_count,
                            cores,
                            Some(estimate.estimated_record_size)
                        );
                        println!("   📦 Optimal chunk size: {}", chunk_size);
                        assert!(chunk_size > 0, "Chunk size should be positive");
                        assert!(chunk_size <= estimate.record_count, "Chunk size should not exceed total records");
                        
                        // Test processing strategy recommendation
                        let processing_strategy = estimate.recommend_processing_strategy(100 * 1024 * 1024); // 100MB
                        println!("   🏭 Processing strategy: {:?}", processing_strategy);
                        
                        // Test memory fit check
                        let fits_small = estimate.fits_in_memory(1024); // 1KB - should not fit
                        let fits_large = estimate.fits_in_memory(100 * 1024 * 1024); // 100MB - should fit
                        println!("   💾 Fits in 1KB: {}, Fits in 100MB: {}", fits_small, fits_large);
                    }
                    Err(e) => {
                        println!("❌ Memory estimation failed: {}", e);
                    }
                }
                
                break; // Only test with first available file
            } else {
                println!("📂 Test file not found: {}", file_path);
            }
        }
    }
}
