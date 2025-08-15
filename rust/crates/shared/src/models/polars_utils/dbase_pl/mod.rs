//! dBase to Polars conversion utilities
//! 
//! This module provides functionality for converting dBase files and data types
//! to Polars DataFrames and schemas. It includes type mappings, value conversions,
//! file description utilities, and other functions for working with dBase data 
//! in the Polars ecosystem.

pub mod mappings;
pub mod describe;
pub mod utilities;
pub mod downcast;
pub mod interface;
pub mod write;
pub mod read;

// Re-export key types and functions from mappings
pub use mappings::{
    DbasePolarsError,
    DbaseType,
    map_dbase_to_polars,
    dbase_field_to_polars_type,
    convert_dbase_value_to_polars_value,
};

// Re-export key types and functions from describe
pub use describe::{
    DescribeConfig,
    DbaseFileSummary,
    DbaseFieldInfo,
    get_dbase_schema,
    get_dbase_row_count,
    get_dbase_file_summary,
    describe_dbase_file,
    dbase_schema_with_metadata,
};

// Re-export key types and functions from utilities
pub use utilities::{
    ParallelizationStrategy,
    ProcessingStrategy,
    FileMemoryEstimate,
    ProgressIndicator,
    get_thread_count,
    determine_parallelization_strategy,
    calculate_optimal_chunk_size,
    estimate_file_memory_usage,
    dbase_date_to_naive_date,
    naive_date_to_dbase_date,
};

// Re-export key types and functions from downcast
pub use downcast::{
    DbaseDowncastConfig,
    DowncastResult,
    DowncastSummary,
    intelligent_dbase_downcast,
    intelligent_dbase_downcast_df,
};

// Re-export key types and functions from interface
pub use interface::{
    DbaseInterfaceConfig,
    DbaseInterface,
};

// Re-export key types and functions from write
pub use write::{
    ExportConfig,
    ExportFormat,
    ExportResult,
    export_dataframe,
    convert_dbase_to_format,
    batch_convert_dbase_files,
    export_dataframe_optimized,
    create_export_report,
};

// Re-export key types and functions from read
pub use read::{
    DbaseReadConfig,
    DbaseReadResult,
    dbase_file_exists,
    scan_dbase_lazyframe,
    read_dbase_dataframe,
    quick_read_dbase,
    read_dbase_with_filter,
    read_multiple_dbase_files,
    read_dbase_directory,
    create_sample_from_dbase_schema,
};