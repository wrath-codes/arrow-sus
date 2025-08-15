//! Polars IO plugin for reading DBC (compressed DBF) files
//!
//! This module provides functionality to read DBC files directly into Polars DataFrames
//! with efficient streaming and parallel chunk processing for maximum performance.

pub mod error;
pub mod des;
pub mod scan;

pub use error::{DbcError, DbcResult};
pub use des::{
    arrow_schema_to_polars, dbf_header_to_polars_schema, dbf_header_to_polars_schema_with_metadata,
    dbc_to_polars_schema, create_dbf_reader_from_file,
};
pub use scan::{
    DbcScanner, DbcConfig, read_dbc, read_dbc_with_config, read_dbc_columns, scan_dbc_lazy,
    read_dbf, read_dbf_columns, scan_dbf_lazy, scan_dbc, scan_dbf,
};
