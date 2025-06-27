use pyo3::prelude::*;

#[pyfunction]
fn hello_from_bin() -> String {
    "Hello from arrow-sus-rs!".to_string()
}

/// A Python module implemented in Rust. The name of this function must match
/// the `lib.name` setting in the `Cargo.toml`, else Python will not be able to
/// import the module.
#[pymodule]
fn _core(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(hello_from_bin, m)?)?;
    Ok(())
}

// use dbase::{File as DbfFile, FieldType};
// use explode::ExplodeReader;
// use polars::prelude::*;
// use pyo3::exceptions::{PyIOError, PyValueError};
// use pyo3::prelude::*;
// use pyo3_polars::error::PyPolarsErr;
// use pyo3_polars::{PyDataFrame, PyExpr, PySchema};
// use std::io::{Chain, Cursor, Read};
// use std::sync::Arc;

// type DbfReader<R> = Chain<Chain<Cursor<[u8; 10]>, Cursor<Vec<u8>>>, ExplodeReader<R>>;

// #[derive(Debug)]
// pub enum SihError {
//     Io(std::io::Error),
//     Decompression(String),
//     InvalidDate(String),
//     InvalidUf(String),
//     DbfParsing(String),
// }

// impl From<SihError> for PyErr {
//     fn from(err: SihError) -> PyErr {
//         match err {
//             SihError::Io(e) => PyIOError::new_err(e.to_string()),
//             SihError::InvalidDate(msg) | SihError::InvalidUf(msg) => {
//                 PyValueError::new_err(msg)
//             }
//             _ => PyIOError::new_err(format!("{:?}", err)),
//         }
//     }
// }

// #[pyclass]
// pub struct SihSource {
//     year: u16,
//     month: u8,
//     uf: String,
//     batch_size: usize,
//     rows_remaining: Option<usize>,

//     // Streaming state
//     dbf_reader: Option<DbfFile<DbfReader<std::fs::File>>>,
//     schema: Option<Arc<Schema>>,

//     // Pushdown optimizations
//     predicate: Option<Expr>,
//     with_columns: Option<Vec<usize>>,
// }

// #[pymethods]
// impl SihSource {
//     #[new]
//     #[pyo3(signature = (year, month, uf, batch_size=None, n_rows=None))]
//     fn new(
//         year: u16,
//         month: u8,
//         uf: String,
//         batch_size: Option<usize>,
//         n_rows: Option<usize>,
//     ) -> PyResult<Self> {
//         // Validate inputs
//         if !(2008..=2024).contains(&year) {
//             return Err(SihError::InvalidDate(
//                 "SIH data available from 2008 onwards".to_string(),
//             )
//             .into());
//         }

//         if !(1..=12).contains(&month) {
//             return Err(SihError::InvalidDate("Invalid month".to_string()).into());
//         }

//         let uf = uf.to_uppercase();
//         if uf.len() != 2 {
//             return Err(SihError::InvalidUf("UF must be 2 characters".to_string()).into());
//         }

//         Ok(Self {
//             year,
//             month,
//             uf,
//             batch_size: batch_size.unwrap_or(10_000),
//             rows_remaining: n_rows,
//             dbf_reader: None,
//             schema: None,
//             predicate: None,
//             with_columns: None,
//         })
//     }

//     fn schema(&mut self) -> PyResult<PySchema> {
//         if self.schema.is_none() {
//             self.initialize_reader()?;
//         }
//         Ok(PySchema(self.schema.as_ref().unwrap().clone()))
//     }

//     fn try_set_predicate(&mut self, predicate: PyExpr) {
//         self.predicate = Some(predicate.0);
//     }

//     fn set_with_columns(&mut self, columns: Vec<String>) {
//         if let Some(schema) = &self.schema {
//             let indexes = columns
//                 .iter()
//                 .filter_map(|name| schema.index_of(name.as_ref()).ok())
//                 .collect();
//             self.with_columns = Some(indexes);
//         }
//     }

//     fn next(&mut self) -> PyResult<Option<PyDataFrame>> {
//         // Check if we're done
//         if let Some(remaining) = self.rows_remaining {
//             if remaining == 0 {
//                 return Ok(None);
//             }
//         }

//         // Initialize reader if needed
//         if self.dbf_reader.is_none() {
//             self.initialize_reader()?;
//         }

//         let reader = self.dbf_reader.as_mut().unwrap();
//         let schema = self.schema.as_ref().unwrap();

//         // Read batch of records
//         let batch_size = if let Some(remaining) = self.rows_remaining {
//             std::cmp::min(self.batch_size, remaining)
//         } else {
//             self.batch_size
//         };

//         let mut batch_records = Vec::new();

//         // Read records from DBF
//         for _ in 0..batch_size {
//             match reader.records().next() {
//                 Some(Ok(record)) => batch_records.push(record),
//                 Some(Err(_)) => break, // Error reading record, stop batch
//                 None => break, // End of file
//             }
//         }

//         if batch_records.is_empty() {
//             return Ok(None);
//         }

//         // Convert DBF records to Polars DataFrame
//         let mut df = self.records_to_dataframe(batch_records, schema)?;

//         // Update remaining count
//         if let Some(remaining) = &mut self.rows_remaining {
//             *remaining = remaining.saturating_sub(df.height());
//         }

//         // Apply column projection
//         if let Some(column_indexes) = &self.with_columns {
//             let selected_columns: Vec<&str> = column_indexes
//                 .iter()
//                 .filter_map(|&i| schema.get_at_index(i).map(|(name, _)| name.as_str()))
//                 .collect();

//             df = df.select(selected_columns).map_err(PyPolarsErr::from)?;
//         }

//         // Apply predicate pushdown
//         if let Some(predicate) = &self.predicate {
//             df = df
//                 .lazy()
//                 .filter(predicate.clone())
//                 ._with_eager(true)
//                 .collect()
//                 .map_err(PyPolarsErr::from)?;
//         }

//         Ok(Some(PyDataFrame(df)))
//     }
// }

// impl SihSource {
//     fn initialize_reader(&mut self) -> PyResult<()> {
//         // Build file path for SIH data
//         // Format: /SIHSUS/RD/RDUF{year}{month:02}.dbc
//         let file_path = format!(
//             "/SIHSUS/RD/RD{}{:04}{:02}.dbc",
//             self.uf, self.year, self.month
//         );

//         // For POC, assume local file - in real implementation, this would be downloaded
//         let local_path = format!("./data{}", file_path);

//         let dbc_file = std::fs::File::open(&local_path)
//             .map_err(|e| SihError::Io(e))?;

//         // Decompress DBC to DBF stream
//         let dbf_reader = into_dbf_reader(dbc_file)
//             .map_err(|e| SihError::Decompression(e.to_string()))?;

//         // Create DBF file reader
//         let dbf_file = DbfFile::from_reader(dbf_reader)
//             .map_err(|e| SihError::DbfParsing(e.to_string()))?;

//         // Extract schema from DBF fields
//         let schema = self.build_sih_schema(dbf_file.fields())?;

//         self.schema = Some(Arc::new(schema));
//         self.dbf_reader = Some(dbf_file);

//         Ok(())
//     }

//     fn build_sih_schema(&self, fields: &[dbase::Field]) -> PyResult<Schema> {
//         let mut schema_fields = Vec::new();

//         for field in fields {
//             let name = field.name().to_string();
//             let data_type = match field.field_type() {
//                 FieldType::Character => DataType::String,
//                 FieldType::Numeric => {
//                     if field.num_decimal_places() > 0 {
//                         DataType::Float64
//                     } else {
//                         DataType::Int64
//                     }
//                 }
//                 FieldType::Logical => DataType::Boolean,
//                 FieldType::Date => DataType::Date,
//                 _ => DataType::String, // Fallback
//             };

//             schema_fields.push(Field::new(&name, data_type));
//         }

//         Ok(Schema::from_iter(schema_fields))
//     }

//     fn records_to_dataframe(
//         &self,
//         records: Vec<dbase::Record>,
//         schema: &Schema,
//     ) -> PyResult<DataFrame> {
//         let mut series_vec = Vec::new();

//         // Initialize series for each field
//         for (field_name, data_type) in schema.iter() {
//             match data_type {
//                 DataType::String => {
//                     let mut values = Vec::with_capacity(records.len());
//                     for record in &records {
//                         let value = record.get(field_name)
//                             .and_then(|v| v.as_string())
//                             .unwrap_or_default();
//                         values.push(Some(value));
//                     }
//                     series_vec.push(Series::new(field_name, values));
//                 }
//                 DataType::Int64 => {
//                     let mut values = Vec::with_capacity(records.len());
//                     for record in &records {
//                         let value = record.get(field_name)
//                             .and_then(|v| v.as_numeric())
//                             .and_then(|n| n.parse::<i64>().ok());
//                         values.push(value);
//                     }
//                     series_vec.push(Series::new(field_name, values));
//                 }
//                 DataType::Float64 => {
//                     let mut values = Vec::with_capacity(records.len());
//                     for record in &records {
//                         let value = record.get(field_name)
//                             .and_then(|v| v.as_numeric())
//                             .and_then(|n| n.parse::<f64>().ok());
//                         values.push(value);
//                     }
//                     series_vec.push(Series::new(field_name, values));
//                 }
//                 _ => {
//                     // Fallback to string
//                     let mut values = Vec::with_capacity(records.len());
//                     for record in &records {
//                         let value = record.get(field_name)
//                             .map(|v| format!("{:?}", v))
//                             .unwrap_or_default();
//                         values.push(Some(value));
//                     }
//                     series_vec.push(Series::new(field_name, values));
//                 }
//             }
//         }

//         DataFrame::new(series_vec).map_err(PyPolarsErr::from)
//     }
// }

// // DBC decompression logic (from dbc_datasus crate)
// fn into_dbf_reader<R>(mut dbc_reader: R) -> Result<DbfReader<R>, String>
// where
//     R: Read,
// {
//     let mut pre_header: [u8; 10] = Default::default();
//     let mut crc32: [u8; 4] = Default::default();

//     dbc_reader
//         .read_exact(&mut pre_header)
//         .map_err(|_| "Missing DBC header".to_string())?;

//     let header_size: usize = usize::from(pre_header[8]) + (usize::from(pre_header[9]) << 8);

//     let mut header: Vec<u8> = vec![0; header_size - 10];
//     dbc_reader
//         .read_exact(&mut header)
//         .map_err(|_| "Invalid header size".to_string())?;
//     dbc_reader
//         .read_exact(&mut crc32)
//         .map_err(|_| "Invalid header size".to_string())?;

//     let pre_header_reader = Cursor::new(pre_header);
//     let header_reader = Cursor::new(header);
//     let compressed_content_reader = ExplodeReader::new(dbc_reader);

//     let dbf_reader = pre_header_reader
//         .chain(header_reader)
//         .chain(compressed_content_reader);

//     Ok(dbf_reader)
// }

// // Python API function
// #[pyfunction]
// #[pyo3(signature = (year, month, uf, batch_size=None, n_rows=None))]
// pub fn scan_sih(
//     year: u16,
//     month: u8,
//     uf: String,
//     batch_size: Option<usize>,
//     n_rows: Option<usize>,
// ) -> PyResult<PyObject> {
//     Python::with_gil(|py| {
//         let source = SihSource::new(year, month, uf, batch_size, n_rows)?;
//         Ok(source.into_py(py))
//     })
// }

// #[pymodule]
// fn polars_datasus_sih(m: &Bound<PyModule>) -> PyResult<()> {
//     m.add_class::<SihSource>()?;
//     m.add_function(wrap_pyfunction!(scan_sih, m)?)?;
//     Ok(())
// }

// use dbase::{File as DbfFile, FieldType};
// use explode::ExplodeReader;
// use polars::prelude::*;
// use pyo3::prelude::*;
// use pyo3_polars::{PyDataFrame, PyExpr, PySchema};
// use std::io::{Chain, Cursor, Read};
// use std::sync::{Arc, Mutex};

// type DbfReader<R> = Chain<Chain<Cursor<[u8; 10]>, Cursor<Vec<u8>>>, ExplodeReader<R>>;

// // Core trait for data sources - no PyO3 here
// pub trait DataSource: Send {
//     fn name(&self) -> &str;
//     fn schema(&self) -> PolarsResult<Schema>;
//     fn next_batch(&mut self, batch_size: usize) -> PolarsResult<Option<DataFrame>>;
//     fn set_predicate(&mut self, predicate: Expr);
//     fn set_projection(&mut self, columns: Vec<String>);
//     fn remaining_rows(&self) -> Option<usize>;
// }

// // Pure Rust SIH implementation
// struct SihDataSource {
//     name: String,
//     year: u16,
//     month: u8,
//     uf: String,
//     rows_remaining: Option<usize>,

//     // Streaming state
//     dbf_reader: Option<DbfFile<DbfReader<std::fs::File>>>,
//     cached_schema: Option<Schema>,

//     // Pushdown state
//     predicate: Option<Expr>,
//     projection: Option<Vec<String>>,
// }

// impl SihDataSource {
//     fn new(year: u16, month: u8, uf: String, n_rows: Option<usize>) -> PolarsResult<Self> {
//         // Validation
//         if !(2008..=2024).contains(&year) {
//             return Err(PolarsError::InvalidOperation(
//                 "SIH data available from 2008 onwards".into(),
//             ));
//         }

//         if !(1..=12).contains(&month) {
//             return Err(PolarsError::InvalidOperation("Invalid month".into()));
//         }

//         let uf = uf.to_uppercase();
//         if uf.len() != 2 {
//             return Err(PolarsError::InvalidOperation("UF must be 2 characters".into()));
//         }

//         let name = format!("SIH-{}-{:04}-{:02}", uf, year, month);

//         Ok(Self {
//             name,
//             year,
//             month,
//             uf,
//             rows_remaining: n_rows,
//             dbf_reader: None,
//             cached_schema: None,
//             predicate: None,
//             projection: None,
//         })
//     }

//     fn initialize_if_needed(&mut self) -> PolarsResult<()> {
//         if self.dbf_reader.is_some() {
//             return Ok(());
//         }

//         // Build file path
//         let file_path = format!(
//             "./data/SIHSUS/RD/RD{}{:04}{:02}.dbc",
//             self.uf, self.year, self.month
//         );

//         let dbc_file = std::fs::File::open(&file_path)
//             .map_err(|e| PolarsError::Io(Arc::new(e)))?;

//         // Decompress DBC to DBF stream
//         let dbf_reader = into_dbf_reader(dbc_file)
//             .map_err(|e| PolarsError::InvalidOperation(e.into()))?;

//         // Create DBF file reader
//         let dbf_file = DbfFile::from_reader(dbf_reader)
//             .map_err(|e| PolarsError::InvalidOperation(format!("DBF parsing error: {}", e).into()))?;

//         // Cache schema
//         let schema = build_sih_schema(dbf_file.fields())?;
//         self.cached_schema = Some(schema);
//         self.dbf_reader = Some(dbf_file);

//         Ok(())
//     }
// }

// impl DataSource for SihDataSource {
//     fn name(&self) -> &str {
//         &self.name
//     }

//     fn schema(&self) -> PolarsResult<Schema> {
//         if let Some(ref schema) = self.cached_schema {
//             return Ok(schema.clone());
//         }

//         // Initialize to get schema
//         let mut temp_self = self.clone();
//         temp_self.initialize_if_needed()?;
//         Ok(temp_self.cached_schema.unwrap())
//     }

//     fn next_batch(&mut self, batch_size: usize) -> PolarsResult<Option<DataFrame>> {
//         // Check if we're done
//         if let Some(remaining) = self.rows_remaining {
//             if remaining == 0 {
//                 return Ok(None);
//             }
//         }

//         self.initialize_if_needed()?;

//         let reader = self.dbf_reader.as_mut().unwrap();
//         let schema = self.cached_schema.as_ref().unwrap();

//         // Determine actual batch size
//         let actual_batch_size = if let Some(remaining) = self.rows_remaining {
//             std::cmp::min(batch_size, remaining)
//         } else {
//             batch_size
//         };

//         // Read records from DBF
//         let mut batch_records = Vec::new();
//         for _ in 0..actual_batch_size {
//             match reader.records().next() {
//                 Some(Ok(record)) => batch_records.push(record),
//                 Some(Err(_)) => break,
//                 None => break,
//             }
//         }

//         if batch_records.is_empty() {
//             return Ok(None);
//         }

//         // Convert to DataFrame
//         let mut df = records_to_dataframe(batch_records, schema)?;

//         // Update remaining count
//         if let Some(remaining) = &mut self.rows_remaining {
//             *remaining = remaining.saturating_sub(df.height());
//         }

//         // Apply projection
//         if let Some(ref projection) = self.projection {
//             let columns: Vec<&str> = projection.iter().map(|s| s.as_str()).collect();
//             df = df.select(columns)?;
//         }

//         // Apply predicate
//         if let Some(ref predicate) = self.predicate {
//             df = df.lazy().filter(predicate.clone()).collect()?;
//         }

//         Ok(Some(df))
//     }

//     fn set_predicate(&mut self, predicate: Expr) {
//         self.predicate = Some(predicate);
//     }

//     fn set_projection(&mut self, columns: Vec<String>) {
//         self.projection = Some(columns);
//     }

//     fn remaining_rows(&self) -> Option<usize> {
//         self.rows_remaining
//     }
// }

// // PyO3 wrapper - minimal GIL interaction
// #[pyclass]
// #[derive(Clone)]
// pub struct PySihSource(pub Arc<Mutex<Box<dyn DataSource>>>);

// #[pymethods]
// impl PySihSource {
//     fn schema(&self) -> PySchema {
//         let source = self.0.lock().unwrap();
//         let schema = source.schema().unwrap();
//         PySchema(Arc::new(schema))
//     }

//     fn next_batch(&self, batch_size: usize) -> Option<PyDataFrame> {
//         let mut source = self.0.lock().unwrap();
//         source
//             .next_batch(batch_size)
//             .unwrap()
//             .map(PyDataFrame)
//     }

//     fn set_predicate(&self, predicate: PyExpr) {
//         let mut source = self.0.lock().unwrap();
//         source.set_predicate(predicate.0);
//     }

//     fn set_projection(&self, columns: Vec<String>) {
//         let mut source = self.0.lock().unwrap();
//         source.set_projection(columns);
//     }

//     fn remaining_rows(&self) -> Option<usize> {
//         let source = self.0.lock().unwrap();
//         source.remaining_rows()
//     }
// }

// // Factory function - like new_uniform/new_bernoulli
// #[pyfunction]
// pub fn new_sih_source(
//     year: u16,
//     month: u8,
//     uf: String,
//     n_rows: Option<usize>,
// ) -> PySihSource {
//     let source = SihDataSource::new(year, month, uf, n_rows).unwrap();
//     PySihSource(Arc::new(Mutex::new(Box::new(source))))
// }

// // Helper functions - pure Rust, no PyO3
// fn build_sih_schema(fields: &[dbase::Field]) -> PolarsResult<Schema> {
//     let mut schema_fields = Vec::new();

//     for field in fields {
//         let name = field.name().to_string();
//         let data_type = match field.field_type() {
//             FieldType::Character => DataType::String,
//             FieldType::Numeric => {
//                 if field.num_decimal_places() > 0 {
//                     DataType::Float64
//                 } else {
//                     DataType::Int64
//                 }
//             }
//             FieldType::Logical => DataType::Boolean,
//             FieldType::Date => DataType::Date,
//             _ => DataType::String,
//         };

//         schema_fields.push(Field::new(&name, data_type));
//     }

//     Ok(Schema::from_iter(schema_fields))
// }

// fn records_to_dataframe(
//     records: Vec<dbase::Record>,
//     schema: &Schema,
// ) -> PolarsResult<DataFrame> {
//     let mut series_vec = Vec::new();

//     for (field_name, data_type) in schema.iter() {
//         match data_type {
//             DataType::String => {
//                 let values: Vec<Option<String>> = records
//                     .iter()
//                     .map(|record| {
//                         record
//                             .get(field_name)
//                             .and_then(|v| v.as_string())
//                             .map(|s| s.to_string())
//                     })
//                     .collect();
//                 series_vec.push(Series::new(field_name, values));
//             }
//             DataType::Int64 => {
//                 let values: Vec<Option<i64>> = records
//                     .iter()
//                     .map(|record| {
//                         record
//                             .get(field_name)
//                             .and_then(|v| v.as_numeric())
//                             .and_then(|n| n.parse().ok())
//                     })
//                     .collect();
//                 series_vec.push(Series::new(field_name, values));
//             }
//             DataType::Float64 => {
//                 let values: Vec<Option<f64>> = records
//                     .iter()
//                     .map(|record| {
//                         record
//                             .get(field_name)
//                             .and_then(|v| v.as_numeric())
//                             .and_then(|n| n.parse().ok())
//                     })
//                     .collect();
//                 series_vec.push(Series::new(field_name, values));
//             }
//             _ => {
//                 let values: Vec<Option<String>> = records
//                     .iter()
//                     .map(|record| {
//                         record
//                             .get(field_name)
//                             .map(|v| format!("{:?}", v))
//                     })
//                     .collect();
//                 series_vec.push(Series::new(field_name, values));
//             }
//         }
//     }

//     DataFrame::new(series_vec)
// }

// fn into_dbf_reader<R>(mut dbc_reader: R) -> Result<DbfReader<R>, String>
// where
//     R: Read,
// {
//     let mut pre_header: [u8; 10] = Default::default();
//     let mut crc32: [u8; 4] = Default::default();

//     dbc_reader
//         .read_exact(&mut pre_header)
//         .map_err(|_| "Missing DBC header".to_string())?;

//     let header_size: usize = usize::from(pre_header[8]) + (usize::from(pre_header[9]) << 8);

//     let mut header: Vec<u8> = vec![0; header_size - 10];
//     dbc_reader
//         .read_exact(&mut header)
//         .map_err(|_| "Invalid header size".to_string())?;
//     dbc_reader
//         .read_exact(&mut crc32)
//         .map_err(|_| "Invalid header size".to_string())?;

//     let pre_header_reader = Cursor::new(pre_header);
//     let header_reader = Cursor::new(header);
//     let compressed_content_reader = ExplodeReader::new(dbc_reader);

//     let dbf_reader = pre_header_reader
//         .chain(header_reader)
//         .chain(compressed_content_reader);

//     Ok(dbf_reader)
// }

// // Missing Clone implementation for SihDataSource
// impl Clone for SihDataSource {
//     fn clone(&self) -> Self {
//         Self {
//             name: self.name.clone(),
//             year: self.year,
//             month: self.month,
//             uf: self.uf.clone(),
//             rows_remaining: self.rows_remaining,
//             dbf_reader: None, // Don't clone the reader state
//             cached_schema: self.cached_schema.clone(),
//             predicate: self.predicate.clone(),
//             projection: self.projection.clone(),
//         }
//     }
// }
