use dbase::{FieldInfo, FieldValue};
use polars::prelude::*;
use std::fmt;
use chrono::NaiveDate;
use thiserror::Error;

/// Errors that can occur during dBase to Polars conversion
#[derive(Error, Debug)]
pub enum DbasePolarsError {
    #[error("IO error: {0}")]
    IoError(String),
    #[error("Dbase error: {0}")]
    DbaseError(String),
    #[error("Polars error: {0}")]
    PolarsError(#[from] PolarsError),
    #[error("Conversion error: {0}")]
    ConversionError(String),
}

impl From<std::io::Error> for DbasePolarsError {
    fn from(err: std::io::Error) -> Self {
        DbasePolarsError::IoError(err.to_string())
    }
}

impl From<dbase::Error> for DbasePolarsError {
    fn from(err: dbase::Error) -> Self {
        DbasePolarsError::DbaseError(err.to_string())
    }
}

impl From<crate::models::dbase_utils::DbfEncodingError> for DbasePolarsError {
    fn from(err: crate::models::dbase_utils::DbfEncodingError) -> Self {
        match err {
            crate::models::dbase_utils::DbfEncodingError::IoError(msg) => DbasePolarsError::IoError(msg),
            crate::models::dbase_utils::DbfEncodingError::EncodingFailed(msg) => DbasePolarsError::ConversionError(format!("Encoding failed: {}", msg)),
            crate::models::dbase_utils::DbfEncodingError::DecodingFailed(msg) => DbasePolarsError::ConversionError(format!("Decoding failed: {}", msg)),
            crate::models::dbase_utils::DbfEncodingError::ParseError(msg) => DbasePolarsError::ConversionError(format!("Parse error: {}", msg)),
        }
    }
}

/// Enum representing dBase field types for conversion mapping
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum DbaseType {
    Character,
    Currency,
    Numeric,
    Float,
    Date,
    DateTime,
    Logical,
    Memo,
    Integer,
    Double,
}

impl From<dbase::FieldType> for DbaseType {
    fn from(field_type: dbase::FieldType) -> Self {
        match field_type {
            dbase::FieldType::Character => DbaseType::Character,
            dbase::FieldType::Currency => DbaseType::Currency,
            dbase::FieldType::Numeric => DbaseType::Numeric,
            dbase::FieldType::Float => DbaseType::Float,
            dbase::FieldType::Date => DbaseType::Date,
            dbase::FieldType::DateTime => DbaseType::DateTime,
            dbase::FieldType::Logical => DbaseType::Logical,
            dbase::FieldType::Memo => DbaseType::Memo,
            dbase::FieldType::Integer => DbaseType::Integer,
            dbase::FieldType::Double => DbaseType::Double,
        }
    }
}

impl fmt::Display for DbaseType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DbaseType::Character => write!(f, "Character"),
            DbaseType::Currency => write!(f, "Currency"),
            DbaseType::Numeric => write!(f, "Numeric"),
            DbaseType::Float => write!(f, "Float"),
            DbaseType::Date => write!(f, "Date"),
            DbaseType::DateTime => write!(f, "DateTime"),
            DbaseType::Logical => write!(f, "Logical"),
            DbaseType::Memo => write!(f, "Memo"),
            DbaseType::Integer => write!(f, "Integer"),
            DbaseType::Double => write!(f, "Double"),
        }
    }
}

/// Convert dBase field type to Polars DataType
pub fn map_dbase_to_polars(dbase_type: DbaseType, field_info: Option<&FieldInfo>) -> DataType {
    match dbase_type {
        DbaseType::Character => DataType::String,
        DbaseType::Currency => DataType::Float64,
        DbaseType::Numeric => {
            // Use field info to decide between integer and float types
            if let Some(info) = field_info {
                // For now, use just length since decimal_count() method may not be available
                // TODO: Check if field has decimal places when available
                match info.length() {
                    1..=2 => DataType::Int8,
                    3..=4 => DataType::Int16,
                    5..=9 => DataType::Int32,
                    10..=18 => DataType::Int64,
                    _ => DataType::Float64,
                }
            } else {
                DataType::Float64
            }
        }
        DbaseType::Float => DataType::Float64, // Use Float64 like in dbase_utils.rs
        DbaseType::Date => DataType::Date,
        DbaseType::DateTime => DataType::Datetime(TimeUnit::Milliseconds, None),
        DbaseType::Logical => DataType::Boolean,
        DbaseType::Memo => DataType::String,
        DbaseType::Integer => DataType::Int32,
        DbaseType::Double => DataType::Float64,
    }
}

/// Convert dBase field to Polars DataType with field-specific heuristics
pub fn dbase_field_to_polars_type(field_info: &FieldInfo) -> DataType {
    let dbase_type = DbaseType::from(field_info.field_type());
    map_dbase_to_polars(dbase_type, Some(field_info))
}

/// Convert dBase FieldValue to appropriate Polars AnyValue
pub fn convert_dbase_value_to_polars_value(field_value: FieldValue, target_type: &DataType) -> PolarsResult<AnyValue> {
    match field_value {
        FieldValue::Character(opt_string) => {
            match opt_string {
                Some(s) => {
                    let trimmed = s.trim();
                    if trimmed.is_empty() {
                        Ok(AnyValue::Null)
                    } else {
                        match target_type {
                            DataType::String => Ok(AnyValue::StringOwned(trimmed.to_string().into())),
                            DataType::Int8 => {
                                let num: i8 = trimmed.parse().map_err(|e| 
                                    PolarsError::ComputeError(format!("Cannot convert '{}' to i8: {}", trimmed, e).into()))?;
                                Ok(AnyValue::Int8(num))
                            }
                            DataType::Int16 => {
                                let num: i16 = trimmed.parse().map_err(|e| 
                                    PolarsError::ComputeError(format!("Cannot convert '{}' to i16: {}", trimmed, e).into()))?;
                                Ok(AnyValue::Int16(num))
                            }
                            DataType::Int32 => {
                                let num: i32 = trimmed.parse().map_err(|e| 
                                    PolarsError::ComputeError(format!("Cannot convert '{}' to i32: {}", trimmed, e).into()))?;
                                Ok(AnyValue::Int32(num))
                            }
                            DataType::Int64 => {
                                let num: i64 = trimmed.parse().map_err(|e| 
                                    PolarsError::ComputeError(format!("Cannot convert '{}' to i64: {}", trimmed, e).into()))?;
                                Ok(AnyValue::Int64(num))
                            }
                            DataType::Float32 => {
                                let num: f32 = trimmed.parse().map_err(|e| 
                                    PolarsError::ComputeError(format!("Cannot convert '{}' to f32: {}", trimmed, e).into()))?;
                                Ok(AnyValue::Float32(num))
                            }
                            DataType::Float64 => {
                                let num: f64 = trimmed.parse().map_err(|e| 
                                    PolarsError::ComputeError(format!("Cannot convert '{}' to f64: {}", trimmed, e).into()))?;
                                Ok(AnyValue::Float64(num))
                            }
                            DataType::Boolean => {
                                let bool_val = match trimmed.to_uppercase().as_str() {
                                    "T" | "TRUE" | "Y" | "YES" | "1" => true,
                                    "F" | "FALSE" | "N" | "NO" | "0" => false,
                                    _ => return Err(PolarsError::ComputeError(
                                        format!("Cannot convert '{}' to boolean", trimmed).into()
                                    )),
                                };
                                Ok(AnyValue::Boolean(bool_val))
                            }
                            DataType::Date => {
                                let date = parse_dbase_date(trimmed).map_err(|e| 
                                    PolarsError::ComputeError(format!("Date parse error: {}", e).into()))?;
                                let days_since_epoch = (date - NaiveDate::from_ymd_opt(1970, 1, 1).unwrap()).num_days() as i32;
                                Ok(AnyValue::Date(days_since_epoch))
                            }
                            _ => Ok(AnyValue::StringOwned(trimmed.to_string().into())),
                        }
                    }
                }
                None => Ok(AnyValue::Null),
            }
        }
        FieldValue::Numeric(opt_num) => {
            match opt_num {
                Some(num) => match target_type {
                    DataType::Int8 => Ok(AnyValue::Int8(num as i8)),
                    DataType::Int16 => Ok(AnyValue::Int16(num as i16)),
                    DataType::Int32 => Ok(AnyValue::Int32(num as i32)),
                    DataType::Int64 => Ok(AnyValue::Int64(num as i64)),
                    DataType::Float32 => Ok(AnyValue::Float32(num as f32)),
                    DataType::Float64 => Ok(AnyValue::Float64(num)),
                    _ => Ok(AnyValue::Float64(num)),
                },
                None => Ok(AnyValue::Null),
            }
        }
        FieldValue::Logical(opt_bool) => {
            match opt_bool {
                Some(b) => Ok(AnyValue::Boolean(b)),
                None => Ok(AnyValue::Null),
            }
        }
        FieldValue::Date(opt_date) => {
            match opt_date {
                Some(date) => {
                    let naive_date = NaiveDate::from_ymd_opt(
                        date.year() as i32,
                        date.month() as u32,
                        date.day() as u32
                    ).ok_or_else(|| PolarsError::ComputeError("Invalid date".into()))?;
                    let days_since_epoch = (naive_date - NaiveDate::from_ymd_opt(1970, 1, 1).unwrap()).num_days() as i32;
                    Ok(AnyValue::Date(days_since_epoch))
                }
                None => Ok(AnyValue::Null),
            }
        }
        FieldValue::DateTime(_datetime) => {
            // TODO: Properly handle DateTime conversion - conflicts with Polars methods
            Ok(AnyValue::Null)
        }
        FieldValue::Float(opt_num) => {
            match opt_num {
                Some(num) => match target_type {
                    DataType::Float32 => Ok(AnyValue::Float32(num)),
                    DataType::Float64 => Ok(AnyValue::Float64(num as f64)),
                    _ => Ok(AnyValue::Float64(num as f64)), // Default to Float64 like dbase_utils.rs
                },
                None => Ok(AnyValue::Null),
            }
        }
        FieldValue::Double(num) => Ok(AnyValue::Float64(num)),
        FieldValue::Integer(num) => {
            match target_type {
                DataType::Int8 => Ok(AnyValue::Int8(num as i8)),
                DataType::Int16 => Ok(AnyValue::Int16(num as i16)),
                DataType::Int32 => Ok(AnyValue::Int32(num)),
                DataType::Int64 => Ok(AnyValue::Int64(num as i64)),
                _ => Ok(AnyValue::Int32(num)),
            }
        }
        FieldValue::Currency(val) => Ok(AnyValue::Float64(val)),
        FieldValue::Memo(memo_str) => Ok(AnyValue::StringOwned(memo_str.into())),
    }
}

/// Parse dBase date string (YYYYMMDD format)
fn parse_dbase_date(date_str: &str) -> Result<NaiveDate, DbasePolarsError> {
    if date_str.len() == 8 {
        let year: i32 = date_str[0..4].parse().map_err(|_| 
            DbasePolarsError::ConversionError(format!("Invalid year in date: {}", date_str)))?;
        let month: u32 = date_str[4..6].parse().map_err(|_| 
            DbasePolarsError::ConversionError(format!("Invalid month in date: {}", date_str)))?;
        let day: u32 = date_str[6..8].parse().map_err(|_| 
            DbasePolarsError::ConversionError(format!("Invalid day in date: {}", date_str)))?;
        
        NaiveDate::from_ymd_opt(year, month, day)
            .ok_or_else(|| DbasePolarsError::ConversionError(format!("Invalid date: {}", date_str)))
    } else {
        Err(DbasePolarsError::ConversionError(format!("Date string must be 8 characters: {}", date_str)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_dbase_type_conversion() {
        assert_eq!(map_dbase_to_polars(DbaseType::Character, None), DataType::String);
        assert_eq!(map_dbase_to_polars(DbaseType::Numeric, None), DataType::Float64);
        assert_eq!(map_dbase_to_polars(DbaseType::Date, None), DataType::Date);
        assert_eq!(map_dbase_to_polars(DbaseType::Logical, None), DataType::Boolean);
        assert_eq!(map_dbase_to_polars(DbaseType::Integer, None), DataType::Int32);
    }

    #[test]
    fn test_parse_dbase_date() {
        let date = parse_dbase_date("20230615").unwrap();
        assert_eq!(date.format("%Y").to_string(), "2023");
        assert_eq!(date.format("%m").to_string(), "06");
        assert_eq!(date.format("%d").to_string(), "15");

        assert!(parse_dbase_date("invalid").is_err());
        assert!(parse_dbase_date("20230230").is_err()); // Invalid date
    }

    #[test]
    fn test_character_to_boolean_conversion() {
        let target_type = DataType::Boolean;
        
        let true_val = convert_dbase_value_to_polars_value(
            FieldValue::Character(Some("T".to_string())), 
            &target_type
        ).unwrap();
        assert_eq!(true_val, AnyValue::Boolean(true));
        
        let false_val = convert_dbase_value_to_polars_value(
            FieldValue::Character(Some("F".to_string())), 
            &target_type
        ).unwrap();
        assert_eq!(false_val, AnyValue::Boolean(false));
    }

    #[test]
    fn test_character_to_numeric_conversion() {
        let target_type = DataType::Int32;
        
        let num_val = convert_dbase_value_to_polars_value(
            FieldValue::Character(Some("123".to_string())), 
            &target_type
        ).unwrap();
        assert_eq!(num_val, AnyValue::Int32(123));
        
        // Test invalid conversion
        assert!(convert_dbase_value_to_polars_value(
            FieldValue::Character(Some("abc".to_string())), 
            &target_type
        ).is_err());
    }
}
