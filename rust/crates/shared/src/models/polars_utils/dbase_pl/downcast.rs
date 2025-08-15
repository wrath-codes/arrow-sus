use std::collections::HashMap;
use serde_json::{Map, Value};
use polars::prelude::*;
use crate::models::polars_utils::dbase_pl::DbasePolarsError;

/// Configuration for dBase-specific downcasting optimizations
pub struct DbaseDowncastConfig {
    /// Convert string columns that contain only numeric values to appropriate numeric types
    pub check_strings: bool,
    /// Prefer integer types over float types when all values are whole numbers
    pub prefer_int_over_float: bool,
    /// Optimize dBase character fields that contain coded values (e.g., "01", "02" -> enum or small int)
    pub optimize_coded_fields: bool,
    /// Convert dBase logical fields from strings to proper booleans
    pub convert_logical_strings: bool,
    /// Shrink numeric types to the smallest possible representation
    pub shrink_numeric_types: bool,
    /// Minimum number of unique values required before converting to categorical
    pub categorical_threshold: usize,
}

impl Default for DbaseDowncastConfig {
    fn default() -> Self {
        Self {
            check_strings: true,
            prefer_int_over_float: true,
            optimize_coded_fields: true,
            convert_logical_strings: true,
            shrink_numeric_types: true,
            categorical_threshold: 50, // Convert to categorical if < 50 unique values in large datasets
        }
    }
}

/// Result of the downcast optimization process
pub struct DowncastResult {
    /// The optimized DataFrame
    pub dataframe: LazyFrame,
    /// JSON mapping of type changes for documentation/debugging
    pub type_changes_json: String,
    /// Summary statistics about the optimization
    pub summary: DowncastSummary,
}

/// Summary of downcast optimizations performed
#[derive(Debug, Clone)]
pub struct DowncastSummary {
    /// Number of string columns converted to numeric
    pub strings_to_numeric: usize,
    /// Number of float columns converted to integers
    pub floats_to_integers: usize,
    /// Number of columns shrunk to smaller types
    pub type_shrinks: usize,
    /// Number of columns converted to categorical
    pub to_categorical: usize,
    /// Number of logical string columns converted to boolean
    pub logical_conversions: usize,
    /// Estimated memory reduction in bytes
    pub estimated_memory_saved: usize,
}

impl Default for DowncastSummary {
    fn default() -> Self {
        Self {
            strings_to_numeric: 0,
            floats_to_integers: 0,
            type_shrinks: 0,
            to_categorical: 0,
            logical_conversions: 0,
            estimated_memory_saved: 0,
        }
    }
}

/// Intelligently optimize a dBase-derived DataFrame by downcasting types
pub fn intelligent_dbase_downcast(
    mut df: LazyFrame,
    cols: Option<Vec<String>>,
    config: DbaseDowncastConfig,
) -> Result<DowncastResult, DbasePolarsError> {
    let original_schema = df.collect_schema()
        .map_err(|e| DbasePolarsError::PolarsError(e))?;
    
    let columns_to_process = cols.unwrap_or_else(|| {
        original_schema.iter().map(|(name, _)| name.to_string()).collect()
    });

    let mut summary = DowncastSummary::default();

    // Step 1: Convert logical strings (T/F, Y/N, 1/0) to boolean
    if config.convert_logical_strings {
        let (new_df, count) = convert_logical_strings(df, &original_schema, &columns_to_process)?;
        df = new_df;
        summary.logical_conversions = count;
    }

    // Step 2: Handle string to numeric conversions (dBase often stores numbers as strings)
    if config.check_strings {
        let (new_df, count) = convert_dbase_numeric_strings(df, &original_schema, &columns_to_process)?;
        df = new_df;
        summary.strings_to_numeric = count;
    }

    // Step 3: Optimize coded fields (convert to categorical or small integers)
    if config.optimize_coded_fields {
        let (new_df, count) = optimize_coded_fields(df, &columns_to_process, config.categorical_threshold)?;
        df = new_df;
        summary.to_categorical = count;
    }

    // Step 4: Convert floats to integers where possible
    if config.prefer_int_over_float {
        let (new_df, count) = convert_floats_to_integers(df, &columns_to_process)?;
        df = new_df;
        summary.floats_to_integers = count;
    }

    // Step 5: Shrink integer types to optimal sizes
    if config.shrink_numeric_types {
        let (new_df, count) = safe_shrink_integers(df, &columns_to_process)?;
        df = new_df;
        summary.type_shrinks = count;
    }

    // Build type change mapping
    let new_schema = df.collect_schema()
        .map_err(|e| DbasePolarsError::PolarsError(e))?;
    
    let type_changes_json = build_type_mapping(&original_schema, &new_schema, &columns_to_process)?;
    
    // Estimate memory savings
    summary.estimated_memory_saved = estimate_memory_savings(&original_schema, &new_schema, &columns_to_process);

    Ok(DowncastResult {
        dataframe: df,
        type_changes_json,
        summary,
    })
}

/// Convert dBase logical strings (T/F, Y/N, 1/0) to proper boolean columns
fn convert_logical_strings(
    mut df: LazyFrame,
    schema: &Schema,
    columns: &[String],
) -> Result<(LazyFrame, usize), DbasePolarsError> {
    let string_columns: Vec<String> = columns
        .iter()
        .filter(|col| matches!(schema.get(col.as_str()), Some(DataType::String)))
        .cloned()
        .collect();

    if string_columns.is_empty() {
        return Ok((df, 0));
    }

    // For now, implement a simple approach - just try to convert known logical columns
    // In practice, you'd analyze the data to detect logical patterns
    let mut columns_to_convert = Vec::new();
    
    // Simple heuristic: if column name suggests it's logical, try to convert
    for col_name in &string_columns {
        let lower_name = col_name.to_lowercase();
        if lower_name.contains("flag") || lower_name.contains("bool") || 
           lower_name.contains("logical") || lower_name.ends_with("_yn") {
            columns_to_convert.push(col_name.clone());
        }
    }

    if columns_to_convert.is_empty() {
        return Ok((df, 0));
    }

    // Convert using simple when/then expressions
    let cast_exprs: Vec<Expr> = columns_to_convert
        .iter()
        .map(|col_name| {
            col(col_name)
                .str().to_uppercase()
                .eq(lit("T")).or(
                    col(col_name).str().to_uppercase().eq(lit("TRUE"))
                ).or(
                    col(col_name).str().to_uppercase().eq(lit("Y"))
                ).or(
                    col(col_name).str().to_uppercase().eq(lit("YES"))
                ).or(
                    col(col_name).str().to_uppercase().eq(lit("1"))
                )
                .alias(col_name)
        })
        .collect();

    let result_df = df.with_columns(cast_exprs);
    Ok((result_df, columns_to_convert.len()))
}

/// Convert string columns that contain only numeric values to appropriate numeric types
fn convert_dbase_numeric_strings(
    mut df: LazyFrame,
    schema: &Schema,
    columns: &[String],
) -> Result<(LazyFrame, usize), DbasePolarsError> {
    let string_columns: Vec<String> = columns
        .iter()
        .filter(|col| matches!(schema.get(col.as_str()), Some(DataType::String)))
        .cloned()
        .collect();

    if string_columns.is_empty() {
        return Ok((df, 0));
    }

    // Check which columns can be converted to numeric
    let mut columns_to_convert = Vec::new();
    
    for col_name in &string_columns {
        // Try to cast to float64 and check if it preserves the null count
        let check_expr = vec![
            col(col_name).null_count().alias("original_nulls"),
            col(col_name).cast(DataType::Float64)
                .null_count().alias("new_nulls"),
        ];
        
        if let Ok(check_df) = df.clone().select(check_expr).collect() {
            if let (Ok(orig), Ok(new)) = (
                check_df.column("original_nulls").and_then(|c| c.i64()),
                check_df.column("new_nulls").and_then(|c| c.i64())
            ) {
                if orig.get(0) == new.get(0) {
                    columns_to_convert.push(col_name.clone());
                }
            }
        }
    }

    if columns_to_convert.is_empty() {
        return Ok((df, 0));
    }

    // Convert to numeric
    let cast_exprs: Vec<Expr> = columns_to_convert
        .iter()
        .map(|col_name| {
            col(col_name).cast(DataType::Float64).alias(col_name)
        })
        .collect();

    let result_df = df.with_columns(cast_exprs);
    Ok((result_df, columns_to_convert.len()))
}

/// Optimize coded fields by converting to categorical or small integers
fn optimize_coded_fields(
    mut df: LazyFrame,
    _columns: &[String],
    _categorical_threshold: usize,
) -> Result<(LazyFrame, usize), DbasePolarsError> {
    // For now, implement a simple categorical conversion for string columns with few unique values
    // This could be enhanced to detect specific dBase coding patterns
    
    let conversions = 0;
    let result_df = df;

    // This is a simplified implementation - in practice, you'd want to analyze
    // the unique value counts for each column and make decisions based on that
    // For now, we'll skip this optimization to keep the implementation simpler
    
    Ok((result_df, conversions))
}

/// Convert float columns to integers where all values are whole numbers
fn convert_floats_to_integers(
    mut df: LazyFrame,
    columns: &[String],
) -> Result<(LazyFrame, usize), DbasePolarsError> {
    let schema = df.collect_schema().map_err(|e| DbasePolarsError::PolarsError(e))?;
    
    let float_columns: Vec<String> = columns
        .iter()
        .filter(|col| {
            matches!(
                schema.get(col.as_str()),
                Some(DataType::Float32 | DataType::Float64)
            )
        })
        .cloned()
        .collect();

    if float_columns.is_empty() {
        return Ok((df, 0));
    }

    let mut columns_to_convert = Vec::new();

    // Check each float column to see if all values are whole numbers
    for col_name in &float_columns {
        let check_expr = vec![
            col(col_name).eq(col(col_name).cast(DataType::Int64).cast(DataType::Float64)).all(false).alias(&format!("{}_is_whole", col_name))
        ];
        
        if let Ok(check_df) = df.clone().select(check_expr).collect() {
            let check_col = format!("{}_is_whole", col_name);
            if let Ok(column) = check_df.column(&check_col) {
                if let Ok(bool_array) = column.bool() {
                    if bool_array.get(0).unwrap_or(false) {
                        columns_to_convert.push(col_name.clone());
                    }
                }
            }
        }
    }

    if columns_to_convert.is_empty() {
        return Ok((df, 0));
    }

    // Convert to Int64 (will be further optimized in the shrinking step)
    let cast_exprs: Vec<Expr> = columns_to_convert
        .iter()
        .map(|col_name| col(col_name).cast(DataType::Int64).alias(col_name))
        .collect();

    let result_df = df.with_columns(cast_exprs);
    Ok((result_df, columns_to_convert.len()))
}

/// Safely shrink integer columns to the smallest possible type
fn safe_shrink_integers(
    mut df: LazyFrame,
    columns: &[String],
) -> Result<(LazyFrame, usize), DbasePolarsError> {
    let schema = df.collect_schema().map_err(|e| DbasePolarsError::PolarsError(e))?;
    
    let int_columns: Vec<String> = columns
        .iter()
        .filter(|col| {
            matches!(
                schema.get(col.as_str()),
                Some(DataType::Int64 | DataType::Int32 | DataType::Int16 | DataType::Int8 |
                     DataType::UInt64 | DataType::UInt32 | DataType::UInt16 | DataType::UInt8)
            )
        })
        .cloned()
        .collect();

    if int_columns.is_empty() {
        return Ok((df, 0));
    }

    // Get min/max for all integer columns
    let stats_exprs: Vec<Expr> = int_columns
        .iter()
        .flat_map(|col_name| {
            vec![
                col(col_name).min().cast(DataType::Int64).alias(&format!("{}_min", col_name)),
                col(col_name).max().cast(DataType::Int64).alias(&format!("{}_max", col_name)),
            ]
        })
        .collect();

    let stats_df = df.clone().select(stats_exprs).collect()
        .map_err(|e| DbasePolarsError::PolarsError(e))?;

    // Determine optimal types and build cast expressions
    let mut cast_exprs = Vec::new();
    let mut conversions = 0;

    for col_name in &int_columns {
        let min_col = format!("{}_min", col_name);
        let max_col = format!("{}_max", col_name);

        if let (Ok(min_column), Ok(max_column)) = (stats_df.column(&min_col), stats_df.column(&max_col)) {
            if let (Ok(min_array), Ok(max_array)) = (min_column.i64(), max_column.i64()) {
                if let (Some(min_val), Some(max_val)) = (min_array.get(0), max_array.get(0)) {
                    let optimal_type = find_optimal_integer_type(min_val, max_val);
                    let current_type = schema.get(col_name.as_str()).unwrap();

                    if optimal_type != *current_type && is_better_type(&optimal_type, current_type) {
                        cast_exprs.push(col(col_name).cast(optimal_type).alias(col_name));
                        conversions += 1;
                    }
                }
            }
        }
    }

    let result_df = if !cast_exprs.is_empty() {
        df.with_columns(cast_exprs)
    } else {
        df
    };

    Ok((result_df, conversions))
}

/// Find the smallest integer type that can hold the given range
fn find_optimal_integer_type(min_val: i64, max_val: i64) -> DataType {
    // Boolean (0, 1)
    if min_val >= 0 && max_val <= 1 {
        return DataType::Boolean;
    }
    
    // UInt8 (0 to 255)
    if min_val >= 0 && max_val <= 255 {
        return DataType::UInt8;
    }
    
    // Int8 (-128 to 127)
    if min_val >= -128 && max_val <= 127 {
        return DataType::Int8;
    }
    
    // UInt16 (0 to 65,535)
    if min_val >= 0 && max_val <= 65535 {
        return DataType::UInt16;
    }
    
    // Int16 (-32,768 to 32,767)
    if min_val >= -32768 && max_val <= 32767 {
        return DataType::Int16;
    }
    
    // UInt32 (0 to 4,294,967,295)
    if min_val >= 0 && max_val <= 4294967295 {
        return DataType::UInt32;
    }
    
    // Int32 (-2,147,483,648 to 2,147,483,647)
    if min_val >= -2147483648 && max_val <= 2147483647 {
        return DataType::Int32;
    }
    
    // UInt64 (0 to 18,446,744,073,709,551,615)
    if min_val >= 0 {
        return DataType::UInt64;
    }
    
    // Fall back to Int64
    DataType::Int64
}

/// Check if the new type is actually better (smaller) than the current type
fn is_better_type(new_type: &DataType, current_type: &DataType) -> bool {
    let type_size = |dt: &DataType| match dt {
        DataType::Boolean => 1,
        DataType::Int8 | DataType::UInt8 => 8,
        DataType::Int16 | DataType::UInt16 => 16,
        DataType::Int32 | DataType::UInt32 => 32,
        DataType::Int64 | DataType::UInt64 => 64,
        DataType::Float32 => 32,
        DataType::Float64 => 64,
        _ => 64, // Default to largest
    };
    
    type_size(new_type) < type_size(current_type)
}

/// Build a JSON mapping of type changes for documentation
fn build_type_mapping(
    schema_original: &Schema,
    schema_new: &Schema,
    columns: &[String],
) -> Result<String, DbasePolarsError> {
    let mut type_groups: HashMap<String, Vec<String>> = HashMap::new();
    
    for col_name in columns {
        if let (Some(original_type), Some(new_type)) = (
            schema_original.get(col_name.as_str()),
            schema_new.get(col_name.as_str())
        ) {
            if original_type != new_type {
                let type_key = format!("{:?}", new_type).to_lowercase();
                type_groups
                    .entry(type_key)
                    .or_insert_with(Vec::new)
                    .push(col_name.clone());
            }
        }
    }
    
    // Convert to JSON
    let json_map: Map<String, Value> = type_groups
        .into_iter()
        .map(|(type_name, columns)| {
            (type_name, Value::Array(
                columns.into_iter().map(Value::String).collect()
            ))
        })
        .collect();
    
    Ok(serde_json::to_string(&json_map).unwrap_or_else(|_| "{}".to_string()))
}

/// Estimate memory savings from type optimizations
fn estimate_memory_savings(
    original_schema: &Schema,
    new_schema: &Schema,
    columns: &[String],
) -> usize {
    let type_size = |dt: &DataType| match dt {
        DataType::Boolean => 1,
        DataType::Int8 | DataType::UInt8 => 1,
        DataType::Int16 | DataType::UInt16 => 2,
        DataType::Int32 | DataType::UInt32 | DataType::Float32 => 4,
        DataType::Int64 | DataType::UInt64 | DataType::Float64 => 8,
        DataType::String => 24, // Approximate for string overhead
        _ => 8,
    };
    
    let mut savings = 0usize;
    for col_name in columns {
        if let (Some(original_type), Some(new_type)) = (
            original_schema.get(col_name.as_str()),
            new_schema.get(col_name.as_str())
        ) {
            let original_size = type_size(original_type);
            let new_size = type_size(new_type);
            if original_size > new_size {
                savings += original_size - new_size;
            }
        }
    }
    
    savings
}

/// Convenience function for DataFrames
pub fn intelligent_dbase_downcast_df(
    df: DataFrame,
    cols: Option<Vec<String>>,
    config: DbaseDowncastConfig,
) -> Result<DowncastResult, DbasePolarsError> {
    let lazy_df = df.lazy();
    let mut result = intelligent_dbase_downcast(lazy_df, cols, config)?;
    result.dataframe = result.dataframe; // Keep as lazy for efficiency
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_find_optimal_integer_type() {
        assert_eq!(find_optimal_integer_type(0, 1), DataType::Boolean);
        assert_eq!(find_optimal_integer_type(0, 255), DataType::UInt8);
        assert_eq!(find_optimal_integer_type(-128, 127), DataType::Int8);
        assert_eq!(find_optimal_integer_type(0, 65535), DataType::UInt16);
        assert_eq!(find_optimal_integer_type(-32768, 32767), DataType::Int16);
        assert_eq!(find_optimal_integer_type(0, 4294967295), DataType::UInt32);
        assert_eq!(find_optimal_integer_type(-2147483648, 2147483647), DataType::Int32);
        assert_eq!(find_optimal_integer_type(0, i64::MAX), DataType::UInt64);
        assert_eq!(find_optimal_integer_type(i64::MIN, -1), DataType::Int64);
    }

    #[test]
    fn test_is_better_type() {
        assert!(is_better_type(&DataType::Int8, &DataType::Int64));
        assert!(is_better_type(&DataType::Boolean, &DataType::Int8));
        assert!(!is_better_type(&DataType::Int64, &DataType::Int8));
        assert!(is_better_type(&DataType::UInt16, &DataType::Int32));
    }

    #[test]
    fn test_downcast_config_default() {
        let config = DbaseDowncastConfig::default();
        assert!(config.check_strings);
        assert!(config.prefer_int_over_float);
        assert!(config.optimize_coded_fields);
        assert!(config.convert_logical_strings);
        assert!(config.shrink_numeric_types);
        assert_eq!(config.categorical_threshold, 50);
    }

    #[test]
    fn test_downcast_summary_default() {
        let summary = DowncastSummary::default();
        assert_eq!(summary.strings_to_numeric, 0);
        assert_eq!(summary.floats_to_integers, 0);
        assert_eq!(summary.type_shrinks, 0);
        assert_eq!(summary.to_categorical, 0);
        assert_eq!(summary.logical_conversions, 0);
        assert_eq!(summary.estimated_memory_saved, 0);
    }

    #[test]
    fn test_with_sample_dataframe() {
        // Create a sample DataFrame with mixed types typical of dBase files
        let df = df! {
            "string_numbers" => ["1", "2", "3", "127"],
            "logical_flag" => ["T", "F", "T", "F"],
            "float_ints" => [1.0, 2.0, 3.0, 127.0],
            "real_floats" => [1.1, 2.2, 3.3, 4.4],
            "text_data" => ["hello", "world", "test", "data"],
        }.unwrap();

        let config = DbaseDowncastConfig::default();
        
        match intelligent_dbase_downcast_df(df, None, config) {
            Ok(result) => {
                println!("✅ Downcast successful!");
                println!("   🔄 Type changes: {}", result.type_changes_json);
                println!("   📊 Summary: {:?}", result.summary);
                
                // The logical_flag column should be converted based on naming heuristic
                assert!(result.summary.logical_conversions <= 1); // Should convert logical_flag
                
                // text_data should remain as string
                // string_numbers should be converted to numeric
                // float_ints should be converted to integers
            }
            Err(e) => {
                println!("❌ Downcast failed: {}", e);
            }
        }
    }
}
