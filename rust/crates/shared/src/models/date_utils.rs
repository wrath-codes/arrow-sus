use chrono::{Datelike, Local};
use std::collections::HashMap;
use once_cell::sync::Lazy;

/// Mapping of month numbers to Portuguese month names
pub static MONTHS: Lazy<HashMap<u8, &'static str>> = Lazy::new(|| {
    let mut months = HashMap::new();
    months.insert(1, "Janeiro");
    months.insert(2, "Fevereiro");
    months.insert(3, "Março");
    months.insert(4, "Abril");
    months.insert(5, "Maio");
    months.insert(6, "Junho");
    months.insert(7, "Julho");
    months.insert(8, "Agosto");
    months.insert(9, "Setembro");
    months.insert(10, "Outubro");
    months.insert(11, "Novembro");
    months.insert(12, "Dezembro");
    months
});

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TimeFormatError {
    pub format: String,
    pub value: String,
    pub message: String,
}

impl TimeFormatError {
    pub fn new(format: &str, value: &str, message: &str) -> Self {
        Self {
            format: format.to_string(),
            value: value.to_string(),
            message: message.to_string(),
        }
    }
}

impl std::fmt::Display for TimeFormatError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: expected format '{}', got '{}'", self.message, self.format, self.value)
    }
}

impl std::error::Error for TimeFormatError {}

/// Fills year with leading zeros and returns an integer.
/// Handles 2-digit years by determining the appropriate century.
/// 
/// # Arguments
/// * `year` - Year as string or number to be filled
/// 
/// # Returns
/// * `Ok(i32)` - Full year as integer
/// * `Err(TimeFormatError)` - Error if format is invalid
/// 
/// # Example
/// ```rust
/// use shared::models::date_utils::zfill_year;
/// 
/// assert_eq!(zfill_year("24").unwrap(), 2024);
/// assert_eq!(zfill_year("24").unwrap(), 2024);
/// assert_eq!(zfill_year("2024").unwrap(), 2024);
/// assert_eq!(zfill_year("2024").unwrap(), 2024);
/// ```
pub fn zfill_year(year: &str) -> Result<i32, TimeFormatError> {
    let year_str = year.trim();
    
    // Check if string contains only digits
    if !year_str.chars().all(|c| c.is_ascii_digit()) {
        return Err(TimeFormatError::new(
            "YYYY",
            year_str,
            "Ano deve conter apenas dígitos"
        ));
    }
    
    let year_num: i32 = year_str.parse()
        .map_err(|_| TimeFormatError::new("YYYY", year_str, "Falha ao converter ano para número"))?;
    
    // Handle 2-digit years
    if year_num < 100 {
        let current_year = Local::now().year();
        let century = (current_year / 100) * 100;
        let current_year_2digit = current_year % 100;
        
        let full_year = if year_num > current_year_2digit {
            century - 100 + year_num
        } else {
            century + year_num
        };
        
        Ok(full_year)
    } else {
        // Handle 4-digit years
        Ok(year_num)
    }
}

/// Async version of zfill_year
pub async fn zfill_year_async(year: &str) -> Result<i32, TimeFormatError> {
    // For this simple operation, we just wrap the sync version
    // In a real async context, this might involve async I/O operations
    tokio::task::yield_now().await;
    zfill_year(year)
}

/// Converts month number to Portuguese month name.
/// 
/// # Arguments
/// * `month` - Month number (1-12)
/// 
/// # Returns
/// * `Ok(String)` - Portuguese month name
/// * `Err(TimeFormatError)` - Error if month is invalid
/// 
/// # Example
/// ```rust
/// use shared::models::date_utils::get_month;
/// 
/// assert_eq!(get_month(1).unwrap(), "Janeiro");
/// assert_eq!(get_month(12).unwrap(), "Dezembro");
/// ```
pub fn get_month(month: u8) -> Result<&'static str, TimeFormatError> {
    if !(1..=12).contains(&month) {
        return Err(TimeFormatError::new(
            "1-12",
            &month.to_string(),
            "Mês deve estar entre 1 e 12"
        ));
    }
    
    MONTHS.get(&month)
        .copied()
        .ok_or_else(|| TimeFormatError::new(
            "1-12",
            &month.to_string(),
            "Mês não encontrado no mapeamento"
        ))
}

/// Async version of get_month
pub async fn get_month_async(month: u8) -> Result<&'static str, TimeFormatError> {
    // For this simple operation, we just wrap the sync version
    // In a real async context, this might involve async I/O operations
    tokio::task::yield_now().await;
    get_month(month)
}

/// Converts month string to Portuguese month name.
/// Handles string inputs that may have leading zeros.
/// 
/// # Arguments
/// * `month_str` - Month as string (e.g., "01", "1", "12")
/// 
/// # Returns
/// * `Ok(String)` - Portuguese month name
/// * `Err(TimeFormatError)` - Error if month is invalid
pub fn get_month_from_str(month_str: &str) -> Result<&'static str, TimeFormatError> {
    let month_str = month_str.trim();
    
    if !month_str.chars().all(|c| c.is_ascii_digit()) {
        return Err(TimeFormatError::new(
            "1-12",
            month_str,
            "Mês deve ser um número"
        ));
    }
    
    // Remove leading zeros and parse
    let month_num: u8 = month_str.trim_start_matches('0')
        .parse()
        .unwrap_or(0);
    
    if month_num == 0 && month_str.chars().all(|c| c == '0') {
        return Err(TimeFormatError::new(
            "1-12",
            month_str,
            "Mês não pode ser zero"
        ));
    }
    
    get_month(month_num)
}

/// Async version of get_month_from_str
pub async fn get_month_from_str_async(month_str: &str) -> Result<&'static str, TimeFormatError> {
    tokio::task::yield_now().await;
    get_month_from_str(month_str)
}

/// Formats a date label in Portuguese
/// 
/// # Arguments
/// * `month` - Month number (1-12)
/// * `year` - Year as integer
/// 
/// # Returns
/// * `Ok(String)` - Formatted date as "Month/Year"
/// * `Err(TimeFormatError)` - Error if month is invalid
/// 
/// # Example
/// ```rust
/// use shared::models::date_utils::format_date_label;
/// 
/// assert_eq!(format_date_label(1, 2024).unwrap(), "Janeiro/2024");
/// ```
pub fn format_date_label(month: u8, year: i32) -> Result<String, TimeFormatError> {
    let month_name = get_month(month)?;
    Ok(format!("{}/{}", month_name, year))
}

/// Async version of format_date_label
pub async fn format_date_label_async(month: u8, year: i32) -> Result<String, TimeFormatError> {
    let month_name = get_month_async(month).await?;
    Ok(format!("{}/{}", month_name, year))
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_months_mapping() {
        assert_eq!(MONTHS.get(&1), Some(&"Janeiro"));
        assert_eq!(MONTHS.get(&12), Some(&"Dezembro"));
        assert_eq!(MONTHS.get(&13), None);
    }
    
    #[test]
    fn test_zfill_year() {
        // Test 2-digit years
        let result = zfill_year("24");
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 2024);
        
        // Test 4-digit years
        assert_eq!(zfill_year("2024").unwrap(), 2024);
        assert_eq!(zfill_year("1999").unwrap(), 1999);
        
        // Test invalid input
        assert!(zfill_year("abc").is_err());
        assert!(zfill_year("20a4").is_err());
    }
    
    #[test]
    fn test_get_month() {
        assert_eq!(get_month(1).unwrap(), "Janeiro");
        assert_eq!(get_month(12).unwrap(), "Dezembro");
        
        // Test invalid months
        assert!(get_month(0).is_err());
        assert!(get_month(13).is_err());
    }
    
    #[test]
    fn test_get_month_from_str() {
        assert_eq!(get_month_from_str("1").unwrap(), "Janeiro");
        assert_eq!(get_month_from_str("01").unwrap(), "Janeiro");
        assert_eq!(get_month_from_str("12").unwrap(), "Dezembro");
        
        // Test invalid inputs
        assert!(get_month_from_str("0").is_err());
        assert!(get_month_from_str("00").is_err());
        assert!(get_month_from_str("13").is_err());
        assert!(get_month_from_str("abc").is_err());
    }
    
    #[test]
    fn test_format_date_label() {
        assert_eq!(format_date_label(1, 2024).unwrap(), "Janeiro/2024");
        assert_eq!(format_date_label(12, 2023).unwrap(), "Dezembro/2023");
        
        // Test invalid month
        assert!(format_date_label(13, 2024).is_err());
    }
    
    #[tokio::test]
    async fn test_async_functions() {
        // Test async variants work the same as sync
        assert_eq!(zfill_year_async("24").await.unwrap(), 2024);
        assert_eq!(get_month_async(1).await.unwrap(), "Janeiro");
        assert_eq!(get_month_from_str_async("01").await.unwrap(), "Janeiro");
        assert_eq!(format_date_label_async(1, 2024).await.unwrap(), "Janeiro/2024");
    }
}
