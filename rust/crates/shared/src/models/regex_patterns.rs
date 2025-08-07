use regex::Regex;
use once_cell::sync::Lazy;

/// Regex pattern for DATASUS files following the pattern:
/// [group_name][uf_code][year(2 digits)][month(2 digits)].dbc
/// 
/// The pattern is built from right to left:
/// - Extension: .dbc or .DBC
/// - Month: 2 digits (01-12)
/// - Year: 2 digits (00-99)
/// - UF code: 2 characters (state code)
/// - Group name: variable length alphanumeric
pub static DATASUS_FILE_PATTERN: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"^(?P<group>[A-Za-z0-9]+)(?P<uf>[A-Z]{2})(?P<year>\d{2})(?P<month>0[1-9]|1[0-2])\.(?i:dbc)$")
        .expect("Invalid regex pattern for DATASUS files")
});

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DataSusFileInfo {
    pub group_name: String,
    pub uf_code: String,
    pub year: u8,
    pub month: u8,
    pub full_filename: String,
}

impl DataSusFileInfo {
    /// Parse a DATASUS filename and extract its components
    pub fn parse(filename: &str) -> Option<Self> {
        let captures = DATASUS_FILE_PATTERN.captures(filename)?;
        
        let group_name = captures.name("group")?.as_str().to_string();
        let uf_code = captures.name("uf")?.as_str().to_string();
        let year_str = captures.name("year")?.as_str();
        let month_str = captures.name("month")?.as_str();
        
        let year = year_str.parse::<u8>().ok()?;
        let month = month_str.parse::<u8>().ok()?;
        
        // Validate month range
        if !(1..=12).contains(&month) {
            return None;
        }
        
        Some(DataSusFileInfo {
            group_name,
            uf_code,
            year,
            month,
            full_filename: filename.to_string(),
        })
    }
    
    /// Get the full year (assuming 2000s for years 00-30, 1900s for years 31-99)
    pub fn full_year(&self) -> u16 {
        if self.year <= 30 {
            2000 + self.year as u16
        } else {
            1900 + self.year as u16
        }
    }
    
    /// Generate a filename with the given components
    pub fn generate_filename(group_name: &str, uf_code: &str, year: u8, month: u8) -> String {
        format!("{}{}{:02}{:02}.dbc", group_name, uf_code, year, month)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_datasus_file_pattern() {
        // Test valid filenames
        let valid_files = vec![
            "PAAL2301.dbc",
            "PAAL2301.DBC", 
            "RDAL2301.dbc",
            "STAL2301.dbc",
            "LTSP2212.DBC",
        ];
        
        for file in valid_files {
            assert!(DATASUS_FILE_PATTERN.is_match(file), "Should match: {}", file);
        }
        
        // Test invalid filenames
        let invalid_files = vec![
            "PAAL231.dbc",      // Wrong year format
            "PAAL2313.dbc",     // Invalid month
            "PAAL2300.dbc",     // Invalid month (00)
            "PA_AL2301.dbc",    // Underscore in group
            "PAAL2301.txt",     // Wrong extension
            "PAAL2301",         // No extension
        ];
        
        for file in invalid_files {
            assert!(!DATASUS_FILE_PATTERN.is_match(file), "Should not match: {}", file);
        }
    }
    
    #[test]
    fn test_parse_filename() {
        let info = DataSusFileInfo::parse("PAAL2301.dbc").unwrap();
        assert_eq!(info.group_name, "PA");
        assert_eq!(info.uf_code, "AL");
        assert_eq!(info.year, 23);
        assert_eq!(info.month, 1);
        assert_eq!(info.full_year(), 2023);
        
        // Test case insensitive extension
        let info2 = DataSusFileInfo::parse("RDSP2212.DBC").unwrap();
        assert_eq!(info2.group_name, "RD");
        assert_eq!(info2.uf_code, "SP");
        assert_eq!(info2.year, 22);
        assert_eq!(info2.month, 12);
        assert_eq!(info2.full_year(), 2022);
        
        // Test invalid filename
        assert!(DataSusFileInfo::parse("invalid.txt").is_none());
    }
    
    #[test]
    fn test_generate_filename() {
        let filename = DataSusFileInfo::generate_filename("PA", "AL", 23, 1);
        assert_eq!(filename, "PAAL2301.dbc");
        
        let filename2 = DataSusFileInfo::generate_filename("RD", "SP", 22, 12);
        assert_eq!(filename2, "RDSP2212.dbc");
    }
}
