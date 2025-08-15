use std::collections::HashMap;
use std::env;
use serde_json::{Map, Value};
use crate::models::polars_utils::dbase_pl::{
    DbasePolarsError,
    DowncastResult,
    DbaseFileSummary,
};

/// Interface configuration for dBase processing operations
#[derive(Debug, Clone)]
pub struct DbaseInterfaceConfig {
    /// Whether to output progress and status messages
    pub verbose: bool,
    /// Whether to write results to environment variables
    pub set_env_vars: bool,
    /// Whether to export results as JSON
    pub json_output: bool,
}

impl Default for DbaseInterfaceConfig {
    fn default() -> Self {
        Self {
            verbose: true,
            set_env_vars: false,
            json_output: false,
        }
    }
}

/// Interface for standardized output and result handling
pub struct DbaseInterface {
    config: DbaseInterfaceConfig,
}

impl DbaseInterface {
    pub fn new(config: DbaseInterfaceConfig) -> Self {
        Self { config }
    }

    /// Display a message if verbose mode is enabled
    pub fn display(&self, message: &str) {
        if self.config.verbose {
            println!("{}", message);
        }
    }

    /// Display a formatted status message
    pub fn status(&self, operation: &str, status: &str) {
        self.display(&format!("📋 {}: {}", operation, status));
    }

    /// Display an error message
    pub fn error(&self, operation: &str, error: &DbasePolarsError) {
        self.display(&format!("❌ {}: {}", operation, error));
    }

    /// Display a success message with optional details
    pub fn success(&self, operation: &str, details: Option<&str>) {
        match details {
            Some(details) => self.display(&format!("✅ {}: {}", operation, details)),
            None => self.display(&format!("✅ {}", operation)),
        }
    }

    /// Set an environment variable if enabled
    pub fn set_env_var(&self, name: &str, value: &str) {
        if self.config.set_env_vars {
            unsafe { env::set_var(name, value); }
            self.display(&format!("🔧 Set {}: {}", name, value));
        }
    }

    /// Export file summary results
    pub fn export_file_summary(&self, summary: &DbaseFileSummary, file_path: &str) {
        self.success("File Analysis", Some(&format!(
            "{} - {} columns, {} rows", 
            file_path, summary.n_columns, summary.n_rows
        )));

        if self.config.set_env_vars {
            self.set_env_var("DBASE_COLUMNS", &summary.n_columns.to_string());
            self.set_env_var("DBASE_ROWS", &summary.n_rows.to_string());
        }

        if self.config.json_output {
            let json_summary = self.file_summary_to_json(summary);
            self.display(&format!("📄 JSON Summary: {}", json_summary));
        }

        if self.config.verbose {
            self.display("📊 Field Details:");
            for (i, field) in summary.field_info.iter().enumerate() {
                self.display(&format!(
                    "   {}: {} ({}) -> {:?} [{}]",
                    i + 1,
                    field.name,
                    field.dbase_type,
                    field.polars_type,
                    field.length
                ));
            }
        }
    }

    /// Export downcast optimization results
    pub fn export_downcast_results(&self, result: &DowncastResult) {
        self.success("Type Optimization", Some(&format!(
            "Memory saved: {} bytes, {} conversions",
            result.summary.estimated_memory_saved,
            result.summary.strings_to_numeric + result.summary.floats_to_integers + 
            result.summary.type_shrinks + result.summary.logical_conversions
        )));

        if self.config.set_env_vars {
            self.set_env_var("DBASE_MEMORY_SAVED", &result.summary.estimated_memory_saved.to_string());
            self.set_env_var("DBASE_TYPE_CHANGES", &result.type_changes_json);
        }

        if self.config.json_output {
            self.display(&format!("🔄 Type Changes: {}", result.type_changes_json));
        }

        if self.config.verbose {
            self.display("🔧 Optimization Summary:");
            self.display(&format!("   String→Numeric: {}", result.summary.strings_to_numeric));
            self.display(&format!("   Float→Integer: {}", result.summary.floats_to_integers));
            self.display(&format!("   Type Shrinks: {}", result.summary.type_shrinks));
            self.display(&format!("   Logical Conversions: {}", result.summary.logical_conversions));
            self.display(&format!("   Categorical: {}", result.summary.to_categorical));
        }
    }

    /// Convert file summary to JSON
    fn file_summary_to_json(&self, summary: &DbaseFileSummary) -> String {
        let mut json_obj = Map::new();
        json_obj.insert("n_columns".to_string(), Value::Number(summary.n_columns.into()));
        json_obj.insert("n_rows".to_string(), Value::Number(summary.n_rows.into()));
        
        let fields: Vec<Value> = summary.field_info.iter().map(|field| {
            let mut field_obj = Map::new();
            field_obj.insert("name".to_string(), Value::String(field.name.clone()));
            field_obj.insert("dbase_type".to_string(), Value::String(field.dbase_type.clone()));
            field_obj.insert("length".to_string(), Value::Number(field.length.into()));
            field_obj.insert("polars_type".to_string(), Value::String(format!("{:?}", field.polars_type)));
            Value::Object(field_obj)
        }).collect();
        
        json_obj.insert("fields".to_string(), Value::Array(fields));
        
        serde_json::to_string(&json_obj).unwrap_or_else(|_| "{}".to_string())
    }

    /// Display a progress bar for operations
    pub fn progress(&self, current: usize, total: usize, operation: &str) {
        if self.config.verbose && total > 0 {
            let percent = (current * 100) / total;
            if percent % 10 == 0 {
                self.display(&format!("⏳ {}: {}% ({}/{})", operation, percent, current, total));
            }
        }
    }

    /// Get configuration from environment variables
    pub fn from_env() -> Self {
        let config = DbaseInterfaceConfig {
            verbose: env::var("DBASE_VERBOSE")
                .map(|v| v.to_lowercase() == "true" || v == "1")
                .unwrap_or(true),
            set_env_vars: env::var("DBASE_SET_ENV_VARS")
                .map(|v| v.to_lowercase() == "true" || v == "1")
                .unwrap_or(false),
            json_output: env::var("DBASE_JSON_OUTPUT")
                .map(|v| v.to_lowercase() == "true" || v == "1")
                .unwrap_or(false),
        };
        
        Self::new(config)
    }
}

/// Utility functions for common interface operations
pub mod utils {
    use super::*;

    /// Create a standard interface for file processing
    pub fn create_file_processor_interface() -> DbaseInterface {
        DbaseInterface::from_env()
    }

    /// Process a file and export all results using the interface
    pub fn process_and_export_file<P: AsRef<std::path::Path>>(
        file_path: P,
        interface: &DbaseInterface,
    ) -> Result<(), DbasePolarsError> {
        use crate::models::polars_utils::dbase_pl::{
            get_dbase_file_summary,
            DescribeConfig,
        };

        let path_str = file_path.as_ref().to_string_lossy();
        interface.status("Processing", &format!("Reading {}", path_str));

        // Get file summary
        let config = DescribeConfig::default();
        match get_dbase_file_summary(file_path.as_ref(), config) {
            Ok(summary) => {
                interface.export_file_summary(&summary, &path_str);
                Ok(())
            }
            Err(e) => {
                interface.error("File Processing", &e);
                Err(e)
            }
        }
    }

    /// Get environment variable with default value
    pub fn get_env_var_or_default(var_name: &str, default: &str) -> String {
        env::var(var_name).unwrap_or_else(|_| default.to_string())
    }

    /// Set multiple environment variables from a HashMap
    pub fn set_env_vars(vars: &HashMap<String, String>) {
        for (key, value) in vars {
            unsafe { env::set_var(key, value); }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_interface_config_default() {
        let config = DbaseInterfaceConfig::default();
        assert!(config.verbose);
        assert!(!config.set_env_vars);
        assert!(!config.json_output);
        // Basic config test
        assert_eq!(config.verbose, true);
    }

    #[test]
    fn test_interface_creation() {
        let config = DbaseInterfaceConfig {
            verbose: false,
            set_env_vars: true,
            json_output: true,
        };
        
        let interface = DbaseInterface::new(config);
        assert!(!interface.config.verbose);
        assert!(interface.config.set_env_vars);
        assert!(interface.config.json_output);
    }

    #[test]
    fn test_env_var_utilities() {
        // Test setting and getting env vars
        unsafe { env::set_var("TEST_DBASE_VAR", "test_value"); }
        let value = utils::get_env_var_or_default("TEST_DBASE_VAR", "default");
        assert_eq!(value, "test_value");
        
        let default_value = utils::get_env_var_or_default("NONEXISTENT_VAR", "default");
        assert_eq!(default_value, "default");
        
        // Test batch setting
        let mut vars = HashMap::new();
        vars.insert("BATCH_VAR1".to_string(), "value1".to_string());
        vars.insert("BATCH_VAR2".to_string(), "value2".to_string());
        utils::set_env_vars(&vars);
        
        assert_eq!(env::var("BATCH_VAR1").unwrap(), "value1");
        assert_eq!(env::var("BATCH_VAR2").unwrap(), "value2");
    }

    #[test]
    fn test_from_env() {
        // Set test environment variables
        unsafe { 
            env::set_var("DBASE_VERBOSE", "false");
            env::set_var("DBASE_SET_ENV_VARS", "true");
            env::set_var("DBASE_JSON_OUTPUT", "true");
        }
        
        let interface = DbaseInterface::from_env();
        assert!(!interface.config.verbose);
        assert!(interface.config.set_env_vars);
        assert!(interface.config.json_output);
        
        // Clean up
        unsafe {
            env::remove_var("DBASE_VERBOSE");
            env::remove_var("DBASE_SET_ENV_VARS");
            env::remove_var("DBASE_JSON_OUTPUT");
        }
    }

    #[test]
    fn test_process_file_interface() {
        let test_files = [
            "/Users/wrath/projects/arrow-sus/rust/downloads/parallel/CHBR1901.dbc",
        ];

        for file_path in &test_files {
            if std::path::Path::new(file_path).exists() {
                println!("\n🧪 Testing interface with actual dBase file: {}", file_path);
                
                let config = DbaseInterfaceConfig {
                    verbose: true,
                    set_env_vars: false,
                    json_output: false,
                };
                
                let interface = DbaseInterface::new(config);
                
                match utils::process_and_export_file(file_path, &interface) {
                    Ok(()) => {
                        println!("✅ Interface processing successful!");
                    }
                    Err(e) => {
                        println!("❌ Interface processing failed: {}", e);
                    }
                }
                
                break; // Only test with first available file
            }
        }
    }
}
