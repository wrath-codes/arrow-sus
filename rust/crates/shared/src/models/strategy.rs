use crate::models::{File, FileInfo};
use async_trait::async_trait;
use std::collections::HashMap;
use thiserror::Error;

/// Errors that can occur during connection operations
#[derive(Error, Debug)]
pub enum ConnectionError {
    #[error("FTP connection error: {message}")]
    FtpConnection { message: String },
    #[error("Library not found: {lib_name} - {message}")]
    LibraryNotFound { lib_name: String, message: String },
    #[error("Connection failed: {message}")]
    Connection { message: String },
}

/// Trait defining the interface for data source access strategies
#[async_trait]
pub trait SourceStrategy: Send + Sync {
    /// Verifies if the connection to the data source is available
    async fn verify_connection(&self) -> bool;

    /// Lists the contents of a directory
    async fn list_directory(
        &self,
        path: &str,
    ) -> Result<HashMap<String, DirectoryItem>, ConnectionError>;
}

/// Enum representing different types of items in a directory
#[derive(Debug, Clone)]
pub enum DirectoryItem {
    File(File),
    Directory(Directory),
}

/// Forward declaration for Directory - will be defined in directory.rs
#[derive(Debug, Clone)]
pub struct Directory {
    pub name: String,
    pub path: String,
    pub loaded: bool,
}

impl Directory {
    pub fn new(path: String, name: String) -> Self {
        Self {
            name,
            path,
            loaded: false,
        }
    }
}

/// Available connection strategies
#[derive(Debug, Clone)]
pub enum ConnectionStrategy {
    DatasusFtp,
    DatasusS3,
}

impl ConnectionStrategy {
    /// Creates a strategy instance by name
    pub async fn create(strategy_name: &str) -> Result<Box<dyn SourceStrategy>, ConnectionError> {
        match strategy_name.to_uppercase().as_str() {
            "DATASUS_FTP" => {
                let ftp_strategy = crate::models::ftp_strategy::FtpStrategy::new()?;
                Ok(Box::new(ftp_strategy))
            }
            "DATASUS_S3" => {
                Err(ConnectionError::LibraryNotFound {
                    lib_name: "S3Strategy".to_string(),
                    message: "S3 strategy not implemented yet".to_string(),
                })
            }
            _ => Err(ConnectionError::LibraryNotFound {
                lib_name: strategy_name.to_string(),
                message: "Strategy not implemented".to_string(),
            }),
        }
    }

    /// Creates the first available strategy from a list, with fallback
    pub async fn create_with_fallback(
        strategies: Option<Vec<&str>>,
    ) -> Result<(Box<dyn SourceStrategy>, String), ConnectionError> {
        let strategies = strategies.unwrap_or(vec!["DATASUS_FTP", "DATASUS_S3"]);

        for strategy_name in strategies {
            if let Ok(strategy) = Self::create(strategy_name).await {
                if strategy.verify_connection().await {
                    return Ok((strategy, strategy_name.to_string()));
                }
            }
        }

        Err(ConnectionError::LibraryNotFound {
            lib_name: "all_strategies".to_string(),
            message: "No connection strategy available".to_string(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_connection_strategy_creation() {
        // Test invalid strategy
        let result = ConnectionStrategy::create("INVALID").await;
        assert!(result.is_err());
    }

    #[test]
    fn test_directory_creation() {
        let dir = Directory::new("/test/path".to_string(), "test".to_string());
        assert_eq!(dir.name, "test");
        assert_eq!(dir.path, "/test/path");
        assert!(!dir.loaded);
    }
}
