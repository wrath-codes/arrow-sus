use crate::models::strategy::ConnectionStrategy;

/// Integration tests for the ConnectionStrategy
#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    #[ignore] // Integration test - requires network
    async fn test_connection_strategy_create_datasus_ftp() {
        let result = ConnectionStrategy::create("DATASUS_FTP").await;
        assert!(result.is_ok(), "Should be able to create DATASUS_FTP strategy");
        
        let strategy = result.unwrap();
        assert!(strategy.verify_connection().await, "Should be able to verify FTP connection");
    }

    #[tokio::test]
    #[ignore] // Integration test - requires network
    async fn test_connection_strategy_with_fallback() {
        let result = ConnectionStrategy::create_with_fallback(None).await;
        assert!(result.is_ok(), "Should be able to create strategy with fallback");
        
        let (strategy, strategy_name) = result.unwrap();
        println!("Selected strategy: {}", strategy_name);
        assert_eq!(strategy_name, "DATASUS_FTP");
        assert!(strategy.verify_connection().await, "Should be able to verify connection");
    }

    #[tokio::test]
    #[ignore] // Integration test - requires network
    async fn test_list_datasus_directories() {
        let strategy_result = ConnectionStrategy::create("DATASUS_FTP").await;
        assert!(strategy_result.is_ok());
        
        let strategy = strategy_result.unwrap();
        
        let listing_result = strategy.list_directory("").await;
        assert!(listing_result.is_ok(), "Should be able to list root directory");
        
        let content = listing_result.unwrap();
        assert!(!content.is_empty(), "Root directory should not be empty");
        
        println!("Found {} items in root directory", content.len());
        
        // Should contain key DATASUS directories
        assert!(content.contains_key("SIASUS"), "Should contain SIASUS directory");
        assert!(content.contains_key("SIHSUS"), "Should contain SIHSUS directory");
        assert!(content.contains_key("SIM"), "Should contain SIM directory");
    }

    #[tokio::test]
    #[ignore] // Integration test - requires network
    async fn test_list_siasus_subdirectories() {
        let strategy_result = ConnectionStrategy::create("DATASUS_FTP").await;
        assert!(strategy_result.is_ok());
        
        let strategy = strategy_result.unwrap();
        
        let listing_result = strategy.list_directory("SIASUS").await;
        assert!(listing_result.is_ok(), "Should be able to list SIASUS directory");
        
        let content = listing_result.unwrap();
        assert!(!content.is_empty(), "SIASUS directory should not be empty");
        
        println!("Found {} items in SIASUS directory", content.len());
        for (name, item) in &content {
            match item {
                crate::models::strategy::DirectoryItem::Directory(_) => {
                    println!("  DIR:  {}", name);
                }
                crate::models::strategy::DirectoryItem::File(_) => {
                    println!("  FILE: {}", name);
                }
            }
        }
        
        // Should contain typical SIASUS subdirectories
        let has_historical = content.contains_key("199407_200712");
        let has_recent = content.contains_key("200801_");
        let has_old = content.contains_key("Anteriores_a_1994");
        
        println!("Historical data: {}", has_historical);
        println!("Recent data: {}", has_recent);
        println!("Old data: {}", has_old);
        
        assert!(
            has_historical || has_recent || has_old,
            "Should contain at least one of the expected SIASUS subdirectories"
        );
    }
}
