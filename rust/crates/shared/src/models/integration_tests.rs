use crate::models::ftp_manager::FtpConnectionManager;
use crate::models::strategy::ConnectionError;
use std::time::Duration;
use suppaftp::AsyncFtpStream;

/// Integration tests that connect to the real DATASUS FTP server
/// These tests require internet connectivity and the DATASUS server to be operational

const DATASUS_FTP_HOST: &str = "ftp.datasus.gov.br";
const DATASUS_BASE_PATH: &str = "/dissemin/publicos";

#[cfg(test)]
mod integration_tests {
    use super::*;

    #[tokio::test]
    #[ignore] // Use `cargo test -- --ignored` to run integration tests
    async fn test_datasus_ftp_connection() {
        let manager = FtpConnectionManager::new(DATASUS_FTP_HOST.to_string());
        
        let is_connected = manager.test_connection().await;
        assert!(is_connected, "Should be able to connect to DATASUS FTP server");
    }

    #[tokio::test]
    #[ignore] // Use `cargo test -- --ignored` to run integration tests
    async fn test_datasus_ftp_login_and_pwd() {
        let manager = FtpConnectionManager::new(DATASUS_FTP_HOST.to_string());
        
        let result = manager.with_connection(|ftp| {
            Box::pin(async move {
                match ftp.pwd().await {
                    Ok(current_dir) => {
                        println!("Current directory: {}", current_dir);
                        Ok(current_dir)
                    }
                    Err(e) => Err(ConnectionError::FtpConnection {
                        message: format!("PWD failed: {}", e),
                    }),
                }
            })
        }).await;

        assert!(result.is_ok(), "Should be able to get current directory");
    }

    #[tokio::test]
    #[ignore] // Use `cargo test -- --ignored` to run integration tests
    async fn test_datasus_navigate_to_base_path() {
        let manager = FtpConnectionManager::new(DATASUS_FTP_HOST.to_string());
        
        let result = manager.with_connection(|ftp| {
            Box::pin(async move {
                match ftp.cwd(DATASUS_BASE_PATH).await {
                    Ok(_) => {
                        println!("Successfully navigated to: {}", DATASUS_BASE_PATH);
                        // Get current directory to confirm
                        match ftp.pwd().await {
                            Ok(current_dir) => {
                                println!("Current directory after CWD: {}", current_dir);
                                Ok(current_dir)
                            }
                            Err(e) => Err(ConnectionError::FtpConnection {
                                message: format!("PWD after CWD failed: {}", e),
                            }),
                        }
                    }
                    Err(e) => Err(ConnectionError::FtpConnection {
                        message: format!("CWD to {} failed: {}", DATASUS_BASE_PATH, e),
                    }),
                }
            })
        }).await;

        assert!(result.is_ok(), "Should be able to navigate to base path");
    }

    #[tokio::test]
    #[ignore] // Use `cargo test -- --ignored` to run integration tests
    async fn test_datasus_list_root_directories() {
        let manager = FtpConnectionManager::new(DATASUS_FTP_HOST.to_string());
        
        let result = manager.with_connection(|ftp| {
            Box::pin(async move {
                // Navigate to base path
                if let Err(e) = ftp.cwd(DATASUS_BASE_PATH).await {
                    return Err(ConnectionError::FtpConnection {
                        message: format!("CWD failed: {}", e),
                    });
                }

                // List directory contents using NLST (simpler, faster)
                match ftp.nlst(None).await {
                    Ok(listing) => {
                        println!("Directory listing for {}:", DATASUS_BASE_PATH);
                        for name in listing.iter().take(10) { // Show first 10 entries
                            println!("  {}", name);
                        }
                        Ok(listing)
                    }
                    Err(e) => Err(ConnectionError::FtpConnection {
                        message: format!("NLST failed: {}", e),
                    }),
                }
            })
        }).await;

        assert!(result.is_ok(), "Should be able to list directory contents");
        let listing = result.unwrap();
        assert!(!listing.is_empty(), "Directory listing should not be empty");
        
        // Check if we can find common DATASUS directories
        let listing_str = listing.join("\n");
        let has_siasus = listing_str.contains("SIASUS");
        let has_sih = listing_str.contains("SIH");
        let has_sim = listing_str.contains("SIM");
        
        println!("Found SIASUS: {}", has_siasus);
        println!("Found SIH: {}", has_sih);
        println!("Found SIM: {}", has_sim);
        
        assert!(
            has_siasus || has_sih || has_sim,
            "Should find at least one of the common DATASUS directories"
        );
    }

    #[tokio::test]
    #[ignore] // Use `cargo test -- --ignored` to run integration tests
    async fn test_datasus_list_siasus_directory() {
        let manager = FtpConnectionManager::new(DATASUS_FTP_HOST.to_string());
        
        let result = manager.with_connection(|ftp| {
            Box::pin(async move {
                // Navigate to SIASUS directory
                let siasus_path = format!("{}/SIASUS", DATASUS_BASE_PATH);
                if let Err(e) = ftp.cwd(&siasus_path).await {
                    return Err(ConnectionError::FtpConnection {
                        message: format!("CWD to {} failed: {}", siasus_path, e),
                    });
                }

                // List directory contents
                match ftp.list(None).await {
                    Ok(listing) => {
                        println!("Directory listing for {}:", siasus_path);
                        for line in listing.iter().take(10) { // Show first 10 entries
                            println!("  {}", line);
                        }
                        Ok(listing)
                    }
                    Err(e) => Err(ConnectionError::FtpConnection {
                        message: format!("LIST failed: {}", e),
                    }),
                }
            })
        }).await;

        assert!(result.is_ok(), "Should be able to list SIASUS directory contents");
        let listing = result.unwrap();
        assert!(!listing.is_empty(), "SIASUS directory listing should not be empty");
    }

    #[tokio::test]
    #[ignore] // Use `cargo test -- --ignored` to run integration tests
    async fn test_datasus_connection_timeout() {
        let manager = FtpConnectionManager::with_timeout(
            DATASUS_FTP_HOST.to_string(),
            Duration::from_secs(5),
        );
        
        let result = manager.test_connection().await;
        // Even with a short timeout, the connection should succeed for a fast server
        assert!(result, "Should be able to connect even with short timeout");
    }

    #[tokio::test]
    #[ignore] // Use `cargo test -- --ignored` to run integration tests
    async fn test_datasus_invalid_directory() {
        let manager = FtpConnectionManager::new(DATASUS_FTP_HOST.to_string());
        
        let result = manager.with_connection(|ftp| {
            Box::pin(async move {
                // First navigate to base path to establish a known state
                if let Err(e) = ftp.cwd(DATASUS_BASE_PATH).await {
                    return Err(ConnectionError::FtpConnection {
                        message: format!("Failed to navigate to base path: {}", e),
                    });
                }

                // Try to navigate to a non-existent directory from the base path
                let invalid_path = "this_directory_definitely_does_not_exist_12345";
                match ftp.cwd(invalid_path).await {
                    Ok(_) => {
                        // If it succeeded, let's see what directory we're actually in
                        match ftp.pwd().await {
                            Ok(current_dir) => {
                                println!("Unexpectedly navigated to: {}", current_dir);
                                Err(ConnectionError::FtpConnection {
                                    message: format!("Should not be able to navigate to invalid directory, but ended up in: {}", current_dir),
                                })
                            }
                            Err(e) => Err(ConnectionError::FtpConnection {
                                message: format!("CWD succeeded but PWD failed: {}", e),
                            })
                        }
                    },
                    Err(e) => {
                        println!("Expected error when navigating to invalid directory: {}", e);
                        Ok(()) // Expected error
                    }
                }
            })
        }).await;

        assert!(result.is_ok(), "Should properly handle invalid directory navigation");
    }

    /// Helper function to manually test FTP connection
    /// Run with: cargo test test_manual_ftp_connection -- --ignored --nocapture
    #[tokio::test]
    #[ignore]
    async fn test_manual_ftp_connection() {
        println!("Testing manual FTP connection to DATASUS...");
        
        let manager = FtpConnectionManager::new(DATASUS_FTP_HOST.to_string());
        
        match manager.connect().await {
            Ok(mut ftp) => {
                println!("✓ Connected successfully");
                
                match ftp.pwd().await {
                    Ok(pwd) => println!("✓ Current directory: {}", pwd),
                    Err(e) => println!("✗ PWD failed: {}", e),
                }
                
                match ftp.cwd(DATASUS_BASE_PATH).await {
                    Ok(_) => {
                        println!("✓ Changed to base path: {}", DATASUS_BASE_PATH);
                        
                        match ftp.nlst(None).await {
                        Ok(listing) => {
                        println!("✓ Directory listing ({} entries):", listing.len());
                        for (i, name) in listing.iter().enumerate().take(5) {
                        println!("  {}: {}", i + 1, name);
                        }
                        if listing.len() > 5 {
                        println!("  ... and {} more entries", listing.len() - 5);
                        }
                        }
                        Err(e) => println!("✗ NLST failed: {}", e),
                        }
                    }
                    Err(e) => println!("✗ CWD failed: {}", e),
                }
                
                let _ = ftp.quit().await;
                println!("✓ Connection closed");
            }
            Err(e) => {
                println!("✗ Connection failed: {:?}", e);
            }
        }
    }
}
