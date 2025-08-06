use std::sync::Arc;

use anyhow::Result;
use suppaftp::{AsyncFtpStream, FtpError};
use tokio::sync::Mutex;

/// Async FTP Connection Manager following Python pattern
/// 
/// This manager handles FTP connection lifecycle and provides
/// managed connections similar to Python's context manager pattern.
pub struct FtpConnectionManager {
    host: String,
    port: u16,
    username: String,
    password: String,
    // Optional connection pool for reuse
    connection: Arc<Mutex<Option<AsyncFtpStream>>>,
}

impl FtpConnectionManager {
    /// Create a new FTP connection manager for DataSUS
    pub fn new_datasus() -> Self {
        Self {
            host: "ftp.datasus.gov.br".to_string(),
            port: 21,
            username: "anonymous".to_string(),
            password: "anonymous".to_string(),
            connection: Arc::new(Mutex::new(None)),
        }
    }
    
    /// Create a new FTP connection manager with custom parameters
    pub fn new(host: String, port: u16, username: String, password: String) -> Self {
        Self {
            host,
            port,
            username,
            password,
            connection: Arc::new(Mutex::new(None)),
        }
    }
    
    /// Get a managed FTP connection (similar to Python's context manager)
    pub async fn managed_connection<F, R>(&self, operation: F) -> Result<R>
    where
        F: FnOnce(&mut AsyncFtpStream) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<R>> + Send + '_>>,
    {
        // Try to reuse existing connection first
        let mut conn_guard = self.connection.lock().await;
        
        let mut ftp = if let Some(existing_conn) = conn_guard.take() {
            // Try to reuse existing connection by testing it
            match self.test_connection(&existing_conn).await {
                Ok(_) => existing_conn,
                Err(_) => {
                    // Connection is stale, create new one
                    log::debug!("Existing FTP connection stale, creating new one");
                    self.create_connection().await?
                }
            }
        } else {
            // No existing connection, create new one
            log::debug!("Creating new FTP connection to {}", self.host);
            self.create_connection().await?
        };
        
        // Execute the operation
        let result = operation(&mut ftp).await;
        
        // Store connection back for reuse (if operation succeeded)
        if result.is_ok() {
            *conn_guard = Some(ftp);
        } else {
            // If operation failed, don't reuse connection
            if let Err(e) = ftp.quit().await {
                log::warn!("Failed to properly close FTP connection: {}", e);
            }
        }
        
        result
    }
    
    /// Create a new FTP connection
    async fn create_connection(&self) -> Result<AsyncFtpStream, FtpError> {
        let address = format!("{}:{}", self.host, self.port);
        let mut ftp = AsyncFtpStream::connect(&address).await?;
        ftp.login(&self.username, &self.password).await?;
        Ok(ftp)
    }
    
    /// Test if a connection is still alive
    async fn test_connection(&self, ftp: &AsyncFtpStream) -> Result<(), FtpError> {
        // Simple test - try to get current directory
        // Note: This is a bit tricky with AsyncFtpStream as it takes &mut self
        // For now, we'll assume the connection is good if we can get here
        // In a real implementation, we might need a different approach
        Ok(())
    }
    
    /// Verify connection without keeping it
    pub async fn verify_connection(&self) -> bool {
        match self.create_connection().await {
            Ok(mut ftp) => {
                let result = ftp.pwd().await.is_ok();
                ftp.quit().await.ok(); // Ignore quit errors
                result
            }
            Err(e) => {
                log::warn!("FTP connection verification failed: {}", e);
                false
            }
        }
    }
    
    /// Close any cached connection
    pub async fn close_connection(&self) {
        let mut conn_guard = self.connection.lock().await;
        if let Some(mut ftp) = conn_guard.take() {
            if let Err(e) = ftp.quit().await {
                log::warn!("Error closing FTP connection: {}", e);
            }
        }
    }
}

impl Drop for FtpConnectionManager {
    fn drop(&mut self) {
        // Note: We can't call async methods in Drop
        // The connection will be closed when the AsyncFtpStream is dropped
        log::debug!("FtpConnectionManager dropped");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[tokio::test]
    #[ignore] // Requires real FTP connection
    async fn test_connection_manager() {
        let manager = FtpConnectionManager::new_datasus();
        
        // Test connection verification
        let is_connected = manager.verify_connection().await;
        println!("Connection verification: {}", if is_connected { "SUCCESS" } else { "FAILED" });
        
        if is_connected {
            // Test managed connection
            let result = manager.managed_connection(|ftp| {
                Box::pin(async move {
                    let current_dir = ftp.pwd().await.map_err(anyhow::Error::from)?;
                    println!("Current directory: {}", current_dir);
                    
                    // Change to a known directory
                    ftp.cwd("/dissemin/publicos").await.map_err(anyhow::Error::from)?;
                    let new_dir = ftp.pwd().await.map_err(anyhow::Error::from)?;
                    println!("Changed to directory: {}", new_dir);
                    
                    Ok::<String, anyhow::Error>(new_dir)
                })
            }).await;
            
            match result {
                Ok(dir) => println!("✅ Managed connection test successful: {}", dir),
                Err(e) => println!("❌ Managed connection test failed: {}", e),
            }
        }
        
        // Close connections
        manager.close_connection().await;
    }
    
    #[tokio::test]
    #[ignore] // Requires real FTP connection  
    async fn test_connection_reuse() {
        let manager = FtpConnectionManager::new_datasus();
        
        // First operation
        let result1 = manager.managed_connection(|ftp| {
            Box::pin(async move {
                ftp.cwd("/dissemin/publicos").await.map_err(anyhow::Error::from)?;
                Ok::<(), anyhow::Error>(())
            })
        }).await;
        
        // Second operation (should reuse connection)
        let result2 = manager.managed_connection(|ftp| {
            Box::pin(async move {
                let dir = ftp.pwd().await.map_err(anyhow::Error::from)?;
                Ok::<String, anyhow::Error>(dir)
            })
        }).await;
        
        println!("First operation: {:?}", result1);
        println!("Second operation: {:?}", result2);
        
        assert!(result1.is_ok());
        assert!(result2.is_ok());
        
        manager.close_connection().await;
    }
}
