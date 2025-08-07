use crate::models::strategy::ConnectionError;
use suppaftp::{AsyncFtpStream, Mode};
use std::time::Duration;

/// FTP connection manager that handles connections using suppaftp
#[derive(Debug, Clone)]
pub struct FtpConnectionManager {
    host: String,
    port: u16,
    timeout: Duration,
}

impl FtpConnectionManager {
    /// Creates a new FTP connection manager
    pub fn new(host: String) -> Self {
        Self {
            host,
            port: 21,
            timeout: Duration::from_secs(30),
        }
    }

    /// Creates a new FTP connection manager with custom port
    pub fn with_port(host: String, port: u16) -> Self {
        Self {
            host,
            port,
            timeout: Duration::from_secs(30),
        }
    }

    /// Creates a new FTP connection manager with custom timeout
    pub fn with_timeout(host: String, timeout: Duration) -> Self {
        Self {
            host,
            port: 21,
            timeout,
        }
    }

    /// Establishes a connection to the FTP server
    pub async fn connect(&self) -> Result<AsyncFtpStream, ConnectionError> {
        let address = format!("{}:{}", self.host, self.port);
        
        match AsyncFtpStream::connect(&address).await {
            Ok(mut ftp_stream) => {
                // Login anonymously (DATASUS allows anonymous access)
                match ftp_stream.login("anonymous", "").await {
                    Ok(_) => {
                        // TODO: Set passive mode when suppaftp supports it properly
                        Ok(ftp_stream)
                    },
                    Err(e) => Err(ConnectionError::FtpConnection {
                        message: format!("Login failed: {}", e),
                    }),
                }
            }
            Err(e) => Err(ConnectionError::FtpConnection {
                message: format!("Connection failed to {}: {}", address, e),
            }),
        }
    }

    /// Establishes a secure TLS connection to the FTP server
    /// Note: Secure connections are more complex and will be implemented later
    pub async fn connect_secure(&self) -> Result<AsyncFtpStream, ConnectionError> {
        // For now, just return a regular connection
        // TODO: Implement proper TLS support
        self.connect().await
    }

    /// Tests if the FTP server is reachable
    pub async fn test_connection(&self) -> bool {
        match self.connect().await {
            Ok(mut ftp_stream) => {
                // Try to list current directory to verify connection
                match ftp_stream.pwd().await {
                    Ok(_) => {
                        // Cleanly quit the connection
                        let _ = ftp_stream.quit().await;
                        true
                    }
                    Err(_) => {
                        let _ = ftp_stream.quit().await;
                        false
                    }
                }
            }
            Err(_) => false,
        }
    }

    /// Manages an FTP connection with automatic cleanup
    pub async fn with_connection<F, T>(&self, operation: F) -> Result<T, ConnectionError>
    where
        F: FnOnce(&mut AsyncFtpStream) -> futures::future::BoxFuture<'_, Result<T, ConnectionError>>,
    {
        let mut ftp_stream = self.connect().await?;
        
        let result = operation(&mut ftp_stream).await;
        
        // Always try to close the connection gracefully
        let _ = ftp_stream.quit().await;
        
        result
    }

    /// Manages a secure FTP connection with automatic cleanup
    pub async fn with_secure_connection<F, T>(&self, operation: F) -> Result<T, ConnectionError>
    where
        F: FnOnce(&mut AsyncFtpStream) -> futures::future::BoxFuture<'_, Result<T, ConnectionError>>,
    {
        let mut ftp_stream = self.connect_secure().await?;
        
        let result = operation(&mut ftp_stream).await;
        
        // Always try to close the connection gracefully
        let _ = ftp_stream.quit().await;
        
        result
    }

    /// Gets the configured host
    pub fn host(&self) -> &str {
        &self.host
    }

    /// Gets the configured port
    pub fn port(&self) -> u16 {
        self.port
    }

    /// Gets the configured timeout
    pub fn timeout(&self) -> Duration {
        self.timeout
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ftp_manager_creation() {
        let manager = FtpConnectionManager::new("ftp.example.com".to_string());
        assert_eq!(manager.host(), "ftp.example.com");
        assert_eq!(manager.port(), 21);
        assert_eq!(manager.timeout(), Duration::from_secs(30));
    }

    #[test]
    fn test_ftp_manager_with_port() {
        let manager = FtpConnectionManager::with_port("ftp.example.com".to_string(), 2121);
        assert_eq!(manager.host(), "ftp.example.com");
        assert_eq!(manager.port(), 2121);
    }

    #[test]
    fn test_ftp_manager_with_timeout() {
        let manager = FtpConnectionManager::with_timeout(
            "ftp.example.com".to_string(),
            Duration::from_secs(60),
        );
        assert_eq!(manager.host(), "ftp.example.com");
        assert_eq!(manager.timeout(), Duration::from_secs(60));
    }
}
