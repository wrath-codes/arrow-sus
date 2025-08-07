use shared::models::directory::{Directory, DirectoryEntry, FtpFileSystemProvider, FileSystemProvider};
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    println!("=== DATASUS FTP Directory Operations Example ===");
    
    // Create DATASUS FTP provider
    let ftp_provider = Arc::new(FtpFileSystemProvider::new_datasus());
    println!("FTP Provider: {}", ftp_provider.provider_name());
    println!("Host: {}", ftp_provider.host);
    println!("Base path: {}", ftp_provider.base_path);
    println!("Port: {}", ftp_provider.port);
    
    println!("\n=== Testing FTP Connection (Optional - requires network) ===");
    
    // Note: The following operations require network connectivity to DATASUS FTP server
    // Uncomment and run only if you have internet access and want to test real FTP operations
    
    /*
    // Test connection by checking if root directory exists
    match ftp_provider.exists("/").await {
        Ok(exists) => {
            println!("Root directory exists: {}", exists);
            
            if exists {
                println!("\n=== Listing Root Directory ===");
                
                // List root directory content
                match ftp_provider.list_directory("/").await {
                    Ok(content) => {
                        println!("Total items in root: {}", content.len());
                        
                        for (name, entry) in content.iter().take(10) { // Show first 10 items
                            match entry {
                                DirectoryEntry::File(file) => {
                                    let info = file.info();
                                    println!("📄 File: {} ({})", 
                                        file.basename, 
                                        info.get("size").unwrap_or(&"unknown".to_string())
                                    );
                                }
                                DirectoryEntry::Directory(dir) => {
                                    println!("📁 Directory: {}", dir.name);
                                }
                            }
                        }
                        
                        if content.len() > 10 {
                            println!("... and {} more items", content.len() - 10);
                        }
                    }
                    Err(e) => {
                        println!("Error listing directory: {}", e);
                    }
                }
                
                println!("\n=== Testing Specific Directories ===");
                
                // Test some known DATASUS directories
                let test_paths = vec!["/SIASUS", "/SIM", "/CIHA"];
                
                for path in test_paths {
                    match ftp_provider.exists(path).await {
                        Ok(exists) => {
                            println!("{}: {}", path, if exists { "EXISTS" } else { "NOT FOUND" });
                        }
                        Err(e) => {
                            println!("{}: ERROR - {}", path, e);
                        }
                    }
                }
            }
        }
        Err(e) => {
            println!("Error connecting to FTP server: {}", e);
            println!("This is expected if you don't have internet connectivity.");
        }
    }
    */
    
    println!("\n=== FTP Provider Testing (without network) ===");
    
    // Test FTP line parsing (doesn't require network)
    println!("Testing FTP line parsing...");
    
    let test_lines = vec![
        "12-01-23 02:30PM    <DIR>          SIASUS",
        "12-01-23 02:30PM              1024 test.txt",
        "01-15-24 10:45AM           2048576 data.dbc",
        "01-15-24 10:45AM           1024000 data.dbf",  // This should be filtered if .dbc exists
    ];
    
    for line in test_lines {
        if let Some((name, entry)) = ftp_provider.parse_ftp_line(line, "/test") {
            match entry {
                DirectoryEntry::File(file) => {
                    let info = file.info();
                    println!("Parsed file: {} - {} ({})", 
                        name,
                        info.get("type").unwrap_or(&"unknown".to_string()),
                        info.get("size").unwrap_or(&"unknown".to_string())
                    );
                }
                DirectoryEntry::Directory(dir) => {
                    println!("Parsed directory: {} -> {}", name, dir.path);
                }
            }
        } else {
            println!("Failed to parse line: {}", line);
        }
    }
    
    println!("\n=== Directory Creation with FTP Provider ===");
    
    // Create a directory instance with FTP provider
    let ftp_directory = Directory::new_with_provider(
        "/SIASUS".to_string(),
        ftp_provider.clone()
    ).await?;
    
    println!("Created FTP directory: {}", ftp_directory.path);
    println!("Provider type: {}", ftp_directory.provider_type);
    println!("Directory name: {}", ftp_directory.name);
    
    println!("\n=== Comparison with Local Provider ===");
    
    // Compare with local provider
    let local_dir = Directory::new("/tmp".to_string()).await?;
    println!("Local directory provider: {}", local_dir.provider_type);
    println!("FTP directory provider: {}", ftp_directory.provider_type);
    
    println!("\n=== Custom FTP Provider ===");
    
    // Create custom FTP provider
    let custom_ftp = FtpFileSystemProvider::new(
        "example.com".to_string(),
        "/custom/path".to_string(),
        Some(2121)
    );
    
    println!("Custom FTP host: {}", custom_ftp.host);
    println!("Custom FTP base path: {}", custom_ftp.base_path);
    println!("Custom FTP port: {}", custom_ftp.port);
    
    println!("\n=== Example Complete ===");
    println!("FTP provider is ready for use with DATASUS server!");
    println!("Uncomment the network testing section to test real FTP operations.");
    
    Ok(())
}
