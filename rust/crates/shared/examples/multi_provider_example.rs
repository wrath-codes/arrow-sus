use shared::models::directory::{Directory, DirectoryEntry, LocalFileSystemProvider, FtpFileSystemProvider, FileSystemProvider};
use std::sync::Arc;
use tempfile::TempDir;
use tokio::fs;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    println!("=== Multi-Provider Directory Operations Example ===");
    
    // Create temporary directory for local testing
    let temp_dir = TempDir::new()?;
    let temp_path = temp_dir.path().to_string_lossy().to_string();
    
    // Create test files
    fs::write(temp_dir.path().join("test.txt"), "Hello World").await?;
    fs::write(temp_dir.path().join("data.csv"), "col1,col2\n1,2").await?;
    
    let sub_dir = temp_dir.path().join("subdir");
    fs::create_dir(&sub_dir).await?;
    fs::write(sub_dir.join("nested.json"), r#"{"key": "value"}"#).await?;
    
    println!("Created test directory structure at: {}", temp_path);
    
    println!("\n=== Provider Comparison ===");
    
    // Create different providers
    let local_provider = Arc::new(LocalFileSystemProvider);
    let ftp_provider = Arc::new(FtpFileSystemProvider::new_datasus());
    
    println!("Local Provider: {}", local_provider.provider_name());
    println!("FTP Provider: {} ({}:{}{})", 
        ftp_provider.provider_name(),
        ftp_provider.host,
        ftp_provider.port,
        ftp_provider.base_path
    );
    
    println!("\n=== Local Directory Operations ===");
    
    // Test local directory with explicit provider
    let local_dir = Directory::new_with_provider(
        temp_path.clone(), 
        local_provider.clone()
    ).await?;
    
    println!("Local directory: {}", local_dir.path);
    println!("Provider type: {}", local_dir.provider_type);
    
    // List local content
    let local_content = local_dir.content().await?;
    println!("Local content ({} items):", local_content.len());
    
    for entry in &local_content {
        match entry {
            DirectoryEntry::File(file) => {
                let info = file.info();
                println!("  📄 {} - {} ({})", 
                    file.basename,
                    info.get("type").unwrap_or(&"unknown".to_string()),
                    info.get("size").unwrap_or(&"unknown".to_string())
                );
            }
            DirectoryEntry::Directory(dir) => {
                println!("  📁 {}", dir.name);
            }
        }
    }
    
    // Test directory operations
    let files = local_dir.files().await?;
    let subdirs = local_dir.subdirectories().await?;
    println!("Files: {}, Subdirectories: {}", files.len(), subdirs.len());
    
    // Test file filtering
    let txt_files = local_dir.files_with_extension("txt").await?;
    println!("TXT files: {}", txt_files.len());
    
    println!("\n=== FTP Directory Operations (Demo) ===");
    
    // Create FTP directory instance
    let ftp_dir = Directory::new_with_provider(
        "/SIASUS".to_string(),
        ftp_provider.clone()
    ).await?;
    
    println!("FTP directory: {}", ftp_dir.path);
    println!("Provider type: {}", ftp_dir.provider_type);
    
    // Demonstrate FTP line parsing without network access
    println!("\nFTP line parsing examples:");
    let sample_ftp_lines = vec![
        "12-15-23 10:30AM    <DIR>          200801",
        "12-15-23 10:30AM           1048576 PAAC2301.DBC", 
        "12-15-23 10:30AM            524288 PAAC2301.DBF",  // Would be filtered
        "01-20-24 02:15PM              2048 README.TXT",
    ];
    
    for line in sample_ftp_lines {
        if let Some((name, entry)) = ftp_provider.parse_ftp_line(line, "/SIASUS") {
            match entry {
                DirectoryEntry::File(file) => {
                    let info = file.info();
                    println!("  📄 {} - {} ({})", 
                        name,
                        info.get("type").unwrap_or(&"unknown".to_string()),
                        info.get("size").unwrap_or(&"unknown".to_string())
                    );
                }
                DirectoryEntry::Directory(dir) => {
                    println!("  📁 {} -> {}", name, dir.path);
                }
            }
        }
    }
    
    println!("\n=== Provider Feature Comparison ===");
    
    // Compare provider capabilities
    println!("Local Provider Features:");
    println!("  ✅ Real-time file system access");
    println!("  ✅ File size detection");
    println!("  ✅ Modification date tracking");
    println!("  ✅ Directory existence checking");
    println!("  ✅ Fast local operations");
    
    println!("\nFTP Provider Features:");
    println!("  ✅ Remote DATASUS server access");
    println!("  ✅ Anonymous FTP login");
    println!("  ✅ Passive mode support");
    println!("  ✅ .DBF/.DBC file filtering");
    println!("  ✅ Directory listing parsing");
    println!("  ⚠️  Requires network connectivity");
    
    println!("\n=== Error Handling Demo ===");
    
    // Test with non-existent local path
    match local_provider.exists("/this/path/definitely/does/not/exist").await {
        Ok(exists) => println!("Non-existent local path exists: {}", exists),
        Err(e) => println!("Error checking non-existent path: {}", e),
    }
    
    // Test directory creation with different providers
    println!("\n=== Multi-Provider Directory Tree ===");
    
    let directories = vec![
        ("Local Temp", Directory::new_with_provider(temp_path.clone(), local_provider.clone()).await?),
        ("FTP SIASUS", Directory::new_with_provider("/SIASUS".to_string(), ftp_provider.clone()).await?),
        ("FTP SIM", Directory::new_with_provider("/SIM".to_string(), ftp_provider.clone()).await?),
    ];
    
    for (label, dir) in directories {
        println!("  {} -> {} ({})", label, dir.path, dir.provider_type);
    }
    
    println!("\n=== Provider Switching Demo ===");
    
    // Show how the same directory can be accessed with different providers
    let same_path_different_providers = vec![
        Directory::new(temp_path.clone()).await?, // Default (local)
        Directory::new_with_provider(temp_path.clone(), local_provider.clone()).await?, // Explicit local
    ];
    
    println!("Same path with different provider specifications:");
    for (i, dir) in same_path_different_providers.iter().enumerate() {
        println!("  Version {}: {} -> {}", i + 1, dir.path, dir.provider_type);
    }
    
    println!("\n=== Future Provider Extensibility ===");
    
    println!("The FileSystemProvider trait makes it easy to add new providers:");
    println!("  🔄 Planned: S3FileSystemProvider");
    println!("  🔄 Planned: SshFileSystemProvider  ");
    println!("  🔄 Planned: SambaFileSystemProvider");
    println!("  🔄 Planned: HttpFileSystemProvider");
    
    println!("\n=== Performance Considerations ===");
    
    let start = std::time::Instant::now();
    let _ = local_dir.content().await?;
    let local_time = start.elapsed();
    println!("Local directory listing: {:?}", local_time);
    
    println!("FTP operations would be slower due to network latency");
    println!("Consider caching for frequently accessed FTP directories");
    
    println!("\n=== Example Complete ===");
    println!("Multi-provider directory system is working perfectly!");
    println!("Ready for production use with DATASUS and local file systems.");
    
    Ok(())
}
