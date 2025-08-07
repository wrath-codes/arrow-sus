use shared::models::directory::{Directory, DirectoryEntry, FtpFileSystemProvider, FileSystemProvider};
use std::sync::Arc;
use std::time::Instant;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    println!("=== DATASUS FTP Live Server Test ===");
    
    // Create DATASUS FTP provider
    let ftp_provider = Arc::new(FtpFileSystemProvider::new_datasus());
    println!("Testing connection to: {}:{}", ftp_provider.host, ftp_provider.port);
    println!("Base path: {}", ftp_provider.base_path);
    
    println!("\n=== Testing FTP Connection ===");
    
    // Test basic connectivity
    let start = Instant::now();
    match ftp_provider.exists("/").await {
        Ok(exists) => {
            let duration = start.elapsed();
            println!("✅ Connection successful in {:?}", duration);
            println!("Root directory exists: {}", exists);
            
            if exists {
                println!("\n=== Listing Root Directory ===");
                
                let start = Instant::now();
                match ftp_provider.list_directory("/").await {
                    Ok(content) => {
                        let duration = start.elapsed();
                        println!("✅ Root directory listed in {:?}", duration);
                        println!("Total items in root: {}", content.len());
                        
                        // Show first 15 items
                        let items: Vec<_> = content.iter().take(15).collect();
                        for (name, entry) in items {
                            match entry {
                                DirectoryEntry::File(file) => {
                                    let info = file.info();
                                    println!("📄 File: {} ({})", 
                                        name,
                                        info.get("size").unwrap_or(&"unknown".to_string())
                                    );
                                }
                                DirectoryEntry::Directory(dir) => {
                                    println!("📁 Directory: {}", name);
                                }
                            }
                        }
                        
                        if content.len() > 15 {
                            println!("... and {} more items", content.len() - 15);
                        }
                        
                        // Test specific known directories
                        println!("\n=== Testing Known DATASUS Directories ===");
                        let known_dirs = vec!["SIASUS", "SIM", "CIHA", "CNES", "SIA"];
                        
                        for dir_name in known_dirs {
                            if content.contains_key(dir_name) {
                                println!("✅ Found expected directory: {}", dir_name);
                                
                                // Test listing subdirectory
                                let subdir_path = format!("/{}", dir_name);
                                let start = Instant::now();
                                match ftp_provider.list_directory(&subdir_path).await {
                                    Ok(subdir_content) => {
                                        let duration = start.elapsed();
                                        println!("   📁 {} contains {} items (listed in {:?})", 
                                            dir_name, subdir_content.len(), duration);
                                        
                                        // Show a few items from subdirectory
                                        let sub_items: Vec<_> = subdir_content.iter().take(5).collect();
                                        for (sub_name, sub_entry) in sub_items {
                                            match sub_entry {
                                                DirectoryEntry::File(file) => {
                                                    let info = file.info();
                                                    println!("      📄 {} ({})", 
                                                        sub_name,
                                                        info.get("size").unwrap_or(&"unknown".to_string())
                                                    );
                                                }
                                                DirectoryEntry::Directory(_) => {
                                                    println!("      📁 {}", sub_name);
                                                }
                                            }
                                        }
                                        if subdir_content.len() > 5 {
                                            println!("      ... and {} more items", subdir_content.len() - 5);
                                        }
                                    }
                                    Err(e) => {
                                        println!("   ❌ Error listing {}: {}", dir_name, e);
                                    }
                                }
                            } else {
                                println!("⚠️  Directory not found: {}", dir_name);
                            }
                        }
                        
                        println!("\n=== Testing Directory Operations ===");
                        
                        // Create Directory instance with FTP provider
                        let ftp_dir = Directory::new_with_provider(
                            "/SIASUS".to_string(),
                            ftp_provider.clone()
                        ).await?;
                        
                        println!("Created FTP Directory: {}", ftp_dir.path);
                        println!("Provider type: {}", ftp_dir.provider_type);
                        
                        // Test directory content method
                        let start = Instant::now();
                        match ftp_dir.content().await {
                            Ok(dir_content) => {
                                let duration = start.elapsed();
                                println!("✅ Directory.content() worked in {:?}", duration);
                                println!("SIASUS directory contains {} items", dir_content.len());
                                
                                // Test file filtering
                                match ftp_dir.files().await {
                                    Ok(files) => {
                                        println!("Files in SIASUS: {}", files.len());
                                    }
                                    Err(e) => {
                                        println!("Error getting files: {}", e);
                                    }
                                }
                                
                                match ftp_dir.subdirectories().await {
                                    Ok(subdirs) => {
                                        println!("Subdirectories in SIASUS: {}", subdirs.len());
                                        
                                        // Show some subdirectories
                                        for (i, subdir) in subdirs.iter().take(3).enumerate() {
                                            println!("  {}. 📁 {}", i + 1, subdir.name);
                                        }
                                        if subdirs.len() > 3 {
                                            println!("  ... and {} more subdirectories", subdirs.len() - 3);
                                        }
                                    }
                                    Err(e) => {
                                        println!("Error getting subdirectories: {}", e);
                                    }
                                }
                            }
                            Err(e) => {
                                println!("❌ Error with Directory.content(): {}", e);
                            }
                        }
                        
                        println!("\n=== File Extension Testing ===");
                        
                        // Test file extension filtering on a subdirectory that likely has files
                        if let Some(DirectoryEntry::Directory(first_subdir)) = 
                            content.values().find(|entry| matches!(entry, DirectoryEntry::Directory(_))) {
                            
                            let test_dir = Directory::new_with_provider(
                                first_subdir.path.clone(),
                                ftp_provider.clone()
                            ).await?;
                            
                            match test_dir.files_with_extension("dbc").await {
                                Ok(dbc_files) => {
                                    println!("Found {} .DBC files in {}", dbc_files.len(), first_subdir.name);
                                }
                                Err(e) => {
                                    println!("Error finding .DBC files: {}", e);
                                }
                            }
                            
                            match test_dir.files_with_extension("dbf").await {
                                Ok(dbf_files) => {
                                    println!("Found {} .DBF files in {} (should be filtered)", 
                                        dbf_files.len(), first_subdir.name);
                                }
                                Err(e) => {
                                    println!("Error finding .DBF files: {}", e);
                                }
                            }
                        }
                        
                    }
                    Err(e) => {
                        println!("❌ Error listing root directory: {}", e);
                        println!("This could be due to:");
                        println!("  - Network connectivity issues");
                        println!("  - FTP server temporarily unavailable");
                        println!("  - Firewall blocking FTP connections");
                        println!("  - FTP passive mode issues");
                    }
                }
            } else {
                println!("⚠️  Root directory reported as not existing");
            }
        }
        Err(e) => {
            let duration = start.elapsed();
            println!("❌ Connection failed after {:?}", duration);
            println!("Error: {}", e);
            println!("\nPossible causes:");
            println!("  1. No internet connectivity");
            println!("  2. DATASUS FTP server is down");
            println!("  3. Firewall blocking FTP (port 21)");
            println!("  4. DNS resolution issues for ftp.datasus.gov.br");
            println!("  5. FTP passive mode not supported by network");
            
            println!("\nTroubleshooting steps:");
            println!("  1. Check internet connection");
            println!("  2. Try: ping ftp.datasus.gov.br");
            println!("  3. Try: telnet ftp.datasus.gov.br 21");
            println!("  4. Check if corporate firewall blocks FTP");
        }
    }
    
    println!("\n=== Performance Summary ===");
    println!("FTP operations are naturally slower than local operations due to:");
    println!("  - Network latency");
    println!("  - FTP protocol overhead");
    println!("  - Server processing time");
    println!("Consider caching for frequently accessed directories!");
    
    println!("\n=== Test Complete ===");
    
    Ok(())
}
