use shared::models::directory::{Directory, DirectoryEntry, FtpFileSystemProvider, FileSystemProvider};
use std::sync::Arc;
use std::time::Instant;
use futures::future::join_all;
use tokio;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let ftp_provider = Arc::new(FtpFileSystemProvider::new_datasus());

    let directories = vec![
        "/SIASUS/199407_200712/Dados",
        "/SIASUS/200801_/Dados",
    ];

    println!("=== Sequential Execution ===");
    
    for dir_path in &directories {
        println!("\n=== Listing contents of: {} ===", dir_path);
        
        let _directory = Directory::new_with_provider(dir_path.to_string(), ftp_provider.clone()).await?;
        
        let start = Instant::now();
        match ftp_provider.list_directory(dir_path).await {
            Ok(content) => {
                let duration = start.elapsed();
                println!("Found {} items in {:?}", content.len(), duration);
                
                // Show first 5 items (reduced for brevity)
                let items: Vec<_> = content.iter().take(5).collect();
                for (name, entry) in items {
                    match entry {
                        DirectoryEntry::File(file) => {
                            let info = file.info();
                            println!("  📄 {} ({})", 
                                name,
                                info.get("size").unwrap_or(&"unknown".to_string())
                            );
                        }
                        DirectoryEntry::Directory(_) => {
                            println!("  📁 {}", name);
                        }
                    }
                }
                
                if content.len() > 5 {
                    println!("  ... and {} more items", content.len() - 5);
                }
            }
            Err(e) => {
                println!("❌ Error listing {}: {}", dir_path, e);
            }
        }
    }

    println!("\n\n=== Parallel Execution ===");
    
    // Create futures for all directory listings
    let futures: Vec<_> = directories.iter().map(|dir_path| {
        let provider = ftp_provider.clone();
        let path = dir_path.to_string();
        async move {
            let start = Instant::now();
            let result = provider.list_directory(&path).await;
            let duration = start.elapsed();
            (path, result, duration)
        }
    }).collect();
    
    // Execute all futures in parallel
    println!("Starting parallel execution of {} directories...", directories.len());
    let parallel_start = Instant::now();
    let results = join_all(futures).await;
    let total_parallel_duration = parallel_start.elapsed();
    
    println!("✅ Parallel execution completed in {:?}", total_parallel_duration);
    
    // Process results
    for (dir_path, result, duration) in results {
        println!("\n=== Results for: {} ===", dir_path);
        match result {
            Ok(content) => {
                println!("Found {} items in {:?}", content.len(), duration);
                
                // Show first 5 items
                let items: Vec<_> = content.iter().take(5).collect();
                for (name, entry) in items {
                    match entry {
                        DirectoryEntry::File(file) => {
                            let info = file.info();
                            println!("  📄 {} ({})", 
                                name,
                                info.get("size").unwrap_or(&"unknown".to_string())
                            );
                        }
                        DirectoryEntry::Directory(_) => {
                            println!("  📁 {}", name);
                        }
                    }
                }
                
                if content.len() > 5 {
                    println!("  ... and {} more items", content.len() - 5);
                }
            }
            Err(e) => {
                println!("❌ Error listing {}: {}", dir_path, e);
            }
        }
    }

    Ok(())
}
