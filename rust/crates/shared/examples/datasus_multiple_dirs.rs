use shared::models::directory::{Directory, DirectoryEntry, FtpFileSystemProvider, FileSystemProvider};
use std::sync::Arc;
use std::time::Instant;
use tokio;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let ftp_provider = Arc::new(FtpFileSystemProvider::new_datasus());

    let directories = vec![
        "/SIASUS/199407_200712/Dados",
        "/SIASUS/200801_/Dados",
    ];

    for dir_path in directories {
        println!("\n=== Listing contents of: {} ===", dir_path);
        
        let _directory = Directory::new_with_provider(dir_path.to_string(), ftp_provider.clone()).await?;
        
        let start = Instant::now();
        match ftp_provider.list_directory(dir_path).await {
            Ok(content) => {
                let duration = start.elapsed();
                println!("Found {} items in {:?}", content.len(), duration);
                
                // Show first 10 items
                let items: Vec<_> = content.iter().take(10).collect();
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
                
                if content.len() > 10 {
                    println!("  ... and {} more items", content.len() - 10);
                }
            }
            Err(e) => {
                println!("❌ Error listing {}: {}", dir_path, e);
            }
        }
    }

    Ok(())
}
