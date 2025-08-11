use shared::models;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    env_logger::init();

    println!("🚀 Working Download Demo");
    
    // Create FTP provider to get real file information
    let ftp_provider = models::directory::FtpFileSystemProvider::new_datasus();
    
    println!("📂 Listing FTP directory to get real files...");
    
    // List a real directory to get actual files
    let directory_content = ftp_provider.list_directory("/SIHSUS/200801_/Dados").await?;
    
    // Find actual .dbc files
    let mut files_to_download = Vec::new();
    for (_name, entry) in directory_content.iter().take(2) { // Limit to first 2 files
        if let models::directory::DirectoryEntry::File(file) = entry {
            if file.has_extension("dbc") {
                // Only get small files for demo
                if let Some(size) = file.size_bytes() {
                    if size < 1024 * 200 { // Less than 200KB
                        files_to_download.push(file);
                        println!("  📄 Found: {} ({} bytes)", file.basename, size);
                    }
                }
            }
        }
    }

    if files_to_download.is_empty() {
        println!("❌ No small .dbc files found in the directory");
        return Ok(());
    }

    // Create a downloader with custom configuration
    let config = models::download::DownloadConfig {
        output_dir: "./demo_downloads".to_string(),
        preserve_structure: true,
        max_concurrent: 2,
        buffer_size: 8192,
        overwrite: true,
    };

    let downloader = models::download::FtpDownloader::new_datasus().with_config(config);

    // Download the first file
    let first_file = files_to_download[0];
    println!("\n📄 Downloading: {}", first_file.basename);
    
    match downloader.download_file(first_file).await {
        Ok(result) => {
            println!("✅ Downloaded: {} -> {} ({} bytes in {}ms)", 
                result.ftp_path, 
                result.local_path, 
                result.size_bytes,
                result.duration_ms
            );
        }
        Err(e) => {
            println!("❌ Download failed: {}", e);
        }
    }

    // Download multiple files if we have them
    if files_to_download.len() > 1 {
        println!("\n📦 Downloading {} files concurrently...", files_to_download.len());
        
        match downloader.download_files(files_to_download).await {
            Ok(results) => {
                println!("✅ Batch download completed!");
                for result in results {
                    if result.success {
                        println!("  ✓ {} -> {} ({} bytes in {}ms)", 
                            result.ftp_path, 
                            result.local_path, 
                            result.size_bytes,
                            result.duration_ms
                        );
                    } else {
                        println!("  ✗ {} failed: {}", 
                            result.ftp_path, 
                            result.error.unwrap_or_else(|| "Unknown error".to_string())
                        );
                    }
                }
            }
            Err(e) => {
                println!("❌ Batch download failed: {}", e);
            }
        }
    }

    println!("\n🎉 Demo completed!");
    
    Ok(())
}
