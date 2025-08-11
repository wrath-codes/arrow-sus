use anyhow::Result;
use shared::models::*;

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

    println!("🚀 Simple FTP Download Demo");
    
    // Create FTP provider to get real file information
    let ftp_provider = FtpFileSystemProvider::new_datasus();
    
    println!("📂 Listing FTP directory to get real files...");
    
    // List a real directory to get actual files
    let directory_content = ftp_provider.list_directory("/SIHSUS/200801_/Dados").await
        .map_err(|e| anyhow::anyhow!("Failed to list directory: {}", e))?;
    
    // Find actual .dbc files
    let mut files_to_download = Vec::new();
    for (_name, entry) in directory_content.iter().take(2) { // Limit to first 2 files
        if let DirectoryEntry::File(file) = entry {
            if file.has_extension("dbc") {
                files_to_download.push(file);
                println!("  📄 Found: {} ({} bytes)", file.basename, file.size_bytes().unwrap_or(0));
            }
        }
    }

    if files_to_download.is_empty() {
        println!("❌ No .dbc files found in the directory");
        return Ok(());
    }

    // Create a downloader with custom configuration
    let config = DownloadConfig {
        output_dir: "./downloads".to_string(),
        preserve_structure: true,
        max_concurrent: 2,
        buffer_size: 8192,
        overwrite: true,
    };

    let downloader = FtpDownloader::new_datasus().with_config(config);

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

    println!("\n🎉 Demo completed!");
    
    Ok(())
}
