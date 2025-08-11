use shared::models::download::{FtpDownloader, DownloadConfig};
use shared::models::directory::{FtpFileSystemProvider, DirectoryEntry, FileSystemProvider};
use shared::models::file::File;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🚀 FTP Download Demo with Beautiful Progress Bars");
    println!();

    // Create downloader with custom config
    let config = DownloadConfig {
        output_dir: std::env::temp_dir().join("demo_downloads").to_string_lossy().to_string(),
        preserve_structure: false,
        max_concurrent: 3,
        buffer_size: 16384,
        overwrite: true,
    };

    let downloader = FtpDownloader::new_datasus().with_config(config);

    // Get some files to download
    let provider = FtpFileSystemProvider::new_datasus();
    
    println!("📂 Browsing FTP for demo files...");
    let directory_content = provider.list_directory("/SIHSUS/200801_/Dados").await?;
    
    // Select a few interesting files for demo
    let demo_files: Vec<&File> = directory_content
        .values()
        .filter_map(|entry| match entry {
            DirectoryEntry::File(file) => Some(file),
            DirectoryEntry::Directory(_) => None,
        })
        .filter(|f| f.basename.ends_with(".dbc"))
        .filter(|f| {
            let size = f.size_bytes().unwrap_or(0);
            size > 5_000_000 && size < 50_000_000  // 5MB to 50MB range for good demo
        })
        .take(3)
        .collect();

    if demo_files.is_empty() {
        println!("❌ No suitable demo files found");
        return Ok(());
    }

    println!("✨ Selected {} files for download demo:", demo_files.len());
    for file in &demo_files {
        let size_mb = file.size_bytes().unwrap_or(0) as f64 / (1024.0 * 1024.0);
        println!("   📄 {} ({:.1} MB)", file.basename, size_mb);
    }
    println!();

    // Start the beautiful download process!
    let results = downloader.download_files(demo_files).await?;

    // Print final summary
    println!();
    let successful = results.iter().filter(|r| r.success).count();
    let failed = results.iter().filter(|r| !r.success).count();
    
    if failed == 0 {
        println!("🎉 Demo completed successfully! All {} files downloaded.", successful);
    } else {
        println!("⚠️  Demo completed with {} successful and {} failed downloads.", successful, failed);
    }

    Ok(())
}
