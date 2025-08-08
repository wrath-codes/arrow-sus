use anyhow::Result;
use shared::models::download::{download_file_with_progress, download_multiple_files};
use suppaftp::AsyncFtpStream;

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

    println!("🎨 Rich Progress Bar Download Demo");
    println!("===================================");
    
    println!("\n🚀 Connecting to DATASUS FTP server...");
    
    // Connect to DATASUS FTP server
    let mut ftp_stream = AsyncFtpStream::connect("ftp.datasus.gov.br:21").await?;
    ftp_stream.login("anonymous", "anonymous").await?;
    
    println!("✅ Connected successfully!");

    // Navigate to a directory with various file sizes
    ftp_stream.cwd("/dissemin/publicos/SIHSUS/200801_/Dados/").await?;
    
    // Get list of files to show different file sizes
    let files = ftp_stream.nlst(None).await?;
    
    // Demo 1: Single file with rich progress
    println!("\n🎯 Demo 1: Single File Download with Rich Progress");
    println!("=================================================");
    
    if let Some(test_file) = files.first() {
        let remote_path = format!("/dissemin/publicos/SIHSUS/200801_/Dados/{}", test_file);
        let local_path = format!("./demo_downloads/single/{}", test_file);
        
        match download_file_with_progress(&mut ftp_stream, &remote_path, &local_path).await {
            Ok(bytes) => {
                println!("✨ Downloaded with beautiful progress bar: {} bytes", bytes);
            }
            Err(e) => {
                eprintln!("❌ Download failed: {}", e);
            }
        }
    }

    // Demo 2: Multiple files showcase
    println!("\n🎯 Demo 2: Batch Download with Individual Progress Bars");
    println!("======================================================");
    
    let files_to_download: Vec<(String, String)> = files
        .iter()
        .take(5) // Download first 5 files to show variety
        .enumerate()
        .map(|(i, file)| {
            let remote_path = format!("/dissemin/publicos/SIHSUS/200801_/Dados/{}", file);
            let local_path = format!("./demo_downloads/batch/{:02}_{}", i + 1, file);
            (remote_path, local_path)
        })
        .collect();
    
    if !files_to_download.is_empty() {
        let file_refs: Vec<(&str, &str)> = files_to_download
            .iter()
            .map(|(remote, local)| (remote.as_str(), local.as_str()))
            .collect();
        
        match download_multiple_files(&mut ftp_stream, file_refs).await {
            Ok(results) => {
                println!("\n🌟 Batch Download Summary:");
                println!("========================");
                let total_bytes: u64 = results.iter().map(|(_, bytes)| bytes).sum();
                
                for (i, (filename, bytes)) in results.iter().enumerate() {
                    println!("  {}. 📁 {}: {} bytes", i + 1, filename, bytes);
                }
                
                println!("\n📊 Total downloaded: {} bytes ({:.2} MB)", 
                    total_bytes, 
                    total_bytes as f64 / 1_048_576.0
                );
            }
            Err(e) => {
                eprintln!("❌ Batch download failed: {}", e);
            }
        }
    }

    // Demo 3: Show individual features explanation
    println!("\n🎯 Demo 3: Progress Bar Features Explained");
    println!("==========================================");
    println!("🌀 Spinner: Animated spinner showing activity");
    println!("📥 Icon & Name: Clear file identification");
    println!("• Separators: Clean visual separation");
    println!("━━━ Animation: Visual progress representation");
    println!("📊 Percentage: Exact completion percentage");
    println!("📈 Count/Total: Current/Total bytes (human readable)");
    println!("🚀 Speed: Real-time download speed");
    println!("⏱️  ETA: Estimated time remaining");

    // Cleanup
    ftp_stream.quit().await?;
    
    println!("\n🎉 Rich Progress Demo Completed!");
    println!("================================");
    println!("✨ Features showcased:");
    println!("   • Animated spinners");
    println!("   • File-specific progress bars");
    println!("   • Real-time speed calculation");
    println!("   • ETA estimation");
    println!("   • Clean visual design");
    println!("   • Batch download tracking");
    
    Ok(())
}
