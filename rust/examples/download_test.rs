use anyhow::Result;
use shared::models::download::{download_file_with_progress, download_multiple_files};
use suppaftp::AsyncFtpStream;

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

    println!("🚀 Connecting to DATASUS FTP server...");
    
    // Connect to DATASUS FTP server
    let mut ftp_stream = AsyncFtpStream::connect("ftp.datasus.gov.br:21").await?;
    ftp_stream.login("anonymous", "anonymous").await?;
    
    println!("✅ Connected successfully!");

    // Test single file download
    println!("\n=== Testing Single File Download ===");
    
    // Navigate to a directory with small files for testing
    ftp_stream.cwd("/dissemin/publicos/SIHSUS/200801_/Dados/").await?;
    
    // Get list of files to find a small one for testing
    let files = ftp_stream.nlst(None).await?;
    
    if let Some(test_file) = files.first() {
        println!("📁 Found test file: {}", test_file);
        
        let remote_path = format!("/dissemin/publicos/SIHSUS/200801_/Dados/{}", test_file);
        let local_path = format!("./downloads/{}", test_file);
        
        match download_file_with_progress(&mut ftp_stream, &remote_path, &local_path).await {
            Ok(bytes) => {
                println!("✅ Single file download successful: {} bytes", bytes);
            }
            Err(e) => {
                eprintln!("❌ Single file download failed: {}", e);
            }
        }
    }

    // Test multiple file download (with first 3 files)
    println!("\n=== Testing Multiple File Download ===");
    
    let files_to_download: Vec<(String, String)> = files
        .iter()
        .take(3) // Only download first 3 files for testing
        .map(|file| {
            let remote_path = format!("/dissemin/publicos/SIHSUS/200801_/Dados/{}", file);
            let local_path = format!("./downloads/batch/{}", file);
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
                println!("✅ Batch download completed!");
                for (filename, bytes) in results {
                    println!("  📁 {}: {} bytes", filename, bytes);
                }
            }
            Err(e) => {
                eprintln!("❌ Batch download failed: {}", e);
            }
        }
    }

    // Cleanup
    ftp_stream.quit().await?;
    println!("\n🎉 Download test completed!");
    
    Ok(())
}
