fn main() {
    println!("Testing what's available in shared::models:");
    
    // Try to reference the download types directly
    let _config: shared::models::download::DownloadConfig = shared::models::download::DownloadConfig::default();
    let _downloader = shared::models::download::FtpDownloader::new_datasus();
    
    println!("Download types are accessible!");
}
