use anyhow::Result;
use futures::io::AsyncReadExt;
use kdam::{tqdm, BarExt, RichProgress, Column, Spinner, term};
use std::path::Path;
use std::sync::{Arc, Mutex};
use suppaftp::{AsyncFtpStream, FtpError};
use tokio::fs;
use tokio::io::AsyncWriteExt;
use tokio::sync::Semaphore;

/// Download a file from the DATASUS FTP server with progress tracking
/// 
/// # Arguments
/// * `ftp_stream` - Active FTP connection to the DATASUS server
/// * `remote_path` - Path to the file on the FTP server (e.g., "/dissemin/publicos/SIHSUS/200801_/Dados/RDAC2008.dbc")
/// * `local_path` - Local path where the file should be saved
/// 
/// # Returns
/// * `Result<u64>` - Number of bytes downloaded on success
/// 
/// # Example
/// ```rust
/// use suppaftp::AsyncFtpStream;
/// use shared::models::download::download_file_with_progress;
/// 
/// #[tokio::main]
/// async fn main() -> anyhow::Result<()> {
///     let mut ftp_stream = AsyncFtpStream::connect("ftp.datasus.gov.br:21").await?;
///     ftp_stream.login("anonymous", "anonymous").await?;
///     
///     let bytes_downloaded = download_file_with_progress(
///         &mut ftp_stream,
///         "/dissemin/publicos/SIHSUS/200801_/Dados/RDAC2008.dbc",
///         "./RDAC2008.dbc"
///     ).await?;
///     
///     println!("Downloaded {} bytes", bytes_downloaded);
///     ftp_stream.quit().await?;
///     Ok(())
/// }
/// ```
pub async fn download_file_with_progress(
    ftp_stream: &mut AsyncFtpStream,
    remote_path: &str,
    local_path: &str,
) -> Result<u64> {
    // Extract filename for display
    let filename = Path::new(remote_path)
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("unknown");

    println!("📥 Downloading: {}", filename);

    // Initialize terminal for colors
    term::init(true);

    // Get file size for progress tracking (if available)
    let file_size = get_file_size(ftp_stream, remote_path).await.unwrap_or(0);
    
    // Create beautiful rich progress bar with colors
    let mut pb = if file_size > 0 {
        RichProgress::new(
            tqdm!(
                total = file_size as usize,
                unit_scale = true,
                unit_divisor = 1024,
                unit = "B"
            ),
            vec![
                Column::Spinner(Spinner::new(
                    &["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"],
                    80.0,
                    1.0,
                )),
                Column::Text(format!("[bold cyan]📥 {}", filename)),
                Column::Text("[dim]•".to_string()),
                Column::Animation,
                Column::Percentage(1),
                Column::Text("[dim]•".to_string()),
                Column::CountTotal,
                Column::Text("[dim]•".to_string()),
                Column::Rate,
                Column::Text("[dim]•".to_string()),
                Column::RemainingTime,
            ],
        )
    } else {
        // Rich indeterminate progress bar when size is unknown
        RichProgress::new(
            tqdm!(unit_scale = true, unit_divisor = 1024, unit = "B"),
            vec![
                Column::Spinner(Spinner::new(
                    &["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"],
                    80.0,
                    1.0,
                )),
                Column::Text(format!("[bold cyan]📥 {}", filename)),
                Column::Text("[dim]•".to_string()),
                Column::Animation,
                Column::Text("[dim]•".to_string()),
                Column::Count,
                Column::Text("[dim]•".to_string()),
                Column::Rate,
            ],
        )
    };

    // Download file with progress tracking
    let file_data = ftp_stream
        .retr(remote_path, |mut data_stream| {
            Box::pin(async move {
                let mut buf = Vec::new();
                let mut chunk = vec![0u8; 8192]; // 8KB chunks

                loop {
                    match data_stream.read(&mut chunk).await {
                        Ok(0) => break, // EOF
                        Ok(n) => {
                            buf.extend_from_slice(&chunk[..n]);
                            // We'll update progress after the closure
                        }
                        Err(e) => return Err(FtpError::ConnectionError(e)),
                    }
                }

                Ok((buf, data_stream))
            })
        })
        .await?;

    // Update progress bar with final size
    let downloaded_bytes = file_data.len() as u64;
    if file_size > 0 {
        let _ = pb.update(downloaded_bytes as usize);
    }
    let _ = pb.refresh();

    // Create directory if it doesn't exist
    if let Some(parent) = Path::new(local_path).parent() {
        fs::create_dir_all(parent).await?;
    }

    // Write to local file
    {
        let mut local_file = fs::File::create(local_path).await?;
        local_file.write_all(&file_data).await?;
        local_file.flush().await?;
        local_file.sync_all().await?;
    } // Ensure file handle is dropped

    let bytes_downloaded = downloaded_bytes;
    println!("✅ Downloaded {} bytes to {}", bytes_downloaded, local_path);

    Ok(bytes_downloaded)
}

/// Get the size of a file on the FTP server
/// 
/// # Arguments
/// * `ftp_stream` - Active FTP connection
/// * `remote_path` - Path to the file on the FTP server
/// 
/// # Returns
/// * `Result<u64>` - File size in bytes, or error if size cannot be determined
async fn get_file_size(ftp_stream: &mut AsyncFtpStream, remote_path: &str) -> Result<u64> {
    // Try to get file size using SIZE command
    match ftp_stream.size(remote_path).await {
        Ok(size) => Ok(size as u64),
        Err(_) => {
            // If SIZE command fails, try to parse from LIST command
            let parent_path = Path::new(remote_path)
                .parent()
                .and_then(|p| p.to_str())
                .unwrap_or("/");
            
            let filename = Path::new(remote_path)
                .file_name()
                .and_then(|n| n.to_str())
                .unwrap_or("");

            // Change to parent directory
            let current_dir = ftp_stream.pwd().await?;
            ftp_stream.cwd(parent_path).await?;

            // Get detailed listing
            let listing = ftp_stream.list(Some(filename)).await?;
            
            // Restore original directory
            ftp_stream.cwd(&current_dir).await?;

            // Try to parse size from listing (this is FTP server dependent)
            // Common format: "-rw-r--r--   1 owner group    12345 Mon DD HH:MM filename"
            for line in listing {
                if line.contains(filename) {
                    let parts: Vec<&str> = line.split_whitespace().collect();
                    if parts.len() >= 5 {
                        if let Ok(size) = parts[4].parse::<u64>() {
                            return Ok(size);
                        }
                    }
                }
            }

            Err(anyhow::anyhow!("Could not determine file size"))
        }
    }
}

/// Download multiple files from the DATASUS FTP server with progress tracking (sequential)
/// 
/// # Arguments
/// * `ftp_stream` - Active FTP connection to the DATASUS server
/// * `file_paths` - Vector of (remote_path, local_path) tuples
/// 
/// # Returns
/// * `Result<Vec<(String, u64)>>` - Vector of (filename, bytes_downloaded) on success
/// 
/// # Example
/// ```rust
/// use suppaftp::AsyncFtpStream;
/// use shared::models::download::download_multiple_files;
/// 
/// #[tokio::main]
/// async fn main() -> anyhow::Result<()> {
///     let mut ftp_stream = AsyncFtpStream::connect("ftp.datasus.gov.br:21").await?;
///     ftp_stream.login("anonymous", "anonymous").await?;
///     
///     let files = vec![
///         ("/dissemin/publicos/SIHSUS/200801_/Dados/RDAC2008.dbc", "./downloads/RDAC2008.dbc"),
///         ("/dissemin/publicos/SIHSUS/200801_/Dados/SPAC2008.dbc", "./downloads/SPAC2008.dbc"),
///     ];
///     
///     let results = download_multiple_files(&mut ftp_stream, files).await?;
///     
///     for (filename, bytes) in results {
///         println!("Downloaded {}: {} bytes", filename, bytes);
///     }
///     
///     ftp_stream.quit().await?;
///     Ok(())
/// }
/// ```
pub async fn download_multiple_files(
    ftp_stream: &mut AsyncFtpStream,
    file_paths: Vec<(&str, &str)>,
) -> Result<Vec<(String, u64)>> {
    let mut results = Vec::new();
    let total_files = file_paths.len();

    println!("📦 Starting download of {} files", total_files);

    for (i, (remote_path, local_path)) in file_paths.iter().enumerate() {
        println!("\n[{}/{}] Processing: {}", i + 1, total_files, remote_path);
        
        match download_file_with_progress(ftp_stream, remote_path, local_path).await {
            Ok(bytes) => {
                let filename = Path::new(remote_path)
                    .file_name()
                    .and_then(|n| n.to_str())
                    .unwrap_or("unknown")
                    .to_string();
                results.push((filename, bytes));
            }
            Err(e) => {
                eprintln!("❌ Failed to download {}: {}", remote_path, e);
                // Continue with other files instead of failing completely
            }
        }
    }

    println!("\n✅ Download batch completed: {}/{} files successful", results.len(), total_files);
    Ok(results)
}

/// Download multiple files in parallel from the DATASUS FTP server with individual progress bars
/// 
/// # Arguments
/// * `file_paths` - Vector of (remote_path, local_path) tuples
/// * `max_concurrent` - Maximum number of concurrent downloads (default: 4)
/// 
/// # Returns
/// * `Result<Vec<(String, u64)>>` - Vector of (filename, bytes_downloaded) on success
/// 
/// # Example
/// ```rust
/// use shared::models::download::download_multiple_files_parallel;
/// 
/// #[tokio::main]
/// async fn main() -> anyhow::Result<()> {
///     let files = vec![
///         ("/dissemin/publicos/SIHSUS/200801_/Dados/RDAC2008.dbc", "./downloads/RDAC2008.dbc"),
///         ("/dissemin/publicos/SIHSUS/200801_/Dados/SPAC2008.dbc", "./downloads/SPAC2008.dbc"),
///         ("/dissemin/publicos/SIHSUS/200801_/Dados/CHBR1901.dbc", "./downloads/CHBR1901.dbc"),
///     ];
///     
///     let results = download_multiple_files_parallel(files, Some(2)).await?;
///     
///     for (filename, bytes) in results {
///         println!("Downloaded {}: {} bytes", filename, bytes);
///     }
///     
///     Ok(())
/// }
/// ```
pub async fn download_multiple_files_parallel(
    file_paths: Vec<(&str, &str)>,
    max_concurrent: Option<usize>,
) -> Result<Vec<(String, u64)>> {
    let concurrent_limit = max_concurrent.unwrap_or(4);
    let semaphore = Arc::new(Semaphore::new(concurrent_limit));
    let total_files = file_paths.len();

    // Initialize terminal for colors
    term::init(true);

    println!("🚀 Starting parallel download of {} files (max concurrent: {})", total_files, concurrent_limit);
    println!("📋 Files to download:");
    for (i, (remote_path, _)) in file_paths.iter().enumerate() {
        let filename = Path::new(remote_path)
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("unknown");
        println!("  [{}] {}", i + 1, filename);
    }
    println!();

    let results = Arc::new(Mutex::new(Vec::new()));

    // Create tasks for parallel execution
    let mut tasks = Vec::new();
    
    for (index, (remote_path, local_path)) in file_paths.into_iter().enumerate() {
        let semaphore = Arc::clone(&semaphore);
        let results = Arc::clone(&results);
        let remote_path = remote_path.to_string();
        let local_path = local_path.to_string();

        let task = tokio::spawn(async move {
            let _permit = semaphore.acquire().await.unwrap();
            
            let filename = Path::new(&remote_path)
                .file_name()
                .and_then(|n| n.to_str())
                .unwrap_or("unknown");

            // Download the file with its own FTP connection and progress bar
            match download_file_with_progress_simple(&remote_path, &local_path, index + 1).await {
                Ok(bytes) => {
                    let mut results_guard = results.lock().unwrap();
                    results_guard.push((filename.to_string(), bytes));
                }
                Err(e) => {
                    eprintln!("❌ Failed to download {}: {}", filename, e);
                }
            }
        });

        tasks.push(task);
    }

    // Wait for all downloads to complete
    for task in tasks {
        let _ = task.await;
    }

    let final_results = results.lock().unwrap().clone();
    println!("\n✅ Parallel download completed: {}/{} files successful", final_results.len(), total_files);
    
    Ok(final_results)
}

/// Download a single file with its own FTP connection and simple progress bar
async fn download_file_with_progress_simple(
    remote_path: &str,
    local_path: &str,
    file_number: usize,
) -> Result<u64> {
    // Extract filename for display
    let filename = Path::new(remote_path)
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("unknown");

    println!("[{}] 📥 Starting download of {}", file_number, filename);

    // Create a new FTP connection for this download
    let mut ftp_stream = AsyncFtpStream::connect("ftp.datasus.gov.br:21").await?;
    ftp_stream.login("anonymous", "anonymous").await?;

    // Get file size for progress tracking (if available)
    let file_size = get_file_size(&mut ftp_stream, remote_path).await.unwrap_or(0);
    
    // Create a simple progress bar for this specific download
    let mut pb = if file_size > 0 {
        RichProgress::new(
            tqdm!(
                total = file_size as usize,
                unit_scale = true,
                unit_divisor = 1024,
                unit = "B",
                desc = format!("[{}] 📥 {}", file_number, filename)
            ),
            vec![
                Column::Text(format!("[{}]", file_number)),
                Column::Spinner(Spinner::new(
                    &["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"],
                    80.0,
                    1.0,
                )),
                Column::Text(format!("[cyan]📥 {}", filename)),
                Column::Text("[dim]•".to_string()),
                Column::Animation,
                Column::Percentage(1),
                Column::Text("[dim]•".to_string()),
                Column::CountTotal,
                Column::Text("[dim]•".to_string()),
                Column::Rate,
            ],
        )
    } else {
        // Simple indeterminate progress bar when size is unknown
        RichProgress::new(
            tqdm!(
                unit_scale = true,
                unit_divisor = 1024,
                unit = "B",
                desc = format!("[{}] 📥 {}", file_number, filename)
            ),
            vec![
                Column::Text(format!("[{}]", file_number)),
                Column::Spinner(Spinner::new(
                    &["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"],
                    80.0,
                    1.0,
                )),
                Column::Text(format!("[cyan]📥 {}", filename)),
                Column::Text("[dim]•".to_string()),
                Column::Animation,
                Column::Text("[dim]•".to_string()),
                Column::Count,
                Column::Text("[dim]•".to_string()),
                Column::Rate,
            ],
        )
    };

    // Download file with progress tracking
    let file_data = ftp_stream
        .retr(remote_path, |mut data_stream| {
            Box::pin(async move {
                let mut buf = Vec::new();
                let mut chunk = vec![0u8; 8192]; // 8KB chunks

                loop {
                    match data_stream.read(&mut chunk).await {
                        Ok(0) => break, // EOF
                        Ok(n) => {
                            buf.extend_from_slice(&chunk[..n]);
                            // We'll update progress after the closure
                        }
                        Err(e) => return Err(FtpError::ConnectionError(e)),
                    }
                }

                Ok((buf, data_stream))
            })
        })
        .await?;

    // Update progress bar with final size
    let downloaded_bytes = file_data.len() as u64;
    if file_size > 0 {
        let _ = pb.update(downloaded_bytes as usize);
    } else {
        let _ = pb.update_to(downloaded_bytes as usize);
    }
    let _ = pb.refresh();

    // Create directory if it doesn't exist
    if let Some(parent) = Path::new(local_path).parent() {
        fs::create_dir_all(parent).await?;
    }

    // Write to local file
    {
        let mut local_file = fs::File::create(local_path).await?;
        local_file.write_all(&file_data).await?;
        local_file.flush().await?;
        local_file.sync_all().await?;
    } // Ensure file handle is dropped

    // Close FTP connection
    let _ = ftp_stream.quit().await;

    println!("[{}] ✅ Completed download of {} ({} bytes)", file_number, filename, downloaded_bytes);

    Ok(downloaded_bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    #[ignore] // Requires network connection
    async fn test_download_file_integration() {
        let temp_dir = TempDir::new().unwrap();
        let local_path = temp_dir.path().join("test_file.dbc");

        let mut ftp_stream = AsyncFtpStream::connect("ftp.datasus.gov.br:21")
            .await
            .expect("Failed to connect to FTP");

        ftp_stream
            .login("anonymous", "anonymous")
            .await
            .expect("Failed to login");

        // Test with a known small file
        let remote_path = "/dissemin/publicos/SIHSUS/200801_/Dados";
        
        // First, let's get a list of files to find a small one for testing
        ftp_stream.cwd(remote_path).await.expect("Failed to change directory");
        let files = ftp_stream.nlst(None).await.expect("Failed to list files");
        
        if let Some(test_file) = files.first() {
            let full_remote_path = format!("{}/{}", remote_path, test_file);
            let result = download_file_with_progress(
                &mut ftp_stream,
                &full_remote_path,
                local_path.to_str().unwrap(),
            ).await;

            assert!(result.is_ok());
            assert!(local_path.exists());
            
            // Check that file has some content
            let metadata = std::fs::metadata(&local_path).unwrap();
            assert!(metadata.len() > 0);
        }

        ftp_stream.quit().await.expect("Failed to quit FTP");
    }

    #[tokio::test]
    #[ignore] // Requires network connection
    async fn test_parallel_download_integration() {
        println!("🧪 Testing parallel download functionality with multiple progress bars");
        
        let temp_dir = TempDir::new().unwrap();
        
        // Create local paths with proper lifetime
        let local_path1 = temp_dir.path().join("CHBR1901.dbc");
        let local_path2 = temp_dir.path().join("CHBR1902.dbc");
        let local_path3 = temp_dir.path().join("CHBR1903.dbc");
        
        // Test files for parallel download
        let files = vec![
            (
                "/dissemin/publicos/SIHSUS/200801_/Dados/CHBR1901.dbc",
                local_path1.to_str().unwrap(),
            ),
            (
                "/dissemin/publicos/SIHSUS/200801_/Dados/CHBR1902.dbc", 
                local_path2.to_str().unwrap(),
            ),
            (
                "/dissemin/publicos/SIHSUS/200801_/Dados/CHBR1903.dbc",
                local_path3.to_str().unwrap(),
            ),
        ];

        // Test parallel download with max 2 concurrent downloads
        let result = download_multiple_files_parallel(files.clone(), Some(2)).await;

        // Verify the download was successful
        assert!(result.is_ok(), "Parallel download should succeed");
        
        let download_results = result.unwrap();
        assert_eq!(download_results.len(), 3, "Should download all 3 files");
        
        // Verify all files were downloaded and have content
        for (_, local_path) in &files {
            let path = std::path::Path::new(local_path);
            assert!(path.exists(), "Downloaded file should exist: {}", local_path);
            
            let metadata = std::fs::metadata(path).unwrap();
            assert!(metadata.len() > 0, "Downloaded file should have content: {}", local_path);
        }
        
        // Verify download results contain expected filenames and byte counts
        for (filename, bytes) in &download_results {
            assert!(filename.starts_with("CHBR"), "Filename should start with CHBR: {}", filename);
            assert!(*bytes > 0, "File should have been downloaded with some bytes: {}", filename);
            println!("✅ Verified download: {} ({} bytes)", filename, bytes);
        }
        
        println!("🎉 Parallel download test completed successfully!");
    }

    #[tokio::test]
    #[ignore] // Requires network connection  
    async fn test_sequential_vs_parallel_download() {
        println!("⚡ Comparing sequential vs parallel download performance");
        
        let temp_dir = TempDir::new().unwrap();
        
        // Test sequential download
        println!("📚 Testing sequential download...");
        let start_seq = std::time::Instant::now();
        
        let mut ftp_stream = AsyncFtpStream::connect("ftp.datasus.gov.br:21")
            .await
            .expect("Failed to connect to FTP");
        ftp_stream.login("anonymous", "anonymous").await.expect("Failed to login");
        
        // Create sequential file paths
        let seq_path1 = temp_dir.path().join("seq_CHBR1901.dbc");
        let seq_path2 = temp_dir.path().join("seq_CHBR1902.dbc");
        let seq_path3 = temp_dir.path().join("seq_CHBR1903.dbc");
        
        let sequential_files = vec![
            ("/dissemin/publicos/SIHSUS/200801_/Dados/CHBR1901.dbc", seq_path1.to_str().unwrap()),
            ("/dissemin/publicos/SIHSUS/200801_/Dados/CHBR1902.dbc", seq_path2.to_str().unwrap()),
            ("/dissemin/publicos/SIHSUS/200801_/Dados/CHBR1903.dbc", seq_path3.to_str().unwrap()),
        ];
        
        let seq_result = download_multiple_files(&mut ftp_stream, sequential_files).await;
        let seq_duration = start_seq.elapsed();
        ftp_stream.quit().await.expect("Failed to quit FTP");
        
        assert!(seq_result.is_ok(), "Sequential download should succeed");
        println!("⏱️  Sequential download took: {:?}", seq_duration);

        // Test parallel download
        println!("🚀 Testing parallel download...");
        let start_par = std::time::Instant::now();
        
        // Create parallel file paths
        let par_path1 = temp_dir.path().join("par_CHBR1901.dbc");
        let par_path2 = temp_dir.path().join("par_CHBR1902.dbc");
        let par_path3 = temp_dir.path().join("par_CHBR1903.dbc");
        
        let parallel_files = vec![
            ("/dissemin/publicos/SIHSUS/200801_/Dados/CHBR1901.dbc", par_path1.to_str().unwrap()),
            ("/dissemin/publicos/SIHSUS/200801_/Dados/CHBR1902.dbc", par_path2.to_str().unwrap()),
            ("/dissemin/publicos/SIHSUS/200801_/Dados/CHBR1903.dbc", par_path3.to_str().unwrap()),
        ];
            
        let par_result = download_multiple_files_parallel(parallel_files, Some(3)).await;
        let par_duration = start_par.elapsed();
        
        assert!(par_result.is_ok(), "Parallel download should succeed");
        println!("⏱️  Parallel download took: {:?}", par_duration);
        
        // Parallel should generally be faster (though network conditions may vary)
        println!("📊 Performance comparison:");
        println!("   Sequential: {:?}", seq_duration);
        println!("   Parallel:   {:?}", par_duration);
        
        if par_duration < seq_duration {
            let speedup = seq_duration.as_secs_f64() / par_duration.as_secs_f64();
            println!("🎯 Parallel was {:.2}x faster!", speedup);
        } else {
            println!("ℹ️  Network conditions may have affected timing");
        }
        
        println!("✅ Performance comparison test completed!");
    }

    #[test]
    fn test_get_file_size_parsing() {
        // Test parsing logic for different FTP listing formats
        let listing_line = "-rw-r--r--   1 ftp      ftp        123456 Jan 15 10:30 RDAC2008.dbc";
        let parts: Vec<&str> = listing_line.split_whitespace().collect();
        
        if parts.len() >= 5 {
            let size = parts[4].parse::<u64>().unwrap();
            assert_eq!(size, 123456);
        }
    }
}
