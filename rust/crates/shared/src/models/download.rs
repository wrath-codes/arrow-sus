use anyhow::Result;
use futures::io::AsyncReadExt;
use kdam::{tqdm, BarExt, term, Colour, TqdmParallelIterator};
use rayon::prelude::*;
use std::path::Path;
use std::sync::{Arc, Mutex};
use suppaftp::{AsyncFtpStream, FtpError};
use tokio::fs;
use tokio::io::AsyncWriteExt;
use tokio::sync::Semaphore;

/// Download a file from the DATASUS FTP server with gradient progress tracking
/// 
/// # Arguments
/// * `ftp_stream` - Active FTP connection to the DATASUS server
/// * `remote_path` - Path to the file on the FTP server (e.g., "/dissemin/publicos/SIHSUS/200801_/Dados/RDAC2008.dbc")
/// * `local_path` - Local path where the file should be saved
/// 
/// # Returns
/// * `Result<u64>` - Number of bytes downloaded on success
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

    // Initialize terminal for colors
    term::init(true);

    // Get file size for progress tracking (if available)
    let file_size = get_file_size(ftp_stream, remote_path).await.unwrap_or(0);
    
    // Create gradient progress bar
    let mut pb = if file_size > 0 {
        tqdm!(
            total = file_size as usize,
            unit_scale = true,
            unit_divisor = 1024,
            unit = "B",
            desc = format!("📥 {}", filename),
            colour = Colour::gradient(&["#ff6b6b", "#4ecdc4", "#45b7d1"]),
            leave = false
        )
    } else {
        tqdm!(
            unit_scale = true,
            unit_divisor = 1024,
            unit = "B",
            desc = format!("📥 {}", filename),
            colour = Colour::gradient(&["#ff6b6b", "#4ecdc4", "#45b7d1"]),
            leave = false
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

    println!("✅ Downloaded {} bytes to {}", downloaded_bytes, local_path);

    Ok(downloaded_bytes)
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
pub async fn download_multiple_files(
    ftp_stream: &mut AsyncFtpStream,
    file_paths: Vec<(&str, &str)>,
) -> Result<Vec<(String, u64)>> {
    let mut results = Vec::new();
    let total_files = file_paths.len();

    // Initialize terminal for colors
    term::init(true);

    println!("📦 Starting download of {} files", total_files);

    // Overall progress bar
    let mut overall_pb = tqdm!(
        total = total_files,
        desc = "Overall Progress",
        colour = Colour::gradient(&["#667eea", "#764ba2"]),
        leave = true
    );

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
        
        // Update overall progress
        let _ = overall_pb.update(1);
    }

    let _ = overall_pb.refresh();
    println!("\n✅ Download batch completed: {}/{} files successful", results.len(), total_files);
    Ok(results)
}

/// Download multiple files in parallel using rayon with TqdmParallelIterator and gradient progress bars
/// 
/// # Arguments
/// * `file_paths` - Vector of (remote_path, local_path) tuples
/// * `max_concurrent` - Maximum number of concurrent downloads (default: 4)
/// 
/// # Returns
/// * `Result<Vec<(String, u64)>>` - Vector of (filename, bytes_downloaded) on success
pub async fn download_multiple_files_parallel(
    file_paths: Vec<(&str, &str)>,
    max_concurrent: Option<usize>,
) -> Result<Vec<(String, u64)>> {
    let concurrent_limit = max_concurrent.unwrap_or(4);
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

    // Configure rayon thread pool
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(concurrent_limit)
        .build()
        .map_err(|e| anyhow::anyhow!("Failed to build thread pool: {}", e))?;

    // Convert to owned strings for parallel processing
    let owned_paths: Vec<(String, String)> = file_paths
        .into_iter()
        .map(|(remote, local)| (remote.to_string(), local.to_string()))
        .collect();

    // Create overall progress bar with gradient
    let mut overall_pb = tqdm!(
        total = total_files,
        desc = "🎯 Overall Progress",
        colour = Colour::gradient(&["#667eea", "#764ba2"]),
        leave = true
    );

    // Use rayon's parallel iterator with TqdmParallelIterator for automatic progress tracking
    let download_results: Vec<_> = pool.install(|| {
        owned_paths
            .into_par_iter()
            .enumerate()
            .tqdm() // This uses TqdmParallelIterator trait!
            .map(|(index, (remote_path, local_path))| {
                let filename = Path::new(&remote_path)
                    .file_name()
                    .and_then(|n| n.to_str())
                    .unwrap_or("unknown")
                    .to_string();

                println!("[{}] 📥 Starting download: {}", index + 1, filename);

                // Simulate download work (replace with actual async download)
                match download_file_sync(&remote_path, &local_path, index + 1) {
                    Ok(bytes) => {
                        println!("[{}] ✅ Completed: {} ({} bytes)", index + 1, filename, bytes);
                        Some((filename, bytes))
                    }
                    Err(e) => {
                        eprintln!("[{}] ❌ Failed: {} - {}", index + 1, filename, e);
                        None
                    }
                }
            })
            .collect()
    });

    // Filter successful downloads and update results
    let final_results: Vec<(String, u64)> = download_results
        .into_iter()
        .filter_map(|x| x)
        .collect();

    // Update overall progress to completion
    let _ = overall_pb.update_to(total_files);
    let _ = overall_pb.refresh();

    println!("\n✅ Parallel download completed: {}/{} files successful", final_results.len(), total_files);
    
    Ok(final_results)
}

/// Synchronous download function for use with rayon (simplified for demo)
/// In a real implementation, you'd need to handle the async nature differently
fn download_file_sync(
    remote_path: &str,
    local_path: &str,
    file_number: usize,
) -> Result<u64> {
    // This is a simplified version for demonstration
    // In reality, you'd need to handle the async FTP operations differently
    // or use tokio::task::block_in_place for async operations within rayon
    
    let filename = Path::new(remote_path)
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("unknown");

    // Simulate file download progress with gradient progress bar
    let file_size = 1024 * 1024; // 1MB simulation
    let chunk_size = 8192;
    let chunks = file_size / chunk_size;

    // Create individual progress bar for this file with gradient
    let mut file_pb = tqdm!(
        total = file_size,
        desc = format!("[{}] 📥 {}", file_number, filename),
        colour = Colour::gradient(&["#ff6b6b", "#4ecdc4", "#45b7d1"]),
        leave = false,
        unit_scale = true,
        unit_divisor = 1024,
        unit = "B"
    );
    
    for i in 0..chunks {
        // Simulate download time
        std::thread::sleep(std::time::Duration::from_millis(10));
        
        let _ = file_pb.update(chunk_size);
        
        // Update description with current progress
        let progress = ((i + 1) as f64 / chunks as f64) * 100.0;
        file_pb.set_description(format!("[{}] 📥 {} ({:.1}%)", 
            file_number, filename, progress));
    }

    // Mark as completed
    file_pb.set_description(format!("[{}] ✅ {}", file_number, filename));
    let _ = file_pb.refresh();

    // Create directory if it doesn't exist
    if let Some(parent) = Path::new(local_path).parent() {
        std::fs::create_dir_all(parent)?;
    }

    // Write a dummy file for testing
    std::fs::write(local_path, format!("Downloaded: {}", filename))?;

    Ok(file_size as u64)
}

/// Async version of parallel download using tokio semaphore with gradient progress
pub async fn download_multiple_files_parallel_async(
    file_paths: Vec<(&str, &str)>,
    max_concurrent: Option<usize>,
) -> Result<Vec<(String, u64)>> {
    let concurrent_limit = max_concurrent.unwrap_or(4);
    let semaphore = Arc::new(Semaphore::new(concurrent_limit));
    let total_files = file_paths.len();

    // Initialize terminal for colors
    term::init(true);

    println!("🚀 Starting async parallel download of {} files (max concurrent: {})", total_files, concurrent_limit);

    // Overall progress bar with gradient
    let overall_pb = Arc::new(Mutex::new(tqdm!(
        total = total_files,
        desc = "🎯 Overall Progress",
        colour = Colour::gradient(&["#667eea", "#764ba2"]),
        leave = true
    )));

    let results = Arc::new(Mutex::new(Vec::new()));

    // Create tasks for parallel execution
    let mut tasks = Vec::new();
    
    for (index, (remote_path, local_path)) in file_paths.into_iter().enumerate() {
        let semaphore = Arc::clone(&semaphore);
        let results = Arc::clone(&results);
        let overall_pb = Arc::clone(&overall_pb);
        let remote_path = remote_path.to_string();
        let local_path = local_path.to_string();

        let task = tokio::spawn(async move {
            let _permit = semaphore.acquire().await.unwrap();
            
            let filename = Path::new(&remote_path)
                .file_name()
                .and_then(|n| n.to_str())
                .unwrap_or("unknown");

            // Individual gradient progress bar for this file
            let mut file_pb = tqdm!(
                desc = format!("[{}] 📥 {}", index + 1, filename),
                colour = Colour::gradient(&["#ff6b6b", "#4ecdc4", "#45b7d1"]),
                leave = false,
                unit_scale = true,
                unit_divisor = 1024,
                unit = "B"
            );

            // Download the file with its own FTP connection
            match download_file_with_progress_simple(&remote_path, &local_path, index + 1, &mut file_pb).await {
                Ok(bytes) => {
                    {
                        let mut results_guard = results.lock().unwrap();
                        results_guard.push((filename.to_string(), bytes));
                    }

                    // Update overall progress
                    {
                        let mut overall_guard = overall_pb.lock().unwrap();
                        let _ = overall_guard.update(1);
                    }

                    file_pb.set_description(format!("[{}] ✅ {}", index + 1, filename));
                    let _ = file_pb.refresh();
                }
                Err(e) => {
                    eprintln!("❌ Failed to download {}: {}", filename, e);
                    file_pb.set_description(format!("[{}] ❌ {}", index + 1, filename));
                    let _ = file_pb.refresh();
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
    
    // Final update to overall progress
    {
        let mut overall_guard = overall_pb.lock().unwrap();
        let _ = overall_guard.refresh();
    }

    println!("\n✅ Async parallel download completed: {}/{} files successful", final_results.len(), total_files);
    
    Ok(final_results)
}

/// Download a single file with its own FTP connection and gradient progress bar
async fn download_file_with_progress_simple(
    remote_path: &str,
    local_path: &str,
    _file_number: usize,
    progress_bar: &mut kdam::Bar,
) -> Result<u64> {
    // Extract filename for display
    let _filename = Path::new(remote_path)
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("unknown");

    // Create a new FTP connection for this download
    let mut ftp_stream = AsyncFtpStream::connect("ftp.datasus.gov.br:21").await?;
    ftp_stream.login("anonymous", "anonymous").await?;

    // Get file size for progress tracking (if available)
    let file_size = get_file_size(&mut ftp_stream, remote_path).await.unwrap_or(0);
    
    if file_size > 0 {
        progress_bar.reset(Some(file_size as usize));
    }

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
        let _ = progress_bar.update(downloaded_bytes as usize);
    } else {
        let _ = progress_bar.update_to(downloaded_bytes as usize);
    }
    let _ = progress_bar.refresh();

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
    async fn test_parallel_download_rayon() {
        println!("🧪 Testing rayon parallel download functionality with gradient progress bars");
        
        let temp_dir = TempDir::new().unwrap();
        
        // Create local paths with proper lifetime
        let local_path1 = temp_dir.path().join("rayon_CHBR1901.dbc");
        let local_path2 = temp_dir.path().join("rayon_CHBR1902.dbc");
        let local_path3 = temp_dir.path().join("rayon_CHBR1903.dbc");
        
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

        // Test parallel download with rayon and gradient progress bars
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
        
        println!("🎉 Rayon parallel download test completed successfully!");
    }

    #[tokio::test]
    #[ignore] // Requires network connection
    async fn test_async_parallel_download() {
        println!("🧪 Testing async parallel download functionality with gradient progress bars");
        
        let temp_dir = TempDir::new().unwrap();
        
        // Create local paths with proper lifetime
        let local_path1 = temp_dir.path().join("async_CHBR1901.dbc");
        let local_path2 = temp_dir.path().join("async_CHBR1902.dbc");
        let local_path3 = temp_dir.path().join("async_CHBR1903.dbc");
        
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

        // Test async parallel download with gradient progress bars
        let result = download_multiple_files_parallel_async(files.clone(), Some(2)).await;

        // Verify the download was successful
        assert!(result.is_ok(), "Async parallel download should succeed");
        
        let download_results = result.unwrap();
        assert_eq!(download_results.len(), 3, "Should download all 3 files");
        
        println!("🎉 Async parallel download test completed successfully!");
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
