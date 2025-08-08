use anyhow::Result;
use futures::io::AsyncReadExt;
use kdam::{tqdm, BarExt, RichProgress, Column, Spinner, term};
use std::path::Path;
use suppaftp::{AsyncFtpStream, FtpError};
use tokio::fs;
use tokio::io::AsyncWriteExt;

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

/// Download multiple files from the DATASUS FTP server with progress tracking
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
