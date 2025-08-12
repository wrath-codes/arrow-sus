use crate::models::file::File;
use crate::models::directory::FtpFileSystemProvider;
use crate::models::async_utils::async_path_utils::{path_exists_async, ensure_dir_async, get_file_size_async, cache_path_async};
use indicatif::{ProgressBar, ProgressStyle, MultiProgress, HumanDuration};
use console::{Style, Term};

use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use rayon::prelude::*;
use tokio::fs::File as TokioFile;
use tokio::io::AsyncWriteExt;
use futures::io::AsyncReadExt;
use serde::{Deserialize, Serialize};
use anyhow::{anyhow, Result};

/// Download configuration for customizing download behavior
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DownloadConfig {
    /// Local directory to save files to
    pub output_dir: String,
    /// Whether to preserve the FTP directory structure locally
    pub preserve_structure: bool,
    /// Maximum number of concurrent downloads
    pub max_concurrent: usize,
    /// Buffer size for streaming downloads (in bytes)
    pub buffer_size: usize,
    /// Whether to overwrite existing files
    pub overwrite: bool,
}

impl Default for DownloadConfig {
    fn default() -> Self {
        Self {
            output_dir: "./downloads".to_string(), // Will be improved with cache_path_async in new_with_cache()
            preserve_structure: true,
            max_concurrent: 4,
            buffer_size: 8192,
            overwrite: false,
        }
    }
}

/// Download result information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DownloadResult {
    /// Original FTP file path
    pub ftp_path: String,
    /// Local file path where it was saved
    pub local_path: String,
    /// File size in bytes
    pub size_bytes: u64,
    /// Whether the download was successful
    pub success: bool,
    /// Error message if download failed
    pub error: Option<String>,
    /// Download duration in milliseconds
    pub duration_ms: u64,
}

/// Progress callback type for monitoring downloads
pub type ProgressCallback = Arc<dyn Fn(u64, u64, &str) + Send + Sync>;

/// FTP file downloader with progress tracking
#[derive(Clone)]
pub struct FtpDownloader {
    /// FTP provider for connections
    provider: FtpFileSystemProvider,
    /// Download configuration
    config: DownloadConfig,
    /// Optional progress callback
    progress_callback: Option<ProgressCallback>,
}

impl FtpDownloader {
    /// Create a new downloader with DATASUS defaults
    pub fn new_datasus() -> Self {
        Self {
            provider: FtpFileSystemProvider::new_datasus(),
            config: DownloadConfig::default(),
            progress_callback: None,
        }
    }

    /// Create a new DATASUS downloader using cache directory for downloads
    pub async fn new_datasus_with_cache() -> Result<Self> {
        let cache_downloads_path = cache_path_async("downloads").await;
        let config = DownloadConfig {
            output_dir: cache_downloads_path.to_string_lossy().to_string(),
            preserve_structure: true,
            max_concurrent: 4,
            buffer_size: 8192,
            overwrite: false,
        };
        
        Ok(Self {
            provider: FtpFileSystemProvider::new_datasus(),
            config,
            progress_callback: None,
        })
    }

    /// Create a new downloader with custom provider and config
    pub fn new(provider: FtpFileSystemProvider, config: DownloadConfig) -> Self {
        Self {
            provider,
            config,
            progress_callback: None,
        }
    }

    /// Set a progress callback for monitoring download progress
    pub fn with_progress_callback(mut self, callback: ProgressCallback) -> Self {
        self.progress_callback = Some(callback);
        self
    }

    /// Update the download configuration
    pub fn with_config(mut self, config: DownloadConfig) -> Self {
        self.config = config;
        self
    }

    /// Download a single file with progress bar
    pub async fn download_file(&self, file: &File) -> Result<DownloadResult> {
        let start_time = std::time::Instant::now();

        // Determine local path
        let local_path = self.get_local_path(file)?;
        
        // Check if file exists and should not be overwritten
        if path_exists_async(&local_path).await && !self.config.overwrite {
            return Ok(DownloadResult {
                ftp_path: file.path.clone(),
                local_path: local_path.to_string_lossy().to_string(),
                size_bytes: file.size_bytes().unwrap_or(0),
                success: false,
                error: Some("File exists and overwrite is disabled".to_string()),
                duration_ms: start_time.elapsed().as_millis() as u64,
            });
        }

        // Create parent directories if needed
        if let Some(parent) = local_path.parent() {
            ensure_dir_async(parent).await?;
        }

        // Get file size for progress tracking
        let total_size = file.size_bytes().unwrap_or(0);

        // Create progress bar with beautiful styling
        let pb = ProgressBar::new(total_size);
        pb.set_style(
            ProgressStyle::with_template("{msg}\n{spinner:.yellow} [{elapsed_precise}] [{wide_bar:.magenta}] {bytes:>8.blue}/{total_bytes:<8.blue} ({bytes_per_sec:>10.blue}, {eta:>4.blue})")
                .map_err(|e| anyhow!("Failed to set progress bar template: {}", e))?
                .progress_chars("█▉▊▋▌▍▎▏ ")
                .tick_strings(&[
                    "⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"
                ])
        );
        pb.set_message(format!("Downloading {}", file.basename));
        pb.enable_steady_tick(std::time::Duration::from_millis(100));

        // Perform the download
        let result = self.download_file_with_progress(file, &local_path, &pb).await;

        let duration = start_time.elapsed();
        
        match result {
            Ok(bytes_downloaded) => {
                // Verify file was written correctly
                let actual_size = get_file_size_async(&local_path).await.unwrap_or(0);
                let verification_ok = actual_size == bytes_downloaded;
                
                pb.finish_with_message(format!("✓ Downloaded {} ({} bytes){}", 
                    file.basename, 
                    bytes_downloaded,
                    if verification_ok { "" } else { " - Size mismatch!" }
                ));
                
                Ok(DownloadResult {
                    ftp_path: file.path.clone(),
                    local_path: local_path.to_string_lossy().to_string(),
                    size_bytes: bytes_downloaded,
                    success: verification_ok,
                    error: if verification_ok { None } else { Some(format!("Size mismatch: expected {}, got {}", bytes_downloaded, actual_size)) },
                    duration_ms: duration.as_millis() as u64,
                })
            }
            Err(e) => {
                pb.finish_with_message(format!("✗ Failed to download {}", file.basename));
                
                Ok(DownloadResult {
                    ftp_path: file.path.clone(),
                    local_path: local_path.to_string_lossy().to_string(),
                    size_bytes: 0,
                    success: false,
                    error: Some(e.to_string()),
                    duration_ms: duration.as_millis() as u64,
                })
            }
        }
    }

    /// Download multiple files concurrently with beautiful progress bars
    pub async fn download_files(&self, files: Vec<&File>) -> Result<Vec<DownloadResult>> {
        let start_time = std::time::Instant::now();
        
        // Calculate total size for overall progress
        let total_size: u64 = files.iter().map(|f| f.size_bytes().unwrap_or(0)).sum();
        let overall_progress = Arc::new(AtomicU64::new(0));
        
        // Create beautiful colored styles
        let green_bold = Style::new().green().bold();
        let yellow = Style::new().yellow();
        let blue_bold = Style::new().blue().bold();
        let blue = Style::new().blue();
        
        // Create a single MultiProgress instance to manage all progress bars
        let mp = MultiProgress::new();
        
        // Show startup message
        mp.println(format!("    {} Downloading {} files ({:.1} MB total)", 
            blue_bold.apply_to("Starting"), 
            files.len(),
            total_size as f64 / (1024.0 * 1024.0)
        )).unwrap();
        
        // Determine appropriate template based on terminal width
        let term_width = Term::stdout().size().1;
        
        // Create OVERALL progress bar FIRST (at the top) with beautiful styling
        let overall_template = if term_width > 100 {
            "{prefix:>12.yellow.bold} [{wide_bar:.cyan}] {bytes:>10.blue}/{total_bytes:<10.blue} ({bytes_per_sec:>12.blue}) {msg}"
        } else {
            "{prefix:>12.yellow.bold} [{wide_bar:.cyan}] {bytes.blue}/{total_bytes.blue} {msg}"
        };
        
        let overall_pb = mp.add(ProgressBar::new(total_size));
        overall_pb.set_style(
            ProgressStyle::with_template(&overall_template)
                .map_err(|e| anyhow!("Failed to set overall progress bar template: {}", e))?
                .progress_chars("█▉▊▋▌▍▎▏ ")
        );
        overall_pb.set_prefix("Downloading");
        overall_pb.set_message("files...");
        
        // Pre-create all individual progress bars with beautiful styling (yellow spinners, yellow file names, blue bars)
        let mut progress_bars = Vec::new();
        
        let file_template = if term_width > 100 {
            "{spinner:.yellow} {msg:<18.yellow} [{wide_bar:.magenta}] {bytes:>8.blue}/{total_bytes:<8.blue} ({bytes_per_sec:>10.blue}, {eta:>4.blue})"
        } else {
            "{spinner:.yellow} {msg:<12.yellow} [{wide_bar:.magenta}] {bytes.blue}/{total_bytes.blue} ({eta.blue})"
        };
        
        for file in &files {
            let pb = mp.add(ProgressBar::new(file.size_bytes().unwrap_or(0)));
            pb.set_style(
                ProgressStyle::with_template(&file_template)
                    .map_err(|e| anyhow!("Failed to set progress bar template: {}", e))?
                    .progress_chars("█▉▊▋▌▍▎▏ ")
                    .tick_strings(&[
                        "⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"
                    ])
            );
            pb.set_message(format!("{}", file.basename));
            pb.enable_steady_tick(std::time::Duration::from_millis(100));
            progress_bars.push(pb);
        }

        // Convert async method to sync for rayon
        let rt = tokio::runtime::Handle::current();
        let downloader_ref = Arc::new(self.clone());
        let mp_clone = mp.clone();
        
        // Use rayon to download files in parallel with pre-assigned progress bars
        let results: Result<Vec<DownloadResult>> = files
            .par_iter()
            .zip(progress_bars.par_iter())
            .map(|(file, pb)| {
                let overall_pb = overall_pb.clone();
                let overall_progress = overall_progress.clone();
                let downloader = downloader_ref.clone();
                let file = (*file).clone();
                let mp_clone = mp_clone.clone();
                let yellow = yellow.clone();
                let blue = blue.clone();

                // Use tokio block_in_place to run async code in rayon thread
                let result = tokio::task::block_in_place(|| {
                    rt.block_on(async {
                        let start_time = std::time::Instant::now();
                        let local_path = downloader.get_local_path(&file)?;
                        
                        // Check if file exists and should not be overwritten
                        if local_path.exists() && !downloader.config.overwrite {
                            return Ok(DownloadResult {
                                ftp_path: file.path.clone(),
                                local_path: local_path.to_string_lossy().to_string(),
                                size_bytes: file.size_bytes().unwrap_or(0),
                                success: false,
                                error: Some("File exists and overwrite is disabled".to_string()),
                                duration_ms: start_time.elapsed().as_millis() as u64,
                            });
                        }

                        // Create parent directories if needed
                        if let Some(parent) = local_path.parent() {
                            tokio::fs::create_dir_all(parent).await?;
                        }

                        // Show download start message
                        let size_mb = file.size_bytes().unwrap_or(0) as f64 / (1024.0 * 1024.0);
                        mp_clone.println(format!(
                            "    {:>12} {} ({:.1} MB)",
                            yellow.apply_to("Downloading"),
                            file.basename,
                            size_mb
                        )).unwrap();

                        // Download with dual progress tracking (individual + overall)
                        let result = downloader.download_file_with_dual_progress(&file, &local_path, pb, &overall_progress, &overall_pb).await;
                        let duration = start_time.elapsed();

                        match result {
                            Ok(bytes_downloaded) => {
                                let mb_downloaded = bytes_downloaded as f64 / (1024.0 * 1024.0);
                                let speed = mb_downloaded / duration.as_secs_f64();
                                
                                pb.finish_and_clear();
                                
                                mp_clone.println(format!(
                                    "    {:>12} {} {}",
                                    Style::new().green().bold().apply_to("✓ Finished"),
                                    file.basename,
                                    blue.apply_to(format!("({:.1} MB in {} @ {:.1} MB/s)", mb_downloaded, HumanDuration(duration), speed))
                                )).unwrap();
                                
                                Ok(DownloadResult {
                                    ftp_path: file.path.clone(),
                                    local_path: local_path.to_string_lossy().to_string(),
                                    size_bytes: bytes_downloaded,
                                    success: true,
                                    error: None,
                                    duration_ms: duration.as_millis() as u64,
                                })
                            }
                            Err(e) => {
                                pb.finish_and_clear();
                                
                                mp_clone.println(format!(
                                    "    {:>12} {} ({})",
                                    Style::new().red().bold().apply_to("✗ Failed"),
                                    file.basename,
                                    e
                                )).unwrap();
                                
                                Ok(DownloadResult {
                                    ftp_path: file.path.clone(),
                                    local_path: local_path.to_string_lossy().to_string(),
                                    size_bytes: 0,
                                    success: false,
                                    error: Some(e.to_string()),
                                    duration_ms: duration.as_millis() as u64,
                                })
                            }
                        }
                    })
                });

                result
            })
            .collect();

        // Finish overall progress bar with beautiful completion message
        let total_duration = start_time.elapsed();
        let total_mb = total_size as f64 / (1024.0 * 1024.0);
        let avg_speed = total_mb / total_duration.as_secs_f64();
        
        overall_pb.finish_and_clear();
        
        mp.println(format!(
            "    {:>12} {} files {}",
            green_bold.apply_to("✅ Completed"),
            files.len(),
            blue.apply_to(format!("({:.1} MB) in {} @ {:.1} MB/s average", total_mb, HumanDuration(total_duration), avg_speed))
        )).unwrap();

        results
    }

    /// Internal method to download a file with dual progress tracking (individual + overall)
    async fn download_file_with_dual_progress(
        &self,
        file: &File,
        local_path: &Path,
        pb: &ProgressBar,
        overall_progress: &Arc<AtomicU64>,
        overall_pb: &ProgressBar,
    ) -> Result<u64> {
        use suppaftp::{AsyncRustlsFtpStream, Mode, FtpError};

        // Create FTP connection
        let mut ftp_stream = AsyncRustlsFtpStream::connect(&format!("{}:{}", self.provider.host, self.provider.port)).await?;
        ftp_stream.login("anonymous", "").await?;
        ftp_stream.set_mode(Mode::Passive);

        // Navigate to the file's directory
        let ftp_dir = if let Some(parent) = std::path::Path::new(&file.path).parent() {
            parent.to_string_lossy().to_string()
        } else {
            "/".to_string()
        };

        let full_ftp_path = if ftp_dir.starts_with('/') {
            format!("{}{}", self.provider.base_path, ftp_dir)
        } else {
            format!("{}/{}", self.provider.base_path, ftp_dir)
        };

        ftp_stream.cwd(&full_ftp_path).await?;

        // Open file for writing
        let mut local_file = TokioFile::create(local_path).await?;

        // Clone for use in the closure
        let pb_clone = pb.clone();
        let overall_progress_clone = overall_progress.clone();
        let overall_pb_clone = overall_pb.clone();
        let callback = self.progress_callback.clone();
        let file_basename = file.basename.clone();
        let expected_size = file.size_bytes().unwrap_or(0);

        // Use the retr method with a closure for dual progress tracking
        let file_data = ftp_stream
            .retr(&file.basename, move |mut data_stream| {
                let pb_clone = pb_clone.clone();
                let overall_progress_clone = overall_progress_clone.clone();
                let overall_pb_clone = overall_pb_clone.clone();
                let callback = callback.clone();
                let file_basename = file_basename.clone();
                
                Box::pin(async move {
                    let mut file_buffer = Vec::new();
                    let mut total_downloaded = 0u64;
                    let mut chunk_buffer = vec![0u8; 8192]; // Use reasonable buffer size

                    loop {
                        match data_stream.read(&mut chunk_buffer).await {
                            Ok(0) => break, // EOF
                            Ok(n) => {
                                // Append this chunk to our file buffer
                                file_buffer.extend_from_slice(&chunk_buffer[..n]);
                                total_downloaded += n as u64;
                                
                                // Update individual progress bar
                                pb_clone.set_position(total_downloaded);
                                
                                // Update overall progress
                                let current_overall = overall_progress_clone.fetch_add(n as u64, Ordering::SeqCst) + n as u64;
                                overall_pb_clone.set_position(current_overall);

                                // Call progress callback if provided
                                if let Some(ref callback) = callback {
                                    callback(total_downloaded, expected_size, &file_basename);
                                }
                            }
                            Err(e) => return Err(FtpError::ConnectionError(e)),
                        }
                    }

                    // Return all data and the stream
                    Ok((file_buffer, data_stream))
                })
            })
            .await?;

        // Write all data to file
        if !file_data.is_empty() {
            local_file.write_all(&file_data).await?;
        }

        // Close FTP connection
        let _ = ftp_stream.quit().await;

        Ok(file_data.len() as u64)
    }

    /// Internal method to download a file with progress tracking
    async fn download_file_with_progress(
        &self,
        file: &File,
        local_path: &Path,
        pb: &ProgressBar,
    ) -> Result<u64> {
        use suppaftp::{AsyncRustlsFtpStream, Mode, FtpError};

        // Create FTP connection
        let mut ftp_stream = AsyncRustlsFtpStream::connect(&format!("{}:{}", self.provider.host, self.provider.port)).await?;
        ftp_stream.login("anonymous", "").await?;
        ftp_stream.set_mode(Mode::Passive);

        // Navigate to the file's directory
        let ftp_dir = if let Some(parent) = std::path::Path::new(&file.path).parent() {
            parent.to_string_lossy().to_string()
        } else {
            "/".to_string()
        };

        let full_ftp_path = if ftp_dir.starts_with('/') {
            format!("{}{}", self.provider.base_path, ftp_dir)
        } else {
            format!("{}/{}", self.provider.base_path, ftp_dir)
        };

        ftp_stream.cwd(&full_ftp_path).await?;

        // Create local file
        let mut local_file = TokioFile::create(local_path).await?;

        // Progress tracking variables
        let pb_clone = pb.clone();
        let callback = self.progress_callback.clone();
        let file_basename = file.basename.clone();
        let expected_size = file.size_bytes().unwrap_or(0);

        // Use the retr method with a closure for progress tracking
        let file_data = ftp_stream
            .retr(&file.basename, move |mut data_stream| {
                let pb_clone = pb_clone.clone();
                let callback = callback.clone();
                let file_basename = file_basename.clone();
                
                Box::pin(async move {
                    let mut file_buffer = Vec::new();
                    let mut total_downloaded = 0u64;
                    let mut chunk_buffer = vec![0u8; 8192]; // Use reasonable buffer size

                    loop {
                        match data_stream.read(&mut chunk_buffer).await {
                            Ok(0) => break, // EOF
                            Ok(n) => {
                                // Append this chunk to our file buffer
                                file_buffer.extend_from_slice(&chunk_buffer[..n]);
                                total_downloaded += n as u64;
                                
                                // Update progress bar
                                pb_clone.set_position(total_downloaded);

                                // Call progress callback if provided
                                if let Some(ref callback) = callback {
                                    callback(total_downloaded, expected_size, &file_basename);
                                }
                            }
                            Err(e) => return Err(FtpError::ConnectionError(e)),
                        }
                    }

                    // Return all data and the stream
                    Ok((file_buffer, data_stream))
                })
            })
            .await?;

        // Write all data to file
        if !file_data.is_empty() {
            local_file.write_all(&file_data).await?;
        }

        // Close FTP connection
        let _ = ftp_stream.quit().await;

        Ok(file_data.len() as u64)
    }

    /// Get the local path for a file based on the configuration
    fn get_local_path(&self, file: &File) -> Result<std::path::PathBuf> {
        let mut local_path = std::path::PathBuf::from(&self.config.output_dir);

        if self.config.preserve_structure {
            // Remove the base path from the FTP path to get relative path
            let relative_path = if file.path.starts_with(&self.provider.base_path) {
                file.path.strip_prefix(&self.provider.base_path)
                    .unwrap_or(&file.path)
                    .trim_start_matches('/')
            } else {
                file.path.trim_start_matches('/')
            };

            // If there's a directory structure, add it
            if let Some(parent) = std::path::Path::new(relative_path).parent() {
                if !parent.as_os_str().is_empty() {
                    local_path.push(parent);
                }
            }
        }

        local_path.push(&file.basename);
        Ok(local_path)
    }

    /// Create a silent progress callback (indicatif handles visual progress)
    pub fn create_console_progress_callback() -> ProgressCallback {
        Arc::new(|_downloaded: u64, _total: u64, _filename: &str| {
            // No console output - indicatif handles the visual progress display
        })
    }
}

/// Convenience functions for common download scenarios

/// Download a single DATASUS file to the default downloads directory
pub async fn download_datasus_file(file: &File) -> Result<DownloadResult> {
    let downloader = FtpDownloader::new_datasus();
    downloader.download_file(file).await
}

/// Download multiple DATASUS files concurrently to the default downloads directory
pub async fn download_datasus_files(files: Vec<&File>) -> Result<Vec<DownloadResult>> {
    let downloader = FtpDownloader::new_datasus();
    downloader.download_files(files).await
}

/// Download DATASUS files to a specific directory with custom configuration
pub async fn download_datasus_files_with_config(
    files: Vec<&File>,
    config: DownloadConfig,
) -> Result<Vec<DownloadResult>> {
    let downloader = FtpDownloader::new_datasus().with_config(config);
    downloader.download_files(files).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::file_info::{FileInfo, FileSize};
    use chrono::Utc;

    fn create_test_file() -> File {
        let info = FileInfo::new(
            FileSize::from_bytes(1024),
            ".txt".to_string(),
            Utc::now(),
        );
        File::new("/test/path", "test_file.txt", info)
    }

    #[test]
    fn test_download_config_default() {
        let config = DownloadConfig::default();
        assert_eq!(config.output_dir, "./downloads");
        assert!(config.preserve_structure);
        assert_eq!(config.max_concurrent, 4);
        assert_eq!(config.buffer_size, 8192);
        assert!(!config.overwrite);
    }

    #[test]
    fn test_downloader_creation() {
        let downloader = FtpDownloader::new_datasus();
        assert_eq!(downloader.provider.host, "ftp.datasus.gov.br");
        assert_eq!(downloader.provider.base_path, "/dissemin/publicos");
        assert_eq!(downloader.config.output_dir, "./downloads");
    }

    #[test]
    fn test_local_path_generation() {
        let downloader = FtpDownloader::new_datasus();
        let file = create_test_file();
        
        let local_path = downloader.get_local_path(&file).unwrap();
        
        // Should preserve structure by default
        assert!(local_path.to_string_lossy().contains("downloads"));
        assert!(local_path.to_string_lossy().ends_with("test_file.txt"));
    }

    #[test]
    fn test_local_path_no_structure() {
        let mut config = DownloadConfig::default();
        config.preserve_structure = false;
        
        let downloader = FtpDownloader::new_datasus().with_config(config);
        let file = create_test_file();
        
        let local_path = downloader.get_local_path(&file).unwrap();
        
        // Should only contain filename without path structure
        assert_eq!(local_path.file_name().unwrap().to_str().unwrap(), "test_file.txt");
    }

    #[test]
    fn test_progress_callback_creation() {
        let callback = FtpDownloader::create_console_progress_callback();
        
        // Test that callback can be called without panicking
        callback(512, 1024, "test.txt");
        callback(1024, 1024, "test.txt");
        callback(100, 0, "unknown_size.txt"); // Test with unknown total
    }

    #[tokio::test]
    async fn test_datasus_with_cache_constructor() {
        let downloader = FtpDownloader::new_datasus_with_cache().await;
        assert!(downloader.is_ok());
        
        let downloader = downloader.unwrap();
        
        // Should use cache directory for downloads
        assert!(downloader.config.output_dir.contains("downloads"));
        assert!(downloader.config.preserve_structure);
        assert_eq!(downloader.config.max_concurrent, 4);
        assert_eq!(downloader.config.buffer_size, 8192);
        assert!(!downloader.config.overwrite);
    }

    #[tokio::test]
    async fn test_real_ftp_download() {
        use crate::models::directory::{FtpFileSystemProvider, DirectoryEntry, FileSystemProvider};
        
        // Only run this test if explicitly enabled
        if std::env::var("RUN_INTEGRATION_TESTS").is_err() {
            return;
        }

        println!("🧪 Running real FTP download test...");
        
        // Create FTP provider
        let ftp_provider = FtpFileSystemProvider::new_datasus();
        
        // List a directory to get real files
        let directory_content = match ftp_provider.list_directory("/SIHSUS/200801_/Dados").await {
            Ok(content) => content,
            Err(e) => {
                println!("❌ Failed to list directory: {}", e);
                return;
            }
        };
        
        // Find a small .dbc file
        let mut test_file = None;
        for (_name, entry) in directory_content.iter() {
            if let DirectoryEntry::File(file) = entry {
                if file.has_extension("dbc") {
                    if let Some(size) = file.size_bytes() {
                        if size < 1024 * 100 { // Less than 100KB
                            test_file = Some(file);
                            break;
                        }
                    }
                }
            }
        }

        let file = match test_file {
            Some(f) => f,
            None => {
                println!("❌ No small .dbc files found for testing");
                return;
            }
        };

        println!("📄 Testing download of: {} ({} bytes)", file.basename, file.size_bytes().unwrap_or(0));

        // Create temporary directory for test downloads
        let temp_dir = std::env::temp_dir().join("arrow_sus_test_downloads");
        std::fs::create_dir_all(&temp_dir).unwrap();
        
        // Create downloader
        let config = DownloadConfig {
            output_dir: temp_dir.to_string_lossy().to_string(),
            preserve_structure: false,
            max_concurrent: 1,
            buffer_size: 4096,
            overwrite: true,
        };

        let downloader = FtpDownloader::new_datasus().with_config(config);

        // Try to download
        match downloader.download_file(file).await {
            Ok(result) => {
                println!("✅ Download successful: {} -> {} ({} bytes in {}ms)", 
                    result.ftp_path, 
                    result.local_path, 
                    result.size_bytes,
                    result.duration_ms
                );
                
                // Verify file exists locally
                assert!(std::path::Path::new(&result.local_path).exists());
                assert!(result.success);
                assert!(result.size_bytes > 0);
            }
            Err(e) => {
                println!("❌ Download failed: {}", e);
                panic!("Download test failed: {}", e);
            }
        }
    }

    #[tokio::test]
    async fn test_large_file_download() {
        use crate::models::directory::{FtpFileSystemProvider, DirectoryEntry, FileSystemProvider};
        
        // Only run this test if explicitly enabled
        if std::env::var("RUN_LARGE_DOWNLOAD_TEST").is_err() {
            return;
        }

        println!("🧪 Running large file download test...");
        
        // Create FTP provider
        let ftp_provider = FtpFileSystemProvider::new_datasus();
        
        // List the specific directory to find PAPA2404.dbc
        let directory_content = match ftp_provider.list_directory("/SIASUS/200801_/Dados").await {
            Ok(content) => content,
            Err(e) => {
                println!("❌ Failed to list directory: {}", e);
                return;
            }
        };
        
        // Find the specific file PAPA2404.dbc
        let mut target_file = None;
        for (_name, entry) in directory_content.iter() {
            if let DirectoryEntry::File(file) = entry {
                if file.basename == "PAPA2404.dbc" {
                    target_file = Some(file);
                    break;
                }
            }
        }

        let file = match target_file {
            Some(f) => f,
            None => {
                println!("❌ PAPA2404.dbc not found in /SIASUS/200801_/Dados");
                // List what files are available for debugging
                println!("Available .dbc files:");
                for (_name, entry) in directory_content.iter().take(10) {
                    if let DirectoryEntry::File(file) = entry {
                        if file.has_extension("dbc") {
                            println!("  - {} ({} bytes)", file.basename, file.size_bytes().unwrap_or(0));
                        }
                    }
                }
                return;
            }
        };

        println!("📄 Testing download of large file: {} ({} bytes)", 
            file.basename, 
            file.size_bytes().unwrap_or(0)
        );

        // Create temporary directory for large test downloads
        let temp_dir = std::env::temp_dir().join("arrow_sus_large_downloads");
        std::fs::create_dir_all(&temp_dir).unwrap();
        
        // Create downloader with progress callback
        let config = DownloadConfig {
            output_dir: temp_dir.to_string_lossy().to_string(),
            preserve_structure: false,
            max_concurrent: 1,
            buffer_size: 16384, // Larger buffer for big file
            overwrite: true,
        };

        let downloader = FtpDownloader::new_datasus()
            .with_config(config)
            .with_progress_callback(FtpDownloader::create_console_progress_callback());

        let start_time = std::time::Instant::now();

        // Try to download
        match downloader.download_file(file).await {
            Ok(result) => {
                // Add newline after progress bar
                println!();
                
                let elapsed = start_time.elapsed();
                let mb_downloaded = result.size_bytes as f64 / (1024.0 * 1024.0);
                let speed_mbps = mb_downloaded / elapsed.as_secs_f64();

                println!("✅ Large download successful!");
                println!("   File: {} -> {}", result.ftp_path, result.local_path);
                println!("   Size: {:.2} MB ({} bytes)", mb_downloaded, result.size_bytes);
                println!("   Time: {:.2}s", elapsed.as_secs_f64());
                println!("   Speed: {:.2} MB/s", speed_mbps);
                
                // Verify file exists locally
                assert!(std::path::Path::new(&result.local_path).exists());
                assert!(result.success);
                assert!(result.size_bytes > 0);
                
                // Check file size matches what we expected
                let local_file_size = std::fs::metadata(&result.local_path).unwrap().len();
                assert_eq!(local_file_size, result.size_bytes);
                
                println!("✅ File integrity verified!");
            }
            Err(e) => {
                println!("❌ Large download failed: {}", e);
                panic!("Large download test failed: {}", e);
            }
        }
    }

    #[tokio::test]
    async fn test_multiple_large_files_download() {
        use crate::models::directory::{FtpFileSystemProvider, DirectoryEntry, FileSystemProvider};
        
        // Only run this test if explicitly enabled
        if std::env::var("RUN_MULTI_LARGE_DOWNLOAD_TEST").is_err() {
            return;
        }

        println!("🧪 Running multiple large files download test...");
        
        // Create FTP provider
        let ftp_provider = FtpFileSystemProvider::new_datasus();
        
        // List the directory to find multiple large files
        let directory_content = match ftp_provider.list_directory("/SIASUS/200801_/Dados").await {
            Ok(content) => content,
            Err(e) => {
                println!("❌ Failed to list directory: {}", e);
                return;
            }
        };
        
        // Find multiple large .dbc files (at least 5MB each)
        let mut large_files = Vec::new();
        for (_name, entry) in directory_content.iter() {
            if let DirectoryEntry::File(file) = entry {
                if file.has_extension("dbc") {
                    if let Some(size) = file.size_bytes() {
                        if size > 1024 * 1024 * 5 { // More than 5MB
                            large_files.push(file);
                        }
                    }
                }
            }
        }

        // Take the first 3 large files for testing
        large_files.truncate(3);

        if large_files.len() < 2 {
            println!("❌ Not enough large files found for testing (need at least 2, found {})", large_files.len());
            // List what files are available for debugging
            println!("Available large .dbc files (>5MB):");
            for (_name, entry) in directory_content.iter() {
                if let DirectoryEntry::File(file) = entry {
                    if file.has_extension("dbc") {
                        if let Some(size) = file.size_bytes() {
                            if size > 1024 * 1024 * 5 {
                                println!("  - {} ({:.1} MB)", file.basename, size as f64 / (1024.0 * 1024.0));
                            }
                        }
                    }
                }
            }
            return;
        }

        let total_size: u64 = large_files.iter()
            .map(|f| f.size_bytes().unwrap_or(0))
            .sum();
        let total_mb = total_size as f64 / (1024.0 * 1024.0);

        println!("📦 Testing download of {} large files (total: {:.1} MB):", large_files.len(), total_mb);
        for file in &large_files {
            let size_mb = file.size_bytes().unwrap_or(0) as f64 / (1024.0 * 1024.0);
            println!("  - {} ({:.1} MB)", file.basename, size_mb);
        }

        // Create temporary directory for multi large test downloads
        let temp_dir = std::env::temp_dir().join("arrow_sus_multi_large_downloads");
        std::fs::create_dir_all(&temp_dir).unwrap();
        
        // Create downloader with concurrent downloads enabled
        let config = DownloadConfig {
            output_dir: temp_dir.to_string_lossy().to_string(),
            preserve_structure: false,
            max_concurrent: 2, // Download 2 files at once
            buffer_size: 16384, // Larger buffer for big files
            overwrite: true,
        };

        let downloader = FtpDownloader::new_datasus().with_config(config);

        let start_time = std::time::Instant::now();

        // Try to download all files concurrently
        match downloader.download_files(large_files.clone()).await {
            Ok(results) => {
                let elapsed = start_time.elapsed();
                let total_downloaded: u64 = results.iter()
                    .filter(|r| r.success)
                    .map(|r| r.size_bytes)
                    .sum();
                let total_mb_downloaded = total_downloaded as f64 / (1024.0 * 1024.0);
                let avg_speed = total_mb_downloaded / elapsed.as_secs_f64();

                println!("\n✅ Multiple large files download completed!");
                println!("   Total files: {}", results.len());
                println!("   Successful downloads: {}", results.iter().filter(|r| r.success).count());
                println!("   Failed downloads: {}", results.iter().filter(|r| !r.success).count());
                println!("   Total size: {:.1} MB ({} bytes)", total_mb_downloaded, total_downloaded);
                println!("   Total time: {:.2}s", elapsed.as_secs_f64());
                println!("   Average speed: {:.2} MB/s", avg_speed);

                println!("\n📊 Individual file results:");
                for result in &results {
                    let mb_size = result.size_bytes as f64 / (1024.0 * 1024.0);
                    let speed = mb_size / (result.duration_ms as f64 / 1000.0);
                    
                    if result.success {
                        println!("  ✓ {} - {:.1}MB in {:.2}s ({:.2}MB/s)", 
                            std::path::Path::new(&result.local_path).file_name().unwrap().to_str().unwrap(),
                            mb_size, 
                            result.duration_ms as f64 / 1000.0,
                            speed
                        );
                        
                        // Verify file exists locally and has correct size
                        assert!(std::path::Path::new(&result.local_path).exists());
                        let local_file_size = std::fs::metadata(&result.local_path).unwrap().len();
                        assert_eq!(local_file_size, result.size_bytes);
                    } else {
                        println!("  ✗ {} - Error: {}", 
                            result.local_path,
                            result.error.as_ref().unwrap_or(&"Unknown error".to_string())
                        );
                    }
                }

                // Verify all downloads were successful
                let failed_count = results.iter().filter(|r| !r.success).count();
                assert_eq!(failed_count, 0, "Some downloads failed");
                
                // Verify we downloaded what we expected
                assert_eq!(results.len(), large_files.len());

                println!("\n✅ All file integrity checks passed!");
            }
            Err(e) => {
                println!("❌ Multiple large files download failed: {}", e);
                panic!("Multiple download test failed: {}", e);
            }
        }
    }
}
