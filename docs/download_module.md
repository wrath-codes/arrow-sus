# Download Module

## Overview

The download module provides a comprehensive file download system that integrates with the existing FTP abstraction and uses indicatif for progress visualization. Built without modifying any existing code, this module adds powerful download capabilities to the arrow-sus project.

## Features

### Core Functionality

- **Single file downloads** with progress tracking
- **Batch directory downloads** with recursive file discovery
- **Concurrent downloads** with configurable limits
- **Retry logic** with exponential backoff
- **Progress visualization** with indicatif progress bars

### Progress Tracking Modes

- **Off**: No progress tracking
- **Single**: Single progress bar for total bytes
- **Multi**: Multiple progress bars (one per file + overall progress)

### Provider Support

- **FTP Downloads**: Full integration with existing `FtpFileSystemProvider`
- **Local File System**: Copy operations for local files
- **Extensible**: Easy to add support for other providers (HTTP, S3, etc.)

## Architecture

### Key Components

#### `Downloader<P: FileSystemProvider>`

The main orchestrator that manages downloads with any file system provider.

#### `DownloadOptions`

Configuration for download behavior:

```rust
pub struct DownloadOptions {
    pub overwrite: bool,           // Overwrite existing files
    pub concurrency: usize,        // Max concurrent downloads (default: 3)
    pub progress: ProgressMode,    // Progress tracking mode
    pub dest_dir: PathBuf,         // Destination directory
    pub retries: usize,           // Retry attempts (default: 3)
    pub retry_delay_ms: u64,      // Delay between retries
    pub max_depth: Option<usize>, // Max depth for recursive downloads
    pub buffer_size: usize,       // Transfer buffer size (default: 64KB)
}
```

#### `DownloadItem`

Represents what to download:

```rust
pub enum DownloadItem {
    File(File),                   // Single file
    Directory(Directory),         // Directory (recursive)
}
```

#### `ProgressManager`

Handles progress bar creation and management for different modes.

#### `FtpDownloadBackend`

FTP-specific implementation that reuses existing FTP connections and caching.

### Integration Points

The download module integrates seamlessly with existing components:

- **FileSystemProvider trait**: Used for metadata queries and file discovery
- **FtpFileSystemProvider**: Reuses connection management and caching
- **File and Directory models**: Works with existing data structures
- **Async utilities**: Leverages existing async patterns

## Usage Examples

### Simple File Download

```rust
use shared::models::download::download_file_ftp;

// Download a single file from DATASUS FTP
let stats = download_file_ftp(file, "./downloads").await?;
println!("Downloaded {} bytes", stats.bytes_downloaded);
```

### Advanced Multi-File Download

```rust
use shared::models::{
    directory::FtpFileSystemProvider,
    download::{Downloader, DownloadOptions, DownloadItem, ProgressMode}
};

let provider = Arc::new(FtpFileSystemProvider::new_datasus());
let options = DownloadOptions {
    progress: ProgressMode::Multi,
    concurrency: 4,
    dest_dir: "./downloads".into(),
    ..Default::default()
};

let downloader = Downloader::new(provider, options);
let items = vec![DownloadItem::File(file1), DownloadItem::File(file2)];
let stats = downloader.download_items(items).await?;
```

### Directory Download

```rust
// Download entire directory recursively
let directory = Directory::new("/SIASUS/200801_/Dados".to_string()).await?;
let stats = downloader.download_item(
    DownloadItem::Directory(directory),
    "./downloads"
).await?;
```

## Performance Features

### Concurrency Control

- Configurable number of parallel downloads
- Uses `futures::stream::buffer_unordered` for efficient async processing
- Respects FTP server connection limits

### Caching Integration

- Reuses existing FTP connection caching
- Leverages directory listing cache from the existing system
- Minimal FTP connections for maximum efficiency

### Progress Visualization

- Real-time progress bars using indicatif
- Bytes per second calculations
- ETA estimates for large downloads

### Error Handling

- Comprehensive retry logic with exponential backoff
- Graceful handling of network interruptions
- Detailed error reporting with context

## File Structure

```
src/models/download.rs          # Main download module
examples/download_example.rs    # Comprehensive usage example
```

The module is fully integrated into the existing `mod.rs` structure and exports all public APIs.

## Testing

The module includes:

- **Unit tests** for core functionality
- **Integration tests** with temporary directories
- **Example application** demonstrating all features
- **Real-world FTP testing** with DATASUS server

## Future Enhancements

The modular architecture makes it easy to add:

- **HTTP/HTTPS downloads** via reqwest integration
- **S3 downloads** with AWS SDK
- **Resume capability** for interrupted downloads
- **Checksum verification** for downloaded files
- **Download queuing** and scheduling
- **Bandwidth throttling** for rate limiting

## Dependencies

The module uses existing project dependencies:

- `indicatif` (0.18.0) - Progress bars
- `tokio` - Async runtime
- `futures` - Stream processing
- `suppaftp` - FTP client (via existing integration)
- `serde` - Serialization

No additional dependencies were added to implement this functionality.

## Conclusion

The download module successfully adds robust file download capabilities to arrow-sus while:

- **Maintaining backward compatibility** - No existing code was modified
- **Following existing patterns** - Uses established async and error handling patterns
- **Leveraging existing infrastructure** - Builds on FTP abstraction and caching
- **Providing excellent UX** - Progress tracking and detailed feedback
- **Being extensible** - Easy to add new download sources and features

The implementation demonstrates how new features can be added to the existing codebase in a clean, non-intrusive way while providing significant value to users.
