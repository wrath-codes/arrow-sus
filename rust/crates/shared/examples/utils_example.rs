use shared::models::{to_vec, to_vec_async, path_utils, async_path_utils, cache, async_cache};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Sync Utils Examples ===");
    
    // Sync version examples
    let sync_vec = to_vec(Some(42));
    println!("Sync to_vec(Some(42)): {:?}", sync_vec);
    
    let sync_empty = to_vec(None::<String>);
    println!("Sync to_vec(None): {:?}", sync_empty);
    
    // Path utilities
    let cache_dir = path_utils::get_cache_dir();
    println!("Cache directory: {}", cache_dir.display());
    
    let test_path = path_utils::cache_path("test.txt");
    println!("Test file path: {}", test_path.display());
    
    // Cache operations
    cache::cache_directory("sync_key".to_string(), "sync_value".to_string());
    if let Some(value) = cache::get_cached_directory("sync_key") {
        println!("Cached value (sync): {}", value);
    }
    println!("Cache size (sync): {}", cache::cache_size());
    
    println!("\n=== Async Utils Examples ===");
    
    // Async version examples
    let async_vec = to_vec_async(Some(42)).await;
    println!("Async to_vec(Some(42)): {:?}", async_vec);
    
    let async_empty = to_vec_async(None::<String>).await;
    println!("Async to_vec(None): {:?}", async_empty);
    
    // Async path utilities
    let async_cache_dir = async_path_utils::get_cache_dir_async().await;
    println!("Async cache directory: {}", async_cache_dir.display());
    
    let async_test_path = async_path_utils::cache_path_async("async_test.txt").await;
    println!("Async test file path: {}", async_test_path.display());
    
    // Initialize cache directory
    match async_path_utils::ensure_cache_dir_async().await {
        Ok(path) => println!("Cache directory ensured: {}", path.display()),
        Err(e) => println!("Error ensuring cache directory: {}", e),
    }
    
    // Async cache operations
    async_cache::cache_directory_async("async_key".to_string(), "async_value".to_string()).await;
    if let Some(value) = async_cache::get_cached_directory_async("async_key").await {
        println!("Cached value (async): {}", value);
    }
    println!("Cache size (async): {}", async_cache::cache_size_async().await);
    
    // File operations
    let current_dir = std::path::PathBuf::from(".");
    let exists = async_path_utils::path_exists_async(&current_dir).await;
    println!("Current directory exists: {}", exists);
    
    let is_dir = async_path_utils::is_dir_async(&current_dir).await;
    println!("Current path is directory: {}", is_dir);
    
    // List directory contents
    match async_path_utils::list_dir_async(&current_dir).await {
        Ok(entries) => {
            println!("Directory entries: {} items", entries.len());
            for (i, entry) in entries.iter().take(3).enumerate() {
                println!("  {}: {}", i + 1, entry.display());
            }
            if entries.len() > 3 {
                println!("  ... and {} more", entries.len() - 3);
            }
        }
        Err(e) => println!("Error listing directory: {}", e),
    }
    
    println!("\n=== Stream Processing Example ===");
    
    // Stream utilities
    use shared::models::async_stream_utils;
    use futures::stream;
    
    let data = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
    let stream = stream::iter(data.clone());
    
    let chunks = async_stream_utils::stream_to_chunks(stream, 3).await;
    println!("Stream processed into {} chunks:", chunks.len());
    for (i, chunk) in chunks.iter().enumerate() {
        println!("  Chunk {}: {:?}", i + 1, chunk);
    }
    
    // Clean up test keys
    cache::clear_cache();
    async_cache::clear_cache_async().await;
    
    Ok(())
}
