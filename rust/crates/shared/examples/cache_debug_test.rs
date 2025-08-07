use shared::models::directory::{FtpFileSystemProvider, FileSystemProvider};
use shared::models::async_utils::content_cache;
use std::sync::Arc;
use std::time::Instant;
use tokio;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    println!("=== Cache Debug Test ===");
    
    let ftp_provider = Arc::new(FtpFileSystemProvider::new_datasus());
    let test_path = "/SIM/CID10"; // Small directory for testing
    
    // Clear cache
    content_cache::clear_content_cache().await;
    
    println!("Testing small directory: {}", test_path);
    
    // First call - should populate cache
    println!("\n=== First call ===");
    let start = Instant::now();
    let result1 = ftp_provider.list_directory(test_path).await?;
    let duration1 = start.elapsed();
    println!("First call: {:?} - {} items", duration1, result1.len());
    
    // Check if cache was populated
    let cache_key = content_cache::generate_ftp_cache_key(&ftp_provider.host, test_path);
    println!("Cache key: {}", cache_key);
    
    let is_cached = content_cache::is_cached(&cache_key).await;
    println!("Is cached: {}", is_cached);
    
    // Try getting from cache directly
    println!("\n=== Direct cache test ===");
    let start = Instant::now();
    let cached_content = content_cache::get_cached_content(&cache_key).await;
    let cache_get_duration = start.elapsed();
    println!("Cache get duration: {:?}", cache_get_duration);
    
    if let Some(cached_str) = cached_content {
        println!("Found cached content, length: {}", cached_str.len());
        
        // Try deserializing
        let start = Instant::now();
        let deserialization_result = serde_json::from_str::<std::collections::HashMap<String, shared::models::directory::DirectoryEntry>>(&cached_str);
        let deserialize_duration = start.elapsed();
        println!("Deserialization duration: {:?}", deserialize_duration);
        
        match deserialization_result {
            Ok(content) => {
                println!("✅ Deserialization successful - {} items", content.len());
            }
            Err(e) => {
                println!("❌ Deserialization failed: {}", e);
                println!("First 200 chars of cached content: {}", &cached_str[..cached_str.len().min(200)]);
            }
        }
    } else {
        println!("❌ No cached content found");
    }
    
    // Second call through FTP provider
    println!("\n=== Second call (should use cache) ===");
    let start = Instant::now();
    let result2 = ftp_provider.list_directory(test_path).await?;
    let duration2 = start.elapsed();
    println!("Second call: {:?} - {} items", duration2, result2.len());
    
    // Compare results
    if result1.len() == result2.len() {
        println!("✅ Results consistent between calls");
    } else {
        println!("❌ Results differ: {} vs {}", result1.len(), result2.len());
    }
    
    Ok(())
}
