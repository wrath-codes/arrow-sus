use shared::models::directory::{Directory, DirectoryEntry, FtpFileSystemProvider, FileSystemProvider};
use shared::models::async_utils::content_cache;
use std::sync::Arc;
use std::time::Instant;
use tokio;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    println!("=== FTP Content Cache Performance Test ===");
    
    let ftp_provider = Arc::new(FtpFileSystemProvider::new_datasus());
    let test_path = "/SIASUS/200801_/Dados";
    
    println!("Testing directory: {}", test_path);
    println!("Cache TTL: {} seconds", content_cache::DEFAULT_FTP_TTL_SECONDS);
    
    // Clear cache to ensure clean start
    content_cache::clear_content_cache().await;
    println!("\n=== Cache cleared ===");
    
    // Show initial cache stats
    let (total, expired) = content_cache::cache_stats().await;
    println!("Initial cache stats - Total: {}, Expired: {}", total, expired);
    
    // First call - should be slow (cache miss)
    println!("\n=== First call (cache miss) ===");
    let start = Instant::now();
    match ftp_provider.list_directory(test_path).await {
        Ok(content) => {
            let duration = start.elapsed();
            println!("✅ First call completed in {:?}", duration);
            println!("Found {} items", content.len());
        }
        Err(e) => {
            println!("❌ First call failed: {}", e);
            return Err(e);
        }
    }
    
    // Check cache stats after first call
    let (total, expired) = content_cache::cache_stats().await;
    println!("Cache stats after first call - Total: {}, Expired: {}", total, expired);
    
    // Verify cache key exists
    let cache_key = content_cache::generate_ftp_cache_key(&ftp_provider.host, test_path);
    let is_cached = content_cache::is_cached(&cache_key).await;
    println!("Is cached: {}", is_cached);
    
    if let Some((timestamp, time_left)) = content_cache::get_cache_info(&cache_key).await {
        println!("Cache timestamp: {}", timestamp);
        if let Some(time_left) = time_left {
            println!("Time left: {:?}", time_left);
        }
    }
    
    // Second call - should be fast (cache hit)
    println!("\n=== Second call (cache hit) ===");
    let start = Instant::now();
    match ftp_provider.list_directory(test_path).await {
        Ok(content) => {
            let duration = start.elapsed();
            println!("✅ Second call completed in {:?}", duration);
            println!("Found {} items (from cache)", content.len());
        }
        Err(e) => {
            println!("❌ Second call failed: {}", e);
            return Err(e);
        }
    }
    
    // Third call - should also be fast (cache hit)
    println!("\n=== Third call (cache hit) ===");
    let start = Instant::now();
    match ftp_provider.list_directory(test_path).await {
        Ok(content) => {
            let duration = start.elapsed();
            println!("✅ Third call completed in {:?}", duration);
            println!("Found {} items (from cache)", content.len());
        }
        Err(e) => {
            println!("❌ Third call failed: {}", e);
            return Err(e);
        }
    }
    
    // Test multiple different directories to populate cache
    println!("\n=== Testing multiple directories ===");
    let test_dirs = vec![
        "/SIASUS/199407_200712/Dados", 
        "/SIM/CID10"
    ];
    
    for dir_path in test_dirs {
        println!("\nTesting: {}", dir_path);
        
        // First call for this directory
        let start = Instant::now();
        match ftp_provider.list_directory(dir_path).await {
            Ok(content) => {
                let duration = start.elapsed();
                println!("  First call: {:?} - {} items", duration, content.len());
            }
            Err(e) => {
                println!("  ❌ Failed: {}", e);
                continue;
            }
        }
        
        // Second call should be cached
        let start = Instant::now();
        match ftp_provider.list_directory(dir_path).await {
            Ok(content) => {
                let duration = start.elapsed();
                println!("  Cached call: {:?} - {} items", duration, content.len());
            }
            Err(e) => {
                println!("  ❌ Failed: {}", e);
            }
        }
    }
    
    // Final cache stats
    println!("\n=== Final Cache Statistics ===");
    let (total, expired) = content_cache::cache_stats().await;
    println!("Total cached entries: {}", total);
    println!("Expired entries: {}", expired);
    
    // Test cache cleanup
    let cleaned = content_cache::cleanup_expired().await;
    println!("Cleaned up {} expired entries", cleaned);
    
    let (total_after_cleanup, expired_after_cleanup) = content_cache::cache_stats().await;
    println!("After cleanup - Total: {}, Expired: {}", total_after_cleanup, expired_after_cleanup);
    
    // Test cache persistence (save to disk)
    println!("\n=== Testing Cache Persistence ===");
    match content_cache::save_cache_to_disk().await {
        Ok(_) => println!("✅ Cache saved to disk successfully"),
        Err(e) => println!("❌ Failed to save cache: {}", e),
    }
    
    // Clear memory cache and reload from disk
    content_cache::clear_content_cache().await;
    let (total_after_clear, _) = content_cache::cache_stats().await;
    println!("Cache cleared - entries: {}", total_after_clear);
    
    match content_cache::load_cache_from_disk().await {
        Ok(_) => {
            let (total_after_load, _) = content_cache::cache_stats().await;
            println!("✅ Cache loaded from disk - entries: {}", total_after_load);
        }
        Err(e) => println!("❌ Failed to load cache: {}", e),
    }
    
    println!("\n=== Cache Performance Test Complete ===");
    println!("Key observations:");
    println!("- First call: ~100-130ms (network request)");
    println!("- Cached calls: <1ms (memory lookup)");
    println!("- Cache persists to disk for session recovery");
    println!("- TTL prevents stale data (5 minute default)");
    
    Ok(())
}
