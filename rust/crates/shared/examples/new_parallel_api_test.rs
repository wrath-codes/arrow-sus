use shared::models::directory::{FtpFileSystemProvider, FileSystemProvider};
use std::sync::Arc;
use std::time::Instant;
use tokio;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    println!("=== Testing New Parallel API ===");
    
    let ftp_provider = Arc::new(FtpFileSystemProvider::new_datasus());
    
    let test_directories = vec![
        "/SIASUS/199407_200712/Dados",
        "/SIASUS/200801_/Dados",
        "/SIM/CID10",
    ];
    
    println!("Testing {} directories", test_directories.len());
    
    // Test 1: Using the trait method (available on any FileSystemProvider)
    println!("\n🔧 Testing trait method: list_directories_parallel");
    let start = Instant::now();
    let results = ftp_provider.list_directories_parallel(test_directories.clone()).await;
    let duration = start.elapsed();
    
    println!("✅ Trait method completed in {:?}", duration);
    for (path, result) in &results {
        match result {
            Ok(content) => println!("  {} - {} items", path, content.len()),
            Err(e) => println!("  {} - ❌ Error: {}", path, e),
        }
    }
    
    // Test 2: Using the enhanced method with timing (FTP-specific)
    println!("\n⏱️  Testing enhanced method: list_directories_with_timing");
    let start = Instant::now();
    let timed_results = ftp_provider.list_directories_with_timing(test_directories.clone()).await;
    let total_duration = start.elapsed();
    
    println!("✅ Enhanced method completed in {:?}", total_duration);
    for (path, result, individual_duration) in &timed_results {
        match result {
            Ok(content) => println!("  {} - {} items in {:?}", path, content.len(), individual_duration),
            Err(e) => println!("  {} - ❌ Error in {:?}: {}", path, individual_duration, e),
        }
    }
    
    // Test 3: Using the DATASUS convenience method
    println!("\n🏥 Testing DATASUS convenience method: list_datasus_directories_parallel");
    let start = Instant::now();
    let datasus_results = ftp_provider.list_datasus_directories_parallel(test_directories.clone()).await;
    let duration = start.elapsed();
    
    println!("✅ DATASUS method completed in {:?}", duration);
    for (path, result) in &datasus_results {
        match result {
            Ok(content) => println!("  {} - {} items", path, content.len()),
            Err(e) => println!("  {} - ❌ Error: {}", path, e),
        }
    }
    
    // Performance comparison
    println!("\n📊 Performance Summary:");
    println!("  All methods should have similar performance due to caching");
    println!("  First run populates cache, subsequent runs benefit from it");
    println!("  Enhanced method provides individual timing information");
    
    // Demonstrate cache benefits with second run
    println!("\n🚀 Second run (should be much faster due to cache):");
    let cached_start = Instant::now();
    let cached_results = ftp_provider.list_directories_parallel(vec!["/SIM/CID10"]).await;
    let cached_duration = cached_start.elapsed();
    
    println!("✅ Cached run completed in {:?}", cached_duration);
    for (path, result) in &cached_results {
        match result {
            Ok(content) => println!("  {} - {} items (from cache)", path, content.len()),
            Err(e) => println!("  {} - ❌ Error: {}", path, e),
        }
    }
    
    println!("\n💡 New Parallel API Features:");
    println!("  ✅ Trait method: Works with any FileSystemProvider");
    println!("  ✅ Enhanced timing: FTP-specific method with individual timings");
    println!("  ✅ DATASUS convenience: Easy method for common use cases");
    println!("  ✅ Cache integration: All methods benefit from content caching");
    println!("  ✅ Error isolation: One failed directory doesn't affect others");
    
    Ok(())
}
