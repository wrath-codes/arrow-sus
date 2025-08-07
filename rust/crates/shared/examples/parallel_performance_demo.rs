use shared::models::directory::{FtpFileSystemProvider, FileSystemProvider};
use shared::models::async_utils::content_cache;
use std::sync::Arc;
use std::time::Instant;
use futures::future::join_all;
use tokio;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    println!("=== Parallel vs Sequential Performance Demo ===");
    
    let ftp_provider = Arc::new(FtpFileSystemProvider::new_datasus());
    
    // Test with multiple directories of varying sizes
    let directories = vec![
        "/SIASUS/199407_200712/Dados",  // ~4,374 items
        "/SIASUS/200801_/Dados",        // ~50,504 items
        "/SIM/CID10",                   // ~5 items
        "/CNES/200508_",                // Variable size
        "/CIHA/201101_",                // Variable size
    ];
    
    println!("Testing {} directories", directories.len());
    println!("Cache TTL: {} seconds", content_cache::DEFAULT_FTP_TTL_SECONDS);
    
    // Clear cache for fair comparison
    content_cache::clear_content_cache().await;
    println!("\n=== Cache cleared for fresh test ===");
    
    // Sequential execution
    println!("\n🔄 Sequential Execution:");
    let sequential_start = Instant::now();
    let mut sequential_results = Vec::new();
    
    for (i, dir_path) in directories.iter().enumerate() {
        let start = Instant::now();
        match ftp_provider.list_directory(dir_path).await {
            Ok(content) => {
                let duration = start.elapsed();
                println!("  {}. {} - {} items in {:?}", 
                    i + 1, dir_path, content.len(), duration);
                sequential_results.push((dir_path, content.len(), duration));
            }
            Err(e) => {
                println!("  {}. {} - ❌ Error: {}", i + 1, dir_path, e);
            }
        }
    }
    
    let total_sequential = sequential_start.elapsed();
    println!("📊 Sequential total: {:?}", total_sequential);
    
    // Clear cache again for parallel test
    content_cache::clear_content_cache().await;
    
    // Parallel execution
    println!("\n⚡ Parallel Execution:");
    let parallel_start = Instant::now();
    
    let futures: Vec<_> = directories.iter().enumerate().map(|(i, dir_path)| {
        let provider = ftp_provider.clone();
        let path = dir_path.to_string();
        let index = i + 1;
        async move {
            let start = Instant::now();
            let result = provider.list_directory(&path).await;
            let duration = start.elapsed();
            (index, path, result, duration)
        }
    }).collect();
    
    let results = join_all(futures).await;
    let total_parallel = parallel_start.elapsed();
    
    println!("📊 Parallel total: {:?}", total_parallel);
    
    // Process and display parallel results
    for (index, dir_path, result, duration) in results {
        match result {
            Ok(content) => {
                println!("  {}. {} - {} items in {:?}", 
                    index, dir_path, content.len(), duration);
            }
            Err(e) => {
                println!("  {}. {} - ❌ Error: {}", index, dir_path, e);
            }
        }
    }
    
    // Performance comparison
    println!("\n📈 Performance Analysis:");
    let speedup = total_sequential.as_millis() as f64 / total_parallel.as_millis() as f64;
    let improvement = ((total_sequential.as_millis() - total_parallel.as_millis()) as f64 / total_sequential.as_millis() as f64) * 100.0;
    
    println!("  Sequential: {:?}", total_sequential);
    println!("  Parallel:   {:?}", total_parallel);
    println!("  Speedup:    {:.2}x", speedup);
    println!("  Improvement: {:.1}% faster", improvement);
    
    // Test with cache hits (second run)
    println!("\n🚀 Second Run (Cache Test):");
    
    // Sequential with cache
    println!("\n🔄 Sequential (cached):");
    let cached_sequential_start = Instant::now();
    for (i, dir_path) in directories.iter().enumerate() {
        let start = Instant::now();
        match ftp_provider.list_directory(dir_path).await {
            Ok(content) => {
                let duration = start.elapsed();
                println!("  {}. {} - {} items in {:?} (cached)", 
                    i + 1, dir_path, content.len(), duration);
            }
            Err(e) => {
                println!("  {}. {} - ❌ Error: {}", i + 1, dir_path, e);
            }
        }
    }
    let total_cached_sequential = cached_sequential_start.elapsed();
    
    // Parallel with cache
    println!("\n⚡ Parallel (cached):");
    let cached_parallel_start = Instant::now();
    
    let cached_futures: Vec<_> = directories.iter().enumerate().map(|(i, dir_path)| {
        let provider = ftp_provider.clone();
        let path = dir_path.to_string();
        let index = i + 1;
        async move {
            let start = Instant::now();
            let result = provider.list_directory(&path).await;
            let duration = start.elapsed();
            (index, path, result, duration)
        }
    }).collect();
    
    let cached_results = join_all(cached_futures).await;
    let total_cached_parallel = cached_parallel_start.elapsed();
    
    for (index, dir_path, result, duration) in cached_results {
        match result {
            Ok(content) => {
                println!("  {}. {} - {} items in {:?} (cached)", 
                    index, dir_path, content.len(), duration);
            }
            Err(e) => {
                println!("  {}. {} - ❌ Error: {}", index, dir_path, e);
            }
        }
    }
    
    // Final comparison
    println!("\n🏆 Final Performance Summary:");
    println!("  Network Sequential: {:?}", total_sequential);
    println!("  Network Parallel:   {:?} ({:.1}% faster)", 
        total_parallel, 
        ((total_sequential.as_millis() - total_parallel.as_millis()) as f64 / total_sequential.as_millis() as f64) * 100.0
    );
    println!("  Cached Sequential:  {:?}", total_cached_sequential);
    println!("  Cached Parallel:    {:?}", total_cached_parallel);
    
    println!("\n💡 Key Insights:");
    println!("  - Parallel execution provides significant speedup for network operations");
    println!("  - Cache hits make both approaches extremely fast");
    println!("  - Parallel + cache = optimal performance for multiple directory listings");
    println!("  - Each directory listing creates independent FTP connections");
    
    Ok(())
}
