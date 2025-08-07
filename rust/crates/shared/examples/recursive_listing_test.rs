use shared::models::directory::{FtpFileSystemProvider, FileSystemProvider, DirectoryEntry, FlatDirectoryEntry};
use std::sync::Arc;
use std::time::Instant;
use tokio;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    println!("=== Recursive Directory Listing Test ===");
    
    let ftp_provider = Arc::new(FtpFileSystemProvider::new_datasus());
    
    // Test with a smaller directory first to avoid overwhelming output
    let test_path = "/SIM/CID10";
    
    println!("Testing recursive listing for: {}", test_path);
    
    // Test 1: Basic recursive listing with depth limit
    println!("\n🌳 Basic recursive listing (depth limit: 3):");
    let start = Instant::now();
    match ftp_provider.list_directory_recursive(test_path, Some(3)).await {
        Ok(entries) => {
            let duration = start.elapsed();
            println!("✅ Found {} total entries in {:?}", entries.len(), duration);
            
            // Group by depth
            let mut by_depth: std::collections::HashMap<usize, Vec<&FlatDirectoryEntry>> = std::collections::HashMap::new();
            for entry in &entries {
                by_depth.entry(entry.depth).or_insert_with(Vec::new).push(entry);
            }
            
            // Show entries by depth
            for depth in 0..=3 {
                if let Some(depth_entries) = by_depth.get(&depth) {
                    println!("  Depth {} ({} items):", depth, depth_entries.len());
                    for entry in depth_entries.iter().take(5) {
                        let entry_type = match &entry.entry {
                            DirectoryEntry::File(_) => "📄",
                            DirectoryEntry::Directory(_) => "📁",
                        };
                        println!("    {} {}", entry_type, entry.path);
                    }
                    if depth_entries.len() > 5 {
                        println!("    ... and {} more items at this depth", depth_entries.len() - 5);
                    }
                }
            }
        }
        Err(e) => {
            println!("❌ Error: {}", e);
        }
    }
    
    // Test 2: Recursive listing with file extension filtering
    println!("\n📂 Extension filtering test (.dbc files):");
    let start = Instant::now();
    match ftp_provider.list_datasus_recursive_with_extension(test_path, "dbc", Some(3)).await {
        Ok(dbc_files) => {
            let duration = start.elapsed();
            println!("✅ Found {} .dbc files in {:?}", dbc_files.len(), duration);
            
            for file in dbc_files.iter().take(10) {
                println!("  📄 {} (depth: {})", file.path, file.depth);
            }
            if dbc_files.len() > 10 {
                println!("  ... and {} more .dbc files", dbc_files.len() - 10);
            }
        }
        Err(e) => {
            println!("❌ Error: {}", e);
        }
    }
    
    // Test 3: Get recursive statistics
    println!("\n📊 Recursive statistics:");
    let start = Instant::now();
    match ftp_provider.get_recursive_stats(test_path, Some(3)).await {
        Ok((file_count, dir_count, total_size)) => {
            let duration = start.elapsed();
            println!("✅ Statistics computed in {:?}", duration);
            println!("  📄 Files: {}", file_count);
            println!("  📁 Directories: {}", dir_count);
            println!("  💾 Total size: ~{} bytes", total_size);
        }
        Err(e) => {
            println!("❌ Error: {}", e);
        }
    }
    
    // Test 4: Larger directory with more depth (be careful!)
    let large_test_path = "/SIASUS";
    println!("\n🏥 Testing larger directory (limited to depth 2): {}", large_test_path);
    
    let start = Instant::now();
    match ftp_provider.list_directory_recursive(large_test_path, Some(2)).await {
        Ok(entries) => {
            let duration = start.elapsed();
            println!("✅ Found {} total entries in {:?}", entries.len(), duration);
            
            // Count by type and depth
            let mut files_by_depth: std::collections::HashMap<usize, usize> = std::collections::HashMap::new();
            let mut dirs_by_depth: std::collections::HashMap<usize, usize> = std::collections::HashMap::new();
            
            for entry in &entries {
                match &entry.entry {
                    DirectoryEntry::File(_) => {
                        *files_by_depth.entry(entry.depth).or_insert(0) += 1;
                    }
                    DirectoryEntry::Directory(_) => {
                        *dirs_by_depth.entry(entry.depth).or_insert(0) += 1;
                    }
                }
            }
            
            println!("  Summary by depth:");
            for depth in 0..=2 {
                let files = files_by_depth.get(&depth).unwrap_or(&0);
                let dirs = dirs_by_depth.get(&depth).unwrap_or(&0);
                println!("    Depth {}: {} files, {} directories", depth, files, dirs);
            }
            
            // Show some sample paths
            println!("  Sample files found:");
            let sample_files: Vec<_> = entries.iter()
                .filter(|e| matches!(e.entry, DirectoryEntry::File(_)))
                .take(5)
                .collect();
            
            for file in sample_files {
                println!("    📄 {} (depth: {})", file.path, file.depth);
            }
        }
        Err(e) => {
            println!("❌ Error: {}", e);
        }
    }
    
    println!("\n💡 Recursive Listing Features:");
    println!("  ✅ Flattened results: All files/dirs in a single list");
    println!("  ✅ Depth information: Know how deep each item is");
    println!("  ✅ Parallel processing: Subdirectories processed concurrently");
    println!("  ✅ Cache integration: Benefits from existing content caching");
    println!("  ✅ Extension filtering: Easy to find specific file types");
    println!("  ✅ Statistics: Get counts and size information");
    println!("  ✅ Depth limiting: Prevent runaway recursion");
    println!("  ✅ Error resilience: Failed subdirectories don't break the whole operation");
    
    Ok(())
}
