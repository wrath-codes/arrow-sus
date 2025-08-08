use shared::models::download::download_multiple_files_parallel;
use tempfile::TempDir;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    println!("🎨 Gradient Parallel Download Demo");
    println!("This demo shows parallel downloads with gradient progress bars using rayon and TqdmParallelIterator\n");

    let temp_dir = TempDir::new()?;
    
    // Prepare test files (simulated paths) - create paths with proper lifetimes
    let path1 = temp_dir.path().join("file1.dbc");
    let path2 = temp_dir.path().join("file2.dbc");
    let path3 = temp_dir.path().join("file3.dbc");
    let path4 = temp_dir.path().join("file4.dbc");
    let path5 = temp_dir.path().join("file5.dbc");
    
    let files = vec![
        ("/test/path/file1.dbc", path1.to_str().unwrap()),
        ("/test/path/file2.dbc", path2.to_str().unwrap()),
        ("/test/path/file3.dbc", path3.to_str().unwrap()),
        ("/test/path/file4.dbc", path4.to_str().unwrap()),
        ("/test/path/file5.dbc", path5.to_str().unwrap()),
    ];

    println!("Starting parallel download with gradient progress bars...\n");

    // Run parallel download with max 3 concurrent downloads
    let results = download_multiple_files_parallel(files, Some(3)).await?;

    println!("\n🎉 Demo completed!");
    println!("📊 Results:");
    for (filename, bytes) in &results {
        println!("  ✅ {}: {} bytes", filename, bytes);
    }

    println!("\n🔍 Features demonstrated:");
    println!("  • TqdmParallelIterator trait for rayon integration");
    println!("  • Gradient progress bars with custom colors");
    println!("  • Individual progress bars for each file");
    println!("  • Overall progress tracking");
    println!("  • Concurrent download limiting");

    Ok(())
}
