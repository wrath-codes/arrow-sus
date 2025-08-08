use anyhow::Result;
use shared::models::download::download_multiple_files_parallel;

#[tokio::main]
async fn main() -> Result<()> {
    println!("🚀 DATASUS Parallel Download Demo");
    println!("==================================");
    
    // Test files for parallel download
    let files = vec![
        (
            "/dissemin/publicos/SIHSUS/200801_/Dados/CHBR1901.dbc",
            "./downloads/parallel/CHBR1901.dbc",
        ),
        (
            "/dissemin/publicos/SIHSUS/200801_/Dados/CHBR1902.dbc",
            "./downloads/parallel/CHBR1902.dbc",
        ),
        (
            "/dissemin/publicos/SIHSUS/200801_/Dados/CHBR1903.dbc",
            "./downloads/parallel/CHBR1903.dbc",
        ),
        (
            "/dissemin/publicos/SIHSUS/200801_/Dados/CHBR1904.dbc",
            "./downloads/parallel/CHBR1904.dbc",
        ),
        (
            "/dissemin/publicos/SIHSUS/200801_/Dados/CHBR1905.dbc",
            "./downloads/parallel/CHBR1905.dbc",
        ),
    ];

    println!("Starting parallel download with multiple progress bars...\n");
    
    // Download with max 3 concurrent downloads
    match download_multiple_files_parallel(files, Some(3)).await {
        Ok(results) => {
            println!("🎉 Parallel download completed successfully!");
            println!("📊 Results:");
            for (filename, bytes) in results {
                println!("  📁 {}: {} bytes", filename, bytes);
            }
        }
        Err(e) => {
            eprintln!("❌ Parallel download failed: {}", e);
        }
    }

    Ok(())
}
