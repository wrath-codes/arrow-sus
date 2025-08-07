use shared::models::{ConnectionStrategy, DirectoryItem};

/// Example demonstrating the FTP strategy usage
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🚀 DATASUS FTP Strategy Example");
    println!("===============================\n");

    // Create the FTP strategy
    println!("📡 Creating DATASUS FTP strategy...");
    let strategy = ConnectionStrategy::create("DATASUS_FTP").await?;
    
    // Verify connection
    println!("🔗 Verifying connection...");
    if strategy.verify_connection().await {
        println!("✅ Connection successful!\n");
    } else {
        println!("❌ Connection failed!");
        return Ok(());
    }

    // List root directory
    println!("📂 Listing root directory contents:");
    let root_content = strategy.list_directory("").await?;
    println!("Found {} items in root directory:\n", root_content.len());
    
    for (name, item) in &root_content {
        match item {
            DirectoryItem::Directory(_) => println!("  📁 {}", name),
            DirectoryItem::File(file) => {
                let size = file.size_bytes().unwrap_or(0);
                println!("  📄 {} ({} bytes)", name, size);
            }
        }
    }

    // List SIASUS directory
    if root_content.contains_key("SIASUS") {
        println!("\n📂 Listing SIASUS directory contents:");
        let siasus_content = strategy.list_directory("SIASUS").await?;
        println!("Found {} items in SIASUS directory:\n", siasus_content.len());
        
        for (name, item) in &siasus_content {
            match item {
                DirectoryItem::Directory(_) => println!("  📁 {}", name),
                DirectoryItem::File(file) => {
                    let size = file.size_bytes().unwrap_or(0);
                    println!("  📄 {} ({} bytes)", name, size);
                }
            }
        }
    }

    // Demo with fallback strategy
    println!("\n🔄 Testing strategy fallback...");
    let (fallback_strategy, strategy_name) = ConnectionStrategy::create_with_fallback(None).await?;
    println!("✅ Using fallback strategy: {}", strategy_name);
    
    if fallback_strategy.verify_connection().await {
        println!("✅ Fallback strategy connection successful!");
    }

    println!("\n🎉 Example completed successfully!");
    Ok(())
}
