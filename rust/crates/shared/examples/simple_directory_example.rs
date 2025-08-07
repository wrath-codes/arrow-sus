use shared::models::directory::*;
use std::sync::Arc;
use tempfile::TempDir;
use tokio::fs;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    println!("=== Simple Directory Operations Example ===");
    
    // Create a temporary directory structure for demonstration
    let temp_dir = TempDir::new()?;
    let temp_path = temp_dir.path().to_string_lossy().to_string();
    println!("Created temporary directory: {}", temp_path);
    
    // Create some test files and subdirectories
    let sub_dir_path = temp_dir.path().join("documents");
    fs::create_dir(&sub_dir_path).await?;
    
    // Create files in root directory
    fs::write(temp_dir.path().join("readme.txt"), "This is a readme file").await?;
    fs::write(temp_dir.path().join("config.json"), r#"{"setting": "value"}"#).await?;
    
    // Create files in documents subdirectory
    fs::write(sub_dir_path.join("report.pdf"), "PDF content here").await?;
    fs::write(sub_dir_path.join("data.csv"), "col1,col2\n1,2\n3,4").await?;
    
    println!("\n=== Basic Directory Operations ===");
    
    // Create Directory instance
    let root_dir = Directory::new(temp_path.clone()).await?;
    println!("Directory: {}", root_dir);
    println!("Name: {}", root_dir.name);
    println!("Path: {}", root_dir.path);
    
    // Check if directory exists
    let exists = root_dir.exists().await?;
    println!("Exists: {}", exists);
    
    println!("\n=== Directory Content Listing ===");
    
    // List directory contents
    let content = root_dir.content().await?;
    println!("Total items: {}", content.len());
    
    for entry in &content {
        match entry {
            DirectoryEntry::File(file) => {
                println!("📄 File: {} ({})", file.basename, file.info().get("size").unwrap_or(&"unknown".to_string()));
            }
            DirectoryEntry::Directory(dir) => {
                println!("📁 Directory: {}", dir.name);
            }
        }
    }
    
    println!("\n=== Filtering by Type ===");
    
    // Get only files
    let files = root_dir.files().await?;
    println!("Files: {}", files.len());
    for file in &files {
        println!("  - {}", file.basename);
    }
    
    // Get only subdirectories
    let subdirs = root_dir.subdirectories().await?;
    println!("Subdirectories: {}", subdirs.len());
    for dir in &subdirs {
        println!("  - {}", dir.name);
    }
    
    println!("\n=== Filtering by Extension ===");
    
    // Get files with specific extension
    let txt_files = root_dir.files_with_extension("txt").await?;
    println!("TXT files: {}", txt_files.len());
    for file in &txt_files {
        println!("  - {}", file.basename);
    }
    
    println!("\n=== Subdirectory Operations ===");
    
    // Explore documents subdirectory
    for subdir in &subdirs {
        if subdir.name == "documents" {
            println!("Exploring '{}' directory:", subdir.name);
            let subdir_files = subdir.files().await?;
            
            for file in &subdir_files {
                let file_info = file.info();
                println!("  📄 {} - {} ({})", 
                    file.basename, 
                    file_info.get("type").unwrap_or(&"unknown".to_string()),
                    file_info.get("size").unwrap_or(&"unknown".to_string())
                );
            }
        }
    }
    
    println!("\n=== Directory Size Calculation ===");
    
    // Calculate directory size (including subdirectories)
    let total_size = directory_utils::get_directory_size(root_dir.clone()).await?;
    println!("Total directory size: {} bytes", total_size);
    
    println!("\n=== Provider Test ===");
    
    // Use provider directly
    let provider = Arc::new(LocalFileSystemProvider);
    println!("Provider: {}", provider.provider_name());
    
    let provider_content = provider.list_directory(&temp_path).await?;
    println!("Items via provider: {}", provider_content.len());
    
    println!("\n=== Example Complete ===");
    println!("Temporary directory will be cleaned up automatically.");
    
    Ok(())
}
