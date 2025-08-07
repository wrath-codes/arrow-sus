// Test simple directory access
use shared;

#[tokio::main]
async fn main() {
    println!("Testing directory access...");
    
    // Try to access directory from the crate directly
    let temp_dir = tempfile::TempDir::new().unwrap();
    let temp_path = temp_dir.path().to_string_lossy().to_string();
    
    // Direct module access
    let dir = shared::models::directory::Directory::new(temp_path).await;
    match dir {
        Ok(d) => println!("Directory created: {}", d.path),
        Err(e) => println!("Error: {}", e),
    }
}
