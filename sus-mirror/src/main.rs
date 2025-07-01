// use anyhow::Result;
// use aws_config::BehaviorVersion;
// use aws_config::meta::region::RegionProviderChain;
// use aws_sdk_s3::config::Region;
// use aws_sdk_s3::primitives::ByteStream;
// use aws_sdk_s3::{Client, Config};

// #[tokio::main]
// async fn main() -> Result<()> {
//     env_logger::init();

//     let region_provider = RegionProviderChain::default_provider().or_else("us-east-1");

//     // ✅ EXPLICIT behavior version here
//     let shared_config = aws_config::defaults(BehaviorVersion::latest())
//         .region(region_provider)
//         .load()
//         .await;

//     // ✅ s3_config from shared_config only
//     let s3_config = Config::from(&shared_config)
//         .to_builder()
//         .region(Region::new("us-east-1"))
//         .endpoint_url(&std::env::var("SPACES_ENDPOINT")?)
//         .build();

//     let client = Client::from_conf(s3_config);

//     let test_file = "test.dbc";
//     let file_data = tokio::fs::read(test_file).await?;

//     let bucket = "sus-ftp-raw";
//     let key = "test/test.dbc";

//     client
//         .put_object()
//         .bucket(bucket)
//         .key(key)
//         .body(ByteStream::from(file_data))
//         .send()
//         .await?;

//     println!("✅ Uploaded {} to {}/{}", test_file, bucket, key);

//     Ok(())
// }

// use anyhow::Result;
// use aws_config::BehaviorVersion;
// use aws_config::meta::region::RegionProviderChain;
// use aws_sdk_s3::config::Region;
// use aws_sdk_s3::primitives::ByteStream;
// use aws_sdk_s3::{Client, Config};
// use futures::io::AsyncReadExt;
// use suppaftp::{AsyncFtpStream, FtpError}; // ✅ USE FUTURES NOT TOKIO!

// #[tokio::main]
// async fn main() -> Result<()> {
//     env_logger::init();

//     // 1️⃣ FTP connect
//     let mut ftp_stream = AsyncFtpStream::connect("ftp.datasus.gov.br:21").await?;
//     ftp_stream.login("anonymous", "anonymous").await?;
//     ftp_stream
//         .cwd("/dissemin/publicos/SIHSUS/200801_/Dados/")
//         .await?;
//     let files = ftp_stream.nlst(None).await?;
//     let file_name = files.get(0).expect("No files!");

//     // 2️⃣ Download with retr
//     let file_data = ftp_stream
//         .retr(file_name, |mut data_stream| {
//             Box::pin(async move {
//                 let mut buf = Vec::new();
//                 data_stream
//                     .read_to_end(&mut buf)
//                     .await
//                     .map_err(FtpError::ConnectionError)?;
//                 Ok((buf, data_stream)) // closure must return tuple
//             })
//         })
//         .await?;

//     // 3️⃣ S3 upload
//     let region_provider = RegionProviderChain::default_provider().or_else("us-east-1");
//     let shared_config = aws_config::defaults(BehaviorVersion::latest())
//         .region(region_provider)
//         .load()
//         .await;

//     let s3_config = Config::from(&shared_config)
//         .to_builder()
//         .region(Region::new("us-east-1"))
//         .endpoint_url(&std::env::var("SPACES_ENDPOINT")?)
//         .build();

//     let client = Client::from_conf(s3_config);
//     let key = format!("mirror/{}", file_name);

//     client
//         .put_object()
//         .bucket("sus-ftp-raw")
//         .key(&key)
//         .body(ByteStream::from(file_data))
//         .send()
//         .await?;

//     println!("✅ Uploaded: {}", key);

//     ftp_stream.quit().await?;
//     Ok(())
// }
//
// use anyhow::Result;
// use std::future::Future;
// use std::pin::Pin;
// use suppaftp::{AsyncFtpStream, FtpError};

// #[tokio::main]
// async fn main() -> Result<()> {
//     env_logger::init();

//     let mut ftp_stream = AsyncFtpStream::connect("ftp.datasus.gov.br:21").await?;
//     ftp_stream.login("anonymous", "anonymous").await?;

//     let base_path = "/dissemin/publicos/SIHSUS/200801_/";
//     println!("🚀 Crawling from: {}", base_path);

//     crawl(&mut ftp_stream, base_path).await?;

//     ftp_stream.quit().await?;
//     Ok(())
// }

// // ✅ Recursion-safe: return boxed future
// fn crawl<'a>(
//     ftp: &'a mut AsyncFtpStream,
//     path: &'a str,
// ) -> Pin<Box<dyn Future<Output = Result<(), FtpError>> + Send + 'a>> {
//     Box::pin(async move {
//         ftp.cwd(path).await?;

//         let entries = ftp.nlst(None).await?;

//         for entry in entries {
//             let entry_path = format!("{}/{}", path.trim_end_matches('/'), entry);

//             match ftp.cwd(&entry_path).await {
//                 Ok(_) => {
//                     println!("📂 Dir: {}", entry_path);
//                     crawl(ftp, &entry_path).await?;
//                     ftp.cdup().await?;
//                 }
//                 Err(_) => {
//                     println!("📄 File: {}", entry_path);
//                 }
//             }
//         }

//         Ok(())
//     })
// }

use anyhow::Result;
use std::future::Future;
use std::pin::Pin;
use suppaftp::{AsyncFtpStream, FtpError};

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

    let mut ftp_stream = AsyncFtpStream::connect("ftp.datasus.gov.br:21").await?;
    ftp_stream.login("anonymous", "anonymous").await?;

    let base_path = "/dissemin/publicos/";
    println!("🚀 Starting crawl from: {}", base_path);

    let mut all_dirs = Vec::new();
    crawl(&mut ftp_stream, base_path, &mut all_dirs).await?;

    ftp_stream.quit().await?;
    println!("\n✅ Done! Found directories:");
    for dir in &all_dirs {
        println!("📂 {}", dir);
    }

    Ok(())
}

// ✅ Recursion-safe: boxed async recursion
fn crawl<'a>(
    ftp: &'a mut AsyncFtpStream,
    path: &'a str,
    all_dirs: &'a mut Vec<String>,
) -> Pin<Box<dyn Future<Output = Result<(), FtpError>> + Send + 'a>> {
    Box::pin(async move {
        ftp.cwd(path).await?;

        let entries = ftp.nlst(None).await?;

        for entry in entries {
            let entry_path = format!("{}/{}", path.trim_end_matches('/'), entry);

            match ftp.cwd(&entry_path).await {
                Ok(_) => {
                    // ✅ It's a directory — store it!
                    all_dirs.push(entry_path.clone());
                    println!("📂 Dir: {}", entry_path);

                    // Recurse
                    crawl(ftp, &entry_path, all_dirs).await?;

                    // Go back up one level
                    ftp.cdup().await?;
                }
                Err(_) => {
                    // ❌ It's probably a file — skip
                }
            }
        }

        Ok(())
    })
}
