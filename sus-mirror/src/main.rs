use anyhow::Result;
use suppaftp::AsyncFtpStream;

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

    let server = "ftp.datasus.gov.br:21";

    let mut ftp_stream = AsyncFtpStream::connect(server).await?;
    println!("Connected to {server}");

    ftp_stream.login("anonymous", "anonymous").await?;
    println!("Logged in anonymously");

    ftp_stream
        .cwd("/dissemin/publicos/SIHSUS/200801_/Dados/")
        .await?;
    let files = ftp_stream.nlst(None).await?;
    println!("Files: {:?}", files);

    ftp_stream.quit().await?;
    println!("Connection closed");

    Ok(())
}
