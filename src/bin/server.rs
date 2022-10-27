use clap::Parser;
use log::info;
use sealfs_rust::service::{self, FsService};
use tonic::transport::Server;

const DEFAULT_PORT: u16 = 8080;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init();

    let cli = Cli::parse();
    let port = cli.port.unwrap_or(DEFAULT_PORT);
    let address = format!("127.0.0.1:{}", port);

    let fs_service = FsService::default();

    info!("Start Server");
    Server::builder()
        .add_service(service::new_fs_service(fs_service))
        .serve(address.parse().unwrap())
        .await?;
    Ok(())
}

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Cli {
    #[arg(long = "port", short = 'p')]
    port: Option<u16>,
}
