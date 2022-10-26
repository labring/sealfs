use clap::Parser;
use log::info;

const DEFAULT_PORT: u16 = 8080;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init();

    let cli = Cli::parse();
    let port = cli.port.unwrap_or(DEFAULT_PORT);
    let address = format!("127.0.0.1:{}", port);
    
    info!("Start Server");
    server::run(address).await?;
    Ok(())
}

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Cli {
    #[arg(long = "port", short = 'p')]
    port: Option<u16>,
}
