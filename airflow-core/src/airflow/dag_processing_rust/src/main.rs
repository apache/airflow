mod config;
use crate::config::DagProcessorManagerConfig;
use tokio::time::Instant;

#[tokio::main]
async fn main() {
    let start = Instant::now();

    let manager_config = DagProcessorManagerConfig::from_cli();
    println!("{:#?}", manager_config);

    let duration = start.elapsed();
    println!("Time elapsed in main() is: {:?}", duration);
}
