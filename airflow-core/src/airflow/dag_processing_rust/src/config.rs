use clap::Parser;
use std::time::Duration;

#[derive(Debug)]
pub struct DagProcessorManagerConfig {
    pub processor_timeout: Duration,
    pub parsing_processes: usize,
    pub parsing_cleanup_interval: Duration,
    pub min_file_process_interval: Duration,
    pub stale_dag_threshold: Duration,
    pub print_stats_interval: Duration,
    pub max_callbacks_per_loop: u32,
    pub base_log_dir: String,
    pub bundle_refresh_check_interval: Duration,
    pub file_parsing_sort_mode: String,
}

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct CliConfig {
    #[arg(long)]
    processor_timeout: u64,
    #[arg(long)]
    parsing_processes: usize,
    #[arg(long)]
    parsing_cleanup_interval: u64,
    #[arg(long)]
    min_file_process_interval: u64,
    #[arg(long)]
    stale_dag_threshold: u64,
    #[arg(long)]
    print_stats_interval: u64,
    #[arg(long)]
    max_callbacks_per_loop: u32,
    #[arg(long)]
    base_log_dir: String,
    #[arg(long)]
    bundle_refresh_check_interval: u64,
    #[arg(long)]
    file_parsing_sort_mode: String,
}

impl DagProcessorManagerConfig {
    pub fn from_cli() -> Self {
        let cli_config = CliConfig::parse();

        Self {
            processor_timeout: Duration::from_secs(cli_config.processor_timeout),
            parsing_processes: cli_config.parsing_processes,
            parsing_cleanup_interval: Duration::from_secs(cli_config.parsing_cleanup_interval),
            min_file_process_interval: Duration::from_secs(cli_config.min_file_process_interval),
            stale_dag_threshold: Duration::from_secs(cli_config.stale_dag_threshold),
            print_stats_interval: Duration::from_secs(cli_config.print_stats_interval),
            max_callbacks_per_loop: cli_config.max_callbacks_per_loop,
            base_log_dir: cli_config.base_log_dir,
            bundle_refresh_check_interval: Duration::from_secs(
                cli_config.bundle_refresh_check_interval,
            ),
            file_parsing_sort_mode: cli_config.file_parsing_sort_mode,
        }
    }
}
