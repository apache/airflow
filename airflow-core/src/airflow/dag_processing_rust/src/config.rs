use clap::Args;
use serde::Deserialize;
use std::time::Duration;

/// Converts a string of seconds into a Duration.
fn de_duration_from_secs<'de, D>(deserializer: D) -> Result<Duration, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let secs: u64 = Deserialize::deserialize(deserializer)?;
    Ok(Duration::from_secs(secs))
}

/// Parses a duration from a string. If no unit is specified, it defaults to seconds.
fn parse_duration_from_string(s: &str) -> Result<Duration, String> {
    if let Ok(secs) = s.parse::<u64>() {
        return Ok(Duration::from_secs(secs));
    }
    humantime::parse_duration(s).map_err(|e| e.to_string())
}

#[derive(Args, Deserialize, Debug)]
pub struct DagProcessorManagerConfig {
    #[arg(long, default_value_t = 10)]
    pub parsing_processes: usize,

    #[arg(long, value_parser = parse_duration_from_string, default_value = "30s")]
    #[serde(deserialize_with = "de_duration_from_secs")]
    pub min_file_process_interval: Duration,

    #[arg(long, default_value = "/files/dags")]
    pub dags_folder: String,

    #[arg(long, value_parser = parse_duration_from_string, default_value = "5s")]
    #[serde(deserialize_with = "de_duration_from_secs")]
    pub bundle_refresh_check_interval: Duration,

    #[arg(long, value_parser = parse_duration_from_string, default_value = "1800s")]
    #[serde(deserialize_with = "de_duration_from_secs")]
    pub processor_timeout: Duration,

    #[arg(long, value_parser = parse_duration_from_string, default_value = "600s")]
    #[serde(deserialize_with = "de_duration_from_secs")]
    pub parsing_cleanup_interval: Duration,
}
