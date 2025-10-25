mod bundles;
mod config;
mod models;
mod utils;

use crate::bundles::local::LocalDagBundle;
use crate::config::DagProcessorManagerConfig;
use anyhow::{Context, Error, Result};
use diesel::prelude::*;
use diesel_async::{AsyncConnection, AsyncConnectionCore, AsyncPgConnection, RunQueryDsl};
use futures::stream::StreamExt;
use log::{debug, info, trace, warn};
use signal_hook::consts::signal::*;
use signal_hook_tokio::Signals;
use std::any::Any;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::process::Command;
use tokio::time::{sleep, Instant};

async fn handle_signals(mut signals: Signals, signal_count: Arc<AtomicUsize>) {
    let mut signals = Box::pin(signals);

    while let Some(signal) = signals.next().await {
        match signal {
            SIGTERM | SIGINT | SIGUSR2 => {
                let prev = signal_count.fetch_add(1, Ordering::SeqCst);
                if prev == 0 {
                    info!("Exiting gracefully upon receiving signal {}", signal);
                } else {
                    // In a test context, we don't want to exit the process
                    #[cfg(not(test))]
                    std::process::exit(1);
                }
            }
            _ => unreachable!(),
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    let start = Instant::now();

    env_logger::init();
    let signals = Signals::new(&[SIGTERM, SIGINT])?;
    let handle = signals.handle();
    let signal_count = Arc::new(AtomicUsize::new(0));
    let signals_task = tokio::spawn(handle_signals(signals, Arc::clone(&signal_count)));

    let manager_config = DagProcessorManagerConfig::from_cli();
    debug!("Configuration: {:#?}", manager_config);

    let sql_alchemy_conn_str = std::env::var("AIRFLOW__DATABASE__SQL_ALCHEMY_CONN".to_string())
        .expect("Missing AIRFLOW__DATABASE__SQL_ALCHEMY_CONN env var");
    let diesel_conn = utils::db::to_diesel_conn_string(&sql_alchemy_conn_str)?;
    let mut connection = AsyncPgConnection::establish(&diesel_conn)
        .await
        .expect("Failed to connect to database");
    debug!("Connected to db");

    info!(
        "Processing files using up to {} processes at a time",
        manager_config.parsing_processes
    );
    info!(
        "Process each file at most once every {} seconds",
        manager_config.min_file_process_interval.as_secs()
    );

    // TODO: Implement other bundle types (Rust-native)
    let bundle = LocalDagBundle {
        path: manager_config.dags_folder,
        refresh_interval: manager_config.bundle_refresh_check_interval.as_secs()
    };
    debug!("Using LocalDagBundle: {:#?}", bundle);
    info!(
        "Checking for new files in local bundle every {} seconds", bundle.refresh_interval
    );

    // while AtomicUsize::load(&signal_count, Ordering::SeqCst) == 0 {
    //
    // }

    let dag_files = utils::file::find_dag_file_paths(&bundle.path);
    for (i, dag_file) in dag_files.iter().enumerate() {
        // if i % 5 == 0 {
        //     sleep(Duration::from_secs(5)).await;
        // }
        info!("Processing DAG file: {}", dag_file);
    }


    let duration = start.elapsed();
    debug!("Time elapsed in main() is: {:?}", duration);

    drop(connection);
    handle.close();
    signals_task.await?;
    Ok(())
}
