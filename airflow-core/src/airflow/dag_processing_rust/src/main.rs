mod bundles;
mod config;
mod schema;
mod utils;

use crate::bundles::local::LocalDagBundle;
use crate::config::DagProcessorManagerConfig;
use crate::schema::serialized_dag;
use anyhow::{Context, Error, Result};
use clap::{Parser, Subcommand};
use diesel::prelude::*;
use diesel_async::pooled_connection::deadpool::Pool;
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use diesel_async::RunQueryDsl;
use serde_json::Value;

use futures::stream::StreamExt;
use log::{debug, info, trace, warn};
use pyo3::prelude::*;
use pyo3::types::PyModule;
use serde::{Deserialize, Serialize};
use signal_hook::consts::signal::*;
use signal_hook_tokio::Signals;

use std::io::{self, BufRead, Write};
use std::process::Stdio;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use chrono::{DateTime, Utc};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, ChildStderr, ChildStdin, ChildStdout, Command};
use tokio::sync::mpsc;
use tokio::time::{sleep, Instant};
use uuid::Uuid;

#[derive(Parser)]
#[command(author, version, about, long_about = None, ignore_errors = true)]
struct Cli {
    #[command(subcommand)]
    command: Option<Commands>,

    #[command(flatten)]
    manager_config: DagProcessorManagerConfig,
}

#[derive(Subcommand)]
enum Commands {
    /// Parses a single Dag file.
    Parse,
}

#[derive(Serialize, Deserialize, Debug)]
struct DagFileParseRequest<'a> {
    file: &'a str,
    bundle_path: &'a str,
    bundle_name: &'a str,
    #[serde(rename = "callback_requests")]
    callback_requests: Vec<()>,
    #[serde(rename = "type")]
    typ: &'a str,
}

#[derive(Deserialize, Serialize, Debug)]
struct Timetable {
    #[serde(rename = "__type")]
    typ: String,
    #[serde(rename = "__var")]
    var: serde_json::Value,
}

#[derive(Deserialize, Serialize, Debug)]
struct Task {
    #[serde(rename = "__var")]
    var: serde_json::Value,
    #[serde(rename = "__type")]
    typ: String,
}

#[derive(Deserialize, Serialize, Debug)]
struct DagModel {
    dag_id: String,
    fileloc: String,
    tasks: Vec<Task>,
    timetable: Timetable,
    // Add other fields from DagModel as needed
}

#[derive(Deserialize, Serialize, Debug)]
struct SerializedDagData {
    dag: DagModel,
}

#[derive(Deserialize, Serialize, Debug)]
struct SerializedDagModel {
    data: SerializedDagData,
    last_loaded: String,
}

#[derive(Deserialize, Serialize, Debug)]
struct DagFileParsingResult {
    fileloc: String,
    serialized_dags: Vec<SerializedDagModel>,
    warnings: Vec<serde_json::Value>,
    import_errors: serde_json::Value,
    #[serde(rename = "type")]
    typ: String,
}

#[derive(Insertable, Debug)]
#[diesel(table_name = serialized_dag)]
pub struct NewSerializedDag {
    pub id: Uuid,
    pub dag_id: String,
    pub data: Option<serde_json::Value>,
    pub data_compressed: Option<Vec<u8>>,
    pub created_at: DateTime<Utc>,
    pub last_updated: DateTime<Utc>,
    pub dag_hash: String,
    pub dag_version_id: Uuid,
}

fn run_single_file_parse() -> Result<(), Error> {
    Python::initialize();

    let stdin = io::stdin();
    for line in stdin.lock().lines() {
        let line = line?;
        if line.is_empty() {
            continue;
        }
        let request: DagFileParseRequest = match serde_json::from_str(&line) {
            Ok(req) => req,
            Err(e) => {
                warn!("Failed to parse request: {}", e);
                continue;
            }
        };

        let result: Result<String, PyErr> = Python::attach(|py| {
            let processor = PyModule::import(py, "airflow.dag_processing.processor")?;
            let entrypoint = processor.getattr("_rust_parse_file_entrypoint")?;

            let request_json = serde_json::to_string(&request).unwrap();

            // The python entrypoint reads from stdin and prints to stdout.
            // We can redirect these in python to capture the output.
            let sys = PyModule::import(py, "sys")?;
            let io = PyModule::import(py, "io")?;

            let old_stdin = sys.getattr("stdin")?.into_pyobject(py)?;
            let old_stdout = sys.getattr("stdout")?.into_pyobject(py)?;

            let new_stdin = io.call_method1("StringIO", (request_json,))?;
            let new_stdout = io.call_method0("StringIO")?;

            sys.setattr("stdin", new_stdin)?;
            sys.setattr("stdout", new_stdout.clone())?;

            let result = entrypoint.call0();

            // Restore stdout/stdin
            sys.setattr("stdin", old_stdin)?;
            sys.setattr("stdout", old_stdout)?;

            // Check if the python call failed
            if let Err(e) = result {
                return Err(e);
            }

            let output: String = new_stdout.call_method0("getvalue")?.extract()?;
            Ok(output)
        });

        match result {
            Ok(result_str) => {
                // Ensure each result is on its own line for the parent to read
                println!("{}", result_str);
                io::stdout().flush()?;
            }
            Err(e) => {
                warn!("Python parser failed. Error: {}", e);
                // We don't return error, just log and continue to next request
            }
        }
    }
    Ok(())
}

async fn handle_signals(signals: Signals, signal_count: Arc<AtomicUsize>) {
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

struct Worker {
    child: Child,
    stdin: ChildStdin,
    stdout: BufReader<ChildStdout>,
    stderr: BufReader<ChildStderr>,
}

impl Worker {
    fn new() -> Result<Self, Error> {
        let mut child = Command::new(std::env::current_exe()?)
            .arg("parse")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()?;

        let stdin = child.stdin.take().context("Failed to open stdin")?;
        let stdout = child.stdout.take().context("Failed to open stdout")?;
        let stderr = child.stderr.take().context("Failed to open stderr")?; // Take stderr

        Ok(Self {
            child,
            stdin,
            stdout: BufReader::new(stdout),
            stderr: BufReader::new(stderr),
        })
    }
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    let start = Instant::now();
    env_logger::init();

    let cli = Cli::parse();
    if let Some(Commands::Parse) = cli.command {
        return run_single_file_parse();
    }

    let signals = Signals::new(&[SIGTERM, SIGINT])?;
    let handle = signals.handle();
    let signal_count = Arc::new(AtomicUsize::new(0));
    let signals_task = tokio::spawn(handle_signals(signals, Arc::clone(&signal_count)));

    let mut manager_config = cli.manager_config;
    debug!("Configuration: {:#?}", manager_config);

    let sql_alchemy_conn_str = std::env::var("AIRFLOW__DATABASE__SQL_ALCHEMY_CONN".to_string())
        .expect("Missing AIRFLOW__DATABASE__SQL_ALCHEMY_CONN env var");
    let diesel_conn = utils::db::to_diesel_conn_string(&sql_alchemy_conn_str)?;

    let config = AsyncDieselConnectionManager::<diesel_async::AsyncPgConnection>::new(diesel_conn);
    let pool = Pool::builder(config)
        .build()
        .expect("Failed to create pool.");
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
        path: manager_config.dags_folder.clone(),
        refresh_interval: manager_config.bundle_refresh_check_interval.as_secs(),
    };
    debug!("Using LocalDagBundle: {:#?}", bundle);

    let mut workers = Vec::new();
    let mut senders = Vec::new();

    manager_config.parsing_processes = 1;

    for i in 0..manager_config.parsing_processes {
        info!("Spawning worker process {}", i);
        let worker = Worker::new()?;
        let Worker {
            child,
            stdin,
            stdout,
            stderr,
        } = worker;

        let (tx, mut rx) = mpsc::channel::<String>(32);
        senders.push(tx);

        let mut stdin = stdin;
        let bundle_path_clone = bundle.path.clone();
        tokio::spawn(async move {
            while let Some(dag_file) = rx.recv().await {
                let request = DagFileParseRequest {
                    file: &dag_file,
                    bundle_path: &bundle_path_clone,
                    bundle_name: "local",
                    callback_requests: vec![],
                    typ: "DagFileParseRequest",
                };
                let mut request_json = serde_json::to_string(&request).unwrap();
                request_json.push('\n');

                if let Err(e) = stdin.write_all(request_json.as_bytes()).await {
                    warn!("Failed to write to worker stdin: {}", e);
                    break;
                }
            }
        });

        let mut stdout = stdout;
        let pool = pool.clone();
        tokio::spawn(async move {
            let mut line = String::new();
            loop {
                match stdout.read_line(&mut line).await {
                    Ok(0) => {
                        // EOF
                        info!("Worker stdout closed.");
                        break;
                    }
                    Ok(_) => {
                        if line.trim().is_empty() {
                            line.clear();
                            continue;
                        }
                        match serde_json::from_str::<DagFileParsingResult>(&line) {
                            Ok(parsed_result) => {
                                info!("Successfully parsed Dags from {:?}:", parsed_result.fileloc);
                                for dag in parsed_result.serialized_dags {
                                    let dag_id = dag.data.dag.dag_id.clone();
                                    let new_serialized_dag = NewSerializedDag {
                                        id: Uuid::new_v4(),
                                        dag_id,
                                        data: Some(serde_json::to_value(dag).unwrap()),
                                        data_compressed: None, // TODO: Implement
                                        dag_hash: "dag_hash".to_string(), // TODO: Implement
                                        dag_version_id: Uuid::new_v4(),
                                        created_at: Utc::now(),
                                        last_updated: Utc::now(),
                                    };
                                    info!(
                                        "Prepared SerializedDag for Dag {} from file {}",
                                        new_serialized_dag.dag_id, parsed_result.fileloc
                                    );

                                    // let mut conn = match pool.get().await {
                                    //     Ok(c) => c,
                                    //     Err(e) => {
                                    //         warn!("Failed to get connection from pool: {}", e);
                                    //         continue;
                                    //     }
                                    // };

                                    // tokio::spawn(async move {
                                    //     let result = diesel::insert_into(serialized_dag::table)
                                    //         .values(&new_serialized_dag)
                                    //         .execute(&mut conn)
                                    //         .await;
                                    //     match result {
                                    //         Ok(_) => info!(
                                    //             "Saved DAG {} to database",
                                    //             new_serialized_dag.dag_id
                                    //         ),
                                    //         Err(e) => warn!(
                                    //             "Failed to save DAG {} to database: {}",
                                    //             new_serialized_dag.dag_id, e
                                    //         ),
                                    //     }
                                    // });
                                }
                            }
                            Err(e) => {
                                warn!(
                                    "Failed to deserialize python output: {}. Output: {}",
                                    e, line
                                );
                            }
                        }
                        line.clear();
                    }
                    Err(e) => {
                        warn!("Failed to read from worker stdout: {}", e);
                        break;
                    }
                }
            }
        });

        let mut stderr = stderr; // Get stderr from the worker struct
        tokio::spawn(async move {
            let mut line = String::new();
            loop {
                match stderr.read_line(&mut line).await {
                    Ok(0) => {
                        // EOF
                        trace!("Worker stderr closed.");
                        break;
                    }
                    Ok(_) => {
                        if !line.trim().is_empty() {
                            warn!("Worker {i} stderr: {}", line.trim()); // Log stderr messages
                        }
                        line.clear();
                    }
                    Err(e) => {
                        warn!("Failed to read from worker {i} stderr: {}", e);
                        break;
                    }
                }
            }
            debug!("Worker {i} stderr task finished.");
        });

        workers.push(child);
    }

    let dag_files = utils::file::find_dag_file_paths(&bundle.path)?;

    for (i, dag_file) in dag_files.into_iter().enumerate() {
        let sender = senders[i % manager_config.parsing_processes].clone();
        info!(
            "Sending dag_file #{} to worker #{}",
            i,
            i % manager_config.parsing_processes
        );

        if let Err(e) = sender.send(dag_file).await {
            warn!("Failed to send file to worker: {}", e);
            // TODO: Decide how to handle failures to send: continue, break, etc.
        }
    }

    // Drop original senders to allow workers to terminate when all files are processed
    drop(senders);

    info!(
        "Checking for new files in local bundle every {} seconds",
        bundle.refresh_interval
    );
    while AtomicUsize::load(&signal_count, Ordering::SeqCst) == 0 {
        info!("In this part we'll handle notifications and process Dag files...");
        sleep(Duration::from_secs(10)).await;
    }

    let duration = start.elapsed();
    debug!("Time elapsed in main() is: {:?}", duration);
    handle.close();
    signals_task.await?;
    Ok(())
}
