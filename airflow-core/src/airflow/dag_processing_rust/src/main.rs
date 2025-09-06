#[macro_use]
extern crate log;

pub mod db;
pub mod schema;

use crate::db::{
    get_connection_pool_with_size, save_dag_code, save_serialized_dag, upsert_dag_bundle, Dag,
    SerializedDag, SerializedDagData,
};
use anyhow::{anyhow, Context, Result};
use diesel::r2d2::{ConnectionManager, Pool};
use diesel::{Connection, PgConnection};
use dotenvy::dotenv;
use md5;
use notify::{Event, EventKind, RecursiveMode, Watcher};
use pyo3::types::{PyBytes, PyDict, PyList};
use pyo3::Bound;
use pyo3::{prelude::*, FromPyObject};
use serde::{Deserialize, Serialize};
use serde_json;
use std::io::BufRead;
use std::path::PathBuf;
use std::process::Stdio;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt};
use tokio::process::{Child, ChildStdin, Command};
use tokio::signal;
use tokio::sync::mpsc;
use uuid::Uuid;
use walkdir::WalkDir;

pub type DbPool = Pool<ConnectionManager<PgConnection>>;

#[derive(Debug, Clone, FromPyObject, Serialize, Deserialize)]
struct EdgeInfoType {
    label: Option<String>,
}

/// Utility function to convert a PyAny object to a serde_json::Value.
fn py_any_to_serde_json_value(
    py: Python<'_>,
    obj: &Bound<'_, PyAny>,
) -> PyResult<serde_json::Value> {
    let json = py.import("json")?;
    let result_str: String = json.call_method1("dumps", (obj,))?.extract()?;
    serde_json::from_str(&result_str).map_err(|e| {
        pyo3::exceptions::PyValueError::new_err(format!("Failed to deserialize JSON: {}", e))
    })
}

/// The core parsing logic for a single DAG file. Reused by the worker.
// CHANGED: Signature no longer takes `py: Python`. The function now acquires the GIL itself.
fn process_dag_file(dags_dir: &str, file_path_str: &str, pool: &DbPool) -> Result<()> {
    // CHANGED: The entire function body is wrapped in `with_gil` to acquire the Python lock.
    Python::with_gil(|py| {
        let builtins = py.import("builtins")?;
        let exec_func = builtins.getattr("exec")?;

        let sys = py.import("sys")?;
        let sys_path_obj = sys.getattr("path")?;
        let sys_path: &Bound<'_, PyList> = sys_path_obj
            .downcast()
            .map_err(|e| anyhow!("sys.path was not a list: {}", e))?;
        sys_path.insert(0, &dags_dir)?;

        let dag_module = py.import("airflow.sdk.definitions.dag")?;
        let dag_class = dag_module.getattr("DAG")?;

        let file_content = std::fs::read(&file_path_str)
            .with_context(|| format!("[WORKER] Failed to read file {}", file_path_str))?;

        let code_bytes = PyBytes::new(py, &file_content);
        let globals = PyDict::new(py);
        globals.set_item("__name__", "__main__")?;
        globals.set_item("__file__", file_path_str)?;

        let result = exec_func.call((code_bytes, globals.clone(), globals.clone()), None);

        match result {
            Ok(_) => {
                let mut dag_found = false;
                for (_name, obj) in globals.iter() {
                    if obj.is_instance(&dag_class)? {
                        dag_found = true;
                        let dag = Dag {
                            dag_id: obj.getattr("dag_id")?.extract()?,
                            description: obj.getattr("description").ok().and_then(|v| v.extract().ok()),
                            start_date: obj.getattr("start_date").ok().and_then(|v| v.extract().ok()),
                            max_active_tasks: obj.getattr("max_active_tasks").ok().and_then(|v| v.extract().ok()),
                            max_active_runs: obj.getattr("max_active_runs").ok().and_then(|v| v.extract().ok()),
                            fileloc: Some(file_path_str.to_string()),
                            // disable_bundle_versioning: obj.getattr("disable_bundle_versioning").ok().and_then(|v| v.extract().ok()).unwrap_or(false),
                            relative_fileloc: Some(
                                PathBuf::from(&file_path_str)
                                    .strip_prefix(&dags_dir)
                                    .map(|p| p.to_string_lossy().into_owned())
                                    .unwrap_or_else(|_| file_path_str.to_string()),
                            ),
                            // catchup: obj.getattr("catchup").ok().and_then(|v| v.extract().ok()).unwrap_or(false),
                            dag_display_name: obj.getattr("dag_display_name").ok().and_then(|v| v.extract().ok()),
                            timetable: obj.getattr("timetable").ok().and_then(|v| v.extract().ok()),
                            default_args: obj.getattr("default_args").ok().and_then(|v| {
                                py_any_to_serde_json_value(py, &v)
                                    .ok()
                                    .and_then(|json_val| serde_json::from_value(json_val).ok())
                            }),
                            fail_fast: None,
                            doc_md: None,
                            edge_info: None,
                            max_consecutive_failed_dag_runs: None,
                            owner_links: None,
                            // tags: None,
                            is_paused_upon_creation: None,
                            timezone: None,
                            task_group: None,
                            deadline: None,
                        };
                        let file_hash = format!("{:x}", md5::compute(&file_content));

                        info!("[WORKER] Saving DAG '{}' from file '{}'", dag.dag_id, file_path_str);
                        let serialized_dag_module = py.import("airflow.serialization.serialized_objects")?;
                        let serialized_dag_class = serialized_dag_module.getattr("SerializedDAG")?;

                        let mut conn = pool.get().context("[WORKER] could not get DB connection from pool")?;
                        let transaction_result = conn.transaction(|conn| -> anyhow::Result<()> {
                            db::save_dag(conn, &dag)?;
                            let new_dag_version_id = db::ensure_dag_version_and_get_id(conn, &dag.dag_id, &file_hash)?;
                            let file_content_str = String::from_utf8_lossy(&file_content).to_string();
                            db::save_dag_code(conn, &dag.dag_id, new_dag_version_id, file_path_str, &file_content_str)?;
                            let serialized_data_py_obj = serialized_dag_class.call_method1("serialize_dag", (obj,))?;
                            let serialized_json_val = py_any_to_serde_json_value(py, &serialized_data_py_obj)?;
                            let mut data_dict = match serialized_json_val {
                                serde_json::Value::Object(d) => Ok(d),
                                _ => Err(anyhow!("Serialized DAG was not a JSON object")),
                            }?;
                            if let Some(fileloc) = &dag.fileloc { data_dict.insert("fileloc".to_string(), serde_json::Value::String(fileloc.clone())); }
                            if let Some(relative_fileloc) = &dag.relative_fileloc { data_dict.insert("relative_fileloc".to_string(), serde_json::Value::String(relative_fileloc.clone())); }
                            let serialized_data: SerializedDagData = serde_json::from_value(serde_json::Value::Object(data_dict))?;
                            let json_string_for_hash = serde_json::to_string(&serialized_data)?;
                            let dag_hash = format!("{:x}", md5::compute(json_string_for_hash.as_bytes()));
                            let s_dag = SerializedDag { id: Uuid::new_v4(), dag_id: serialized_data.dag_id.clone(), dag_hash, data: serialized_data, dag_version_id: new_dag_version_id };
                            db::save_serialized_dag(conn, &s_dag)?;
                            Ok(())
                        });

                        if let Err(e) = transaction_result {
                            error!("[WORKER] DB transaction failed for DAG '{}': {:?}", dag.dag_id, e);
                        }
                        break;
                    }
                }
                if !dag_found {
                    warn!("[WORKER] No instance of DAG found in file '{}'", file_path_str);
                }
            }
            Err(e) => {
                error!("[WORKER] Python error executing file '{}':", file_path_str);
                e.print(py);
            }
        }
        Ok(())
    }) // The `anyhow::Result` from the closure is returned by `with_gil`
}

/// A long-lived worker process that processes files sent via stdin.
fn run_long_lived_worker(dags_dir: String, pool_size: u32) -> Result<()> {
    // NOTE: This MUST be called only once per process, so it stays here.
    pyo3::prepare_freethreaded_python();
    let pool = get_connection_pool_with_size(pool_size);
    info!("[WORKER] Long-lived worker process started with pool size {}. Waiting for file paths on stdin...", pool_size);

    // Set up signal handling for graceful shutdown
    let pool_for_signal = pool.clone();
    std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        rt.block_on(async {
            let mut sigterm = signal::unix::signal(signal::unix::SignalKind::terminate()).unwrap();
            let mut sigint = signal::unix::signal(signal::unix::SignalKind::interrupt()).unwrap();

            tokio::select! {
                _ = sigterm.recv() => {
                    info!("[WORKER] Received SIGTERM, shutting down gracefully...");
                },
                _ = sigint.recv() => {
                    info!("[WORKER] Received SIGINT, shutting down gracefully...");
                }
            }

            // Close all database connections
            info!("[WORKER] Closing database connections...");
            drop(pool_for_signal);
            info!("[WORKER] Database connections closed.");

            std::process::exit(0);
        });
    });

    let stdin = std::io::stdin();
    let reader = stdin.lock();

    // CHANGED: Removed the `Python::with_gil` wrapper from here.
    for line in reader.lines() {
        let file_path_str = line.context("[WORKER] Failed to read line from stdin")?;
        if file_path_str.is_empty() {
            continue;
        }
        info!("[WORKER] Received job for: {}", &file_path_str);
        // CHANGED: Call `process_dag_file` directly. It now manages the GIL itself.
        if let Err(e) = process_dag_file(&dags_dir, &file_path_str, &pool) {
            error!("[WORKER] Failed to process file '{}': {:?}", file_path_str, e);
        }
    }

    info!("[WORKER] Stdin closed. Closing database connections and shutting down gracefully.");
    drop(pool);
    Ok(())
}

/// The main scheduler process that manages the worker pool.
async fn run_scheduler() -> Result<()> {
    let dags_dir = "/files/dags";
    std::fs::create_dir_all(dags_dir)?;
    let dags_dir_path = PathBuf::from(dags_dir);

    let num_workers = 2; // num_cpus::get();
    let num_processes = (num_workers + 1) as u32; // +1 for the scheduler itself

    let total_db_connections_limit = 90;
    let pool_size_per_process = (total_db_connections_limit / num_processes).max(2);

    info!(
        "[SCHEDULER] Total processes: {}. Calculated pool size per process: {}",
        num_processes, pool_size_per_process
    );

    info!("[SCHEDULER] Upserting 'dags-folder' entry...");
    let mut conn = get_connection_pool_with_size(pool_size_per_process).get()?;
    db::upsert_dag_bundle(&mut conn, "dags-folder", true, "1")?;
    info!("[SCHEDULER] DagBundle entry is up to date.");

    info!("[SCHEDULER] Spawning {} long-lived worker processes...", num_workers);

    let mut workers: Vec<(Child, ChildStdin)> = Vec::new();
    for i in 0..num_workers {
        let exe = std::env::current_exe()?;
        let mut cmd = Command::new(exe);
        cmd.arg("--long-lived-worker")
            .arg(dags_dir)
            .arg(pool_size_per_process.to_string())
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());

        let mut child = cmd.spawn().context("Failed to spawn worker process")?;

        let stdin = child.stdin.take().context("Failed to get worker stdin")?;
        let stdout = child.stdout.take().context("Failed to get worker stdout")?;
        let stderr = child.stderr.take().context("Failed to get worker stderr")?;

        tokio::spawn(async move {
            let mut reader = tokio::io::BufReader::new(stdout).lines();
            while let Ok(Some(line)) = reader.next_line().await {
                info!("[WORKER-{}] {}", i, line);
            }
        });
        tokio::spawn(async move {
            let mut reader = tokio::io::BufReader::new(stderr).lines();
            while let Ok(Some(line)) = reader.next_line().await {
                error!("[WORKER-{}] {}", i, line);
            }
        });

        workers.push((child, stdin));
    }

    let (tx, mut rx) = mpsc::channel::<String>(10000);

    info!("[SCHEDULER] Performing initial scan of '{}'...", dags_dir);
    let initial_files: Vec<String> = WalkDir::new(dags_dir)
        .into_iter()
        .filter_map(Result::ok)
        .filter(|e| e.path().is_file() && e.path().extension().map_or(false, |ext| ext == "py"))
        .map(|e| e.path().to_string_lossy().into_owned())
        .collect();

    info!("[SCHEDULER] Found {} initial files to process.", initial_files.len());

    for (i, file_path) in initial_files.into_iter().enumerate() {
        let worker_index = i % num_workers;
        let (_, stdin) = &mut workers[worker_index];
        let line_with_newline = format!("{}\n", file_path);
        stdin
            .write_all(line_with_newline.as_bytes())
            .await
            .context("Failed to write to worker stdin")?;
    }
    info!("[SCHEDULER] Initial file distribution complete.");

    let watcher_tx = tx.clone();
    let mut watcher = notify::recommended_watcher(move |res: Result<Event, notify::Error>| {
        if let Ok(event) = res {
            if let EventKind::Create(_) | EventKind::Modify(_) = event.kind {
                for path in event.paths {
                    if path.is_file() && path.extension().map_or(false, |ext| ext == "py") {
                        if let Err(e) = watcher_tx.blocking_send(path.to_string_lossy().into_owned()) {
                            error!("[FS WATCH] Failed to queue file update: {}", e);
                        }
                    }
                }
            }
        }
    })?;
    watcher.watch(&dags_dir_path, RecursiveMode::Recursive)?;
    info!("[SCHEDULER] Now watching for file changes...");

    // Create signal handler outside the loop
    let mut sigterm = signal::unix::signal(signal::unix::SignalKind::terminate())?;

    let mut next_worker_index = 0;
    loop {
        tokio::select! {
            biased;

            Some(file_path) = rx.recv() => {
                info!("[SCHEDULER] Assigning updated file '{}' to worker {}", file_path, next_worker_index);
                let (_, stdin) = &mut workers[next_worker_index];

                let line_with_newline = format!("{}\n", file_path);
                if let Err(e) = stdin.write_all(line_with_newline.as_bytes()).await {
                    error!("Failed to write to worker {}: {}. It may have crashed.", next_worker_index, e);
                }
                next_worker_index = (next_worker_index + 1) % num_workers;
            },
            _ = signal::ctrl_c() => {
                info!("[SCHEDULER] Received SIGINT (Ctrl+C).");
                break;
            },
            _ = sigterm.recv() => {
                info!("[SCHEDULER] Received SIGTERM.");
                break;
            }
        }
    }

    info!("[SCHEDULER] Shutting down workers...");
    for (mut child, stdin) in workers {
        // Drop stdin to signal EOF to the worker
        drop(stdin);

        // Give the worker a chance to exit gracefully
        let shutdown_timeout = tokio::time::Duration::from_secs(2);
        match tokio::time::timeout(shutdown_timeout, child.wait()).await {
            Ok(Ok(_)) => {
                info!("[SCHEDULER] Worker shut down gracefully");
            }
            Ok(Err(e)) => {
                error!("[SCHEDULER] Worker failed to shut down: {}", e);
            }
            Err(_) => {
                warn!("[SCHEDULER] Worker did not shut down within timeout, killing it");
                if let Err(e) = child.kill().await {
                    error!("[SCHEDULER] Failed to kill worker: {}", e);
                }
            }
        }
    }

    info!("[SCHEDULER] Shutdown complete.");
    Ok(())
}

/// Entry point of the application.
fn main() -> Result<()> {
    // Load .env file for all processes.
    dotenv().ok();
    // Initialize logger.
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    let args: Vec<String> = std::env::args().collect();

    // The worker now expects 4 arguments: <executable> --long-lived-worker <dags_dir> <pool_size>
    if args.len() == 4 && args[1] == "--long-lived-worker" {
        let dags_dir = args[2].clone();
        let pool_size: u32 = args[3]
            .parse()
            .context("Failed to parse pool_size argument for worker")?;
        if let Err(e) = run_long_lived_worker(dags_dir, pool_size) {
            error!("[WORKER] Worker process exited with error: {:?}", e);
            std::process::exit(1);
        }
    } else {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()?;
        rt.block_on(run_scheduler())?;
    }
    Ok(())
}
