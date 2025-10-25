use anyhow::{Context, Result};
use std::any::Any;
use log::{info, trace, warn};

mod config;
use diesel::prelude::*;
use diesel_async::{AsyncConnection, AsyncConnectionCore, AsyncPgConnection, RunQueryDsl};


use crate::config::DagProcessorManagerConfig;
use tokio::time::Instant;
mod utils;
mod models;

#[tokio::main]
async fn main() -> Result<()> {
    let start = Instant::now();

    let manager_config = DagProcessorManagerConfig::from_cli();
    println!("{:#?}", manager_config);

    let conn = std::env::var("AIRFLOW__DATABASE__SQL_ALCHEMY_CONN".to_string()).expect("Missing AIRFLOW__DATABASE__SQL_ALCHEMY_CONN env var");
    let diesel_conn = utils::db::to_diesel_conn_string(&conn)?;
    let mut connection = AsyncPgConnection::establish(&diesel_conn).await.expect("Failed to connect to database");

    info!("Connection is successful");

    let duration = start.elapsed();
    println!("Time elapsed in main() is: {:?}", duration);


    Ok(())
}
