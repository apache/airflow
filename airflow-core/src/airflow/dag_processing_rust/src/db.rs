use crate::schema::{dag as dags, dag_bundle, dag_code, dag_version, serialized_dag};
use crate::EdgeInfoType;
use anyhow::{Context, Result};
use chrono;
use chrono::{DateTime, Utc};
use diesel::pg::PgConnection;
use diesel::prelude::*;
use diesel::r2d2::{ConnectionManager, Pool};
use dotenvy::dotenv;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::json;
use std::env;
use uuid::Uuid;

pub type DbPool = diesel::r2d2::Pool<ConnectionManager<PgConnection>>;

// Change this function to accept a size
pub fn get_connection_pool_with_size(size: u32) -> DbPool {
    dotenv().ok();
    let database_url = env::var("DATABASE_URL").expect("DATABASE_URL must be set");
    let manager = ConnectionManager::<PgConnection>::new(database_url);
    diesel::r2d2::Pool::builder()
        .max_size(size) // Use the provided size
        .build(manager)
        .expect("Failed to create pool.")
}

mod serde_helpers {
    use super::*;
    // Make types from parent module available
    use serde::de;

    /// Deserializes an optional floating-point Unix timestamp into an `Option<DateTime<Utc>>`.
    ///
    /// This function correctly handles a JSON value that is either a number (f64)
    /// or `null`.
    pub fn deserialize_f64_timestamp_option<'de, D>(
        deserializer: D,
    ) -> Result<Option<DateTime<Utc>>, D::Error>
    where
        D: Deserializer<'de>,
    {
        // First, deserialize into an Option<f64> to handle the JSON `null` case.
        let opt_float = Option::<f64>::deserialize(deserializer)?;

        // Match on the result.
        match opt_float {
            Some(float_val) => {
                // If we got a float, convert it to i64 seconds.
                let seconds = float_val as i64;
                // Use the valid `from_timestamp` function. It returns an Option
                // itself, so we handle the case of an out-of-range timestamp.
                match DateTime::from_timestamp(seconds, 0) {
                    Some(dt) => Ok(Some(dt)),
                    None => Err(de::Error::custom(format!(
                        "Invalid or out-of-range Unix timestamp: {}",
                        float_val
                    ))),
                }
            }
            // If the JSON was `null`, we get `None`, which we pass through.
            None => Ok(None),
        }
    }

    pub fn serialize_f64_timestamp_option<S>(
        date: &Option<DateTime<Utc>>,
        serializer: S,
    ) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match date {
            Some(dt) => serializer.serialize_f64(dt.timestamp() as f64),
            None => serializer.serialize_none(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Dag {
    // pub access_control: Option<String>,
    // pub catchup: bool,
    pub dag_display_name: Option<String>,
    pub dag_id: String,
    pub deadline: Option<std::collections::HashMap<String, serde_json::Value>>,
    pub default_args: Option<std::collections::HashMap<String, serde_json::Value>>,
    pub description: Option<String>,
    // pub disable_bundle_versioning: bool,
    pub doc_md: Option<String>,
    pub edge_info: Option<std::collections::HashMap<String, std::collections::HashMap<String, EdgeInfoType>>>,
    pub fail_fast: Option<bool>,
    pub fileloc: Option<String>,
    pub is_paused_upon_creation: Option<bool>,
    pub max_active_runs: Option<u32>,
    pub max_active_tasks: Option<u32>,
    pub max_consecutive_failed_dag_runs: Option<u32>,
    pub owner_links: Option<std::collections::HashMap<String, String>>,
    pub relative_fileloc: Option<String>,
    // pub render_template_as_native_obj: bool,
    pub start_date: Option<chrono::DateTime<chrono::Utc>>,
    // pub tags: Option<Vec<String>>,
    pub task_group: Option<String>,
    pub timetable: Option<String>,
    pub timezone: Option<String>,
}

#[derive(Queryable, Insertable, AsChangeset, Debug)]
#[diesel(table_name = dags)]
pub struct NewDag {
    pub dag_id: String,
    pub is_paused: Option<bool>,
    pub is_stale: Option<bool>,
    pub last_parsed_time: Option<DateTime<Utc>>,
    pub last_expired: Option<DateTime<Utc>>,
    pub fileloc: Option<String>,
    pub relative_fileloc: Option<String>,
    pub bundle_name: Option<String>,
    pub bundle_version: Option<String>,
    pub owners: Option<String>,
    pub dag_display_name: Option<String>,
    pub description: Option<String>,
    pub timetable_summary: Option<String>,
    pub timetable_description: Option<String>,
    pub asset_expression: Option<serde_json::Value>,
    pub deadline: Option<serde_json::Value>,
    pub max_active_tasks: i32,
    pub max_active_runs: Option<i32>,
    pub max_consecutive_failed_dag_runs: i32,
    pub has_task_concurrency_limits: bool,
    pub has_import_errors: Option<bool>,
    pub next_dagrun: Option<DateTime<Utc>>,
    pub next_dagrun_data_interval_start: Option<DateTime<Utc>>,
    pub next_dagrun_data_interval_end: Option<DateTime<Utc>>,
    pub next_dagrun_create_after: Option<DateTime<Utc>>,
}

// NOTE: The unsized `get_connection_pool` function has been removed
// to prevent accidental creation of oversized pools. Always use
// `get_connection_pool_with_size`.

pub fn save_dag(conn: &mut PgConnection, dag: &Dag) -> Result<()> {
    let new_dag = NewDag::from(dag);
    diesel::insert_into(dags::table)
        .values(&new_dag)
        .on_conflict(dags::dag_id)
        .do_update()
        .set(&new_dag)
        .execute(conn)
        .with_context(|| format!("Error saving DAG '{}'", new_dag.dag_id))?;

    Ok(())
}


impl From<&Dag> for NewDag {
    fn from(dag: &Dag) -> Self {
        let owners = dag.default_args
            .as_ref()
            .and_then(|args| args.get("owner").or_else(|| args.get("owners")))
            .and_then(|owner_val| owner_val.as_str())
            .map(|s| s.to_string())
            .unwrap_or_else(|| "airflow".to_string());

        NewDag {
            dag_id: dag.dag_id.clone(),
            is_paused: Option::from(dag.is_paused_upon_creation.unwrap_or(true)),
            is_stale: Option::from(false),
            last_parsed_time: Some(Utc::now()),
            last_expired: None,
            fileloc: dag.fileloc.clone(),
            relative_fileloc: dag.relative_fileloc.clone(),
            owners: Some(owners),
            dag_display_name: dag.dag_display_name.clone(),
            description: dag.description.clone(),
            timetable_summary: dag.timetable.clone(),
            timetable_description: dag.timetable.clone(),
            deadline: dag.deadline.as_ref().and_then(|d| serde_json::to_value(d).ok()),
            max_active_tasks: dag.max_active_tasks.map(|v| v as i32).unwrap_or(0),
            max_active_runs: dag.max_active_runs.map(|v| v as i32),
            max_consecutive_failed_dag_runs: dag.max_consecutive_failed_dag_runs.map(|v| v as i32).unwrap_or(0),
            has_task_concurrency_limits: false,
            has_import_errors: Option::from(false),
            bundle_name: Some("dags-folder".to_string()), // Parametrize later
            bundle_version: None,
            asset_expression: None,
            next_dagrun: None,
            next_dagrun_data_interval_start: None,
            next_dagrun_data_interval_end: None,
            next_dagrun_create_after: None,
        }
    }
}


#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SerializedDagData {
    // These fields are present in the sample JSON
    pub _processor_dags_folder: String,
    // pub catchup: bool,
    pub dag_dependencies: Vec<serde_json::Value>,
    pub dag_id: String,
    pub deadline: Option<serde_json::Value>,
    // pub disable_bundle_versioning: bool,
    pub edge_info: serde_json::Value,
    pub fileloc: String,
    pub params: Vec<serde_json::Value>,
    pub relative_fileloc: String,
    // pub tags: Vec<String>,
    pub task_group: serde_json::Value,
    pub tasks: Vec<serde_json::Value>,
    pub timetable: serde_json::Value,
    pub timezone: String,

    // This field uses our custom serializer/deserializer
    #[serde(
        deserialize_with = "serde_helpers::deserialize_f64_timestamp_option",
        serialize_with = "serde_helpers::serialize_f64_timestamp_option"
    )]
    pub start_date: Option<DateTime<Utc>>,

    // --- REMOVED FIELDS ---
    // The following fields were in the Rust struct but are NOT in the sample JSON.
    // They must be removed to prevent deserialization errors.
    //
    // pub description: Option<String>,
    // pub max_active_tasks: Option<u32>,
    // pub fail_fast: Option<bool>,
    // pub max_active_runs: Option<u32>,
    // pub doc_md: Option<String>,
    // pub max_consecutive_failed_dag_runs: Option<u32>,
    // pub render_template_as_native_obj: bool,
    // pub owner_links: Option<std::collections::HashMap<String, String>>,
    // pub is_paused_upon_creation: Option<bool>,
    // pub default_args: Option<std::collections::HashMap<String, serde_json::Value>>,
    // pub end_date: Option<DateTime<Utc>>,
    // pub dag_display_name: Option<String>,
    // pub access_control: Option<String>,
    // pub dagrun_timeout: Option<serde_json::Value>,
    // pub has_on_success_callback: bool,
    // pub has_on_failure_callback: bool,
}

fn create_wrapped_dag_json(data: &SerializedDagData) -> Option<serde_json::Value> {
    // First, convert the content struct to a serde_json::Value
    serde_json::to_value(data)
        .ok()
        .map(|content| {
            // Then, use the json! macro to create the wrapper object
            json!({
                "__version": 2,
                "dag": content
            })
        })
}

/// 1. A "pure" struct for storing a complete serialized DAG entity.
///
/// This struct holds both the serializable data (`SerializedDagData`) and the
/// metadata required to create a record in the database, such as the `dag_hash`
/// and various IDs.
#[derive(Debug, Clone)]
pub struct SerializedDag {
    pub id: Uuid,
    pub dag_id: String,
    pub dag_hash: String,
    pub data: SerializedDagData,
    pub dag_version_id: Uuid,
}


/// 2. Diesel-specific struct for inserting or updating records in the `serialized_dag` table.
///
/// This struct maps directly to the `serialized_dag` table schema and is used in
/// Diesel operations. It can be created from the pure `SerializedDag` struct.
#[derive(Insertable, AsChangeset, Debug)]
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

impl From<&SerializedDag> for NewSerializedDag {
    /// Converts the pure `SerializedDag` struct into a `NewSerializedDag` struct
    /// that is ready for database insertion or update.
    fn from(s_dag: &SerializedDag) -> Self {
        // The `s_dag.data` struct is serialized into a `serde_json::Value`
        // which corresponds to the `JSON` type in the database.
        // let data_json = serde_json::to_value(&s_dag.data).ok();

        NewSerializedDag {
            id: s_dag.id,
            dag_id: s_dag.dag_id.clone(),
            data: create_wrapped_dag_json(&s_dag.data),
            data_compressed: None, // Compression can be implemented here if needed
            created_at: Utc::now(),
            last_updated: Utc::now(),
            dag_hash: s_dag.dag_hash.clone(),
            dag_version_id: s_dag.dag_version_id,
        }
    }
}

// in db.rs

// This struct is used specifically for updating an existing serialized_dag record.
// It omits fields that should not be changed on update, like `id` and `created_at`.
#[derive(AsChangeset, Debug)]
#[diesel(table_name = serialized_dag)]
pub struct SerializedDagChangeset {
    pub dag_id: String,
    pub data: Option<serde_json::Value>,
    pub data_compressed: Option<Vec<u8>>,
    pub last_updated: DateTime<Utc>,
    pub dag_hash: String,
    pub dag_version_id: Uuid,
}

pub fn save_serialized_dag(conn: &mut PgConnection, s_dag: &SerializedDag) -> Result<()> {
    conn.transaction(|conn| {
        // Find the ID of the most recently updated serialized_dag for this dag_id
        let maybe_target_id: Option<Uuid> = serialized_dag::table
            .filter(serialized_dag::dag_id.eq(&s_dag.dag_id))
            .order(serialized_dag::last_updated.desc())
            .select(serialized_dag::id)
            .first::<Uuid>(conn)
            .optional()?;

        if let Some(target_id) = maybe_target_id {
            // An existing record was found, so we update it.
            let changeset = SerializedDagChangeset {
                dag_id: s_dag.dag_id.clone(),
                data: create_wrapped_dag_json(&s_dag.data),
                data_compressed: None, // Or implement compression
                last_updated: Utc::now(),
                dag_hash: s_dag.dag_hash.clone(),
                dag_version_id: s_dag.dag_version_id,
            };

            diesel::update(serialized_dag::table.find(target_id))
                .set(&changeset)
                .execute(conn)
                .with_context(|| format!("Error updating SerializedDAG for '{}'", s_dag.dag_id))?;
        } else {
            // No existing record was found, so we insert a new one.
            let new_s_dag = NewSerializedDag::from(s_dag);
            diesel::insert_into(serialized_dag::table)
                .values(&new_s_dag)
                .execute(conn)
                .with_context(|| format!("Error inserting new SerializedDAG for '{}'", s_dag.id))?;
        }
        Ok(())
    })
}

// Define a new struct for inserting into the dag_version table
#[derive(Insertable, Debug)]
#[diesel(table_name = dag_version)]
pub struct NewDagVersion {
    pub id: Uuid,
    pub version_number: i32,
    pub dag_id: String,
    pub bundle_name: String,
    pub bundle_version: String,
    pub created_at: DateTime<Utc>,
    pub last_updated: DateTime<Utc>,
}

// This function creates a new DAG version record and returns its UUID.
// It should be placed alongside your other DB functions like `save_dag`.

// in db.rs

// Add the new table to your 'use' statements


// Define a new struct for inserting into the dag_code table
#[derive(Insertable, AsChangeset, Debug)]
#[diesel(table_name = dag_code)]
pub struct NewDagCode {
    pub id: Uuid,
    pub dag_id: String,
    pub fileloc: String,
    pub created_at: DateTime<Utc>,
    pub last_updated: DateTime<Utc>,
    pub source_code: String,
    pub source_code_hash: String,
    pub dag_version_id: Uuid,
}

// This function creates a new dag_code record.
// Place it alongside your other database functions like `save_dag`.
pub fn save_dag_code(
    conn: &mut PgConnection,
    p_dag_id: &str,
    p_dag_version_id: Uuid,
    p_fileloc: &str,
    p_source_code: &str,
) -> Result<()> {
    let hash = format!("{:x}", md5::compute(p_source_code.as_bytes()));

    let new_dag_code = NewDagCode {
        id: Uuid::new_v4(), // Note: `id` is ignored on conflict/update.
        dag_id: p_dag_id.to_string(),
        fileloc: p_fileloc.to_string(),
        created_at: Utc::now(), // Note: `created_at` is ignored on conflict/update.
        last_updated: Utc::now(),
        source_code: p_source_code.to_string(),
        source_code_hash: hash,
        dag_version_id: p_dag_version_id,
    };

    diesel::insert_into(dag_code::table)
        .values(&new_dag_code)
        // On conflict with the unique key `dag_version_id`...
        .on_conflict(dag_code::dag_version_id)
        // ...do an update instead of failing.
        .do_update()
        .set(&new_dag_code)
        .execute(conn)
        .with_context(|| "while upserting DagCode")?;

    Ok(())
}

#[derive(Insertable, AsChangeset)]
#[diesel(table_name = dag_bundle)]
pub struct DagBundle<'a> {
    pub name: &'a str,
    pub active: bool,
    pub version: Option<&'a str>,
    pub last_refreshed: DateTime<Utc>,
}

/// Inserts a new dag_bundle row or updates an existing one on conflict.
pub fn upsert_dag_bundle(
    conn: &mut PgConnection,
    bundle_name: &str,
    is_active: bool,
    bundle_version: &str,
) -> Result<()> {
    let now = Utc::now();
    let bundle = DagBundle {
        name: bundle_name,
        active: is_active,
        version: Some(bundle_version),
        last_refreshed: now,
    };

    diesel::insert_into(dag_bundle::table)
        .values(&bundle)
        .on_conflict(dag_bundle::name)
        // If the row already exists, update these fields
        .do_update()
        .set(&bundle)
        .execute(conn)
        .with_context(|| format!("Error upserting DagBundle '{}'", bundle_name))?;

    Ok(())
}

// Add a new helper struct to query existing hash and version info.
#[derive(Queryable, Debug)]
struct DagCodeInfo {
    pub source_code_hash: String,
    pub dag_version_id: Uuid,
}

// Replace the entire `save_dag_version` function with this new, more intelligent version.
pub fn ensure_dag_version_and_get_id(
    conn: &mut PgConnection,
    p_dag_id: &str,
    new_source_hash: &str,
) -> Result<Uuid> {
    // 1. Find the most recent DagCode record for this DAG.
    let latest_dag_code: Option<DagCodeInfo> = dag_code::table
        .filter(dag_code::dag_id.eq(p_dag_id))
        .order(dag_code::last_updated.desc())
        .select((dag_code::source_code_hash, dag_code::dag_version_id))
        .first::<DagCodeInfo>(conn)
        .optional()?;

    // 2. Check if the source code has changed.
    if let Some(existing_code) = latest_dag_code {
        if existing_code.source_code_hash == new_source_hash {
            // Hashes are the same, no changes. Reuse the existing version ID.
            info!("DAG '{}' source code has not changed. Reusing version_id: {}", p_dag_id, existing_code.dag_version_id);
            return Ok(existing_code.dag_version_id);
        }
    }

    // 3. If code has changed (or this is the first time), create a new version.
    info!("DAG '{}' source code has changed. Creating new version.", p_dag_id);

    // Find the current max version number for this DAG.
    let max_version: Option<i32> = dag_version::table
        .filter(dag_version::dag_id.eq(p_dag_id))
        .select(diesel::dsl::max(dag_version::version_number))
        .get_result(conn)?; // Using get_result which is clearer for single-value queries.
    // --- END FIX ---

    let new_version_number = max_version.unwrap_or(0) + 1;

    let new_version = NewDagVersion {
        id: Uuid::new_v4(),
        version_number: new_version_number,
        dag_id: p_dag_id.to_string(),
        bundle_version: String::from("1"),
        bundle_name: String::from("dags-folder"),
        created_at: Utc::now(),
        last_updated: Utc::now(),
    };

    // 4. Insert the new version and return its ID.
    diesel::insert_into(dag_version::table)
        .values(&new_version)
        .returning(dag_version::id)
        .get_result(conn)
        .with_context(|| format!("Error saving new DagVersion for DAG '{}'", p_dag_id))
}
