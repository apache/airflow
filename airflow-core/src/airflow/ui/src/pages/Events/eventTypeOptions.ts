/*!
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

export const eventTypeOptions = [
  { label: "All Events", value: "all" },
  // CLI Events
  { label: "CLI Scheduler", value: "cli_scheduler" },
  { label: "CLI Worker", value: "cli_worker" },
  { label: "CLI Triggerer", value: "cli_triggerer" },
  { label: "CLI DAG Processor", value: "cli_dag_processor" },
  { label: "CLI API Server", value: "cli_api_server" },
  { label: "CLI Webserver", value: "cli_webserver" },
  { label: "CLI Run", value: "cli_run" },
  // API Events
  { label: "Trigger DAG Run", value: "trigger_dag_run" },
  { label: "Delete DAG Run", value: "delete_dag_run" },
  { label: "Patch DAG Run", value: "patch_dag_run" },
  { label: "Patch DAG", value: "patch_dag" },
  { label: "Delete DAG", value: "delete_dag" },
  { label: "Favorite DAG", value: "favorite_dag" },
  { label: "Unfavorite DAG", value: "unfavorite_dag" },
  // Task Instance Events
  { label: "Clear Task Instances", value: "post_clear_task_instances" },
  { label: "Patch Task Instance", value: "patch_task_instance" },
  { label: "Get Task Instances Batch", value: "get_task_instances_batch" },
  // Connection Events
  { label: "Create Connection", value: "post_connection" },
  { label: "Update Connection", value: "patch_connection" },
  { label: "Delete Connection", value: "delete_connection" },
  { label: "Create Default Connections", value: "create_default_connections" },
  { label: "Bulk Connections", value: "bulk_connections" },
  // Variable Events
  { label: "Create Variable", value: "post_variable" },
  { label: "Update Variable", value: "patch_variable" },
  { label: "Delete Variable", value: "delete_variable" },
  { label: "Bulk Variables", value: "bulk_variables" },
  // Pool Events
  { label: "Create Pool", value: "post_pool" },
  { label: "Update Pool", value: "patch_pool" },
  { label: "Delete Pool", value: "delete_pool" },
  { label: "Bulk Pools", value: "bulk_pools" },
  // Asset Events
  { label: "Create Asset Event", value: "create_asset_event" },
  { label: "Delete Asset Queued Events", value: "delete_asset_queued_events" },
  { label: "Delete DAG Asset Queued Events", value: "delete_dag_asset_queued_events" },
  { label: "Delete DAG Asset Queued Event", value: "delete_dag_asset_queued_event" },
  // Backfill Events
  { label: "Create Backfill", value: "create_backfill" },
  { label: "Cancel Backfill", value: "cancel_backfill" },
  { label: "Pause Backfill", value: "pause_backfill" },
  { label: "Unpause Backfill", value: "unpause_backfill" },
  // XCom Events
  { label: "Create XCom Entry", value: "create_xcom_entry" },
  { label: "Update XCom Entry", value: "update_xcom_entry" },
  // DAG Parsing Events
  { label: "Reparse DAG File", value: "reparse_dag_file" },
];
