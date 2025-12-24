#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""
Simple HBase backup operations example.

This DAG demonstrates basic HBase backup functionality:
1. Creating backup sets
2. Creating full backup
3. Getting backup history

Prerequisites:
- HBase must be running in distributed mode with HDFS
- Create backup directory in HDFS: hdfs dfs -mkdir -p /user/hbase && hdfs dfs -chmod 777 /user/hbase

You need to have a proper HBase setup suitable for backups!
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.hbase.operators.hbase import (
    HBaseBackupHistoryOperator,
    HBaseBackupSetOperator,
    HBaseCreateBackupOperator,
    HBaseCreateTableOperator,
    HBaseDeleteTableOperator,
    HBasePutOperator,
    HBaseRestoreOperator,
    HBaseScanOperator,
)
from airflow.providers.hbase.sensors.hbase import HBaseRowSensor

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "example_hbase_backup_simple",
    default_args=default_args,
    description="Simple HBase backup operations",
    schedule_interval=None,
    catchup=False,
    tags=["example", "hbase", "backup", "simple"],
)

# Delete table if exists for idempotency
delete_table_cleanup = HBaseDeleteTableOperator(
    task_id="delete_table_cleanup",
    table_name="test_table",
    hbase_conn_id="hbase_ssh",
    dag=dag,
)

# Create test table for backup
create_table = HBaseCreateTableOperator(
    task_id="create_table",
    table_name="test_table",
    families={"cf1": {}, "cf2": {}},
    hbase_conn_id="hbase_ssh",
    dag=dag,
)

# Add some test data
put_data = HBasePutOperator(
    task_id="put_data",
    table_name="test_table",
    row_key="test_row",
    data={"cf1:col1": "test_value"},
    hbase_conn_id="hbase_ssh",
    dag=dag,
)

# Create backup set
create_backup_set = HBaseBackupSetOperator(
    task_id="create_backup_set",
    action="add",
    backup_set_name="test_backup_set",
    tables=["test_table"],
    hbase_conn_id="hbase_ssh",
    dag=dag,
)

# List backup sets
list_backup_sets = HBaseBackupSetOperator(
    task_id="list_backup_sets",
    action="list",
    hbase_conn_id="hbase_ssh",
    dag=dag,
)

# Create full backup
create_full_backup = HBaseCreateBackupOperator(
    task_id="create_full_backup",
    backup_type="full",
    backup_path="hbase-backup",
    backup_set_name="test_backup_set",
    workers=1,
    hbase_conn_id="hbase_ssh",
    dag=dag,
)

# Get backup history
get_backup_history = HBaseBackupHistoryOperator(
    task_id="get_backup_history",
    backup_set_name="test_backup_set",
    hbase_conn_id="hbase_ssh",
    dag=dag,
)

# Restore backup (using backup ID from previous backups)
restore_backup = HBaseRestoreOperator(
    task_id="restore_backup",
    backup_path="hbase-backup",
    backup_id="backup_1766156260623",  # Use existing backup ID
    tables=["test_table"],
    overwrite=True,
    hbase_conn_id="hbase_ssh",
    dag=dag,
)

# Verify restored data - check if row exists
verify_row_exists = HBaseRowSensor(
    task_id="verify_row_exists",
    table_name="test_table",
    row_key="test_row",
    hbase_conn_id="hbase_ssh",
    timeout=60,
    poke_interval=10,
    dag=dag,
)

# Verify restored data - scan table to check data content
verify_data_content = HBaseScanOperator(
    task_id="verify_data_content",
    table_name="test_table",
    columns=["cf1:col1"],
    limit=10,
    hbase_conn_id="hbase_ssh",
    dag=dag,
)

# Final cleanup - delete table
final_cleanup = HBaseDeleteTableOperator(
    task_id="final_cleanup",
    table_name="test_table",
    hbase_conn_id="hbase_ssh",
    dag=dag,
)

# Define task dependencies
delete_table_cleanup >> create_table >> put_data >> create_backup_set >> list_backup_sets >> create_full_backup >> get_backup_history >> restore_backup >> verify_row_exists >> verify_data_content >> final_cleanup
