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
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.hbase.operators.hbase import (
    HBaseBackupHistoryOperator,
    HBaseBackupSetOperator,
    HBaseCreateBackupOperator,
)

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
    schedule=None,
    catchup=False,
    tags=["example", "hbase", "backup", "simple"],
)

# Create backup set
create_backup_set = HBaseBackupSetOperator(
    task_id="create_backup_set",
    action="add",
    backup_set_name="test_backup_set",
    tables=["test_table"],
    dag=dag,
)

# List backup sets
list_backup_sets = HBaseBackupSetOperator(
    task_id="list_backup_sets",
    action="list",
    dag=dag,
)

# Create full backup
create_full_backup = HBaseCreateBackupOperator(
    task_id="create_full_backup",
    backup_type="full",
    backup_path="/tmp/hbase-backup",
    backup_set_name="test_backup_set",
    workers=1,
    dag=dag,
)

# Get backup history
get_backup_history = HBaseBackupHistoryOperator(
    task_id="get_backup_history",
    backup_set_name="test_backup_set",
    dag=dag,
)

# Define task dependencies
create_backup_set >> list_backup_sets >> create_full_backup >> get_backup_history