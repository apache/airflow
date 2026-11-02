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
HBase backup consumer DAG - automatically triggered by dataset.

This DAG demonstrates data-aware scheduling with incremental backups:

#. Automatically triggered when test_table_backup is updated
#. Creates backup set
#. Performs FULL backup on first run, then INCREMENTAL backups
#. Gets backup history

Prerequisites:

- HBase must be running in distributed mode with HDFS
- Create backup directory in HDFS:
  ``hdfs dfs -mkdir -p /hbase/backup && hdfs dfs -chmod 777 /hbase/backup``
- Producer DAG must run first to generate data
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

from airflow.providers.apache.hbase.version_compat import AIRFLOW_V_3_0_PLUS

if AIRFLOW_V_3_0_PLUS:
    from airflow.providers.standard.operators.bash import BashOperator
    from airflow.providers.standard.operators.python import BranchPythonOperator
    from airflow.sdk import DAG
else:
    from airflow import DAG
    from airflow.operators.bash import BashOperator  # type: ignore[no-redef]
    from airflow.operators.python import BranchPythonOperator  # type: ignore[no-redef]

from airflow.providers.apache.hbase.datasets.hbase import hbase_table_dataset
from airflow.providers.apache.hbase.hooks.hbase_cli import HBaseCLIHook
from airflow.providers.apache.hbase.operators.hbase import (
    BackupSetAction,
    BackupType,
    HBaseBackupHistoryOperator,
    HBaseBackupSetOperator,
    HBaseCreateBackupOperator,
)

logger = logging.getLogger(__name__)

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Define dataset - same as in producer
backup_table_dataset = hbase_table_dataset(host="hbase", port=9090, table_name="test_table_backup_v2")


def decide_backup_type(**_context) -> str:
    """
    Decide whether to create FULL or INCREMENTAL backup.

    Logic:
    - Check full backup history for the table
    - If no backups exist -> FULL
    - If backups exist -> INCREMENTAL

    Returns:
        Task ID to execute next
    """
    hook = HBaseCLIHook(hbase_conn_id="hbase_thrift2")
    history = hook.get_backup_history()  # Get full history without filter

    # Check if there are any backups for our table
    if history and "test_table_backup_v2" in history:
        logger.info("Found existing backups for test_table_backup_v2")
        logger.info("Creating INCREMENTAL backup.")
        return "create_incremental_backup"

    logger.info("No backups found for test_table_backup_v2. Creating FULL backup.")
    return "create_full_backup"


with DAG(
    "example_hbase_backup_consumer",
    default_args=default_args,
    description="Automatic HBase backup triggered by data updates",
    schedule=[backup_table_dataset],  # Triggered by dataset updates
    catchup=False,
    tags=["example", "hbase", "backup", "consumer"],
    doc_md=__doc__,
) as dag:
    # Cleanup any stuck backup sessions before starting
    cleanup_stuck_sessions = BashOperator(
        task_id="cleanup_stuck_sessions",
        bash_command=("/usr/lib/hbase/bin/hbase backup repair 2>&1 || true"),
    )

    # Create backup set
    create_backup_set = HBaseBackupSetOperator(
        task_id="create_backup_set",
        action=BackupSetAction.ADD,
        backup_set_name="test_backup_set_v2",
        tables=["test_table_backup_v2"],
        hbase_conn_id="hbase_thrift2",
    )

    # Decide backup type based on history
    decide_backup = BranchPythonOperator(
        task_id="decide_backup_type",
        python_callable=decide_backup_type,
    )

    # Create FULL backup (first time)
    # Note: backup_path must be a valid HDFS path
    # Examples:
    #   - hdfs:///hbase/backup (uses default namenode)
    #   - hdfs://namenode:8020/hbase/backup (explicit namenode)
    create_full_backup = HBaseCreateBackupOperator(
        task_id="create_full_backup",
        backup_type=BackupType.FULL,
        backup_path="hdfs:///hbase/backup",  # HDFS URI
        backup_set_name="test_backup_set_v2",
        workers=1,
        hbase_conn_id="hbase_thrift2",
        do_xcom_push=True,  # Push backup ID to XCom
    )

    # Create INCREMENTAL backup (subsequent runs)
    # Only saves changes since last backup (FULL or INCREMENTAL)
    create_incremental_backup = HBaseCreateBackupOperator(
        task_id="create_incremental_backup",
        backup_type=BackupType.INCREMENTAL,
        backup_path="hdfs:///hbase/backup",  # Same path as FULL
        backup_set_name="test_backup_set_v2",
        workers=1,
        hbase_conn_id="hbase_thrift2",
        do_xcom_push=True,  # Push backup ID to XCom
    )

    # Get backup history (runs after either backup type)
    get_backup_history = HBaseBackupHistoryOperator(
        task_id="get_backup_history",
        backup_set_name="test_backup_set_v2",
        hbase_conn_id="hbase_thrift2",
        trigger_rule="none_failed_min_one_success",
    )

    # Define task dependencies
    (cleanup_stuck_sessions >> create_backup_set >> decide_backup)
    (decide_backup >> [create_full_backup, create_incremental_backup])
    ([create_full_backup, create_incremental_backup] >> get_backup_history)
