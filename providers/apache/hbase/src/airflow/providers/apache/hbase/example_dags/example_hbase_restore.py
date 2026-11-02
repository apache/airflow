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
HBase restore operations example.

This DAG demonstrates HBase restore functionality with data verification.
It uses the backup ID from the backup consumer DAG via XCom or manual specification.

Workflow:

#. Delete table (to simulate data loss)
#. Restore from backup using backup ID
#. Verify data was restored correctly

To get backup ID from previous DAG run:

- Use XCom: ``{{ ti.xcom_pull(dag_id='example_hbase_backup_consumer', task_ids='create_full_backup') }}``
- Or specify manually in backup_id parameter
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

from airflow.providers.apache.hbase.version_compat import AIRFLOW_V_3_0_PLUS

if AIRFLOW_V_3_0_PLUS:
    from airflow.providers.standard.operators.python import PythonOperator
    from airflow.sdk import DAG
    from airflow.sdk.definitions.param import Param
else:
    from airflow import DAG
    from airflow.models.param import Param  # type: ignore[no-redef]
    from airflow.operators.python import PythonOperator  # type: ignore[no-redef]

from airflow.providers.apache.hbase.hooks.hbase import HBaseThriftHook
from airflow.providers.apache.hbase.operators.hbase import (
    HBaseDeleteTableOperator,
    HBaseRestoreOperator,
    IfNotExistsAction,
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

dag = DAG(
    "example_hbase_restore",
    default_args=default_args,
    description="HBase restore operations with verification",
    schedule=None,
    catchup=False,
    tags=["example", "hbase", "restore"],
    doc_md=__doc__,
    params={
        "backup_id": Param(
            default="",
            type="string",
            description="Backup ID to restore (e.g., backup_1770197062556). Required.",
        ),
    },
)


def verify_restored_data(**context):
    """Verify that data was restored correctly."""

    hook = HBaseThriftHook(hbase_conn_id="hbase_thrift2")
    table_name = "test_table_backup_v2"

    # Get restore output from previous task
    ti = context["ti"]
    restore_output = ti.xcom_pull(task_ids="restore_backup")
    if restore_output:
        logger.info("Restore operation output:\n%s", restore_output)

        # Check if backup is not enabled
        if "Backup is not enabled" in restore_output:
            logger.warning("=" * 60)
            logger.warning("HBASE BACKUP IS NOT ENABLED ON THE CLUSTER")
            logger.warning("=" * 60)
            logger.info("To enable HBase backup, add to hbase-site.xml:")
            logger.info("")
            logger.info("  <property>")
            logger.info("    <name>hbase.backup.enable</name>")
            logger.info("    <value>true</value>")
            logger.info("  </property>")
            logger.info("")
            logger.info("And configure backup classes (see HBase documentation).")
            logger.info("Then restart HBase cluster.")
            logger.info("See: http://hbase.apache.org/book.html#backuprestore")
            logger.warning("=" * 60)
            return "BACKUP_NOT_ENABLED"

    # Check if table exists
    if not hook.table_exists(table_name):
        logger.error("Table '%s' does not exist after restore!", table_name)
        logger.info("Possible reasons:")
        logger.info("  1. Restore operation failed (check restore task logs above)")
        logger.info("  2. Incorrect backup_id specified")
        logger.info("  3. Backup doesn't contain this table")
        logger.info("  4. Insufficient permissions")
        return "TABLE_NOT_FOUND"

    logger.info("Table '%s' exists", table_name)

    # Scan table to get row count
    rows = list(hook.scan_table(table_name, limit=1000))
    row_count = len(rows)

    logger.info("Restored table '%s' contains %d rows", table_name, row_count)

    if row_count == 0:
        logger.warning("Table exists but is empty after restore!")
        logger.info("This could mean:")
        logger.info("  1. The backup was empty")
        logger.info("  2. The backup_id is incorrect")
        logger.info("  3. The restore operation didn't complete successfully")
        return "TABLE_EMPTY"

    # Show sample data
    logger.info("Sample restored data (first 5 rows):")
    for i, (row_key, data) in enumerate(rows[:5]):
        logger.info("  Row %d: %s -> %s", i + 1, row_key, data)

    logger.info("Restore verification successful: %d rows restored", row_count)
    return f"Restore verified: {row_count} rows"


# Step 1: Delete table to simulate data loss (ignore if not exists)
delete_table = HBaseDeleteTableOperator(
    task_id="delete_table",
    table_name="test_table_backup_v2",
    if_not_exists=IfNotExistsAction.IGNORE,
    hbase_conn_id="hbase_thrift2",
    dag=dag,
)

# Step 2: Restore from backup using backup_id from params
restore_backup = HBaseRestoreOperator(
    task_id="restore_backup",
    backup_path="hdfs:///hbase/backup",
    backup_id="{{ params.backup_id }}",
    tables=["test_table_backup_v2"],
    overwrite=True,
    hbase_conn_id="hbase_thrift2",
    do_xcom_push=True,
    dag=dag,
)

# Step 3: Verify restored data
verify_data = PythonOperator(
    task_id="verify_restored_data",
    python_callable=verify_restored_data,
    dag=dag,
)

# Define task dependencies
delete_table >> restore_backup >> verify_data
