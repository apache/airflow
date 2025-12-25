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

This DAG demonstrates HBase restore functionality.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.hbase.operators.hbase import (
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
    "example_hbase_restore",
    default_args=default_args,
    description="HBase restore operations",
    schedule_interval=None,
    catchup=False,
    tags=["example", "hbase", "restore"],
)

# Restore backup (manually specify backup_id)
restore_backup = HBaseRestoreOperator(
    task_id="restore_backup",
    backup_path="/tmp/hbase-backup",
    backup_id="backup_1766648674630",
    tables=["test_table"],
    overwrite=True,
    hbase_conn_id="hbase_kerberos",
    dag=dag,
)

# Define task dependencies
restore_backup
