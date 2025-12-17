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
Example DAG showing HBase backup and restore operations.

This DAG demonstrates:
1. Creating a table with sample data
2. Creating a backup set
3. Creating full backup
4. Adding more data
5. Creating incremental backup
6. Simulating data loss and restore
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.hbase.operators.hbase import (
    HBaseCreateTableOperator,
    HBaseDeleteTableOperator,
    HBasePutOperator,
    HBaseScanOperator,
)
from airflow.operators.bash import BashOperator

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
    "example_hbase_backup",
    default_args=default_args,
    description="Example HBase backup and restore operations",
    schedule=None,
    catchup=False,
    tags=["example", "hbase", "backup"],
)

# Step 1: Create table
create_table = HBaseCreateTableOperator(
    task_id="create_user_activity_table",
    table_name="user_activity",
    families={
        "cf1": {},
        "cf2": {}
    },
    dag=dag,
)

# Step 2: Insert initial data
insert_user1 = HBasePutOperator(
    task_id="insert_user1",
    table_name="user_activity",
    row_key="user1",
    data={
        "cf1:name": "Alice",
        "cf1:email": "alice@email.com",
        "cf2:last_login": "2024-01-15",
        "cf2:login_count": "5"
    },
    dag=dag,
)

insert_user2 = HBasePutOperator(
    task_id="insert_user2",
    table_name="user_activity",
    row_key="user2",
    data={
        "cf1:name": "Bob",
        "cf1:email": "bob@email.com",
        "cf2:last_login": "2024-01-14",
        "cf2:login_count": "3"
    },
    dag=dag,
)

insert_user3 = HBasePutOperator(
    task_id="insert_user3",
    table_name="user_activity",
    row_key="user3",
    data={
        "cf1:name": "Charlie",
        "cf1:email": "charlie@email.com",
        "cf2:last_login": "2024-01-13",
        "cf2:login_count": "7"
    },
    dag=dag,
)

# Step 3: Scan initial data
scan_initial = HBaseScanOperator(
    task_id="scan_initial_data",
    table_name="user_activity",
    dag=dag,
)

# Step 4: Create backup set
create_backup_set = BashOperator(
    task_id="create_backup_set",
    bash_command="docker exec hbase-standalone hbase shell -n <<< 'snapshot \"user_activity\", \"user_activity_backup_$(date +%Y%m%d_%H%M%S)\"'",
    dag=dag,
)

# Step 5: Create full backup
full_backup = BashOperator(
    task_id="create_full_backup",
    bash_command="ssh -o StrictHostKeyChecking=no ${HBASE_USER:-root}@${HBASE_HOST:-172.17.0.1} 'hbase backup create full hdfs://namenode:9000/tmp/hbase-backup -s user_backup_set -w 3'",
    dag=dag,
)

# Step 6: Add new data after full backup
insert_user4 = HBasePutOperator(
    task_id="insert_user4",
    table_name="user_activity",
    row_key="user4",
    data={
        "cf1:name": "Diana",
        "cf1:email": "diana@email.com",
        "cf2:last_login": "2024-01-16",
        "cf2:login_count": "2"
    },
    dag=dag,
)

# Update existing user
update_user1 = HBasePutOperator(
    task_id="update_user1",
    table_name="user_activity",
    row_key="user1",
    data={
        "cf2:login_count": "6"
    },
    dag=dag,
)

update_user2 = HBasePutOperator(
    task_id="update_user2",
    table_name="user_activity",
    row_key="user2",
    data={
        "cf2:last_login": "2024-01-16"
    },
    dag=dag,
)

# Step 7: Create incremental backup
incremental_backup = BashOperator(
    task_id="create_incremental_backup",
    bash_command="ssh -o StrictHostKeyChecking=no user@hbase-server 'hbase backup create incremental hdfs://namenode:9000/tmp/hbase-backup -s user_backup_set -w 3'",
    dag=dag,
)

# Step 8: Add more data and create second incremental backup
insert_user5 = HBasePutOperator(
    task_id="insert_user5",
    table_name="user_activity",
    row_key="user5",
    data={
        "cf1:name": "Eve",
        "cf1:email": "eve@email.com"
    },
    dag=dag,
)

update_user1_again = HBasePutOperator(
    task_id="update_user1_again",
    table_name="user_activity",
    row_key="user1",
    data={
        "cf2:login_count": "7"
    },
    dag=dag,
)

incremental_backup_2 = BashOperator(
    task_id="create_incremental_backup_2",
    bash_command="ssh -o StrictHostKeyChecking=no user@hbase-server 'hbase backup create incremental hdfs://namenode:9000/tmp/hbase-backup -s user_backup_set -w 3'",
    dag=dag,
)

# Step 9: Scan data before crash simulation
scan_before_crash = HBaseScanOperator(
    task_id="scan_before_crash",
    table_name="user_activity",
    dag=dag,
)

# Step 10: Simulate crash by dropping table
simulate_crash = HBaseDeleteTableOperator(
    task_id="simulate_crash",
    table_name="user_activity",
    disable=True,
    dag=dag,
)

# Step 11: Restore from backup
restore_backup = BashOperator(
    task_id="restore_from_backup",
    bash_command="ssh -o StrictHostKeyChecking=no user@hbase-server 'hbase restore hdfs://namenode:9000/tmp/hbase-backup backup_20241217_130900 -s user_backup_set'",
    dag=dag,
)

# Step 12: Verify restored data
scan_after_restore = HBaseScanOperator(
    task_id="scan_after_restore",
    table_name="user_activity",
    dag=dag,
)

# Define task dependencies
create_table >> [insert_user1, insert_user2, insert_user3]
[insert_user1, insert_user2, insert_user3] >> scan_initial
scan_initial >> create_backup_set
create_backup_set >> full_backup
full_backup >> [insert_user4, update_user1, update_user2]
[insert_user4, update_user1, update_user2] >> incremental_backup
incremental_backup >> [insert_user5, update_user1_again]
[insert_user5, update_user1_again] >> incremental_backup_2
incremental_backup_2 >> scan_before_crash
scan_before_crash >> simulate_crash
simulate_crash >> restore_backup
restore_backup >> scan_after_restore