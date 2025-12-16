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
Advanced example DAG showing HBase provider usage with new operators.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.hbase.operators.hbase import (
    HBaseBatchGetOperator,
    HBaseBatchPutOperator,
    HBaseCreateTableOperator,
    HBaseDeleteTableOperator,
    HBaseScanOperator,
)
from airflow.providers.hbase.sensors.hbase import (
    HBaseColumnValueSensor,
    HBaseRowCountSensor,
    HBaseTableSensor,
)
from airflow.providers.hbase.datasets.hbase import hbase_table_dataset

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Define dataset
test_table_dataset = hbase_table_dataset(
    host="hbase",
    port=9090,
    table_name="advanced_test_table"
)

dag = DAG(
    "example_hbase_advanced",
    default_args=default_args,
    description="Advanced HBase DAG with bulk operations",
    schedule=None,
    catchup=False,
    tags=["example", "hbase", "advanced"],
)

# Create table
create_table = HBaseCreateTableOperator(
    task_id="create_table",
    table_name="advanced_test_table",
    families={
        "cf1": {"max_versions": 3},
        "cf2": {},
    },
    outlets=[test_table_dataset],
    dag=dag,
)

# Check if table exists
check_table = HBaseTableSensor(
    task_id="check_table_exists",
    table_name="advanced_test_table",
    timeout=60,
    poke_interval=10,
    dag=dag,
)

# [START howto_operator_hbase_batch_put]
batch_put = HBaseBatchPutOperator(
    task_id="batch_put_data",
    table_name="advanced_test_table",
    rows=[
        {
            "row_key": "user1",
            "cf1:name": "John Doe",
            "cf1:age": "30",
            "cf2:status": "active",
        },
        {
            "row_key": "user2", 
            "cf1:name": "Jane Smith",
            "cf1:age": "25",
            "cf2:status": "active",
        },
        {
            "row_key": "user3",
            "cf1:name": "Bob Johnson", 
            "cf1:age": "35",
            "cf2:status": "inactive",
        },
    ],
    outlets=[test_table_dataset],
    dag=dag,
)
# [END howto_operator_hbase_batch_put]

# [START howto_sensor_hbase_row_count]
check_row_count = HBaseRowCountSensor(
    task_id="check_row_count",
    table_name="advanced_test_table",
    min_row_count=3,
    timeout=60,
    poke_interval=10,
    dag=dag,
)
# [END howto_sensor_hbase_row_count]

# [START howto_operator_hbase_scan]
scan_table = HBaseScanOperator(
    task_id="scan_table",
    table_name="advanced_test_table",
    columns=["cf1:name", "cf2:status"],
    limit=10,
    dag=dag,
)
# [END howto_operator_hbase_scan]

# [START howto_operator_hbase_batch_get]
batch_get = HBaseBatchGetOperator(
    task_id="batch_get_users",
    table_name="advanced_test_table",
    row_keys=["user1", "user2"],
    columns=["cf1:name", "cf1:age"],
    dag=dag,
)
# [END howto_operator_hbase_batch_get]

# [START howto_sensor_hbase_column_value]
check_column_value = HBaseColumnValueSensor(
    task_id="check_user_status",
    table_name="advanced_test_table",
    row_key="user1",
    column="cf2:status",
    expected_value="active",
    timeout=60,
    poke_interval=10,
    dag=dag,
)
# [END howto_sensor_hbase_column_value]

# Clean up - delete table
delete_table = HBaseDeleteTableOperator(
    task_id="delete_table",
    table_name="advanced_test_table",
    dag=dag,
)

# Set dependencies
create_table >> check_table >> batch_put >> check_row_count
check_row_count >> [scan_table, batch_get, check_column_value] >> delete_table