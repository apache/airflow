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
Example DAG showing HBase provider usage.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.hbase.operators.hbase import (
    HBaseCreateTableOperator,
    HBaseDeleteTableOperator,
    HBasePutOperator,
)
from airflow.providers.hbase.sensors.hbase import HBaseTableSensor, HBaseRowSensor

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
    "example_hbase",
    default_args=default_args,
    description="Example HBase DAG",
    schedule_interval=None,
    catchup=False,
    tags=["example", "hbase"],
)

# Create table
create_table = HBaseCreateTableOperator(
    task_id="create_table",
    table_name="test_table",
    families={
        "cf1": {},  # Column family 1
        "cf2": {},  # Column family 2
    },
    dag=dag,
)

# Check if table exists
check_table = HBaseTableSensor(
    task_id="check_table_exists",
    table_name="test_table",
    timeout=60,
    poke_interval=10,
    dag=dag,
)

# Put data
put_data = HBasePutOperator(
    task_id="put_data",
    table_name="test_table",
    row_key="row1",
    data={
        "cf1:col1": "value1",
        "cf1:col2": "value2",
        "cf2:col1": "value3",
    },
    dag=dag,
)

# Check if row exists
check_row = HBaseRowSensor(
    task_id="check_row_exists",
    table_name="test_table",
    row_key="row1",
    timeout=60,
    poke_interval=10,
    dag=dag,
)

# Clean up - delete table
delete_table = HBaseDeleteTableOperator(
    task_id="delete_table",
    table_name="test_table",
    dag=dag,
)

# Set dependencies
create_table >> check_table >> put_data >> check_row >> delete_table