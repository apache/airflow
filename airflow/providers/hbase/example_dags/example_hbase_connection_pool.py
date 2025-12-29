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
Example DAG demonstrating HBase connection pooling.

This DAG shows the same operations as example_hbase.py but uses connection pooling
for improved performance when multiple tasks access HBase.

## Connection Configuration

Configure your HBase connection with connection pooling enabled:

```json
{
  "connection_mode": "thrift",
  "auth_method": "simple",
  "connection_pool": {
    "enabled": true,
    "size": 10,
    "timeout": 30
  }
}
```
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.hbase.operators.hbase import (
    HBaseCreateTableOperator,
    HBaseDeleteTableOperator,
    HBasePutOperator,
    HBaseBatchPutOperator,
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
    "example_hbase_connection_pool",
    default_args=default_args,
    description="Example HBase DAG with connection pooling",
    schedule_interval=None,
    catchup=False,
    tags=["example", "hbase", "connection-pool"],
)

# Connection ID with pooling enabled
HBASE_CONN_ID = "hbase_pooled"
TABLE_NAME = "pool_test_table"

delete_table_cleanup = HBaseDeleteTableOperator(
    task_id="delete_table_cleanup",
    table_name=TABLE_NAME,
    hbase_conn_id=HBASE_CONN_ID,
    dag=dag,
)

create_table = HBaseCreateTableOperator(
    task_id="create_table",
    table_name=TABLE_NAME,
    families={
        "cf1": {},  # Column family 1
        "cf2": {},  # Column family 2
    },
    hbase_conn_id=HBASE_CONN_ID,
    dag=dag,
)

check_table = HBaseTableSensor(
    task_id="check_table_exists",
    table_name=TABLE_NAME,
    hbase_conn_id=HBASE_CONN_ID,
    timeout=60,
    poke_interval=10,
    dag=dag,
)

put_data = HBasePutOperator(
    task_id="put_data",
    table_name=TABLE_NAME,
    row_key="row1",
    data={
        "cf1:col1": "value1",
        "cf1:col2": "value2",
        "cf2:col1": "value3",
    },
    hbase_conn_id=HBASE_CONN_ID,
    dag=dag,
)

batch_put_data = HBaseBatchPutOperator(
    task_id="batch_put_data",
    table_name=TABLE_NAME,
    rows=[
        {
            "row_key": "row2",
            "cf1:name": "Alice",
            "cf1:age": "25",
            "cf2:city": "New York",
        },
        {
            "row_key": "row3",
            "cf1:name": "Bob",
            "cf1:age": "30",
            "cf2:city": "San Francisco",
        },
    ],
    hbase_conn_id=HBASE_CONN_ID,
    dag=dag,
)

check_row = HBaseRowSensor(
    task_id="check_row_exists",
    table_name=TABLE_NAME,
    row_key="row1",
    hbase_conn_id=HBASE_CONN_ID,
    timeout=60,
    poke_interval=10,
    dag=dag,
)

delete_table = HBaseDeleteTableOperator(
    task_id="delete_table",
    table_name=TABLE_NAME,
    hbase_conn_id=HBASE_CONN_ID,
    dag=dag,
)

# Set dependencies
delete_table_cleanup >> create_table >> check_table >> put_data >> batch_put_data >> check_row >> delete_table
