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
Example DAG showing HBase Thrift2 provider usage with SSL/TLS connection.

This DAG demonstrates secure connections to HBase using Thrift2 protocol with SSL.

Connection Configuration (hbase_thrift2_ssl):
{
  "connection_mode": "thrift2",
  "host": "localhost",
  "port": 9090,
  "use_ssl": true,
  "ssl_verify_mode": "CERT_REQUIRED",
  "ssl_ca_secret": "hbase/ca-cert",
  "ssl_cert_secret": "hbase/client-cert",
  "ssl_key_secret": "hbase/client-key",
  "ssl_min_version": "TLSv1_2"
}

Prerequisites:
1. HBase Thrift2 server with SSL enabled
2. SSL certificates stored in Airflow Variables:
   - hbase/ca-cert: CA certificate
   - hbase/client-cert: Client certificate
   - hbase/client-key: Client private key
3. Create Airflow Connection 'hbase_thrift2_ssl' with above config
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
    "example_hbase_ssl",
    default_args=default_args,
    description="Example HBase Thrift2 DAG with SSL/TLS connection",
    schedule_interval=None,
    catchup=False,
    tags=["example", "hbase", "thrift2", "ssl"],
)

# Connection ID for Thrift2 with SSL
HBASE_CONN_ID = "hbase_thrift2_ssl"
TABLE_NAME = "test_table_ssl"

# Delete table if exists for idempotency
delete_table_cleanup = HBaseDeleteTableOperator(
    task_id="delete_table_cleanup",
    table_name=TABLE_NAME,
    hbase_conn_id=HBASE_CONN_ID,
    dag=dag,
)

# Create table using SSL connection
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
    row_key="ssl_row1",
    data={
        "cf1:col1": "ssl_value1",
        "cf1:col2": "ssl_value2",
        "cf2:col1": "ssl_value3",
    },
    hbase_conn_id=HBASE_CONN_ID,
    dag=dag,
)

check_row = HBaseRowSensor(
    task_id="check_row_exists",
    table_name=TABLE_NAME,
    row_key="ssl_row1",
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
delete_table_cleanup >> create_table >> check_table >> put_data >> check_row >> delete_table
