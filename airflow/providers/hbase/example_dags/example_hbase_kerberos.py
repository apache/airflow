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
Example DAG showing HBase provider usage with Kerberos authentication.

This DAG demonstrates how to use HBase operators and sensors with Kerberos authentication.
Make sure to configure the HBase connection with Kerberos settings in Airflow UI.

Connection Configuration (Admin -> Connections):
- Connection Id: hbase_kerberos
- Connection Type: HBase
- Host: your-hbase-host
- Port: 9090 (or your Thrift port)
- Extra: {
    "auth_method": "kerberos",
    "principal": "your-principal@YOUR.REALM",
    "keytab_path": "/path/to/your.keytab",
    "timeout": 30000
}

Alternative using Airflow secrets:
- Extra: {
    "auth_method": "kerberos", 
    "principal": "your-principal@YOUR.REALM",
    "keytab_secret_key": "HBASE_KEYTAB_SECRET",
    "timeout": 30000
}

Note: keytab_secret_key will be looked up in:
1. Airflow Variables (Admin -> Variables)
2. Environment variables (fallback)
"""

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
)
from airflow.providers.hbase.sensors.hbase import (
    HBaseColumnValueSensor,
    HBaseRowCountSensor,
    HBaseRowSensor,
    HBaseTableSensor,
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
    "example_hbase_kerberos",
    default_args=default_args,
    description="Example HBase DAG with Kerberos authentication",
    schedule_interval=None,
    catchup=False,
    tags=["example", "hbase", "kerberos"],
)

# Note: "hbase_kerberos" is the Connection ID configured in Airflow UI with Kerberos settings
create_table = HBaseCreateTableOperator(
    task_id="create_table_kerberos",
    table_name="test_table_krb",
    families={
        "cf1": {},  # Column family 1
        "cf2": {},  # Column family 2
    },
    hbase_conn_id="hbase_kerberos",  # HBase connection with Kerberos auth
    dag=dag,
)

check_table = HBaseTableSensor(
    task_id="check_table_exists_kerberos",
    table_name="test_table_krb",
    hbase_conn_id="hbase_kerberos",
    timeout=60,
    poke_interval=10,
    dag=dag,
)

put_data = HBasePutOperator(
    task_id="put_data_kerberos",
    table_name="test_table_krb",
    row_key="row1",
    data={
        "cf1:col1": "kerberos_value1",
        "cf1:col2": "kerberos_value2",
        "cf2:col1": "kerberos_value3",
    },
    hbase_conn_id="hbase_kerberos",
    dag=dag,
)

check_row = HBaseRowSensor(
    task_id="check_row_exists_kerberos",
    table_name="test_table_krb",
    row_key="row1",
    hbase_conn_id="hbase_kerberos",
    timeout=60,
    poke_interval=10,
    dag=dag,
)

check_row_count = HBaseRowCountSensor(
    task_id="check_row_count_kerberos",
    table_name="test_table_krb",
    expected_count=1,
    hbase_conn_id="hbase_kerberos",
    timeout=60,
    poke_interval=10,
    dag=dag,
)

check_column_value = HBaseColumnValueSensor(
    task_id="check_column_value_kerberos",
    table_name="test_table_krb",
    row_key="row1",
    column="cf1:col1",
    expected_value="kerberos_value1",
    hbase_conn_id="hbase_kerberos",
    timeout=60,
    poke_interval=10,
    dag=dag,
)

delete_table = HBaseDeleteTableOperator(
    task_id="delete_table_kerberos",
    table_name="test_table_krb",
    hbase_conn_id="hbase_kerberos",
    dag=dag,
)

# Set dependencies - Basic HBase operations
create_table >> check_table >> put_data >> check_row >> check_row_count >> check_column_value

# Backup operations (parallel branch)
create_table >> create_backup_set >> create_backup >> backup_history

# Restore operation (depends on backup)
create_backup >> restore_backup

# Cleanup (after all operations)
[check_column_value, backup_history, restore_backup] >> delete_table
