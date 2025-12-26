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
    HBaseCreateTableOperator,
    HBaseDeleteTableOperator,
)
from airflow.providers.hbase.sensors.hbase import (
    HBaseTableSensor,
)

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
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
    timeout=20,
    poke_interval=5,
    dag=dag,
)

delete_table = HBaseDeleteTableOperator(
    task_id="delete_table_kerberos",
    table_name="test_table_krb",
    hbase_conn_id="hbase_kerberos",
    dag=dag,
)

# Set dependencies - Basic HBase operations
create_table >> check_table >> delete_table
