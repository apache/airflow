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

from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.oracle.hooks.oracle import OracleHook

DOC = """
### Example: Simple Oracle fetch

This DAG demonstrates using `OracleHook` to read from Oracle and push rows
into XCom for downstream tasks. Adapt this pattern for your transfers.

**Prereqs**
- Define a connection `oracle_default` in Airflow.
- Ensure a table `demo_table(id NUMBER, name VARCHAR2(100))` exists (or edit the SQL).
"""


def fetch_rows():
    """Fetch sample rows from Oracle table DEMO_TABLE."""
    hook = OracleHook(oracle_conn_id="oracle_default")
    sql = "SELECT id, name FROM demo_table WHERE ROWNUM <= 5"
    rows = hook.get_records(sql)
    return rows


def print_rows(ti=None):
    """Print rows pulled from Oracle."""
    rows = ti.xcom_pull(task_ids="fetch_rows")
    for r in rows or []:
        print(r)


with DAG(
    dag_id="example_oracle_fetch",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    doc_md=DOC,
    tags=["example", "oracle"],
):
    start = EmptyOperator(task_id="start")

    fetch = PythonOperator(
        task_id="fetch_rows",
        python_callable=fetch_rows,
    )

    show = PythonOperator(
        task_id="print_rows",
        python_callable=print_rows,
    )

    start >> fetch >> show
