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
"""Example DAG demonstrating HBase batch operations using operators."""

from __future__ import annotations

from datetime import datetime

from airflow.providers.apache.hbase.version_compat import AIRFLOW_V_3_0_PLUS

if AIRFLOW_V_3_0_PLUS:
    from airflow.sdk import DAG
else:
    from airflow import DAG
from airflow.providers.apache.hbase.operators.hbase import (
    HBaseBatchGetOperator,
    HBaseBatchPutOperator,
    HBaseCreateTableOperator,
    HBaseDeleteTableOperator,
    HBaseScanOperator,
)

HBASE_CONN_ID = "hbase_thrift2"
TABLE_NAME = "test_batch_table"

with DAG(
    dag_id="example_hbase_batch",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example", "hbase", "batch"],
    doc_md=__doc__,
) as dag:
    delete_if_exists = HBaseDeleteTableOperator(
        task_id="delete_if_exists",
        table_name=TABLE_NAME,
        hbase_conn_id=HBASE_CONN_ID,
    )

    create_table = HBaseCreateTableOperator(
        task_id="create_table",
        table_name=TABLE_NAME,
        families={"cf1": {}, "cf2": {}},
        hbase_conn_id=HBASE_CONN_ID,
    )

    batch_put = HBaseBatchPutOperator(
        task_id="batch_put",
        table_name=TABLE_NAME,
        rows=[
            {
                "row_key": f"row_{i:03d}",
                "cf1:col1": f"value1_{i}",
                "cf1:col2": f"value2_{i}",
                "cf2:col1": f"value3_{i}",
            }
            for i in range(100)
        ],
        batch_size=20,
        max_workers=1,
        hbase_conn_id=HBASE_CONN_ID,
    )

    scan_table = HBaseScanOperator(
        task_id="scan_table",
        table_name=TABLE_NAME,
        columns=["cf1:col1", "cf2:col1"],
        limit=20,
        hbase_conn_id=HBASE_CONN_ID,
    )

    batch_get = HBaseBatchGetOperator(
        task_id="batch_get",
        table_name=TABLE_NAME,
        row_keys=[f"row_{i:03d}" for i in range(10)],
        columns=["cf1:col1", "cf1:col2"],
        hbase_conn_id=HBASE_CONN_ID,
    )

    delete_table = HBaseDeleteTableOperator(
        task_id="delete_table",
        table_name=TABLE_NAME,
        hbase_conn_id=HBASE_CONN_ID,
    )

    (delete_if_exists >> create_table >> batch_put >> scan_table >> batch_get >> delete_table)
