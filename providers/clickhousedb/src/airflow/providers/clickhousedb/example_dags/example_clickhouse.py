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
"""Example DAG demonstrating SQLExecuteQueryOperator with a ClickHouse connection."""

from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.providers.common.sql.hooks.sql import fetch_all_handler
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

CLICKHOUSE_CONN_ID = "clickhouse_default"
CLICKHOUSE_TABLE = "airflow_example"

with DAG(
    dag_id="example_clickhouse",
    start_date=datetime(2021, 1, 1),
    default_args={"conn_id": CLICKHOUSE_CONN_ID},
    tags=["example"],
    schedule="@once",
    catchup=False,
) as dag:
    # [START howto_operator_clickhouse]
    create_table = SQLExecuteQueryOperator(
        task_id="create_table",
        sql=f"""
            CREATE TABLE IF NOT EXISTS {CLICKHOUSE_TABLE} (
                id   UInt32,
                name String,
                ts   DateTime DEFAULT now()
            ) ENGINE = MergeTree()
            ORDER BY id
        """,
    )

    insert_rows = SQLExecuteQueryOperator(
        task_id="insert_rows",
        sql=f"""
            INSERT INTO {CLICKHOUSE_TABLE} (id, name) VALUES
                (1, 'Alice'),
                (2, 'Bob'),
                (3, 'Charlie')
        """,
    )

    # [START howto_operator_clickhouse_query]
    read_rows = SQLExecuteQueryOperator(
        task_id="read_rows",
        sql=f"SELECT id, name FROM {CLICKHOUSE_TABLE} ORDER BY id",
        handler=fetch_all_handler,
    )
    # [END howto_operator_clickhouse_query]

    drop_table = SQLExecuteQueryOperator(
        task_id="drop_table",
        sql=f"DROP TABLE IF EXISTS {CLICKHOUSE_TABLE}",
    )
    # [END howto_operator_clickhouse]

    create_table >> insert_rows >> read_rows >> drop_table
