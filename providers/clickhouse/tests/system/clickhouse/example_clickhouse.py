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
Example DAG demonstrating the use of ClickHouse with Airflow.

Requires a connection with ``conn_id="clickhouse_default"`` pointing to a live
ClickHouse instance.  The DAG creates a temporary table, inserts rows, reads
them back, and drops the table.
"""

from __future__ import annotations

import os
from datetime import datetime

from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

CLICKHOUSE_CONN_ID = os.environ.get("CLICKHOUSE_CONN_ID", "clickhouse_default")
CLICKHOUSE_DATABASE = os.environ.get("CLICKHOUSE_DATABASE", "default")
CLICKHOUSE_TABLE = "airflow_example"
DAG_ID = "example_clickhouse"

CREATE_TABLE_SQL = f"""
CREATE TABLE IF NOT EXISTS {CLICKHOUSE_TABLE} (
    id   UInt32,
    name String,
    ts   DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY id
"""

INSERT_ROWS_SQL = f"""
INSERT INTO {CLICKHOUSE_TABLE} (id, name) VALUES
    (1, 'Alice'),
    (2, 'Bob'),
    (3, 'Charlie')
"""

DROP_TABLE_SQL = f"DROP TABLE IF EXISTS {CLICKHOUSE_TABLE}"

with DAG(
    DAG_ID,
    start_date=datetime(2021, 1, 1),
    default_args={
        "conn_id": CLICKHOUSE_CONN_ID,
        "hook_params": {"database": CLICKHOUSE_DATABASE},
    },
    tags=["example"],
    schedule="@once",
    catchup=False,
) as dag:
    # [START howto_operator_clickhouse]
    create_table = SQLExecuteQueryOperator(
        task_id="create_table",
        sql=CREATE_TABLE_SQL,
    )

    insert_rows = SQLExecuteQueryOperator(
        task_id="insert_rows",
        sql=INSERT_ROWS_SQL,
    )

    # [START howto_operator_clickhouse_query]
    read_rows = SQLExecuteQueryOperator(
        task_id="read_rows",
        sql=f"SELECT id, name FROM {CLICKHOUSE_TABLE} ORDER BY id",
    )
    # [END howto_operator_clickhouse_query]

    drop_table = SQLExecuteQueryOperator(
        task_id="drop_table",
        sql=DROP_TABLE_SQL,
    )
    # [END howto_operator_clickhouse]

    create_table >> insert_rows >> read_rows >> drop_table

from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md)
test_run = get_test_run(dag)
