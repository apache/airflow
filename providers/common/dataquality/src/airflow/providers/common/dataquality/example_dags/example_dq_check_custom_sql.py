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
Example DAG: ``custom_sql`` for checks the built-in catalog can't express.

Built-in checks (``null_count``, ``min``, ``max``, ...) are all single-column. The moment a
rule needs to compare two columns, join against another table, or use a SQL function the
built-in catalog doesn't cover (or that your database's ``DbApiHook`` doesn't support -- see
"Supported checks and databases" in the provider docs), ``custom_sql`` is the escape hatch:
any statement that resolves to a single scalar, evaluated the same way as a built-in check.
"""

from __future__ import annotations

import os
from datetime import datetime
from pathlib import Path

from airflow.providers.common.dataquality.operators.dq_check import DQCheckOperator
from airflow.providers.common.dataquality.rules import Condition, DQRule, RuleSet, Severity
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sdk import DAG

DAG_ID = "example_dq_check_custom_sql"
CONN_ID = "sqlite_default"
TABLE_NAME = "dq_custom_sql_orders"
RESULTS_PATH = Path("/tmp/airflow_dq_example/results")

os.environ.setdefault("AIRFLOW__COMMON_DATAQUALITY__RESULTS_PATH", f"file://{RESULTS_PATH}")

# [START howto_operator_dq_check_custom_sql]
custom_sql_ruleset = RuleSet(
    name="orders_custom_sql",
    rules=(
        DQRule(
            name="shipped_after_ordered",
            check="custom_sql",
            # {table} is substituted by the SQL engine at check time, not an f-string
            # placeholder -- a cross-column comparison no single-column built-in can express.
            sql="SELECT COUNT(*) FROM {table} WHERE shipped_at < ordered_at",
            condition=Condition(equal_to=0),
        ),
        DQRule(
            name="high_value_order_ratio",
            check="custom_sql",
            sql=(
                "SELECT CAST(SUM(CASE WHEN amount > 100 THEN 1 ELSE 0 END) AS REAL) / COUNT(*) FROM {table}"
            ),
            condition=Condition(leq_to=0.5),
            severity=Severity.WARN,
        ),
    ),
)
# [END howto_operator_dq_check_custom_sql]

with DAG(
    dag_id=DAG_ID,
    schedule=None,
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example", "dq"],
) as dag:
    create_table = SQLExecuteQueryOperator(
        task_id="create_table",
        conn_id=CONN_ID,
        sql=[
            f"DROP TABLE IF EXISTS {TABLE_NAME};",
            f"""
            CREATE TABLE {TABLE_NAME} (
                order_id INTEGER,
                amount REAL,
                ordered_at TEXT,
                shipped_at TEXT
            );
            """,
        ],
    )

    insert_orders = SQLExecuteQueryOperator(
        task_id="insert_orders",
        conn_id=CONN_ID,
        sql=f"""
        INSERT INTO {TABLE_NAME} (order_id, amount, ordered_at, shipped_at) VALUES
            (1, 42.0, '2026-07-01', '2026-07-02'),
            (2, 150.0, '2026-07-01', '2026-07-03'),
            (3, 15.5, '2026-07-02', '2026-07-02');
        """,
    )

    check_orders = DQCheckOperator(
        task_id="check_orders",
        conn_id=CONN_ID,
        table=TABLE_NAME,
        ruleset=custom_sql_ruleset,
        fail_on="never",
    )

    create_table >> insert_orders >> check_orders
