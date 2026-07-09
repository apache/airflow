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
Example DAG: ``@task.dq_check`` with a runtime ruleset.

``table`` (or ``asset``) is declared as a decorator argument, exactly like the plain operator.
``ruleset`` may be declared at Dag-parse time, or returned by the decorated function at
execution time. This is useful when an upstream task, variable, or generated value decides which
ruleset to run.
"""

from __future__ import annotations

import os
from datetime import datetime
from pathlib import Path

from airflow.providers.common.dataquality.rules import DQRule, RuleSet
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sdk import DAG, task

DAG_ID = "example_dq_check_decorator_dynamic"
CONN_ID = "sqlite_default"
TABLE_NAME = "dq_dynamic_orders"
RESULTS_PATH = Path("/tmp/airflow_dq_example/results")

os.environ.setdefault("AIRFLOW__DQ__RESULTS_PATH", f"file://{RESULTS_PATH}")

orders_ruleset = RuleSet(
    name="orders_dynamic",
    rules=(
        DQRule(name="order_id_not_null", check="null_count", column="order_id", condition={"equal_to": 0}),
        DQRule(name="row_count_present", check="row_count", condition={"greater_than": 0}),
    ),
)

strict_ruleset = RuleSet(
    name="orders_dynamic_strict",
    rules=(
        *orders_ruleset.rules,
        DQRule(name="amount_min_ge_zero", check="min", column="amount", condition={"geq_to": 0}),
    ),
)

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
            f"CREATE TABLE {TABLE_NAME} (order_id INTEGER, amount REAL, ds TEXT);",
        ],
    )

    insert_orders = SQLExecuteQueryOperator(
        task_id="insert_orders",
        conn_id=CONN_ID,
        sql=f"""
        INSERT INTO {TABLE_NAME} (order_id, amount, ds) VALUES
            (1, 10.0, '{{{{ ds }}}}'),
            (2, 25.5, '{{{{ ds }}}}'),
            (3, 7.25, '2020-01-01');
        """,
    )

    # [START howto_decorator_dq_check_runtime_ruleset]
    @task.dq_check(conn_id=CONN_ID, table=TABLE_NAME, ruleset=orders_ruleset, fail_on="never")
    def check_with_ruleset_from_upstream(strict: bool = True):
        """Swap in a stricter ruleset based on a signal only known at task-execution time."""
        return strict_ruleset if strict else None

    # [END howto_decorator_dq_check_runtime_ruleset]

    create_table >> insert_orders >> check_with_ruleset_from_upstream()
