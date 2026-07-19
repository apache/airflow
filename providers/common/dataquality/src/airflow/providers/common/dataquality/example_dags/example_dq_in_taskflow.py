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
Example Dag: run data quality checks as part of a TaskFlow task.

The task owns the surrounding Python flow -- it can persist DQ results, inspect the summary,
and decide what to return or raise -- while the rule execution still uses the same
``DbApiHook`` path as ``DQCheckOperator``.
"""

from __future__ import annotations

import os
from datetime import datetime
from pathlib import Path

from airflow.providers.common.dataquality.execution import persist_quality_results, run_quality_checks
from airflow.providers.common.dataquality.rules import Condition, DQRule, RuleSet
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sdk import DAG, get_current_context, task

DAG_ID = "example_dq_in_taskflow"
CONN_ID = "sqlite_default"
RAW_TABLE = "dq_taskflow_raw_orders"
READY_TABLE = "dq_taskflow_ready_orders"
RESULTS_PATH = Path("/tmp/airflow_dq_example/results")

# [START howto_rules_dq_ruleset]
orders_ruleset = RuleSet(
    name="orders_taskflow_quality",
    rules=(
        DQRule(
            name="order_id_not_null",
            check="null_count",
            column="order_id",
            condition=Condition(equal_to=0),
        ),
        DQRule(
            name="amount_min_ge_zero",
            check="min",
            column="amount",
            condition=Condition(geq_to=0),
        ),
        DQRule(
            name="row_count_present",
            check="row_count",
            condition=Condition(greater_than=0),
        ),
    ),
)
# [END howto_rules_dq_ruleset]

os.environ.setdefault("AIRFLOW__COMMON_DATAQUALITY__RESULTS_PATH", f"file://{RESULTS_PATH}")

with DAG(
    dag_id=DAG_ID,
    schedule=None,
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example", "dq"],
) as dag:
    create_tables = SQLExecuteQueryOperator(
        task_id="create_tables",
        conn_id=CONN_ID,
        sql=[
            f"DROP TABLE IF EXISTS {RAW_TABLE};",
            f"DROP TABLE IF EXISTS {READY_TABLE};",
            f"CREATE TABLE {RAW_TABLE} (order_id INTEGER, customer_id INTEGER, amount REAL);",
            f"CREATE TABLE {READY_TABLE} (order_id INTEGER, customer_id INTEGER, amount REAL);",
        ],
    )

    load_orders = SQLExecuteQueryOperator(
        task_id="load_orders",
        conn_id=CONN_ID,
        sql=f"""
        INSERT INTO {RAW_TABLE} (order_id, customer_id, amount) VALUES
            (1, 101, 10.0),
            (2, 102, 25.5),
            (3, 103, 7.25);
        """,
    )

    # [START howto_run_dq_inside_task]
    @task
    def validate_and_choose_source() -> str:
        result = run_quality_checks(
            conn_id=CONN_ID,
            table=RAW_TABLE,
            ruleset=orders_ruleset,
        )
        summary = persist_quality_results(result, context=get_current_context())

        if summary["failed"] or summary["errored"]:
            raise ValueError(f"Order quality checks failed: {summary}")

        return RAW_TABLE

    # [END howto_run_dq_inside_task]

    publish_orders = SQLExecuteQueryOperator(
        task_id="publish_orders",
        conn_id=CONN_ID,
        sql=f"INSERT INTO {READY_TABLE} SELECT * FROM {{{{ ti.xcom_pull(task_ids='validate_and_choose_source') }}}};",
    )

    create_tables >> load_orders >> validate_and_choose_source() >> publish_orders
