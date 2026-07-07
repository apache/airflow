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
Example DAG: load a ruleset from a YAML file instead of declaring it in Python.

A path string is accepted anywhere a :class:`~airflow.providers.dq.rules.RuleSet` is --
``DQCheckOperator(ruleset=...)``, ``@task.dq_check(ruleset=...)``, and
:func:`~airflow.providers.dq.assets.asset_quality` all resolve it via
:meth:`~airflow.providers.dq.rules.RuleSet.from_file` at Dag-parse time. This keeps rules
editable by people who don't write Python -- a data steward can change ``orders_ruleset.yaml``
without touching the Dag file.
"""

from __future__ import annotations

import os
from datetime import datetime
from pathlib import Path

from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.dq.operators.dq_check import DQCheckOperator
from airflow.sdk import DAG

DAG_ID = "example_dq_ruleset_from_yaml"
CONN_ID = "sqlite_default"
TABLE_NAME = "dq_yaml_orders"
RESULTS_PATH = Path("/tmp/airflow_dq_example/results")
RULESET_FILE = Path(__file__).parent / "orders_ruleset.yaml"

os.environ.setdefault("AIRFLOW__DQ__RESULTS_PATH", f"file://{RESULTS_PATH}")

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
            f"CREATE TABLE {TABLE_NAME} (order_id INTEGER, amount REAL);",
        ],
    )

    insert_orders = SQLExecuteQueryOperator(
        task_id="insert_orders",
        conn_id=CONN_ID,
        sql=f"""
        INSERT INTO {TABLE_NAME} (order_id, amount) VALUES
            (1, 10.0),
            (2, 25.5),
            (3, 7.25);
        """,
    )

    # [START howto_operator_dq_check_ruleset_from_yaml]
    check_orders = DQCheckOperator(
        task_id="check_orders",
        conn_id=CONN_ID,
        table=TABLE_NAME,
        ruleset=str(RULESET_FILE),
        fail_on="never",
    )
    # [END howto_operator_dq_check_ruleset_from_yaml]

    create_table >> insert_orders >> check_orders
