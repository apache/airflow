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
"""Execute a templated SQL statement with bound parameters on an existing SQL warehouse."""

from __future__ import annotations

import os
from datetime import datetime

from airflow.providers.common.compat.sdk import DAG
from airflow.providers.databricks.operators.databricks import DatabricksSQLStatementsOperator
from airflow.providers.databricks.operators.warehouse import DatabricksStopWarehouseOperator

from tests_common.test_utils.watcher import watcher

WAREHOUSE_ID = os.environ.get("WAREHOUSE_ID", "your-warehouse-id")

with DAG(
    dag_id="example_databricks_sql_statement_parameters",
    schedule=None,
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example"],
    params={"client": "airflow", "expected_count": 2},
) as dag:
    # [START howto_operator_sql_statement_parameters]
    sql_statement = DatabricksSQLStatementsOperator(
        task_id="sql_statement",
        databricks_conn_id="databricks_default",
        warehouse_id=WAREHOUSE_ID,
        statement="""
            SELECT assert_true(:run_date = '{{ ds }}'),
                   assert_true(:client = 'airflow'),
                   assert_true(:row_count = {{ params.expected_count }})
        """,
        parameters=[
            {"name": "run_date", "value": "{{ ds }}", "type": "STRING"},
            {"name": "client", "value": "{{ params.client }}", "type": "STRING"},
            {"name": "row_count", "value": "{{ params.expected_count }}", "type": "INT"},
        ],
    )
    # [END howto_operator_sql_statement_parameters]

    stop_warehouse = DatabricksStopWarehouseOperator(
        task_id="stop_warehouse",
        warehouse_id=WAREHOUSE_ID,
        trigger_rule="all_done",
    )

    sql_statement >> stop_warehouse
    list(dag.tasks) >> watcher()

from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example Dag with pytest (see: contributing-docs/testing/system_tests.rst)
test_run = get_test_run(dag)
