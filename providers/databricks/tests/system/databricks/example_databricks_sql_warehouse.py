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
"""Example Dag for starting and stopping an existing Databricks SQL warehouse."""

from __future__ import annotations

import os
from datetime import datetime

from airflow.providers.common.compat.sdk import DAG
from airflow.providers.databricks.operators.databricks_warehouse import (
    DatabricksStartWarehouseOperator,
    DatabricksStopWarehouseOperator,
)

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
DAG_ID = "example_databricks_sql_warehouse"
WAREHOUSE_ID = os.environ.get("WAREHOUSE_ID", "your-warehouse-id")

with DAG(
    dag_id=DAG_ID,
    schedule="@daily",
    start_date=datetime(2021, 1, 1),
    tags=["example"],
    catchup=False,
) as dag:
    # [START howto_operator_databricks_start_sql_warehouse]
    start_warehouse = DatabricksStartWarehouseOperator(
        task_id="start_warehouse",
        databricks_conn_id="databricks_default",
        warehouse_id=WAREHOUSE_ID,
        wait_for_termination=True,
    )
    # [END howto_operator_databricks_start_sql_warehouse]

    # [START howto_operator_databricks_stop_sql_warehouse]
    stop_warehouse = DatabricksStopWarehouseOperator(
        task_id="stop_warehouse",
        databricks_conn_id="databricks_default",
        warehouse_id=WAREHOUSE_ID,
        wait_for_termination=True,
        trigger_rule="all_done",
    )
    # [END howto_operator_databricks_stop_sql_warehouse]

    start_warehouse >> stop_warehouse

    from tests_common.test_utils.watcher import watcher

    list(dag.tasks) >> watcher()

from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

test_run = get_test_run(dag)
