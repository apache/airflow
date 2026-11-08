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
Example Dag for editing, starting and stopping an existing Databricks SQL warehouse.

The original auto-stop setting is saved before editing and restored during cleanup.
"""

from __future__ import annotations

import os
from datetime import datetime

from airflow.providers.common.compat.sdk import DAG, AirflowSkipException, task
from airflow.providers.databricks.hooks.databricks import DatabricksHook
from airflow.providers.databricks.operators.warehouse import (
    DatabricksEditWarehouseOperator,
    DatabricksStartWarehouseOperator,
    DatabricksStopWarehouseOperator,
)

DAG_ID = "example_databricks_sql_warehouse"
WAREHOUSE_ID = os.environ.get("WAREHOUSE_ID", "your-warehouse-id")

with DAG(
    dag_id=DAG_ID,
    schedule="@daily",
    start_date=datetime(2021, 1, 1),
    tags=["example"],
    catchup=False,
    max_active_runs=1,
) as dag:

    @task
    def get_original_auto_stop_mins() -> int:
        hook = DatabricksHook(databricks_conn_id="databricks_default")
        return hook.get_warehouse(WAREHOUSE_ID)["auto_stop_mins"]

    original_auto_stop_mins = get_original_auto_stop_mins()

    # [START howto_operator_databricks_edit_sql_warehouse]
    edit_warehouse = DatabricksEditWarehouseOperator(
        task_id="edit_warehouse",
        databricks_conn_id="databricks_default",
        warehouse_id=WAREHOUSE_ID,
        json={"auto_stop_mins": 10},
    )
    # [END howto_operator_databricks_edit_sql_warehouse]

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

    @task(trigger_rule="all_done")
    def restore_auto_stop_mins(auto_stop_mins: int | None) -> None:
        if auto_stop_mins is None:
            raise AirflowSkipException("The original auto-stop setting was not saved.")
        hook = DatabricksHook(databricks_conn_id="databricks_default")
        hook.edit_warehouse(WAREHOUSE_ID, {"auto_stop_mins": auto_stop_mins})

    restore_warehouse = restore_auto_stop_mins(original_auto_stop_mins)

    original_auto_stop_mins >> edit_warehouse >> start_warehouse >> stop_warehouse >> restore_warehouse

    from tests_common.test_utils.watcher import watcher

    list(dag.tasks) >> watcher()

from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

test_run = get_test_run(dag)
