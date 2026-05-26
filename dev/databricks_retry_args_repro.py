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
from __future__ import annotations

from unittest import mock

from tenacity import wait_incrementing

from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
from airflow.providers.databricks.triggers.databricks import DatabricksExecutionTrigger

retry_args = {"wait": wait_incrementing(start=1, increment=1, max=3)}

print("Executing deferrable DatabricksSubmitRunOperator with invalid databricks_retry_args...")
with mock.patch("airflow.providers.databricks.operators.databricks.DatabricksHook") as hook_cls:
    hook = hook_cls.return_value
    hook.submit_run.return_value = 123
    hook.get_run_page_url.return_value = "https://example.invalid/databricks-run/123"
    hook.get_run.return_value = {
        "state": {
            "life_cycle_state": "RUNNING",
            "result_state": None,
            "state_message": "still running",
        }
    }

    operator = DatabricksSubmitRunOperator(
        task_id="databricks_retry_args_repro",
        json={"new_cluster": {}, "notebook_task": {"notebook_path": "/noop"}},
        deferrable=True,
        databricks_retry_args=retry_args,
    )

    try:
        operator.execute(context=None)
    except ValueError as err:
        print(f"raised: {type(err).__name__}: {err}")
        print(f"submit_run calls: {hook.submit_run.call_count}")
        print(f"get_run calls: {hook.get_run.call_count}")

print()
print("Creating DatabricksExecutionTrigger with invalid deferrable retry_args...")
with mock.patch("airflow.providers.databricks.triggers.databricks.DatabricksHook") as trigger_hook_cls:
    try:
        DatabricksExecutionTrigger(
            run_id=123,
            databricks_conn_id="databricks_default",
            retry_args=retry_args,
            caller="DatabricksSubmitRunOperator",
        )
    except ValueError as err:
        print(f"raised: {type(err).__name__}: {err}")
        print(f"DatabricksHook called: {trigger_hook_cls.called}")
