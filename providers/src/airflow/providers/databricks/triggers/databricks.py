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
from __future__ import annotations

import asyncio
from typing import Any

from airflow.providers.databricks.hooks.databricks import DatabricksHook
from airflow.triggers.base import BaseTrigger, TriggerEvent


class DatabricksExecutionTrigger(BaseTrigger):
    """
    The trigger handles the logic of async communication with DataBricks API.

    :param run_id: id of the run
    :param databricks_conn_id: Reference to the :ref:`Databricks connection <howto/connection:databricks>`.
    :param polling_period_seconds: Controls the rate of the poll for the result of this run.
        By default, the trigger will poll every 30 seconds.
    :param retry_limit: The number of times to retry the connection in case of service outages.
    :param retry_delay: The number of seconds to wait between retries.
    :param retry_args: An optional dictionary with arguments passed to ``tenacity.Retrying`` class.
    :param run_page_url: The run page url.
    """

    def __init__(
        self,
        run_id: int,
        databricks_conn_id: str,
        polling_period_seconds: int = 30,
        retry_limit: int = 3,
        retry_delay: int = 10,
        retry_args: dict[Any, Any] | None = None,
        run_page_url: str | None = None,
        repair_run: bool = False,
        caller: str = "DatabricksExecutionTrigger",
    ) -> None:
        super().__init__()
        self.run_id = run_id
        self.databricks_conn_id = databricks_conn_id
        self.polling_period_seconds = polling_period_seconds
        self.retry_limit = retry_limit
        self.retry_delay = retry_delay
        self.retry_args = retry_args
        self.run_page_url = run_page_url
        self.repair_run = repair_run
        self.hook = DatabricksHook(
            databricks_conn_id,
            retry_limit=self.retry_limit,
            retry_delay=self.retry_delay,
            retry_args=retry_args,
            caller=caller,
        )

    def serialize(self) -> tuple[str, dict[str, Any]]:
        return (
            "airflow.providers.databricks.triggers.databricks.DatabricksExecutionTrigger",
            {
                "run_id": self.run_id,
                "databricks_conn_id": self.databricks_conn_id,
                "polling_period_seconds": self.polling_period_seconds,
                "retry_limit": self.retry_limit,
                "retry_delay": self.retry_delay,
                "retry_args": self.retry_args,
                "run_page_url": self.run_page_url,
                "repair_run": self.repair_run,
            },
        )

    async def run(self):
        async with self.hook:
            while True:
                run_state = await self.hook.a_get_run_state(self.run_id)
                if not run_state.is_terminal:
                    self.log.info(
                        "run-id %s in run state %s. sleeping for %s seconds",
                        self.run_id,
                        run_state,
                        self.polling_period_seconds,
                    )
                    await asyncio.sleep(self.polling_period_seconds)
                    continue

                failed_tasks = []
                if run_state.result_state == "FAILED":
                    run_info = await self.hook.a_get_run(self.run_id)
                    for task in run_info.get("tasks", []):
                        if task.get("state", {}).get("result_state", "") == "FAILED":
                            task_run_id = task["run_id"]
                            task_key = task["task_key"]
                            run_output = await self.hook.a_get_run_output(task_run_id)
                            if "error" in run_output:
                                error = run_output["error"]
                            else:
                                error = run_state.state_message
                            failed_tasks.append(
                                {
                                    "task_key": task_key,
                                    "run_id": task_run_id,
                                    "error": error,
                                }
                            )
                yield TriggerEvent(
                    {
                        "run_id": self.run_id,
                        "run_page_url": self.run_page_url,
                        "run_state": run_state.to_json(),
                        "repair_run": self.repair_run,
                        "errors": failed_tasks,
                    }
                )
                return
