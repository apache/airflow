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
    """

    def __init__(self, run_id: int, databricks_conn_id: str, polling_period_seconds: int = 30) -> None:
        super().__init__()
        self.run_id = run_id
        self.databricks_conn_id = databricks_conn_id
        self.polling_period_seconds = polling_period_seconds
        self.hook = DatabricksHook(databricks_conn_id)

    def serialize(self) -> tuple[str, dict[str, Any]]:
        return (
            "airflow.providers.databricks.triggers.databricks.DatabricksExecutionTrigger",
            {
                "run_id": self.run_id,
                "databricks_conn_id": self.databricks_conn_id,
                "polling_period_seconds": self.polling_period_seconds,
            },
        )

    async def run(self):
        async with self.hook:
            run_page_url = await self.hook.a_get_run_page_url(self.run_id)
            while True:
                run_state = await self.hook.a_get_run_state(self.run_id)
                if run_state.is_terminal:
                    yield TriggerEvent(
                        {
                            "run_id": self.run_id,
                            "run_state": run_state.to_json(),
                            "run_page_url": run_page_url,
                        }
                    )
                    break
                else:
                    await asyncio.sleep(self.polling_period_seconds)
