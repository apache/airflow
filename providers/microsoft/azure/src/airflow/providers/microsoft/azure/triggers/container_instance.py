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
from collections.abc import AsyncIterator
from typing import Any

from airflow.providers.microsoft.azure.hooks.container_instance import AzureContainerInstanceAsyncHook
from airflow.triggers.base import BaseTrigger, TriggerEvent

TERMINAL_STATES = frozenset({"Terminated", "Succeeded", "Failed", "Unhealthy"})
SUCCESS_STATES = frozenset({"Terminated", "Succeeded"})


class AzureContainerInstanceTrigger(BaseTrigger):
    """
    Poll an Azure Container Instance until it reaches a terminal state.

    :param resource_group: the name of the resource group
    :param name: the name of the container group
    :param ci_conn_id: connection id of the Azure service principal
    :param polling_interval: time in seconds between state polls
    """

    def __init__(
        self,
        resource_group: str,
        name: str,
        ci_conn_id: str,
        polling_interval: float = 5.0,
    ) -> None:
        super().__init__()
        self.resource_group = resource_group
        self.name = name
        self.ci_conn_id = ci_conn_id
        self.polling_interval = polling_interval

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """Serialize trigger arguments and classpath."""
        return (
            "airflow.providers.microsoft.azure.triggers.container_instance.AzureContainerInstanceTrigger",
            {
                "resource_group": self.resource_group,
                "name": self.name,
                "ci_conn_id": self.ci_conn_id,
                "polling_interval": self.polling_interval,
            },
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:
        """Poll ACI until a terminal state is reached, then yield a TriggerEvent."""
        try:
            async with AzureContainerInstanceAsyncHook(azure_conn_id=self.ci_conn_id) as hook:
                while True:
                    cg_state = await hook.get_state(self.resource_group, self.name)
                    instance_view = cg_state.containers[0].instance_view

                    if instance_view is not None:
                        c_state = instance_view.current_state
                        state = c_state.state
                        exit_code = c_state.exit_code
                        detail_status = c_state.detail_status
                    else:
                        state = cg_state.provisioning_state
                        exit_code = 0
                        detail_status = "Provisioning"

                    self.log.info("Container group %s/%s state: %s", self.resource_group, self.name, state)

                    if state in TERMINAL_STATES:
                        if state in SUCCESS_STATES and exit_code == 0:
                            yield TriggerEvent(
                                {
                                    "status": "success",
                                    "exit_code": exit_code,
                                    "detail_status": detail_status,
                                    "resource_group": self.resource_group,
                                    "name": self.name,
                                }
                            )
                        else:
                            yield TriggerEvent(
                                {
                                    "status": "error",
                                    "exit_code": exit_code,
                                    "detail_status": detail_status,
                                    "resource_group": self.resource_group,
                                    "name": self.name,
                                    "message": (
                                        f"Container group {self.resource_group}/{self.name} "
                                        f"reached state {state!r} with exit code {exit_code} "
                                        f"({detail_status})"
                                    ),
                                }
                            )
                        return

                    await asyncio.sleep(self.polling_interval)
        except Exception as e:
            yield TriggerEvent(
                {
                    "status": "error",
                    "message": str(e),
                    "resource_group": self.resource_group,
                    "name": self.name,
                    "exit_code": 1,
                    "detail_status": "",
                }
            )
