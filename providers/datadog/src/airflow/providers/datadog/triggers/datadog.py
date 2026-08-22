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
from collections.abc import AsyncIterator, Sequence
from typing import Any

from datadog import api

from airflow.providers.common.compat.sdk import AirflowException
from airflow.providers.datadog.hooks.datadog import DatadogHook
from airflow.triggers.base import BaseTrigger, TriggerEvent


def get_monitor_state(monitor_id: int, datadog_conn_id: str) -> str:
    """Return the current overall state of a Datadog monitor."""
    # This instantiates the hook, but doesn't need it further, because the
    # API authenticates globally (see DatadogSensor.poke).
    DatadogHook(datadog_conn_id=datadog_conn_id)
    response = api.Monitor.get(monitor_id)
    if isinstance(response, dict) and "errors" in response:
        raise AirflowException(f"Datadog monitor {monitor_id}: {response['errors']}")
    return response["overall_state"]


class DatadogMonitorTrigger(BaseTrigger):
    """
    Polls a Datadog monitor until it reaches one of the target states.

    :param monitor_id: The id of the Datadog monitor to watch.
    :param target_states: Monitor overall states that complete the wait.
    :param datadog_conn_id: The connection to datadog, containing metadata for api keys.
    :param poke_interval: Seconds to wait between checks of the monitor state.
    """

    def __init__(
        self,
        monitor_id: int,
        target_states: Sequence[str],
        datadog_conn_id: str,
        poke_interval: float,
    ):
        super().__init__()
        self.monitor_id = monitor_id
        self.target_states = list(target_states)
        self.datadog_conn_id = datadog_conn_id
        self.poke_interval = poke_interval

    def serialize(self) -> tuple[str, dict[str, Any]]:
        return (
            "airflow.providers.datadog.triggers.datadog.DatadogMonitorTrigger",
            {
                "monitor_id": self.monitor_id,
                "target_states": self.target_states,
                "datadog_conn_id": self.datadog_conn_id,
                "poke_interval": self.poke_interval,
            },
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:
        while True:
            state = await asyncio.to_thread(get_monitor_state, self.monitor_id, self.datadog_conn_id)
            self.log.info("Monitor %s overall_state=%s", self.monitor_id, state)
            if state in self.target_states:
                yield TriggerEvent({"status": "success", "state": state})
                return
            await asyncio.sleep(self.poke_interval)
