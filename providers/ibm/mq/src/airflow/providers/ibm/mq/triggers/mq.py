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

from airflow.providers.ibm.mq.hooks.mq import IBMMQHook
from airflow.providers.ibm.mq.version_compat import AIRFLOW_V_3_0_PLUS
from airflow.triggers.base import TriggerEvent

if AIRFLOW_V_3_0_PLUS:
    from airflow.triggers.base import BaseEventTrigger
else:
    from airflow.triggers.base import BaseTrigger as BaseEventTrigger  # type: ignore


class AwaitMessageTrigger(BaseEventTrigger):
    def __init__(
        self,
        mq_conn_id: str,
        queue_name: str,
        poll_interval: float = 5,
    ) -> None:
        super().__init__()
        self.mq_conn_id = mq_conn_id
        self.queue_name = queue_name
        self.poll_interval = poll_interval

    def serialize(self) -> tuple[str, dict[str, Any]]:
        return (
            f"{self.__class__.__module__}.{self.__class__.__name__}",
            {
                "mq_conn_id": self.mq_conn_id,
                "queue_name": self.queue_name,
                "poll_interval": self.poll_interval,
            },
        )

    async def run(self):
        try:
            event = await IBMMQHook(self.mq_conn_id).consume(
                queue_name=self.queue_name,
                poll_interval=self.poll_interval,
            )
            if event:
                yield TriggerEvent(event)
        except asyncio.CancelledError:
            return
