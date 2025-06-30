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
from datetime import datetime, timezone
from typing import Any
from uuid import UUID

from asgiref.sync import sync_to_async

from airflow.providers.standard.execution_time.hitl import get_hitl_response_content
from airflow.triggers.base import BaseTrigger, TriggerEvent


class HITLTrigger(BaseTrigger):
    """A trigger that checks whether Human-in-the-loop responses are received."""

    def __init__(
        self,
        *,
        ti_id: UUID,
        options: list[str],
        timeout_datetime: datetime | None,
        default: str | None = None,
        multiple: bool = False,
        poke_interval: float = 5.0,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.ti_id = ti_id
        self.options = options
        self.timeout_datetime = timeout_datetime
        self.default = default
        self.multiple = multiple
        self.poke_interval = poke_interval

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """Serialize HITLTrigger arguments and classpath."""
        return (
            "airflow.providers.standard.triggers.hitl.HITLTrigger",
            {
                "ti_id": self.ti_id,
                "options": self.options,
                "timeout_datetime": self.timeout_datetime,
                "default": self.default,
                "multiple": self.multiple,
                "poke_interval": self.poke_interval,
            },
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:
        """Loop until the Human-in-the-loop response received or timeout reached."""
        while True:
            if self.timeout_datetime and self.timeout_datetime < datetime.now(timezone.utc):
                yield TriggerEvent({"content": self.default})
                return

            content = await sync_to_async(get_hitl_response_content)(ti_id=self.ti_id)
            if content:
                yield TriggerEvent({"content": content})
                return
            await asyncio.sleep(self.poke_interval)
