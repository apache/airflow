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
from typing import Any, TypedDict

from asgiref.sync import sync_to_async

from airflow.sdk.execution_time.interactive import fetch_response_content
from airflow.triggers.base import BaseTrigger, TriggerEvent


class _InteractiveResponseTriggerEventPaylod(TypedDict):
    content: str


class _InteractiveMultipleResponseTriggerEventPaylod(TypedDict):
    content: list[str]


class InteractiveTrigger(BaseTrigger):
    """AIP-90 Tirggerer."""

    def __init__(
        self,
        *,
        ti_id,
        options: list[str],
        default: str | None = None,
        multiple: bool = False,
        poke_interval: float = 5.0,
        **kwargs,
    ):
        super().__init__()
        self.ti_id = ti_id
        self.options = options
        self.default = default
        self.multiple = multiple
        self.poke_interval = poke_interval

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """Serialize InteractiveTrigger arguments and classpath."""
        return (
            "airflow.providers.standard.triggers.interactive.InteractiveTrigger",
            {
                "ti_id": self.ti_id,
                "options": self.options,
                "default": self.options,
                "multiple": self.multiple,
                "poke_interval": self.poke_interval,
            },
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:
        """Loop until the relevant files are found."""
        while True:
            content = await sync_to_async(fetch_response_content)(ti_id=self.ti_id)
            if content:
                yield TriggerEvent({"content": content})
                return
            await asyncio.sleep(self.poke_interval)
