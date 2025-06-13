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
import logging
from collections.abc import AsyncIterator
from datetime import datetime, timezone
from typing import Any
from uuid import UUID

from asgiref.sync import sync_to_async

from airflow.providers.standard.version_compat import AIRFLOW_V_3_1_PLUS
from airflow.sdk.execution_time.hitl import (
    get_hitl_response_content_detail,
    update_htil_response_content_detail,
)
from airflow.triggers.base import BaseTrigger, TriggerEvent

log = logging.getLogger(__name__)
if not AIRFLOW_V_3_1_PLUS:
    log.warning("Human in the loop functionality needs Airflow 3.1+..")


class HITLTrigger(BaseTrigger):
    """A trigger that checks whether Human-in-the-loop responses are received."""

    def __init__(
        self,
        *,
        ti_id: UUID,
        options: list[str],
        default: list[str] | None = None,
        params: dict[str, Any],
        multiple: bool = False,
        timeout_datetime: datetime | None,
        poke_interval: float = 5.0,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.ti_id = ti_id
        self.options = options
        self.timeout_datetime = timeout_datetime
        self.default = default
        self.params = params
        self.multiple = multiple
        self.poke_interval = poke_interval

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """Serialize HITLTrigger arguments and classpath."""
        return (
            "airflow.providers.standard.triggers.hitl.HITLTrigger",
            {
                "ti_id": self.ti_id,
                "options": self.options,
                "default": self.default,
                "params": self.params,
                "multiple": self.multiple,
                "timeout_datetime": self.timeout_datetime,
                "poke_interval": self.poke_interval,
            },
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:
        """Loop until the Human-in-the-loop response received or timeout reached."""
        while True:
            if self.timeout_datetime and self.timeout_datetime < datetime.now(timezone.utc):
                if self.default is None:
                    yield TriggerEvent(
                        {
                            "error": 'default" is required when "execution_timeout" is provided.',
                        }
                    )
                    return

                default_content: str = self.default[0] if isinstance(self.default, list) else self.default
                resp = await sync_to_async(update_htil_response_content_detail)(
                    ti_id=self.ti_id, response_content=default_content
                )
                yield TriggerEvent(
                    {
                        "response_content": default_content,
                        "params_input": self.params,
                    }
                )
                return

            resp = await sync_to_async(get_hitl_response_content_detail)(ti_id=self.ti_id)
            if resp.response_received:
                self.log.info("Responded by %s at %s", resp.user_id, resp.response_at)
                yield TriggerEvent(
                    {
                        "response_content": resp.response_content,
                        "params_input": resp.params_input,
                    }
                )
                return
            await asyncio.sleep(self.poke_interval)
