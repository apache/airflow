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
"""Triggers for Databricks agent invocations."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from typing import Any

from airflow.providers.databricks.exceptions import DatabricksApiError
from airflow.providers.databricks.hooks.agent import DatabricksAgentHook
from airflow.triggers.base import BaseTrigger, TriggerEvent


class DatabricksAgentInvocationTrigger(BaseTrigger):
    """
    Wait for an agent invocation to complete, fail or request human input.

    :param app_url: HTTPS base URL of the deployed app.
    :param invocation_id: UUID identifying the invocation to poll.
    :param databricks_conn_id: Databricks connection using service principal OAuth.
        Defaults to ``databricks_default``.
    :param session_id: Conversation ID sent as the routing key. Defaults to ``None``.
    :param polling_period_seconds: Seconds between status checks. Defaults to ``10``.
    """

    def __init__(
        self,
        *,
        app_url: str,
        invocation_id: str,
        databricks_conn_id: str = "databricks_default",
        session_id: str | None = None,
        polling_period_seconds: float = 10,
    ) -> None:
        super().__init__()
        if polling_period_seconds <= 0:
            raise ValueError("polling_period_seconds must be positive")
        self.app_url = app_url
        self.invocation_id = invocation_id
        self.databricks_conn_id = databricks_conn_id
        self.session_id = session_id
        self.polling_period_seconds = polling_period_seconds

    def serialize(self) -> tuple[str, dict[str, Any]]:
        return (
            "airflow.providers.databricks.triggers.agent.DatabricksAgentInvocationTrigger",
            {
                "app_url": self.app_url,
                "invocation_id": self.invocation_id,
                "databricks_conn_id": self.databricks_conn_id,
                "session_id": self.session_id,
                "polling_period_seconds": self.polling_period_seconds,
            },
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:
        try:
            async with DatabricksAgentHook(self.app_url, self.databricks_conn_id) as hook:
                while True:
                    result = await hook.a_get_invocation(self.invocation_id, self.session_id)
                    if result.get("status") in ("completed", "failed", "interrupted"):
                        yield TriggerEvent(
                            {"status": "success", "invocation_id": self.invocation_id, "error_type": None}
                        )
                        return
                    if not result.get("status"):
                        raise ValueError("Databricks agent response is missing its status")
                    await asyncio.sleep(self.polling_period_seconds)
        except Exception as err:
            self.log.exception("Polling Databricks agent invocation %s failed", self.invocation_id)
            if isinstance(err, DatabricksApiError):
                error_type = "api_error"
            elif isinstance(err, ValueError):
                error_type = "invalid_response"
            else:
                error_type = "unexpected_error"
            yield TriggerEvent(
                {"status": "error", "invocation_id": self.invocation_id, "error_type": error_type}
            )
