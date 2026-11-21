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
"""Triggers for Databricks Genie conversations."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from typing import Any
from urllib.parse import quote, urlsplit

from airflow.providers.databricks.exceptions import DatabricksApiError
from airflow.providers.databricks.hooks.genie import _PENDING_STATUSES, DatabricksGenieHook
from airflow.triggers.base import BaseTrigger, TriggerEvent

GENIE_TERMINAL_STATUSES = frozenset({"COMPLETED", "FAILED", "CANCELLED", "QUERY_RESULT_EXPIRED"})


class DatabricksGenieMessageTrigger(BaseTrigger):
    """
    Wait for a Genie message to reach a terminal status.

    The trigger only reads the message status. The worker fetches the answer and query results when the
    task resumes, so the trigger event never carries Genie output or credentials.

    :param space_id: Genie space ID.
    :param conversation_id: Genie conversation ID.
    :param message_id: Genie message ID to poll.
    :param databricks_conn_id: Databricks connection. Defaults to ``databricks_default``.
    :param polling_period_seconds: Seconds between status checks. Defaults to ``5``.
    :param retry_limit: Attempts for each status read. Defaults to ``3``.
    :param retry_delay: Minimum seconds between read attempts. Defaults to ``1``.
    """

    def __init__(
        self,
        *,
        space_id: str,
        conversation_id: str,
        message_id: str,
        databricks_conn_id: str = "databricks_default",
        polling_period_seconds: float = 5,
        retry_limit: int = 3,
        retry_delay: float = 1,
    ) -> None:
        super().__init__()
        if polling_period_seconds <= 0:
            raise ValueError("polling_period_seconds must be positive")
        self.space_id = space_id
        self.conversation_id = conversation_id
        self.message_id = message_id
        self.databricks_conn_id = databricks_conn_id
        self.polling_period_seconds = polling_period_seconds
        self.retry_limit = retry_limit
        self.retry_delay = retry_delay

    def serialize(self) -> tuple[str, dict[str, Any]]:
        return (
            "airflow.providers.databricks.triggers.genie.DatabricksGenieMessageTrigger",
            {
                "space_id": self.space_id,
                "conversation_id": self.conversation_id,
                "message_id": self.message_id,
                "databricks_conn_id": self.databricks_conn_id,
                "polling_period_seconds": self.polling_period_seconds,
                "retry_limit": self.retry_limit,
                "retry_delay": self.retry_delay,
            },
        )

    def _event(self, **fields: Any) -> TriggerEvent:
        return TriggerEvent(
            {"conversation_id": self.conversation_id, "message_id": self.message_id, **fields}
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:
        endpoint = (
            f"2.0/genie/spaces/{quote(self.space_id, safe='')}/conversations/"
            f"{quote(self.conversation_id, safe='')}/messages/{quote(self.message_id, safe='')}"
        )
        try:
            async with DatabricksGenieHook(
                databricks_conn_id=self.databricks_conn_id,
                retry_limit=self.retry_limit,
                retry_delay=self.retry_delay,
                caller="DatabricksGenieMessageTrigger",
            ) as hook:
                await hook._a_cache_databricks_conn()
                url = urlsplit(hook._endpoint_url(f"api/{endpoint}"))
                if url.scheme != "https" and url.hostname not in {"localhost", "127.0.0.1", "::1"}:
                    self.log.error("Databricks Genie requires HTTPS; refusing to poll %s", url.hostname)
                    yield self._event(status="error", error_type="insecure_connection")
                    return
                while True:
                    message = await hook._a_do_api_call(("GET", endpoint))
                    status = message.get("status") if isinstance(message, dict) else None
                    if status in GENIE_TERMINAL_STATUSES:
                        yield self._event(status="success", message_status=status)
                        return
                    if status not in _PENDING_STATUSES:
                        self.log.error("Genie returned an unknown message status: %r", status)
                        yield self._event(status="error", error_type="invalid_response")
                        return
                    self.log.info(
                        "Genie message %s is %s; checking again in %s seconds",
                        self.message_id,
                        status,
                        self.polling_period_seconds,
                    )
                    await asyncio.sleep(self.polling_period_seconds)
        except DatabricksApiError as err:
            self.log.exception("Polling Genie message %s failed", self.message_id)
            yield self._event(status="error", error_type="api_error", status_code=err.http_status_code)
        except Exception:
            self.log.exception("Polling Genie message %s failed", self.message_id)
            yield self._event(status="error", error_type="unexpected_error")
