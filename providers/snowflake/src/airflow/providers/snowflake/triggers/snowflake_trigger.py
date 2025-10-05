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
import uuid
from collections.abc import AsyncIterator
from typing import Any

import aiohttp

from airflow.triggers.base import BaseTrigger, TriggerEvent

print("BaseTrigger module:", BaseTrigger.__module__)


class SnowflakeSqlApiTrigger(BaseTrigger):
    """
    Fetch the status for the query ids passed.

    :param poll_interval:  polling period in seconds to check for the status
    :param query_ids: List of Query ids to run and poll for the status
    :param snowflake_conn_id: Reference to Snowflake connection id
    :param token_life_time: lifetime of the JWT Token in timedelta
    :param token_renewal_delta: Renewal time of the JWT Token in timedelta
    """

    def __init__(self, poll_interval: float, query_ids: list[str], api_url: str, auth_header: dict[str, Any]):
        super().__init__()
        self.poll_interval = poll_interval
        self.query_ids = query_ids
        self.api_url = api_url
        self.auth_header = auth_header

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """Serialize SnowflakeSqlApiTrigger arguments and classpath."""
        return (
            "airflow.providers.snowflake.triggers.snowflake_trigger.SnowflakeSqlApiTrigger",
            {
                "poll_interval": self.poll_interval,
                "query_ids": self.query_ids,
                "api_url": self.api_url,
                "auth_header": self.auth_header,
            },
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:
        """Wait for the query the snowflake query to complete."""
        try:
            for query_id in self.query_ids:
                while True:
                    statement_status = await self.get_query_status(query_id)
                    if statement_status["status"] not in ["running"]:
                        break
                    await asyncio.sleep(self.poll_interval)
                if statement_status["status"] == "error":
                    yield TriggerEvent(statement_status)
                    return
            yield TriggerEvent(
                {
                    "status": "success",
                    "statement_query_ids": self.query_ids,
                }
            )
        except Exception as e:
            yield TriggerEvent({"status": "error", "message": str(e)})

    async def get_query_status(self, query_id: str) -> dict[str, Any]:
        """Async method to check query status using pre-calculated auth."""
        # The full status URL includes the API path: /api/v2/statements/
        status_url = f"{self.api_url}/api/v2/statements/{query_id}"
        req_id = uuid.uuid4()
        params = {"requestId": str(req_id)}

        self.log.info("Checking status for query ID %s at %s", query_id, status_url)

        # Use aiohttp directly with the provided headers
        async with aiohttp.ClientSession() as session:
            try:
                async with session.get(status_url, params=params, headers=self.auth_header) as response:
                    status_code = response.status
                    try:
                        resp = await response.json()
                    except aiohttp.ContentTypeError:
                        self.log.error("API response was not valid JSON (Status: %s)", status_code)
                        resp = {"message": "Non-JSON response received from Snowflake API."}

                    return await self._process_response(status_code, resp)
            except Exception as e:
                self.log.error("API call failed for query %s: %s", query_id, e)
                return {"status": "error", "message": f"API request failed: {e}"}

    async def _process_response(self, status_code: int, resp: dict[str, Any]) -> dict[str, Any]:
        """
        Processes the API response synchronously with the Hook's logic.
        Copied from Hook to make Trigger fully self-contained.
        """
        self.log.info("Snowflake SQL GET statements status API response: %s", resp)
        if status_code == 202:
            return {"status": "running", "message": "Query statements are still running"}
        if status_code == 422:
            return {"status": "error", "message": resp["message"]}
        if status_code == 200:
            if resp_statement_handles := resp.get("statementHandles"):
                statement_handles = resp_statement_handles
            elif resp_statement_handle := resp.get("statementHandle"):
                statement_handles = [resp_statement_handle]
            else:
                statement_handles = []
            return {
                "status": "success",
                "message": resp["message"],
                "statement_handles": statement_handles,
            }
        # Catch-all for unexpected status codes
        return {"status": "error", "message": resp.get("message", f"Unexpected status code {status_code}")}

    def _set_context(self, context):
        pass
