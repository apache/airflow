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
from typing import TYPE_CHECKING, Any

from airflow.providers.snowflake.hooks.snowflake_sql_api import SnowflakeSqlApiHook
from airflow.triggers.base import BaseTrigger, TriggerEvent

if TYPE_CHECKING:
    from datetime import timedelta


class SnowflakeSqlApiTrigger(BaseTrigger):
    """
    Fetch the status for the query ids passed.

    :param poll_interval:  polling period in seconds to check for the status
    :param query_ids: List of Query ids to run and poll for the status
    :param snowflake_conn_id: Reference to Snowflake connection id
    :param token_life_time: lifetime of the JWT Token in timedelta
    :param token_renewal_delta: Renewal time of the JWT Token in timedelta
    """

    def __init__(
        self,
        poll_interval: float,
        query_ids: list[str],
        snowflake_conn_id: str,
        token_life_time: timedelta,
        token_renewal_delta: timedelta,
    ):
        super().__init__()
        self.poll_interval = poll_interval
        self.query_ids = query_ids
        self.snowflake_conn_id = snowflake_conn_id
        self.token_life_time = token_life_time
        self.token_renewal_delta = token_renewal_delta

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """Serialize SnowflakeSqlApiTrigger arguments and classpath."""
        return (
            "airflow.providers.snowflake.triggers.snowflake_trigger.SnowflakeSqlApiTrigger",
            {
                "poll_interval": self.poll_interval,
                "query_ids": self.query_ids,
                "snowflake_conn_id": self.snowflake_conn_id,
                "token_life_time": self.token_life_time,
                "token_renewal_delta": self.token_renewal_delta,
            },
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:
        """Wait for the query the snowflake query to complete."""
        hook = SnowflakeSqlApiHook(
            self.snowflake_conn_id,
            self.token_life_time,
            self.token_renewal_delta,
        )

        try:
            for query_id in self.query_ids:
                while True:
                    statement_status = await self.get_query_status(query_id, hook)
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

    async def get_query_status(
        self, query_id: str, hook: SnowflakeSqlApiHook | None = None
    ) -> dict[str, Any]:
        """Return True if the SQL query is still running otherwise return False."""
        if not hook:
            hook = SnowflakeSqlApiHook(
                self.snowflake_conn_id,
                self.token_life_time,
                self.token_renewal_delta,
            )

        return await hook.get_sql_api_query_status_async(query_id)

    def _set_context(self, context):
        pass
