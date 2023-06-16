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
from datetime import timedelta
from typing import Any, AsyncIterator

from airflow.providers.snowflake.hooks.snowflake_sql_api import (
    SnowflakeSqlApiHook,
)
from airflow.triggers.base import BaseTrigger, TriggerEvent


class SnowflakeSqlApiTrigger(BaseTrigger):
    """
    SnowflakeSqlApi Trigger inherits from the BaseTrigger,it is fired as
    deferred class with params to run the task in trigger worker and
    fetch the status for the query ids passed.

    :param task_id: Reference to task id of the Dag
    :param poll_interval:  polling period in seconds to check for the status
    :param query_ids: List of Query ids to run and poll for the status
    :param snowflake_conn_id: Reference to Snowflake connection id
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
        """Serializes SnowflakeSqlApiTrigger arguments and classpath."""
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
        """Wait for the query the snowflake query to complete"""
        hook = SnowflakeSqlApiHook(
            self.snowflake_conn_id,
            self.token_life_time,
            self.token_renewal_delta,
        )
        try:
            statement_query_ids: list[str] = []
            for query_id in self.query_ids:
                while await self.is_still_running(query_id):
                    await asyncio.sleep(self.poll_interval)
                statement_status = await hook.get_sql_api_query_status_async(query_id)
                if statement_status["status"] == "error":
                    yield TriggerEvent(statement_status)
                if statement_status["status"] == "success":
                    statement_query_ids.extend(statement_status["statement_handles"])
            yield TriggerEvent(
                {
                    "status": "success",
                    "statement_query_ids": statement_query_ids,
                }
            )
        except Exception as e:
            yield TriggerEvent({"status": "error", "message": str(e)})

    async def is_still_running(self, query_id: str) -> bool:
        """
        Async function to check whether the query statement submitted via SQL API is still
        running state and returns True if it is still running else
        return False.
        """
        hook = SnowflakeSqlApiHook(
            self.snowflake_conn_id,
            self.token_life_time,
            self.token_renewal_delta,
        )
        statement_status = await hook.get_sql_api_query_status_async(query_id)
        if statement_status["status"] in ["running"]:
            return True
        return False

    def _set_context(self, context):
        pass
