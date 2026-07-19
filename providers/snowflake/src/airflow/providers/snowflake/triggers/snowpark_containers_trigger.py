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
import time
from collections.abc import AsyncIterator
from enum import Enum
from typing import Any

from airflow.providers.common.sql.hooks.handlers import fetch_one_handler
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.triggers.base import BaseTrigger, TriggerEvent


class SnowparkContainerJobStatus(str, Enum):
    """Statuses of a Snowpark Container Services job service."""

    PENDING = "PENDING"
    RUNNING = "RUNNING"
    CANCELLING = "CANCELLING"
    SUSPENDING = "SUSPENDING"
    DELETING = "DELETING"
    DONE = "DONE"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"
    INTERNAL_ERROR = "INTERNAL_ERROR"


TERMINAL_STATUSES: frozenset[SnowparkContainerJobStatus] = frozenset(
    {
        SnowparkContainerJobStatus.DONE,
        SnowparkContainerJobStatus.FAILED,
        SnowparkContainerJobStatus.CANCELLED,
        SnowparkContainerJobStatus.INTERNAL_ERROR,
    }
)
NON_TERMINAL_STATUSES: frozenset[SnowparkContainerJobStatus] = frozenset(
    {
        SnowparkContainerJobStatus.PENDING,
        SnowparkContainerJobStatus.RUNNING,
        SnowparkContainerJobStatus.CANCELLING,
        SnowparkContainerJobStatus.SUSPENDING,
        SnowparkContainerJobStatus.DELETING,
    }
)


class SnowparkContainerJobTrigger(BaseTrigger):
    """
    Poll a Snowpark Container Services job until it reaches a terminal status.

    :param job_name: name of the submitted job service to poll.
    :param snowflake_conn_id: reference to the Snowflake connection id.
    :param poll_interval: seconds to sleep between ``DESCRIBE SERVICE`` polls.
    :param end_time: epoch deadline (``time.time()`` seconds) after which a ``timeout``
        event is emitted.
    :param database: (Optional) name of database.
    :param schema: (Optional) name of schema.
    :param role: (Optional) name of role.
    :param warehouse: (Optional) name of warehouse.
    """

    def __init__(
        self,
        job_name: str,
        snowflake_conn_id: str,
        poll_interval: float,
        end_time: float,
        database: str | None = None,
        schema: str | None = None,
        role: str | None = None,
        warehouse: str | None = None,
    ) -> None:
        super().__init__()
        self.job_name = job_name
        self.snowflake_conn_id = snowflake_conn_id
        self.poll_interval = poll_interval
        self.end_time = end_time
        self.database = database
        self.schema = schema
        self.role = role
        self.warehouse = warehouse

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """Serialize SnowparkContainerJobTrigger arguments and class path."""
        return (
            "airflow.providers.snowflake.triggers.snowpark_containers_trigger.SnowparkContainerJobTrigger",
            {
                "job_name": self.job_name,
                "snowflake_conn_id": self.snowflake_conn_id,
                "poll_interval": self.poll_interval,
                "end_time": self.end_time,
                "database": self.database,
                "schema": self.schema,
                "role": self.role,
                "warehouse": self.warehouse,
            },
        )

    def _get_hook(self) -> SnowflakeHook:
        """Build a ``SnowflakeHook`` from the trigger's connection settings."""
        return SnowflakeHook(
            snowflake_conn_id=self.snowflake_conn_id,
            warehouse=self.warehouse,
            database=self.database,
            schema=self.schema,
            role=self.role,
        )

    async def _describe_status(self, hook: SnowflakeHook) -> str | None:
        """Return the job's current status via ``DESCRIBE SERVICE``, or ``None`` if absent."""
        # SnowflakeHook is synchronous. Run the blocking poll off the event loop so a
        # single query does not stall every other trigger on this triggerer.
        response: Any = await asyncio.to_thread(
            hook.run,
            f"DESCRIBE SERVICE {self.job_name}",
            handler=fetch_one_handler,
            return_dictionaries=True,
        )
        return response.get("status") if response else None

    async def run(self) -> AsyncIterator[TriggerEvent]:
        """Poll the job status and yield exactly one terminal event."""
        hook = self._get_hook()
        while True:
            try:
                status = await self._describe_status(hook=hook)
            except Exception as e:
                yield TriggerEvent({"status": "error", "job_name": self.job_name, "message": str(e)})
                return

            if status in TERMINAL_STATUSES:
                yield TriggerEvent({"status": status, "job_name": self.job_name})
                return

            if status not in NON_TERMINAL_STATUSES:
                yield TriggerEvent(
                    {
                        "status": "error",
                        "job_name": self.job_name,
                        "message": f"Job {self.job_name} returned unexpected status: {status}",
                    }
                )
                return

            if time.time() > self.end_time:
                yield TriggerEvent(
                    {
                        "status": "timeout",
                        "job_name": self.job_name,
                        "message": f"Job {self.job_name} did not reach a terminal status before the timeout.",
                    }
                )
                return
            await asyncio.sleep(self.poll_interval)

    async def on_kill(self) -> None:
        """Drop the job service when a deferred task is killed."""
        hook = self._get_hook()
        try:
            await asyncio.to_thread(hook.run, f"DROP SERVICE IF EXISTS {self.job_name}")
            self.log.info("on_kill: dropped service %s", self.job_name)
        except Exception as e:
            self.log.error("on_kill: failed to drop service %s: %s", self.job_name, e)
