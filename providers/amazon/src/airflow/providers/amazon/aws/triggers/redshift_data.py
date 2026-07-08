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
from functools import cached_property
from typing import Any

from airflow.providers.amazon.aws.hooks.redshift_data import (
    ABORTED_STATE,
    FAILED_STATE,
    RedshiftDataHook,
    RedshiftDataQueryAbortedError,
    RedshiftDataQueryFailedError,
)
from airflow.triggers.base import BaseTrigger, TriggerEvent


class RedshiftDataTrigger(BaseTrigger):
    """
    RedshiftDataTrigger is fired as deferred class with params to run the task in triggerer.

    :param statement_id: the UUID of the statement
    :param task_id: task ID of the Dag
    :param poll_interval:  polling period in seconds to check for the status
    :param aws_conn_id: AWS connection ID for redshift
    :param region_name: aws region to use
    :param cancel_on_kill: If True (default), cancel the running Redshift statement when the user
        kills the deferred task (mark failed, clear, or mark success). Requires a version of
        ``apache-airflow`` with ``BaseTrigger.on_kill()`` support; on older versions it is inert.
    """

    def __init__(
        self,
        statement_id: str,
        task_id: str,
        poll_interval: int,
        aws_conn_id: str | None = "aws_default",
        region_name: str | None = None,
        verify: bool | str | None = None,
        botocore_config: dict | None = None,
        cancel_on_kill: bool = True,
    ):
        super().__init__()
        self.statement_id = statement_id
        self.task_id = task_id
        self.poll_interval = poll_interval

        self.aws_conn_id = aws_conn_id
        self.region_name = region_name
        self.verify = verify
        self.botocore_config = botocore_config
        self.cancel_on_kill = cancel_on_kill

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """Serialize RedshiftDataTrigger arguments and classpath."""
        return (
            "airflow.providers.amazon.aws.triggers.redshift_data.RedshiftDataTrigger",
            {
                "statement_id": self.statement_id,
                "task_id": self.task_id,
                "aws_conn_id": self.aws_conn_id,
                "poll_interval": self.poll_interval,
                "region_name": self.region_name,
                "verify": self.verify,
                "botocore_config": self.botocore_config,
                "cancel_on_kill": self.cancel_on_kill,
            },
        )

    @cached_property
    def hook(self) -> RedshiftDataHook:
        return RedshiftDataHook(
            aws_conn_id=self.aws_conn_id,
            region_name=self.region_name,
            verify=self.verify,
            config=self.botocore_config,
        )

    async def on_kill(self) -> None:
        """Cancel the running Redshift statement when the user kills the deferred task."""
        if not self.cancel_on_kill or not self.statement_id:
            return
        self.log.info("Cancelling Redshift statement %s.", self.statement_id)
        try:
            async with await self.hook.get_async_conn() as client:
                await client.cancel_statement(Id=self.statement_id)
            self.log.info("Redshift statement %s cancelled.", self.statement_id)
        except Exception:
            self.log.exception(
                "Failed to cancel Redshift statement %s. The query may still be running.",
                self.statement_id,
            )

    async def run(self) -> AsyncIterator[TriggerEvent]:
        try:
            while True:
                if not await self.hook.is_still_running(self.statement_id):
                    break
                await asyncio.sleep(self.poll_interval)

            is_finished = await self.hook.check_query_is_finished_async(self.statement_id)
            if is_finished:
                response = {"status": "success", "statement_id": self.statement_id}
            else:
                response = {
                    "status": "error",
                    "statement_id": self.statement_id,
                    "message": f"{self.task_id} failed",
                }
            yield TriggerEvent(response)
        except (RedshiftDataQueryFailedError, RedshiftDataQueryAbortedError) as error:
            response = {
                "status": "error",
                "statement_id": self.statement_id,
                "message": str(error),
                "type": FAILED_STATE if isinstance(error, RedshiftDataQueryFailedError) else ABORTED_STATE,
            }
            yield TriggerEvent(response)
        except Exception as error:
            yield TriggerEvent({"status": "error", "statement_id": self.statement_id, "message": str(error)})
