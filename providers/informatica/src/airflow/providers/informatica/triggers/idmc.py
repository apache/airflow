#
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
"""Triggers that poll IDMC run status from the Triggerer."""

from __future__ import annotations

import asyncio
import time
from collections.abc import AsyncIterator
from typing import Any

from airflow.providers.informatica.hooks.idmc import (
    IDMCRunStatus,
    InformaticaIDMCError,
    InformaticaIDMCHook,
)
from airflow.triggers.base import BaseTrigger, TriggerEvent


class _BaseInformaticaIDMCRunTrigger(BaseTrigger):
    """
    Common polling loop shared by the task-run and taskflow-run triggers.

    Subclasses provide the async hook method that pulls the current status
    via :attr:`_status_method_name`.
    """

    _status_method_name: str = ""

    def __init__(
        self,
        *,
        conn_id: str,
        run_id: str,
        end_time: float,
        poll_interval: float,
        auth_version: str | None = None,
        hook_params: dict[str, Any] | None = None,
    ) -> None:
        super().__init__()
        self.conn_id = conn_id
        self.run_id = run_id
        self.end_time = end_time
        self.poll_interval = poll_interval
        self.auth_version = auth_version
        self.hook_params = hook_params or {}

    def _classpath(self) -> str:
        return f"airflow.providers.informatica.triggers.idmc.{type(self).__name__}"

    def _serialize_kwargs(self) -> dict[str, Any]:
        return {
            "conn_id": self.conn_id,
            "run_id": self.run_id,
            "end_time": self.end_time,
            "poll_interval": self.poll_interval,
            "auth_version": self.auth_version,
            "hook_params": self.hook_params,
        }

    def serialize(self) -> tuple[str, dict[str, Any]]:
        return self._classpath(), self._serialize_kwargs()

    def _build_hook(self) -> InformaticaIDMCHook:
        return InformaticaIDMCHook(
            informatica_idmc_conn_id=self.conn_id,
            auth_version=self.auth_version,
            **self.hook_params,
        )

    async def _fetch_status(self, hook: InformaticaIDMCHook) -> dict[str, Any]:
        method = getattr(hook, self._status_method_name)
        return await method(self.run_id)

    async def run(self) -> AsyncIterator[TriggerEvent]:
        hook = self._build_hook()
        try:
            while True:
                status_info = await self._fetch_status(hook)
                status = status_info.get("status", IDMCRunStatus.RUNNING.value)
                if IDMCRunStatus.is_terminal(status):
                    yield TriggerEvent(
                        {
                            "status": status,
                            "run_id": self.run_id,
                            "raw_status": status_info.get("raw_status"),
                            "message": (f"IDMC run {self.run_id} reached terminal status {status}."),
                        }
                    )
                    return

                if self.end_time < time.time():
                    # One last status check in case the run finished between
                    # the previous fetch and the timeout deadline.
                    final = await self._fetch_status(hook)
                    final_status = final.get("status", IDMCRunStatus.RUNNING.value)
                    if IDMCRunStatus.is_terminal(final_status):
                        yield TriggerEvent(
                            {
                                "status": final_status,
                                "run_id": self.run_id,
                                "raw_status": final.get("raw_status"),
                                "message": (
                                    f"IDMC run {self.run_id} reached terminal status {final_status}."
                                ),
                            }
                        )
                        return
                    yield TriggerEvent(
                        {
                            "status": "timeout",
                            "run_id": self.run_id,
                            "message": (
                                f"IDMC run {self.run_id} did not reach a terminal status "
                                f"within the configured timeout."
                            ),
                        }
                    )
                    return

                await asyncio.sleep(self.poll_interval)
        except InformaticaIDMCError as exc:
            yield TriggerEvent({"status": "error", "run_id": self.run_id, "message": str(exc)})


class InformaticaIDMCTaskRunTrigger(_BaseInformaticaIDMCRunTrigger):
    """Poll a CDI task run (mapping / sync / PowerCenter / etc.) until terminal."""

    _status_method_name = "aget_task_run_status"


class InformaticaIDMCTaskflowRunTrigger(_BaseInformaticaIDMCRunTrigger):
    """Poll a CDI taskflow run until it reaches a terminal status."""

    _status_method_name = "aget_taskflow_run_status"
