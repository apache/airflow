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
from typing import Any, AsyncIterator, Sequence

from google.cloud.container_v1.types import Operation

from airflow.providers.google.cloud.hooks.kubernetes_engine import AsyncGKEHook
from airflow.triggers.base import BaseTrigger, TriggerEvent


class GKEOperationTrigger(BaseTrigger):
    """Trigger which checks status of the operation."""

    def __init__(
        self,
        operation_name: str,
        project_id: str | None,
        location: str,
        gcp_conn_id: str = "google_cloud_default",
        delegate_to: str | None = None,
        impersonation_chain: str | Sequence[str] | None = None,
        poll_interval: int = 10,
    ):
        super().__init__()

        self.operation_name = operation_name
        self.project_id = project_id
        self.location = location
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to
        self.impersonation_chain = impersonation_chain
        self.poll_interval = poll_interval

        self._hook: AsyncGKEHook | None = None

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """Serializes GKEOperationTrigger arguments and classpath."""
        return (
            "airflow.providers.google.cloud.triggers.kubernetes_engine.GKEOperationTrigger",
            {
                "operation_name": self.operation_name,
                "project_id": self.project_id,
                "location": self.location,
                "gcp_conn_id": self.gcp_conn_id,
                "delegate_to": self.delegate_to,
                "impersonation_chain": self.impersonation_chain,
                "poll_interval": self.poll_interval,
            },
        )

    async def run(self) -> AsyncIterator["TriggerEvent"]:  # type: ignore[override]
        """Gets operation status and yields corresponding event."""
        hook = self._get_hook()
        while True:
            try:
                operation = await hook.get_operation(
                    operation_name=self.operation_name,
                    project_id=self.project_id,
                )

                status = operation.status
                if status == Operation.Status.DONE:
                    yield TriggerEvent(
                        {
                            "status": "success",
                            "message": "Operation is successfully ended.",
                            "operation_name": operation.name,
                        }
                    )
                    return

                elif status == Operation.Status.RUNNING or status == Operation.Status.PENDING:
                    self.log.info("Operation is still running.")
                    self.log.info("Sleeping for %ss...", self.poll_interval)
                    await asyncio.sleep(self.poll_interval)

                else:
                    yield TriggerEvent(
                        {
                            "status": "failed",
                            "message": f"Operation has failed with status: {operation.status}",
                        }
                    )
                    return
            except Exception as e:
                self.log.exception("Exception occurred while checking operation status")
                yield TriggerEvent(
                    {
                        "status": "error",
                        "message": str(e),
                    }
                )
                return

    def _get_hook(self) -> AsyncGKEHook:
        if self._hook is None:
            self._hook = AsyncGKEHook(
                gcp_conn_id=self.gcp_conn_id,
                location=self.location,
                delegate_to=self.delegate_to,
                impersonation_chain=self.impersonation_chain,
            )
        return self._hook
