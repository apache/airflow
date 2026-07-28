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
from typing import Any

from azure.core.exceptions import ServiceRequestError

from airflow.providers.microsoft.azure.hooks.synapse import (
    AzureSynapsePipelineAsyncHook,
    AzureSynapsePipelineRunStatus,
)
from airflow.triggers.base import BaseTrigger, TriggerEvent


class AzureSynapsePipelineTrigger(BaseTrigger):
    """
    Trigger when an Azure Synapse pipeline run reaches a terminal state.

    If an unexpected exception occurs while polling, the trigger attempts to cancel
    the pipeline run to avoid leaving orphaned executions.

    :param run_id: Pipeline run identifier.
    :param azure_synapse_conn_id: Azure Synapse connection id.
    :param azure_synapse_workspace_dev_endpoint: Synapse workspace dev endpoint.
    :param end_time: Epoch timestamp when the trigger should timeout.
    :param check_interval: Poll interval in seconds.
    """

    def __init__(
        self,
        run_id: str,
        azure_synapse_conn_id: str,
        azure_synapse_workspace_dev_endpoint: str,
        end_time: float,
        check_interval: int = 60,
    ):
        super().__init__()

        self.run_id = run_id
        self.azure_synapse_conn_id = azure_synapse_conn_id
        self.azure_synapse_workspace_dev_endpoint = azure_synapse_workspace_dev_endpoint
        self.end_time = end_time
        self.check_interval = check_interval

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """Serialize trigger arguments and classpath."""
        return (
            f"{self.__class__.__module__}.{self.__class__.__name__}",
            {
                "run_id": self.run_id,
                "azure_synapse_conn_id": self.azure_synapse_conn_id,
                "azure_synapse_workspace_dev_endpoint": self.azure_synapse_workspace_dev_endpoint,
                "end_time": self.end_time,
                "check_interval": self.check_interval,
            },
        )

    def _build_trigger_event(self, pipeline_status: str) -> TriggerEvent | None:
        """
        Convert Synapse pipeline status to TriggerEvent.

        Returns None if the pipeline is still running.
        """
        if pipeline_status == AzureSynapsePipelineRunStatus.SUCCEEDED:
            return TriggerEvent(
                {
                    "status": "success",
                    "message": f"Pipeline run {self.run_id} succeeded.",
                    "run_id": self.run_id,
                }
            )

        if pipeline_status in AzureSynapsePipelineRunStatus.FAILURE_STATES:
            return TriggerEvent(
                {
                    "status": "error",
                    "message": f"Pipeline run {self.run_id} finished with state {pipeline_status}.",
                    "run_id": self.run_id,
                }
            )

        return None

    async def run(self) -> AsyncIterator[TriggerEvent]:
        """Poll Synapse pipeline status until completion or timeout."""
        async with AzureSynapsePipelineAsyncHook(
            azure_synapse_conn_id=self.azure_synapse_conn_id,
            azure_synapse_workspace_dev_endpoint=self.azure_synapse_workspace_dev_endpoint,
        ) as hook:
            try:
                executed_after_token_refresh = True

                while time.time() < self.end_time:
                    try:
                        pipeline_status = await hook.get_pipeline_run_status(self.run_id)

                        executed_after_token_refresh = True

                        event = self._build_trigger_event(pipeline_status)

                        if event:
                            yield event
                            return

                        self.log.info(
                            "Pipeline %s not finished yet (state=%s). Sleeping %s seconds.",
                            self.run_id,
                            pipeline_status,
                            self.check_interval,
                        )

                        await asyncio.sleep(self.check_interval)

                    except ServiceRequestError:
                        # Azure token may expire during long pipeline runs.
                        if not executed_after_token_refresh:
                            raise

                        await hook.refresh_conn()
                        executed_after_token_refresh = False

                try:
                    pipeline_status = await hook.get_pipeline_run_status(self.run_id)

                except ServiceRequestError:
                    # Azure token may expire after timeout period has elapsed.
                    await hook.refresh_conn()
                    pipeline_status = await hook.get_pipeline_run_status(self.run_id)

                # Yielding terminal event in case pipeline reaches terminal state
                # i.e. FAILED/SUCCEEDED during last sleep.
                event = self._build_trigger_event(pipeline_status)

                if event:
                    yield event
                    return

                try:
                    await hook.cancel_pipeline_run(self.run_id)
                except Exception:
                    self.log.exception("Pipeline %s cancellation failed.", self.run_id)
                finally:
                    yield TriggerEvent(
                        {
                            "status": "error",
                            "message": f"Timeout waiting for pipeline run {self.run_id}.",
                            "run_id": self.run_id,
                        }
                    )

            except Exception as e:
                self.log.exception(e)

                if self.run_id:
                    try:
                        self.log.info("Cancelling pipeline run %s", self.run_id)
                        await hook.cancel_pipeline_run(self.run_id)
                    except Exception:
                        self.log.exception("Failed to cancel pipeline run %s", self.run_id)

                yield TriggerEvent(
                    {
                        "status": "error",
                        "message": str(e),
                        "run_id": self.run_id,
                    }
                )
