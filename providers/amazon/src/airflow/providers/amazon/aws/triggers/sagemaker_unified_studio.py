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

"""This module contains the Amazon SageMaker Unified Studio Notebook job trigger."""

from __future__ import annotations

import asyncio
from typing import Any

from airflow.providers.amazon.aws.hooks.sagemaker_unified_studio import SageMakerNotebookHook
from airflow.triggers.base import BaseTrigger, TriggerEvent


class SageMakerNotebookJobTrigger(BaseTrigger):
    """Async trigger for SageMaker Unified Studio notebook executions."""

    def __init__(
        self,
        *,
        execution_id: str,
        execution_name: str,
        waiter_delay: int,
        waiter_max_attempts: int,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.execution_id = execution_id
        self.execution_name = execution_name
        self.waiter_delay = waiter_delay
        self.waiter_max_attempts = waiter_max_attempts

    def serialize(self) -> tuple[str, dict[str, Any]]:
        return (
            "airflow.providers.amazon.aws.triggers.sagemaker_unified_studio.SageMakerNotebookJobTrigger",
            {
                "execution_id": self.execution_id,
                "execution_name": self.execution_name,
                "waiter_delay": self.waiter_delay,
                "waiter_max_attempts": self.waiter_max_attempts,
            },
        )

    async def run(self):
        hook = SageMakerNotebookHook(execution_name=self.execution_name)
        attempts = 0

        terminal_success = {"COMPLETED", "SUCCEEDED"}
        terminal_failure = {"FAILED", "ERROR", "CANCELLED", "STOPPED"}

        while attempts < self.waiter_max_attempts:
            attempts += 1

            # CI-safe async execution (NO run_in_executor)
            response = await asyncio.to_thread(
                hook.get_notebook_execution,
                self.execution_id,
            )

            status = response.get("status")
            error_message = response.get("error_details", {}).get("error_message")

            if status in terminal_success:
                yield TriggerEvent(
                    {
                        "status": "success",
                        "execution_id": self.execution_id,
                        "files": response.get("files"),
                        "s3_path": response.get("s3_path"),
                    }
                )
                return

            if status in terminal_failure:
                yield TriggerEvent(
                    {
                        "status": "failed",
                        "execution_id": self.execution_id,
                        "error": error_message or f"Execution ended with status: {status}",
                    }
                )
                return

            await asyncio.sleep(self.waiter_delay)

        yield TriggerEvent(
            {
                "status": "failed",
                "execution_id": self.execution_id,
                "error": "Execution timed out",
            }
        )
