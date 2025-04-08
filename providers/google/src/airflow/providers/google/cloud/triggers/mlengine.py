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
from collections.abc import AsyncIterator, Sequence
from typing import Any

from airflow.providers.google.cloud.hooks.mlengine import MLEngineAsyncHook
from airflow.providers.google.common.hooks.base_google import PROVIDE_PROJECT_ID
from airflow.triggers.base import BaseTrigger, TriggerEvent


class MLEngineStartTrainingJobTrigger(BaseTrigger):
    """
    MLEngineStartTrainingJobTrigger run on the trigger worker to perform starting training job operation.

    :param conn_id: Reference to google cloud connection id
    :param job_id:  The ID of the job. It will be suffixed with hash of job configuration
    :param project_id: Google Cloud Project where the job is running
    :param poll_interval: polling period in seconds to check for the status
    """

    def __init__(
        self,
        conn_id: str,
        job_id: str,
        region: str,
        poll_interval: float = 4.0,
        package_uris: list[str] | None = None,
        training_python_module: str | None = None,
        training_args: list[str] | None = None,
        runtime_version: str | None = None,
        python_version: str | None = None,
        job_dir: str | None = None,
        project_id: str = PROVIDE_PROJECT_ID,
        labels: dict[str, str] | None = None,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
    ):
        super().__init__()
        self.log.info("Using the connection  %s .", conn_id)
        self.conn_id = conn_id
        self.job_id = job_id
        self._job_conn = None
        self.project_id = project_id
        self.region = region
        self.poll_interval = poll_interval
        self.runtime_version = runtime_version
        self.python_version = python_version
        self.job_dir = job_dir
        self.package_uris = package_uris
        self.training_python_module = training_python_module
        self.training_args = training_args
        self.labels = labels
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """Serialize MLEngineStartTrainingJobTrigger arguments and classpath."""
        return (
            "airflow.providers.google.cloud.triggers.mlengine.MLEngineStartTrainingJobTrigger",
            {
                "conn_id": self.conn_id,
                "job_id": self.job_id,
                "poll_interval": self.poll_interval,
                "region": self.region,
                "project_id": self.project_id,
                "runtime_version": self.runtime_version,
                "python_version": self.python_version,
                "job_dir": self.job_dir,
                "package_uris": self.package_uris,
                "training_python_module": self.training_python_module,
                "training_args": self.training_args,
                "labels": self.labels,
            },
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:  # type: ignore[override]
        """Get current job execution status and yields a TriggerEvent."""
        hook = self._get_async_hook()
        try:
            while True:
                # Poll for job execution status
                response_from_hook = await hook.get_job_status(job_id=self.job_id, project_id=self.project_id)
                if response_from_hook == "success":
                    yield TriggerEvent(
                        {
                            "job_id": self.job_id,
                            "status": "success",
                            "message": "Job completed",
                        }
                    )
                    return
                elif response_from_hook == "pending":
                    self.log.info("Job is still running...")
                    self.log.info("Sleeping for %s seconds.", self.poll_interval)
                    await asyncio.sleep(self.poll_interval)
                else:
                    yield TriggerEvent({"status": "error", "message": response_from_hook})
                    return

        except Exception as e:
            self.log.exception("Exception occurred while checking for query completion")
            yield TriggerEvent({"status": "error", "message": str(e)})

    def _get_async_hook(self) -> MLEngineAsyncHook:
        return MLEngineAsyncHook(
            gcp_conn_id=self.conn_id,
            impersonation_chain=self.impersonation_chain,
        )
