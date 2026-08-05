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
from typing import TYPE_CHECKING, Any

from kubernetes.client.exceptions import ApiException

from airflow.providers.ray.constants import TERMINAL_JOB_STATUSES
from airflow.providers.ray.hooks.ray import RayHook
from airflow.triggers.base import BaseTrigger, TriggerEvent

if TYPE_CHECKING:
    from ray.job_submission import JobStatus


class RayJobTrigger(BaseTrigger):
    """
    Triggers and monitors the status of a Ray job.

    This trigger periodically checks the status of a submitted job on a Ray cluster and
    yields events based on the job's status. It handles timeouts and errors during
    the polling process.

    :param job_id: The unique identifier of the Ray job.
    :param conn_id: The connection ID for the Ray cluster.
    :param xcom_dashboard_url: Optional URL for the Ray dashboard.
    :param poll_interval: The interval in seconds at which to poll the job status. Defaults to 30 seconds.
    :param fetch_logs: Whether to fetch and stream logs. Defaults to True.
    """

    def __init__(
        self,
        job_id: str,
        conn_id: str,
        xcom_dashboard_url: str | None,
        ray_cluster_yaml: str | None,
        gpu_device_plugin_yaml: str,
        poll_interval: int = 30,
        fetch_logs: bool = True,
    ):
        super().__init__()  # type: ignore[no-untyped-call]
        self.job_id = job_id
        self.conn_id = conn_id
        self.dashboard_url = xcom_dashboard_url
        self.ray_cluster_yaml = ray_cluster_yaml
        self.gpu_device_plugin_yaml = gpu_device_plugin_yaml
        self.fetch_logs = fetch_logs
        self.poll_interval = poll_interval
        self._job_status: None | JobStatus = None

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """
        Serialize the trigger's configuration.

        :return: A tuple containing the fully qualified class name and a dictionary of its parameters.
        """
        return (
            "airflow.providers.ray.triggers.ray.RayJobTrigger",
            {
                "job_id": self.job_id,
                "conn_id": self.conn_id,
                "xcom_dashboard_url": self.dashboard_url,
                "ray_cluster_yaml": self.ray_cluster_yaml,
                "gpu_device_plugin_yaml": self.gpu_device_plugin_yaml,
                "fetch_logs": self.fetch_logs,
                "poll_interval": self.poll_interval,
            },
        )

    @cached_property
    def hook(self) -> RayHook:
        """
        Lazily initializes and returns a RayHook instance.

        :return: An instance of RayHook configured with the connection ID and dashboard URL.
        """
        return RayHook(conn_id=self.conn_id)

    async def cleanup(self) -> None:
        """
        Cleanup method to ensure resources are properly deleted. This will be called when the trigger encounters an exception.

        Example scenario: A job is submitted using the @ray.task decorator with a Ray specification. After the cluster is started
        and the job is submitted, the trigger begins tracking its progress. However, if the job is stopped through the UI at this stage, the cluster
        resources are not deleted.

        """
        if self.ray_cluster_yaml:
            self.log.info("Attempting to delete Ray cluster using YAML: %s", self.ray_cluster_yaml)
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(
                None, self.hook.delete_ray_cluster, self.ray_cluster_yaml, self.gpu_device_plugin_yaml
            )
            self.log.info("Ray cluster deletion process completed")
        else:
            self.log.info("No Ray cluster YAML provided, skipping cluster deletion")

    async def _poll_status(self) -> None:
        self._job_status = self.hook.get_ray_job_status(self.dashboard_url, self.job_id)
        while self._job_status not in TERMINAL_JOB_STATUSES:
            self.log.info("Status of job %s is: %s", self.job_id, self._job_status)
            await asyncio.sleep(self.poll_interval)
            self._job_status = self.hook.get_ray_job_status(self.dashboard_url, self.job_id)

    async def _stream_logs(self) -> None:
        """Streams logs from the Ray job in real-time."""
        self.log.info("::group::%s logs", self.job_id)
        async for log_lines in self.hook.get_ray_tail_logs(self.dashboard_url, self.job_id):
            for line in log_lines.split("\n"):
                if line.strip():  # Avoid logging empty lines
                    self.log.info(line.strip())
        self.log.info("::endgroup::")

    async def run(self) -> AsyncIterator[TriggerEvent]:
        """
        Asynchronously polls the Ray job status and yields events based on the job's state.

        This method gets job status at each poll interval and streams logs if available.
        It yields a TriggerEvent upon job completion, cancellation, or failure.

        :yield: TriggerEvent containing the status, message, and job ID related to the job.
        """
        self.log.info("::group:: Trigger 1/2: Checking the job status")
        self.log.info("Polling for job %s every %s seconds...", self.job_id, self.poll_interval)

        try:
            tasks = [self._poll_status()]
            if self.fetch_logs:
                tasks.append(self._stream_logs())
            await asyncio.gather(*tasks)
        except ApiException as e:
            error_msg = str(e)
            self.log.info("::endgroup::")
            self.log.error("::group:: Trigger unable to poll job status")
            self.log.exception("Exception details")
            self.log.info("Attempting to clean up...")
            await self.cleanup()
            self.log.info("Cleanup completed!")
            self.log.info("::endgroup::")

            yield TriggerEvent({"status": "EXCEPTION", "message": error_msg, "job_id": self.job_id})  # type: ignore[no-untyped-call]
        else:
            self.log.info("::endgroup::")
            self.log.info("::group:: Trigger 2/2: Job reached a terminal state")
            self.log.info("Status of completed job %s is: %s", self.job_id, self._job_status)
            self.log.info("::endgroup::")

            yield TriggerEvent(  # type: ignore[no-untyped-call]
                {
                    "status": self._job_status,
                    "message": f"Job {self.job_id} completed with status {self._job_status}",
                    "job_id": self.job_id,
                }
            )
