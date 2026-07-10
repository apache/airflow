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
"""This module contains a Google Cloud Ray Job hook."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any
from urllib.parse import urlparse

from ray.job_submission import JobSubmissionClient

from airflow.providers.google.common.hooks.base_google import GoogleBaseHook

if TYPE_CHECKING:
    from ray.dashboard.modules.job.common import JobStatus
    from ray.dashboard.modules.job.pydantic_models import JobDetails

VERTEX_RAY_DOMAIN = "aiplatform-training.googleusercontent.com"


class RayJobHook(GoogleBaseHook):
    """Hook for Jobs APIs."""

    def _is_vertex_ray_address(self, address: str) -> bool:
        """Return True if address points to Vertex Ray dashboard host."""
        parsed = urlparse(address if "://" in address else f"https://{address}")
        hostname = parsed.hostname
        if not hostname:
            return False
        return hostname.endswith(VERTEX_RAY_DOMAIN)

    def get_client(self, address: str) -> JobSubmissionClient:
        """
        Create a client for submitting and interacting with jobs on a remote cluster.

        :param address: Either (1) the address of the Ray cluster, or (2) the HTTP address
            of the dashboard server on the head node, e.g. "http://<head-node-ip>:8265".
            In case (1) it must be specified as an address that can be passed to
            ray.init(), e.g. a Ray Client address (ray://<head_node_host>:10001),
            or "auto", or "localhost:<port>".
        """
        if self._is_vertex_ray_address(address):
            return JobSubmissionClient(f"vertex_ray://{address}")
        return JobSubmissionClient(address=address)

    def serialize_job_obj(self, job_obj: JobDetails) -> dict:
        """Serialize JobDetails to a plain dict."""
        if hasattr(job_obj, "model_dump"):  # Pydantic v2
            return job_obj.model_dump(exclude_none=True)
        if hasattr(job_obj, "dict"):  # Pydantic v1
            return job_obj.dict(exclude_none=True)
        return dict(job_obj)

    def submit_job(
        self,
        entrypoint: str,
        cluster_address: str,
        runtime_env: dict[str, Any] | None = None,
        metadata: dict[str, str] | None = None,
        submission_id: str | None = None,
        entrypoint_num_cpus: int | float | None = None,
        entrypoint_num_gpus: int | float | None = None,
        entrypoint_memory: int | None = None,
        entrypoint_resources: dict[str, float] | None = None,
    ) -> str:
        """
        Submit and execute Job on Ray cluster.

        When a job is submitted, it runs once to completion or failure. Retries or
        different runs with different parameters should be handled by the
        submitter. Jobs are bound to the lifetime of a Ray cluster, so if the
        cluster goes down, all running jobs on that cluster will be terminated.

        :param entrypoint: Required. The shell command to run for this job.
        :param cluster_address: Required. Either (1) the address of the Ray cluster, or (2) the HTTP address
            of the dashboard server on the head node, e.g. "http://<head-node-ip>:8265".
            In case (1) it must be specified as an address that can be passed to
            ray.init(), e.g. a Ray Client address (ray://<head_node_host>:10001),
            or "auto", or "localhost:<port>".
        :param submission_id: A unique ID for this job.
        :param runtime_env: The runtime environment to install and run this job in.
        :param metadata: Arbitrary data to store along with this job.
        :param entrypoint_num_cpus: The quantity of CPU cores to reserve for the execution
            of the entrypoint command, separately from any tasks or actors launched
            by it. Defaults to 0.
        :param entrypoint_num_gpus: The quantity of GPUs to reserve for the execution
            of the entrypoint command, separately from any tasks or actors launched
            by it. Defaults to 0.
        :param entrypoint_memory: The quantity of memory to reserve for the
            execution of the entrypoint command, separately from any tasks or
            actors launched by it. Defaults to 0.
        :param entrypoint_resources: The quantity of custom resources to reserve for the
            execution of the entrypoint command, separately from any tasks or
            actors launched by it.
        """
        job_id = self.get_client(address=cluster_address).submit_job(
            entrypoint=entrypoint,
            runtime_env=runtime_env,
            metadata=metadata,
            submission_id=submission_id,
            entrypoint_num_cpus=entrypoint_num_cpus,
            entrypoint_num_gpus=entrypoint_num_gpus,
            entrypoint_memory=entrypoint_memory,
            entrypoint_resources=entrypoint_resources,
        )
        return job_id

    def stop_job(
        self,
        job_id: str,
        cluster_address: str,
    ) -> bool:
        """
        Stop Job on Ray cluster.

        :param job_id: Required. The job ID or submission ID for the job to be stopped.
        :param cluster_address: Required. Either (1) the address of the Ray cluster, or (2) the HTTP address
            of the dashboard server on the head node, e.g. "http://<head-node-ip>:8265".
            In case (1) it must be specified as an address that can be passed to
            ray.init(), e.g. a Ray Client address (ray://<head_node_host>:10001),
            or "auto", or "localhost:<port>".
        :return: True if the job was stopped, otherwise False.
        """
        return self.get_client(address=cluster_address).stop_job(job_id=job_id)

    def delete_job(
        self,
        job_id: str,
        cluster_address: str,
    ) -> bool:
        """
        Delete Job on Ray cluster in a terminal state and all of its associated data.

        If the job is not already in a terminal state, raises an error.
        This does not delete the job logs from disk.
        Submitting a job with the same submission ID as a previously
        deleted job is not supported and may lead to unexpected behavior.

        :param job_id: Required. The job ID or submission ID for the job to be deleted.
        :param cluster_address: Required. Either (1) the address of the Ray cluster, or (2) the HTTP address
            of the dashboard server on the head node, e.g. "http://<head-node-ip>:8265".
            In case (1) it must be specified as an address that can be passed to
            ray.init(), e.g. a Ray Client address (ray://<head_node_host>:10001),
            or "auto", or "localhost:<port>".
        :return: True if the job was deleted, otherwise False.
        """
        return self.get_client(address=cluster_address).delete_job(job_id=job_id)

    def get_job_info(
        self,
        job_id: str,
        cluster_address: str,
    ) -> JobDetails:
        """
        Get the latest status and other information associated with a Job on Ray cluster.

        :param job_id: Required. The job ID or submission ID for the job to be retrieved.
        :param cluster_address: Required. Either (1) the address of the Ray cluster, or (2) the HTTP address
            of the dashboard server on the head node, e.g. "http://<head-node-ip>:8265".
            In case (1) it must be specified as an address that can be passed to
            ray.init(), e.g. a Ray Client address (ray://<head_node_host>:10001),
            or "auto", or "localhost:<port>".
        :return: The JobDetails for the job.
        """
        return self.get_client(address=cluster_address).get_job_info(job_id=job_id)

    def list_jobs(
        self,
        cluster_address: str,
    ) -> list[JobDetails]:
        """
        List all jobs along with their status and other information.

        Lists all jobs that have ever run on the cluster, including jobs that are
        currently running and jobs that are no longer running.

        :param cluster_address: Required. Either (1) the address of the Ray cluster, or (2) the HTTP address
            of the dashboard server on the head node, e.g. "http://<head-node-ip>:8265".
            In case (1) it must be specified as an address that can be passed to
            ray.init(), e.g. a Ray Client address (ray://<head_node_host>:10001),
            or "auto", or "localhost:<port>".
        """
        return self.get_client(address=cluster_address).list_jobs()

    def get_job_status(
        self,
        job_id: str,
        cluster_address: str,
    ) -> JobStatus:
        """
        Get the most recent status of a Job on Ray cluster.

        :param job_id: Required. The job ID or submission ID for the job to be retrieved.
        :param cluster_address: Required. Either (1) the address of the Ray cluster, or (2) the HTTP address
            of the dashboard server on the head node, e.g. "http://<head-node-ip>:8265".
            In case (1) it must be specified as an address that can be passed to
            ray.init(), e.g. a Ray Client address (ray://<head_node_host>:10001),
            or "auto", or "localhost:<port>".
        :return: The JobStatus of the job.
        """
        return self.get_client(address=cluster_address).get_job_status(job_id=job_id)

    def get_job_logs(
        self,
        job_id: str,
        cluster_address: str,
    ) -> str:
        """
        Get all logs produced by a Job on Ray cluster.

        :param job_id: Required. The job ID or submission ID for the job to be retrieved.
        :param cluster_address: Required. Either (1) the address of the Ray cluster, or (2) the HTTP address
            of the dashboard server on the head node, e.g. "http://<head-node-ip>:8265".
            In case (1) it must be specified as an address that can be passed to
            ray.init(), e.g. a Ray Client address (ray://<head_node_host>:10001),
            or "auto", or "localhost:<port>".
        :return: A string containing the full logs of the job.
        """
        return self.get_client(address=cluster_address).get_job_logs(job_id=job_id)
