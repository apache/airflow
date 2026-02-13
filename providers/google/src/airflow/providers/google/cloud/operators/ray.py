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
"""This module contains a Google Cloud Ray Job operators."""

from __future__ import annotations

import time
from collections.abc import Sequence
from functools import cached_property
from typing import TYPE_CHECKING, Any

from google.api_core.exceptions import NotFound
from ray.dashboard.modules.job.common import JobStatus

from airflow.providers.common.compat.sdk import AirflowNotFoundException, AirflowTaskTimeout
from airflow.providers.google.cloud.hooks.ray import RayJobHook
from airflow.providers.google.cloud.links.ray import RayJobLink
from airflow.providers.google.cloud.operators.cloud_base import GoogleCloudBaseOperator

if TYPE_CHECKING:
    from airflow.providers.common.compat.sdk import Context


TERMINAL_STATUSES = {JobStatus.SUCCEEDED.value, JobStatus.FAILED.value}


class OperationFailedException(Exception):
    """Custom exception to handle failing operations on Jobs."""

    pass


class RayJobBaseOperator(GoogleCloudBaseOperator):
    """
    Base class for Jobs on Ray operators.

    :param gcp_conn_id: The connection ID to use connecting to Google Cloud.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    template_fields: Sequence[str] = (
        "gcp_conn_id",
        "impersonation_chain",
    )

    def __init__(
        self,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    @cached_property
    def hook(self) -> RayJobHook:
        return RayJobHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )


class RaySubmitJobOperator(RayJobBaseOperator):
    """
    Submit and execute Job on Ray cluster.

    When a job is submitted, it runs once to completion or failure. Retries or
    different runs with different parameters should be handled by the
    submitter. Jobs are bound to the lifetime of a Ray cluster, so if the
    cluster goes down, all running jobs on that cluster will be terminated.

    :param cluster_address: Required. Either (1) the address of the Ray cluster, or (2) the HTTP address
        of the dashboard server on the head node, e.g. "http://<head-node-ip>:8265".
        In case (1) it must be specified as an address that can be passed to
        ray.init(), e.g. a Ray Client address (ray://<head_node_host>:10001),
        or "auto", or "localhost:<port>".
    :param entrypoint: Required. The shell command to run for this job.
    :param get_job_logs: If set to True, the operator will wait until the end of
        Job execution and output the logs.
    :param wait_for_job_done: If set to True, the operator will wait until the end of
        Job execution. Please note, that if the Job will fail during execution and
        this parameter is set to False, there will be no indication of the failure.
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
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    template_fields: Sequence[str] = tuple(
        {"entrypoint", "submission_id", "cluster_address"} | set(RayJobBaseOperator.template_fields)
    )
    operator_extra_links = (RayJobLink(),)

    def __init__(
        self,
        cluster_address: str,
        entrypoint: str,
        get_job_logs: bool | None = False,
        wait_for_job_done: bool | None = False,
        runtime_env: dict[str, Any] | None = None,
        metadata: dict[str, str] | None = None,
        submission_id: str | None = None,
        entrypoint_num_cpus: int | float | None = None,
        entrypoint_num_gpus: int | float | None = None,
        entrypoint_memory: int | None = None,
        entrypoint_resources: dict[str, float] | None = None,
        submit_job_timeout: float = 60 * 30,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.cluster_address = cluster_address
        self.get_job_logs = get_job_logs
        self.wait_for_job_done = wait_for_job_done
        self.entrypoint = entrypoint
        self.runtime_env = runtime_env
        self.metadata = metadata
        self.submission_id = submission_id
        self.entrypoint_num_cpus = entrypoint_num_cpus
        self.entrypoint_num_gpus = entrypoint_num_gpus
        self.entrypoint_memory = entrypoint_memory
        self.entrypoint_resources = entrypoint_resources
        self.submit_job_timeout = submit_job_timeout

    def _check_job_status(
        self, cluster_address: str, job_id: str, timeout: float, polling_interval: float = 5.0
    ) -> str:
        "Check if the Job has reached terminated state."
        start = time.monotonic()

        while True:
            job_status = self.hook.get_job_status(cluster_address=cluster_address, job_id=job_id)
            if job_status in TERMINAL_STATUSES:
                self.log.info("Job has finished execution with status: %s", job_status)
                return job_status

            if time.monotonic() - start > timeout:
                raise AirflowTaskTimeout(
                    f"Timeout waiting for Ray Job {job_id} to finish. Last status: {job_status}"
                )

            self.log.info("Job status: %s...", job_status)
            time.sleep(polling_interval)

    def _get_job_logs(self, cluster_address, job_id):
        "Output Job logs."
        logs = self.hook.get_job_logs(cluster_address=cluster_address, job_id=job_id)
        self.log.info("Got job logs:\n%s\n", logs)

    def execute(self, context: Context):
        if self.get_job_logs and not self.wait_for_job_done:
            raise ValueError(
                "Retrieving Job logs can be possible only after Job completion. "
                "Please, enable wait_for_job_done parameter to be able to get logs."
            )
        try:
            self.log.info("Submitting Job on a Ray cluster...")
            submitted_job_id = self.hook.submit_job(
                cluster_address=self.cluster_address,
                entrypoint=self.entrypoint,
                runtime_env=self.runtime_env,
                metadata=self.metadata,
                submission_id=self.submission_id,
                entrypoint_num_cpus=self.entrypoint_num_cpus,
                entrypoint_num_gpus=self.entrypoint_num_gpus,
                entrypoint_memory=self.entrypoint_memory,
                entrypoint_resources=self.entrypoint_resources,
            )
            self.log.info("Submitted Ray Job id=%s", submitted_job_id)
            RayJobLink.persist(
                context=context,
                cluster_address=self.cluster_address,
                job_id=submitted_job_id,
            )
        except RuntimeError as exc:
            raise exc
        if self.wait_for_job_done:
            self._check_job_status(
                cluster_address=self.cluster_address, job_id=submitted_job_id, timeout=self.submit_job_timeout
            )

        if self.get_job_logs:
            self._get_job_logs(cluster_address=self.cluster_address, job_id=submitted_job_id)

        return submitted_job_id


class RayStopJobOperator(RayJobBaseOperator):
    """
    Stop Job on Ray cluster.

    :param cluster_address: Required. Either (1) the address of the Ray cluster, or (2) the HTTP address
        of the dashboard server on the head node, e.g. "http://<head-node-ip>:8265".
        In case (1) it must be specified as an address that can be passed to
        ray.init(), e.g. a Ray Client address (ray://<head_node_host>:10001),
        or "auto", or "localhost:<port>".
    :param job_id: Required. The job ID or submission ID for the job to be stopped.
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    template_fields: Sequence[str] = tuple(
        {"cluster_address", "job_id"} | set(RayJobBaseOperator.template_fields)
    )
    operator_extra_links = ()

    def __init__(
        self,
        cluster_address: str,
        job_id: str,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.cluster_address = cluster_address
        self.job_id = job_id

    def execute(self, context: Context):
        self.log.info("Stopping Job %s on a Ray cluster...", self.job_id)

        try:
            is_stopped = self.hook.stop_job(
                cluster_address=self.cluster_address,
                job_id=self.job_id,
            )
        except NotFound:
            raise AirflowNotFoundException("Job with specified id was not found on cluster.")

        if is_stopped:
            self.log.info("Job was successfully stopped.")
            return

        raise OperationFailedException("Some error happened during stopping the Job. Exiting.")


class RayDeleteJobOperator(RayJobBaseOperator):
    """
    Delete Job on Ray cluster in a terminal state and all of its associated data.

    If the job is not already in a terminal state, raises an error.
    This does not delete the job logs from disk.
    Submitting a job with the same submission ID as a previously
    deleted job is not supported and may lead to unexpected behavior.

    :param cluster_address: Required. Either (1) the address of the Ray cluster, or (2) the HTTP address
        of the dashboard server on the head node, e.g. "http://<head-node-ip>:8265".
        In case (1) it must be specified as an address that can be passed to
        ray.init(), e.g. a Ray Client address (ray://<head_node_host>:10001),
        or "auto", or "localhost:<port>".
    :param job_id: Required. The job ID or submission ID for the job to be stopped.
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    template_fields: Sequence[str] = tuple(
        {"cluster_address", "job_id"} | set(RayJobBaseOperator.template_fields)
    )
    operator_extra_links = ()

    def __init__(
        self,
        cluster_address: str,
        job_id: str,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.cluster_address = cluster_address
        self.job_id = job_id

    def execute(self, context: Context):
        self.log.info("Deleting Job %s on a Ray cluster...", self.job_id)

        try:
            is_deleted = self.hook.delete_job(
                cluster_address=self.cluster_address,
                job_id=self.job_id,
            )
        except NotFound:
            raise AirflowNotFoundException("Job with specified id was not found on cluster.")

        if is_deleted:
            self.log.info("Job was successfully deleted.")
            return

        raise OperationFailedException("Some error happened during deleting the Job. Exiting.")


class RayGetJobInfoOperator(RayJobBaseOperator):
    """
    Get the latest status and other information associated with a Job on Ray cluster.

    :param cluster_address: Required. Either (1) the address of the Ray cluster, or (2) the HTTP address
        of the dashboard server on the head node, e.g. "http://<head-node-ip>:8265".
        In case (1) it must be specified as an address that can be passed to
        ray.init(), e.g. a Ray Client address (ray://<head_node_host>:10001),
        or "auto", or "localhost:<port>".
    :param job_id: Required. The job ID or submission ID for the job to be stopped.
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    template_fields: Sequence[str] = tuple(
        {"cluster_address", "job_id"} | set(RayJobBaseOperator.template_fields)
    )
    operator_extra_links = ()

    def __init__(
        self,
        cluster_address: str,
        job_id: str,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.cluster_address = cluster_address
        self.job_id = job_id

    def execute(self, context: Context):
        self.log.info("Retrieving information about Job %s on a Ray cluster...", self.job_id)

        try:
            job_info = self.hook.get_job_info(
                cluster_address=self.cluster_address,
                job_id=self.job_id,
            )
        except NotFound:
            raise AirflowNotFoundException("Job with specified id was not found on cluster.")

        # Everything below is outside the try/except so serialization/logging errors are not masked
        self.log.info("Job information:\n %s \n", job_info)

        ray_job_dict = self.hook.serialize_job_obj(job_info)
        return ray_job_dict


class RayListJobsOperator(RayJobBaseOperator):
    """
    List all jobs along with their status and other information.

    Lists all jobs that have ever run on the cluster, including jobs that are
    currently running and jobs that are no longer running.

    :param cluster_address: Required. Either (1) the address of the Ray cluster, or (2) the HTTP address
        of the dashboard server on the head node, e.g. "http://<head-node-ip>:8265".
        In case (1) it must be specified as an address that can be passed to
        ray.init(), e.g. a Ray Client address (ray://<head_node_host>:10001),
        or "auto", or "localhost:<port>".
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    template_fields: Sequence[str] = tuple({"cluster_address"} | set(RayJobBaseOperator.template_fields))
    operator_extra_links = ()

    def __init__(
        self,
        cluster_address: str,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.cluster_address = cluster_address

    def execute(self, context: Context):
        self.log.info("Listing Jobs on a Ray cluster...")
        jobs = self.hook.list_jobs(
            cluster_address=self.cluster_address,
        )
        if jobs:
            self.log.info("Outputting first 10 Jobs...")
            for job in jobs[:10]:
                self.log.info("Job:\n %s \n \n", job)
            list_jobs = [self.hook.serialize_job_obj(job) for job in jobs]
            return list_jobs
        self.log.info("No Jobs found.")
        return
