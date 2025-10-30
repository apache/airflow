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
from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING

from google.cloud.batch_v1 import Job, Task

from airflow.configuration import conf
from airflow.exceptions import AirflowException
from airflow.providers.google.cloud.hooks.cloud_batch import CloudBatchHook
from airflow.providers.google.cloud.operators.cloud_base import GoogleCloudBaseOperator
from airflow.providers.google.cloud.triggers.cloud_batch import CloudBatchJobFinishedTrigger

if TYPE_CHECKING:
    from google.api_core import operation

    from airflow.providers.common.compat.sdk import Context


class CloudBatchSubmitJobOperator(GoogleCloudBaseOperator):
    """
    Submit a job and wait for its completion.

    :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
    :param region: Required. The ID of the Google Cloud region that the service belongs to.
    :param job_name: Required. The name of the job to create.
    :param job: Required. The job descriptor containing the configuration of the job to submit.
    :param polling_period_seconds: Optional: Control the rate of the poll for the result of deferrable run.
        By default, the trigger will poll every 10 seconds.
    :param timeout: The timeout for this request.
    :param gcp_conn_id: The connection ID used to connect to Google Cloud.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :param deferrable: Run operator in the deferrable mode

    """

    template_fields = ("project_id", "region", "gcp_conn_id", "impersonation_chain", "job_name")

    def __init__(
        self,
        project_id: str,
        region: str,
        job_name: str,
        job: dict | Job,
        polling_period_seconds: float = 10,
        timeout_seconds: float | None = None,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.project_id = project_id
        self.region = region
        self.job_name = job_name
        self.job = job
        self.polling_period_seconds = polling_period_seconds
        self.timeout_seconds = timeout_seconds
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain
        self.deferrable = deferrable
        self.polling_period_seconds = polling_period_seconds

    def execute(self, context: Context):
        hook: CloudBatchHook = CloudBatchHook(self.gcp_conn_id, self.impersonation_chain)
        job = hook.submit_batch_job(
            job_name=self.job_name, job=self.job, region=self.region, project_id=self.project_id
        )

        if not self.deferrable:
            completed_job = hook.wait_for_job(
                job_name=job.name,
                polling_period_seconds=self.polling_period_seconds,
                timeout=self.timeout_seconds,
            )

            return Job.to_dict(completed_job)

        self.defer(
            trigger=CloudBatchJobFinishedTrigger(
                job_name=job.name,
                project_id=self.project_id,
                gcp_conn_id=self.gcp_conn_id,
                impersonation_chain=self.impersonation_chain,
                location=self.region,
                polling_period_seconds=self.polling_period_seconds,
                timeout=self.timeout_seconds,
            ),
            method_name="execute_complete",
        )

    def execute_complete(self, context: Context, event: dict):
        job_status = event["status"]
        if job_status == "success":
            hook: CloudBatchHook = CloudBatchHook(self.gcp_conn_id, self.impersonation_chain)
            job = hook.get_job(job_name=event["job_name"])
            return Job.to_dict(job)
        raise AirflowException(f"Unexpected error in the operation: {event['message']}")


class CloudBatchDeleteJobOperator(GoogleCloudBaseOperator):
    """
    Deletes a job and wait for the operation to be completed.

    :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
    :param region: Required. The ID of the Google Cloud region that the service belongs to.
    :param job_name: Required. The name of the job to be deleted.
    :param timeout: The timeout for this request.
    :param gcp_conn_id: The connection ID used to connect to Google Cloud.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).

    """

    template_fields = ("project_id", "region", "gcp_conn_id", "impersonation_chain", "job_name")

    def __init__(
        self,
        project_id: str,
        region: str,
        job_name: str,
        timeout: float | None = None,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.project_id = project_id
        self.region = region
        self.job_name = job_name
        self.timeout = timeout
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context):
        hook: CloudBatchHook = CloudBatchHook(self.gcp_conn_id, self.impersonation_chain)

        operation = hook.delete_job(job_name=self.job_name, region=self.region, project_id=self.project_id)

        self._wait_for_operation(operation)

    def _wait_for_operation(self, operation: operation.Operation):
        try:
            return operation.result(timeout=self.timeout)
        except Exception:
            error = operation.exception(timeout=self.timeout)
            raise AirflowException(error)


class CloudBatchListJobsOperator(GoogleCloudBaseOperator):
    """
    List Cloud Batch jobs.

    :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
    :param region: Required. The ID of the Google Cloud region that the service belongs to.
    :param gcp_conn_id: The connection ID used to connect to Google Cloud.
    :param filter: The filter based on which to list the jobs. If left empty, all the jobs are listed.
    :param limit: The number of jobs to list. If left empty,
        all the jobs matching the filter will be returned.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).

    """

    template_fields = (
        "project_id",
        "region",
        "gcp_conn_id",
        "impersonation_chain",
    )

    def __init__(
        self,
        project_id: str,
        region: str,
        gcp_conn_id: str = "google_cloud_default",
        filter: str | None = None,
        limit: int | None = None,
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.project_id = project_id
        self.region = region
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain
        self.filter = filter
        self.limit = limit
        if limit is not None and limit < 0:
            raise AirflowException("The limit for the list jobs request should be greater or equal to zero")

    def execute(self, context: Context):
        hook: CloudBatchHook = CloudBatchHook(self.gcp_conn_id, self.impersonation_chain)

        jobs_list = hook.list_jobs(
            region=self.region, project_id=self.project_id, filter=self.filter, limit=self.limit
        )

        return [Job.to_dict(job) for job in jobs_list]


class CloudBatchListTasksOperator(GoogleCloudBaseOperator):
    """
    List Cloud Batch tasks for a given job.

    :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
    :param region: Required. The ID of the Google Cloud region that the service belongs to.
    :param job_name: Required. The name of the job for which to list tasks.
    :param gcp_conn_id: The connection ID used to connect to Google Cloud.
    :param filter: The filter based on which to list the jobs. If left empty, all the jobs are listed.
    :param group_name: The name of the group that owns the task. By default, it's `group0`.
    :param limit: The number of tasks to list.
        If left empty, all the tasks matching the filter will be returned.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).

    """

    template_fields = ("project_id", "region", "job_name", "gcp_conn_id", "impersonation_chain", "group_name")

    def __init__(
        self,
        project_id: str,
        region: str,
        job_name: str,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        group_name: str = "group0",
        filter: str | None = None,
        limit: int | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.project_id = project_id
        self.region = region
        self.job_name = job_name
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain
        self.group_name = group_name
        self.filter = filter
        self.limit = limit
        if limit is not None and limit < 0:
            raise AirflowException("The limit for the list jobs request should be greater or equal to zero")

    def execute(self, context: Context):
        hook: CloudBatchHook = CloudBatchHook(self.gcp_conn_id, self.impersonation_chain)

        tasks_list = hook.list_tasks(
            region=self.region,
            project_id=self.project_id,
            job_name=self.job_name,
            group_name=self.group_name,
            filter=self.filter,
            limit=self.limit,
        )

        return [Task.to_dict(task) for task in tasks_list]
