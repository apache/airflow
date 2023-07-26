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

from typing import TYPE_CHECKING, Sequence

from google.api_core import operation
from google.cloud.run_v2 import Job
from google.cloud.run_v2.types import Execution

from airflow.configuration import conf
from airflow.exceptions import AirflowException
from airflow.providers.google.cloud.hooks.cloud_run import CloudRunHook
from airflow.providers.google.cloud.operators.cloud_base import GoogleCloudBaseOperator
from airflow.providers.google.cloud.triggers.cloud_run import CloudRunJobFinishedTrigger

if TYPE_CHECKING:
    from airflow.utils.context import Context


class CloudRunCreateJobOperator(GoogleCloudBaseOperator):
    """
    Create a job without executing it.

    :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
    :param region: Required. The ID of the Google Cloud region that the service belongs to.
    :param job_name: Required. The name of the job to create.
    :param job: Required. The job descriptor containing the configuration of the job to submit.
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

    def __init__(
        self,
        project_id: str,
        region: str,
        job_name: str,
        job: Job,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.project_id = project_id
        self.region = region
        self.job_name = job_name
        self.job = job
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context):

        hook: CloudRunHook = CloudRunHook(
            gcp_conn_id=self.gcp_conn_id, impersonation_chain=self.impersonation_chain
        )
        job = hook.create_job(
            job_name=self.job_name, job=self.job, region=self.region, project_id=self.project_id
        )

        return Job.to_dict(job)


class CloudRunUpdateJobOperator(GoogleCloudBaseOperator):
    """
    Update a job and wait for the operation to be completed.

    :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
    :param region: Required. The ID of the Google Cloud region that the service belongs to.
    :param job_name: Required. The name of the job to update.
    :param job: Required. The job descriptor containing the new configuration of the job to update.
        The name field will be replaced by job_name
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

    def __init__(
        self,
        project_id: str,
        region: str,
        job_name: str,
        job: Job,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.project_id = project_id
        self.region = region
        self.job_name = job_name
        self.job = job
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context):

        hook: CloudRunHook = CloudRunHook(
            gcp_conn_id=self.gcp_conn_id, impersonation_chain=self.impersonation_chain
        )
        job = hook.update_job(
            job_name=self.job_name, job=self.job, region=self.region, project_id=self.project_id
        )

        return Job.to_dict(job)


class CloudRunDeleteJobOperator(GoogleCloudBaseOperator):
    """
    Delete a job and wait for the the operation to be completed.

    :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
    :param region: Required. The ID of the Google Cloud region that the service belongs to.
    :param job_name: Required. The name of the job to delete.
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

    def __init__(
        self,
        project_id: str,
        region: str,
        job_name: str,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.project_id = project_id
        self.region = region
        self.job_name = job_name
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context):

        hook: CloudRunHook = CloudRunHook(
            gcp_conn_id=self.gcp_conn_id, impersonation_chain=self.impersonation_chain
        )
        job = hook.delete_job(job_name=self.job_name, region=self.region, project_id=self.project_id)

        return Job.to_dict(job)


class CloudRunListJobsOperator(GoogleCloudBaseOperator):
    """
    List jobs.

    :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
    :param region: Required. The ID of the Google Cloud region that the service belongs to.
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

    def __init__(
        self,
        project_id: str,
        region: str,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        show_deleted: bool = False,
        limit: int | None = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.project_id = project_id
        self.region = region
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain
        self.show_deleted = show_deleted
        self.limit = limit
        if limit is not None and limit < 0:
            raise AirflowException("The limit for the list jobs request should be greater or equal to zero")

    def execute(self, context: Context):
        hook: CloudRunHook = CloudRunHook(
            gcp_conn_id=self.gcp_conn_id, impersonation_chain=self.impersonation_chain
        )
        jobs = hook.list_jobs(
            region=self.region, project_id=self.project_id, show_deleted=self.show_deleted, limit=self.limit
        )

        return [Job.to_dict(job) for job in jobs]


class CloudRunExecuteJobOperator(GoogleCloudBaseOperator):
    """
    Execute a job and wait for the operation to be completed.

    :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
    :param region: Required. The ID of the Google Cloud region that the service belongs to.
    :param job_name: Required. The name of the job to update.
    :param job: Required. The job descriptor containing the new configuration of the job to update.
        The name field will be replaced by job_name
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

    def __init__(
        self,
        project_id: str,
        region: str,
        job_name: str,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        polling_period_seconds: float = 10,
        timeout_seconds: float | None = None,
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.project_id = project_id
        self.region = region
        self.job_name = job_name
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain
        self.polling_period_seconds = polling_period_seconds
        self.timeout_seconds = timeout_seconds
        self.deferrable = deferrable
        self.operation: operation.Operation | None = None

    def execute(self, context: Context):
        hook: CloudRunHook = CloudRunHook(
            gcp_conn_id=self.gcp_conn_id, impersonation_chain=self.impersonation_chain
        )
        self.operation = hook.execute_job(
            region=self.region, project_id=self.project_id, job_name=self.job_name
        )

        if not self.deferrable:
            result: Execution = self._wait_for_operation(self.operation)
            self._fail_if_execution_failed(result)
        else:
            self.defer(
                trigger=CloudRunJobFinishedTrigger(
                    operation_name=self.operation.operation.name,
                    job_name=self.job_name,
                    project_id=self.project_id,
                    location=self.region,
                    gcp_conn_id=self.gcp_conn_id,
                    impersonation_chain=self.impersonation_chain,
                    polling_period_seconds=self.polling_period_seconds,
                ),
                method_name="execute_complete",
            )

    def execute_complete(self, context: Context, event: dict):
        done = event["operation_done"]
        error_code = event["operation_error_code"]
        error_message = event["operation_error_message"]

        if not done:
            raise AirflowException(f"Operation timed out: {error_message}")

        if (error_message is not None) and (error_message != "") and (error_code != 0):
            raise AirflowException(
                f"Operation failed with error code [{error_code}] and error message [{error_message}]"
            )

        hook: CloudRunHook = CloudRunHook(self.gcp_conn_id, self.impersonation_chain)

        job = hook.get_job(job_name=event["job_name"], region=self.region)
        return Job.to_dict(job)

    def _fail_if_execution_failed(self, execution: Execution):
        task_count = execution.task_count
        succeeded_count = execution.succeeded_count
        failed_count = execution.failed_count

        if succeeded_count + failed_count != task_count:
            raise AirflowException("Not all tasks finished execution")

        if failed_count > 0:
            raise AirflowException("Some tasks failed execution")

    def _wait_for_operation(self, operation: operation.Operation):
        try:
            return operation.result(timeout=self.timeout_seconds)
        except Exception:
            error = operation.exception(timeout=self.timeout_seconds)
            raise AirflowException(error)
