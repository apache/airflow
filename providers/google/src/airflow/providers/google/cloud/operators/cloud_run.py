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
from typing import TYPE_CHECKING, Any

import google.cloud.exceptions
from google.api_core.exceptions import AlreadyExists
from google.cloud.run_v2 import Job, Service

from airflow.configuration import conf
from airflow.exceptions import AirflowException
from airflow.providers.google.cloud.hooks.cloud_run import CloudRunHook, CloudRunServiceHook
from airflow.providers.google.cloud.links.cloud_run import CloudRunJobLoggingLink
from airflow.providers.google.cloud.operators.cloud_base import GoogleCloudBaseOperator
from airflow.providers.google.cloud.triggers.cloud_run import CloudRunJobFinishedTrigger, RunJobStatus

if TYPE_CHECKING:
    from google.api_core import operation
    from google.cloud.run_v2.types import Execution

    from airflow.providers.common.compat.sdk import Context


class CloudRunCreateJobOperator(GoogleCloudBaseOperator):
    """
    Creates a job without executing it. Pushes the created job to xcom.

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

    template_fields = ("project_id", "region", "gcp_conn_id", "impersonation_chain", "job_name")

    def __init__(
        self,
        project_id: str,
        region: str,
        job_name: str,
        job: dict | Job,
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
    Updates a job and wait for the operation to be completed. Pushes the updated job to xcom.

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

    template_fields = ("project_id", "region", "gcp_conn_id", "impersonation_chain", "job_name")

    def __init__(
        self,
        project_id: str,
        region: str,
        job_name: str,
        job: dict | Job,
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
    Deletes a job and wait for the operation to be completed. Pushes the deleted job to xcom.

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

    template_fields = ("project_id", "region", "gcp_conn_id", "impersonation_chain", "job_name")

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
    Lists jobs.

    :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
    :param region: Required. The ID of the Google Cloud region that the service belongs to.
    :param show_deleted: If true, returns deleted (but unexpired)
        resources along with active ones.
    :param limit: The number of jobs to list. If left empty,
        all the jobs will be returned.
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
        show_deleted: bool = False,
        limit: int | None = None,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
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
    Executes a job and waits for the operation to be completed. Pushes the executed job to xcom.

    :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
    :param region: Required. The ID of the Google Cloud region that the service belongs to.
    :param job_name: Required. The name of the job to execute.
    :param overrides: Optional map of override values.
    :param gcp_conn_id: The connection ID used to connect to Google Cloud.
    :param polling_period_seconds: Optional. Control the rate of the poll for the result of deferrable run.
        By default, the trigger will poll every 10 seconds.
    :param timeout_seconds: Optional. The timeout for this request, in seconds.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :param deferrable: Run the operator in deferrable mode.
    """

    operator_extra_links = (CloudRunJobLoggingLink(),)
    template_fields = (
        "project_id",
        "region",
        "gcp_conn_id",
        "impersonation_chain",
        "job_name",
        "overrides",
        "polling_period_seconds",
        "timeout_seconds",
    )

    def __init__(
        self,
        project_id: str,
        region: str,
        job_name: str,
        overrides: dict[str, Any] | None = None,
        polling_period_seconds: float = 10,
        timeout_seconds: float | None = None,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.project_id = project_id
        self.region = region
        self.job_name = job_name
        self.overrides = overrides
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
            region=self.region, project_id=self.project_id, job_name=self.job_name, overrides=self.overrides
        )

        if self.operation is None:
            raise AirflowException("Operation is None")

        if self.operation.metadata.log_uri:
            CloudRunJobLoggingLink.persist(
                context=context,
                log_uri=self.operation.metadata.log_uri,
            )

        if not self.deferrable:
            result: Execution = self._wait_for_operation(self.operation)
            self._fail_if_execution_failed(result)
            job = hook.get_job(job_name=result.job, region=self.region, project_id=self.project_id)
            return Job.to_dict(job)
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
        status = event["status"]

        if status == RunJobStatus.TIMEOUT.value:
            raise AirflowException("Operation timed out")

        if status == RunJobStatus.FAIL.value:
            error_code = event["operation_error_code"]
            error_message = event["operation_error_message"]
            raise AirflowException(
                f"Operation failed with error code [{error_code}] and error message [{error_message}]"
            )

        hook: CloudRunHook = CloudRunHook(self.gcp_conn_id, self.impersonation_chain)

        job = hook.get_job(job_name=event["job_name"], region=self.region, project_id=self.project_id)
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


class CloudRunCreateServiceOperator(GoogleCloudBaseOperator):
    """
    Creates a Service without executing it. Pushes the created service to xcom.

    :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
    :param region: Required. The ID of the Google Cloud region that the service belongs to.
    :param service_name: Required. The name of the service to create.
    :param service: The service descriptor containing the configuration of the service to submit.
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

    template_fields = ("project_id", "region", "gcp_conn_id", "impersonation_chain", "service_name")

    def __init__(
        self,
        project_id: str,
        region: str,
        service_name: str,
        service: dict | Service,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.project_id = project_id
        self.region = region
        self.service = service
        self.service_name = service_name
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain
        self._validate_inputs()

    def _validate_inputs(self):
        missing_fields = [k for k in ["project_id", "region", "service_name"] if not getattr(self, k)]
        if not self.project_id or not self.region or not self.service_name:
            raise AirflowException(
                f"Required parameters are missing: {missing_fields}. These parameters be passed either as "
                "keyword parameter or as extra field in Airflow connection definition. Both are not set!"
            )

    def execute(self, context: Context):
        hook: CloudRunServiceHook = CloudRunServiceHook(
            gcp_conn_id=self.gcp_conn_id, impersonation_chain=self.impersonation_chain
        )

        try:
            service = hook.create_service(
                service=self.service,
                service_name=self.service_name,
                region=self.region,
                project_id=self.project_id,
            )
        except AlreadyExists:
            self.log.info(
                "Already existed Cloud run service, service_name=%s, region=%s",
                self.service_name,
                self.region,
            )
            service = hook.get_service(
                service_name=self.service_name, region=self.region, project_id=self.project_id
            )
            return Service.to_dict(service)
        except google.cloud.exceptions.GoogleCloudError as e:
            self.log.error("An error occurred. Exiting.")
            raise e

        return Service.to_dict(service)


class CloudRunDeleteServiceOperator(GoogleCloudBaseOperator):
    """
    Deletes a Service without executing it. Pushes the deleted service to xcom.

    :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
    :param region: Required. The ID of the Google Cloud region that the service belongs to.
    :param service_name: Required. The name of the service to create.
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

    template_fields = ("project_id", "region", "gcp_conn_id", "impersonation_chain", "service_name")

    def __init__(
        self,
        project_id: str,
        region: str,
        service_name: str,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.project_id = project_id
        self.region = region
        self.service_name = service_name
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain
        self._validate_inputs()

    def _validate_inputs(self):
        missing_fields = [k for k in ["project_id", "region", "service_name"] if not getattr(self, k)]
        if not self.project_id or not self.region or not self.service_name:
            raise AirflowException(
                f"Required parameters are missing: {missing_fields}. These parameters be passed either as "
                "keyword parameter or as extra field in Airflow connection definition. Both are not set!"
            )

    def execute(self, context: Context):
        hook: CloudRunServiceHook = CloudRunServiceHook(
            gcp_conn_id=self.gcp_conn_id, impersonation_chain=self.impersonation_chain
        )

        try:
            service = hook.delete_service(
                service_name=self.service_name,
                region=self.region,
                project_id=self.project_id,
            )
        except google.cloud.exceptions.NotFound as e:
            self.log.error("An error occurred. Not Found.")
            raise e

        return Service.to_dict(service)
