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
"""This module contains Google Vertex AI operators."""

from __future__ import annotations

from collections.abc import Sequence
from functools import cached_property
from typing import TYPE_CHECKING, Any

from google.api_core.exceptions import NotFound
from google.api_core.gapic_v1.method import DEFAULT, _MethodDefault
from google.cloud.aiplatform_v1 import types

from airflow.providers.common.compat.sdk import AirflowException, conf
from airflow.providers.google.cloud.hooks.vertex_ai.pipeline_job import PipelineJobHook
from airflow.providers.google.cloud.links.vertex_ai import (
    VertexAIPipelineJobLink,
    VertexAIPipelineJobListLink,
)
from airflow.providers.google.cloud.operators.cloud_base import GoogleCloudBaseOperator
from airflow.providers.google.cloud.triggers.vertex_ai import RunPipelineJobTrigger

if TYPE_CHECKING:
    from google.api_core.retry import Retry
    from google.cloud.aiplatform import PipelineJob
    from google.cloud.aiplatform.metadata import experiment_resources

    from airflow.providers.common.compat.sdk import Context


class RunPipelineJobOperator(GoogleCloudBaseOperator):
    """
    Create and run a Pipeline job.

    :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
    :param region: Required. The ID of the Google Cloud region that the service belongs to.
    :param display_name: Required. The user-defined name of this Pipeline.
    :param template_path: Required. The path of PipelineJob or PipelineSpec JSON or YAML file. It can be
        a local path, a Google Cloud Storage URI (e.g. "gs://project.name"), an Artifact Registry URI
        (e.g. "https://us-central1-kfp.pkg.dev/proj/repo/pack/latest"), or an HTTPS URI.
    :param job_id: Optional. The unique ID of the job run. If not specified, pipeline name + timestamp
        will be used.
    :param pipeline_root: Optional. The root of the pipeline outputs. If not set, the staging bucket set
        in aiplatform.init will be used. If that's not set a pipeline-specific artifacts bucket will be
        used.
    :param parameter_values: Optional. The mapping from runtime parameter names to its values that
        control the pipeline run.
    :param input_artifacts: Optional. The mapping from the runtime parameter name for this artifact to
        its resource id. For example: "vertex_model":"456". Note: full resource name
        ("projects/123/locations/us-central1/metadataStores/default/artifacts/456") cannot be used.
    :param enable_caching: Optional. Whether to turn on caching for the run.
        If this is not set, defaults to the compile time settings, which are True for all tasks by
        default, while users may specify different caching options for individual tasks.
        If this is set, the setting applies to all tasks in the pipeline. Overrides the compile time
        settings.
    :param encryption_spec_key_name: Optional. The Cloud KMS resource identifier of the customer managed
        encryption key used to protect the job. Has the form:
        ``projects/my-project/locations/my-region/keyRings/my-kr/cryptoKeys/my-key``.
        The key needs to be in the same region as where the compute resource is created. If this is set,
        then all resources created by the PipelineJob will be encrypted with the provided encryption key.
        Overrides encryption_spec_key_name set in aiplatform.init.
    :param labels: Optional. The user defined metadata to organize PipelineJob.
    :param failure_policy: Optional. The failure policy - "slow" or "fast". Currently, the default of a
        pipeline is that the pipeline will continue to run until no more tasks can be executed, also
        known as PIPELINE_FAILURE_POLICY_FAIL_SLOW (corresponds to "slow"). However, if a pipeline is set
        to PIPELINE_FAILURE_POLICY_FAIL_FAST (corresponds to "fast"), it will stop scheduling any new
        tasks when a task has failed. Any scheduled tasks will continue to completion.
    :param service_account: Optional. Specifies the service account for workload run-as account. Users
        submitting jobs must have act-as permission on this run-as account.
    :param network: Optional. The full name of the Compute Engine network to which the job should be
        peered. For example, projects/12345/global/networks/myVPC.
        Private services access must already be configured for the network. If left unspecified, the
        network set in aiplatform.init will be used. Otherwise, the job is not peered with any network.
    :param create_request_timeout: Optional. The timeout for the create request in seconds.
    :param experiment: Optional. The Vertex AI experiment name or instance to associate to this PipelineJob.
        Metrics produced by the PipelineJob as system.Metric Artifacts will be associated as metrics
        to the current Experiment Run. Pipeline parameters will be associated as parameters to
        the current Experiment Run.
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :param deferrable: If True, run the task in the deferrable mode.
    :param poll_interval: Time (seconds) to wait between two consecutive calls to check the job.
        The default is 300 seconds.
    """

    template_fields = [
        "region",
        "project_id",
        "input_artifacts",
        "impersonation_chain",
        "template_path",
        "pipeline_root",
        "parameter_values",
        "service_account",
    ]
    operator_extra_links = (VertexAIPipelineJobLink(),)

    def __init__(
        self,
        *,
        project_id: str,
        region: str,
        display_name: str,
        template_path: str,
        job_id: str | None = None,
        pipeline_root: str | None = None,
        parameter_values: dict[str, Any] | None = None,
        input_artifacts: dict[str, str] | None = None,
        enable_caching: bool | None = None,
        encryption_spec_key_name: str | None = None,
        labels: dict[str, str] | None = None,
        failure_policy: str | None = None,
        service_account: str | None = None,
        network: str | None = None,
        create_request_timeout: float | None = None,
        experiment: str | experiment_resources.Experiment | None = None,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        poll_interval: int = 5 * 60,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.region = region
        self.project_id = project_id
        self.display_name = display_name
        self.template_path = template_path
        self.job_id = job_id
        self.pipeline_root = pipeline_root
        self.parameter_values = parameter_values
        self.input_artifacts = input_artifacts
        self.enable_caching = enable_caching
        self.encryption_spec_key_name = encryption_spec_key_name
        self.labels = labels
        self.failure_policy = failure_policy
        self.service_account = service_account
        self.network = network
        self.create_request_timeout = create_request_timeout
        self.experiment = experiment
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain
        self.deferrable = deferrable
        self.poll_interval = poll_interval

    @property
    def extra_links_params(self) -> dict[str, Any]:
        return {
            "region": self.region,
            "project_id": self.project_id,
        }

    def execute(self, context: Context):
        self.log.info("Running Pipeline job")
        pipeline_job_obj: PipelineJob = self.hook.submit_pipeline_job(
            project_id=self.project_id,
            region=self.region,
            display_name=self.display_name,
            template_path=self.template_path,
            job_id=self.job_id,
            pipeline_root=self.pipeline_root,
            parameter_values=self.parameter_values,
            input_artifacts=self.input_artifacts,
            enable_caching=self.enable_caching,
            encryption_spec_key_name=self.encryption_spec_key_name,
            labels=self.labels,
            failure_policy=self.failure_policy,
            service_account=self.service_account,
            network=self.network,
            create_request_timeout=self.create_request_timeout,
            experiment=self.experiment,
        )
        pipeline_job_id = pipeline_job_obj.job_id
        self.log.info("Pipeline job was created. Job id: %s", pipeline_job_id)
        context["ti"].xcom_push(key="pipeline_job_id", value=pipeline_job_id)
        VertexAIPipelineJobLink.persist(context=context, pipeline_id=pipeline_job_id)

        if self.deferrable:
            pipeline_job_obj.wait_for_resource_creation()
            self.defer(
                trigger=RunPipelineJobTrigger(
                    conn_id=self.gcp_conn_id,
                    project_id=self.project_id,
                    location=pipeline_job_obj.location,
                    job_id=pipeline_job_id,
                    poll_interval=self.poll_interval,
                    impersonation_chain=self.impersonation_chain,
                ),
                method_name="execute_complete",
            )

        pipeline_job_obj.wait()
        pipeline_job = pipeline_job_obj.to_dict()
        return pipeline_job

    def execute_complete(self, context: Context, event: dict[str, Any]) -> dict[str, Any]:
        if event["status"] == "error":
            raise AirflowException(event["message"])
        return event["job"]

    def on_kill(self) -> None:
        """Act as a callback called when the operator is killed; cancel any running job."""
        if self.hook:
            self.hook.cancel_pipeline_job()

    @cached_property
    def hook(self) -> PipelineJobHook:
        return PipelineJobHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )


class GetPipelineJobOperator(GoogleCloudBaseOperator):
    """
    Get a Pipeline job.

    :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
    :param region: Required. The ID of the Google Cloud region that the service belongs to.
    :param pipeline_job_id: Required. The ID of the PipelineJob resource.
    :param retry: Designation of what errors, if any, should be retried.
    :param timeout: The timeout for this request.
    :param metadata: Strings which should be sent along with the request as metadata.
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

    template_fields = [
        "region",
        "pipeline_job_id",
        "project_id",
        "impersonation_chain",
    ]
    operator_extra_links = (VertexAIPipelineJobLink(),)

    def __init__(
        self,
        *,
        project_id: str,
        region: str,
        pipeline_job_id: str,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.region = region
        self.project_id = project_id
        self.pipeline_job_id = pipeline_job_id
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    @property
    def extra_links_params(self) -> dict[str, Any]:
        return {
            "region": self.region,
            "project_id": self.project_id,
        }

    def execute(self, context: Context):
        hook = PipelineJobHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )

        try:
            self.log.info("Get Pipeline job: %s", self.pipeline_job_id)
            result = hook.get_pipeline_job(
                project_id=self.project_id,
                region=self.region,
                pipeline_job_id=self.pipeline_job_id,
                retry=self.retry,
                timeout=self.timeout,
                metadata=self.metadata,
            )
            VertexAIPipelineJobLink.persist(context=context, pipeline_id=self.pipeline_job_id)
            self.log.info("Pipeline job was gotten.")
            return types.PipelineJob.to_dict(result)
        except NotFound:
            self.log.info("The Pipeline job %s does not exist.", self.pipeline_job_id)


class ListPipelineJobOperator(GoogleCloudBaseOperator):
    """
    Lists PipelineJob in a Location.

    :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
    :param region: Required. The ID of the Google Cloud region that the service belongs to.
    :param filter: Optional. Lists the PipelineJobs that match the filter expression. The
        following fields are supported:

        -  ``pipeline_name``: Supports ``=`` and ``!=`` comparisons.
        -  ``display_name``: Supports ``=``, ``!=`` comparisons, and
            ``:`` wildcard.
        -  ``pipeline_job_user_id``: Supports ``=``, ``!=``
            comparisons, and ``:`` wildcard. for example, can check
            if pipeline's display_name contains *step* by doing
            display_name:"*step*"
        -  ``create_time``: Supports ``=``, ``!=``, ``<``, ``>``,
            ``<=``, and ``>=`` comparisons. Values must be in RFC
            3339 format.
        -  ``update_time``: Supports ``=``, ``!=``, ``<``, ``>``,
            ``<=``, and ``>=`` comparisons. Values must be in RFC
            3339 format.
        -  ``end_time``: Supports ``=``, ``!=``, ``<``, ``>``,
            ``<=``, and ``>=`` comparisons. Values must be in RFC
            3339 format.
        -  ``labels``: Supports key-value equality and key presence.

        Filter expressions can be combined together using logical
        operators (``AND`` & ``OR``). For example:
        ``pipeline_name="test" AND create_time>"2020-05-18T13:30:00Z"``.

        The syntax to define filter expression is based on
        https://google.aip.dev/160.
    :param page_size: Optional. The standard list page size.
    :param page_token: Optional. The standard list page token. Typically obtained via
        [ListPipelineJobsResponse.next_page_token][google.cloud.aiplatform.v1.ListPipelineJobsResponse.next_page_token]
        of the previous
        [PipelineService.ListPipelineJobs][google.cloud.aiplatform.v1.PipelineService.ListPipelineJobs]
        call.
    :param order_by: Optional. A comma-separated list of fields to order by. The default
        sort order is in ascending order. Use "desc" after a field
        name for descending. You can have multiple order_by fields
        provided e.g. "create_time desc, end_time", "end_time,
        start_time, update_time" For example, using "create_time
        desc, end_time" will order results by create time in
        descending order, and if there are multiple jobs having the
        same create time, order them by the end time in ascending
        order. if order_by is not specified, it will order by
        default order is create time in descending order. Supported
        fields:

        -  ``create_time``
        -  ``update_time``
        -  ``end_time``
        -  ``start_time``
    :param retry: Designation of what errors, if any, should be retried.
    :param timeout: The timeout for this request.
    :param metadata: Strings which should be sent along with the request as metadata.
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

    template_fields = [
        "region",
        "project_id",
        "impersonation_chain",
    ]
    operator_extra_links = [
        VertexAIPipelineJobListLink(),
    ]

    def __init__(
        self,
        *,
        region: str,
        project_id: str,
        page_size: int | None = None,
        page_token: str | None = None,
        filter: str | None = None,
        order_by: str | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.region = region
        self.project_id = project_id
        self.page_size = page_size
        self.page_token = page_token
        self.filter = filter
        self.order_by = order_by
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    @property
    def extra_links_params(self) -> dict[str, Any]:
        return {
            "region": self.region,
            "project_id": self.project_id,
        }

    def execute(self, context: Context):
        hook = PipelineJobHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )
        results = hook.list_pipeline_jobs(
            region=self.region,
            project_id=self.project_id,
            page_size=self.page_size,
            page_token=self.page_token,
            filter=self.filter,
            order_by=self.order_by,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )
        VertexAIPipelineJobListLink.persist(context=context)
        return [types.PipelineJob.to_dict(result) for result in results]


class DeletePipelineJobOperator(GoogleCloudBaseOperator):
    """
    Delete a Pipeline job.

    :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
    :param region: Required. The ID of the Google Cloud region that the service belongs to.
    :param pipeline_job_id: Required. The ID of the PipelineJob resource to be deleted.
    :param retry: Designation of what errors, if any, should be retried.
    :param timeout: The timeout for this request.
    :param metadata: Strings which should be sent along with the request as metadata.
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

    template_fields = [
        "region",
        "project_id",
        "pipeline_job_id",
        "impersonation_chain",
    ]

    def __init__(
        self,
        *,
        project_id: str,
        region: str,
        pipeline_job_id: str,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.region = region
        self.project_id = project_id
        self.pipeline_job_id = pipeline_job_id
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context):
        hook = PipelineJobHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )
        try:
            self.log.info("Deleting Pipeline job: %s", self.pipeline_job_id)
            operation = hook.delete_pipeline_job(
                region=self.region,
                project_id=self.project_id,
                pipeline_job_id=self.pipeline_job_id,
                retry=self.retry,
                timeout=self.timeout,
                metadata=self.metadata,
            )
            hook.wait_for_operation(timeout=self.timeout, operation=operation)
            self.log.info("Pipeline job was deleted.")
        except NotFound:
            self.log.info("The Pipeline Job ID %s does not exist.", self.pipeline_job_id)
