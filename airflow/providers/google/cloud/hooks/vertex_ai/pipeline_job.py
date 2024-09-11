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
"""
This module contains a Google Cloud Vertex AI hook.

.. spelling:word-list::

    aiplatform
"""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any, Sequence

from google.api_core.client_options import ClientOptions
from google.api_core.gapic_v1.method import DEFAULT, _MethodDefault
from google.cloud.aiplatform import PipelineJob
from google.cloud.aiplatform_v1 import (
    PipelineServiceAsyncClient,
    PipelineServiceClient,
    PipelineState,
    types,
)

from airflow.exceptions import AirflowException
from airflow.providers.google.common.consts import CLIENT_INFO
from airflow.providers.google.common.hooks.base_google import GoogleBaseAsyncHook, GoogleBaseHook

if TYPE_CHECKING:
    from google.api_core.operation import Operation
    from google.api_core.retry import AsyncRetry, Retry
    from google.auth.credentials import Credentials
    from google.cloud.aiplatform.metadata import experiment_resources
    from google.cloud.aiplatform_v1.services.pipeline_service.pagers import ListPipelineJobsPager


class PipelineJobHook(GoogleBaseHook):
    """Hook for Google Cloud Vertex AI Pipeline Job APIs."""

    def __init__(
        self,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(
            gcp_conn_id=gcp_conn_id,
            impersonation_chain=impersonation_chain,
        )
        self._pipeline_job: PipelineJob | None = None

    def get_pipeline_service_client(
        self,
        region: str | None = None,
    ) -> PipelineServiceClient:
        """Return PipelineServiceClient object."""
        if region and region != "global":
            client_options = ClientOptions(api_endpoint=f"{region}-aiplatform.googleapis.com:443")
        else:
            client_options = ClientOptions()
        return PipelineServiceClient(
            credentials=self.get_credentials(), client_info=CLIENT_INFO, client_options=client_options
        )

    def get_pipeline_job_object(
        self,
        display_name: str,
        template_path: str,
        job_id: str | None = None,
        pipeline_root: str | None = None,
        parameter_values: dict[str, Any] | None = None,
        input_artifacts: dict[str, str] | None = None,
        enable_caching: bool | None = None,
        encryption_spec_key_name: str | None = None,
        labels: dict[str, str] | None = None,
        project: str | None = None,
        location: str | None = None,
        failure_policy: str | None = None,
    ) -> PipelineJob:
        """Return PipelineJob object."""
        return PipelineJob(
            display_name=display_name,
            template_path=template_path,
            job_id=job_id,
            pipeline_root=pipeline_root,
            parameter_values=parameter_values,
            input_artifacts=input_artifacts,
            enable_caching=enable_caching,
            encryption_spec_key_name=encryption_spec_key_name,
            labels=labels,
            credentials=self.get_credentials(),
            project=project,
            location=location,
            failure_policy=failure_policy,
        )

    def wait_for_operation(self, operation: Operation, timeout: float | None = None):
        """Wait for long-lasting operation to complete."""
        try:
            return operation.result(timeout=timeout)
        except Exception:
            error = operation.exception(timeout=timeout)
            raise AirflowException(error)

    def cancel_pipeline_job(self) -> None:
        """Cancel PipelineJob."""
        if self._pipeline_job:
            self._pipeline_job.cancel()

    @GoogleBaseHook.fallback_to_default_project_id
    def create_pipeline_job(
        self,
        project_id: str,
        region: str,
        pipeline_job: PipelineJob,
        pipeline_job_id: str,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> types.PipelineJob:
        """
        Create a PipelineJob. A PipelineJob will run immediately when created.

        :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
        :param region: Required. The ID of the Google Cloud region that the service belongs to.
        :param pipeline_job:  Required. The PipelineJob to create.
        :param pipeline_job_id:  The ID to use for the PipelineJob, which will become the final component of
            the PipelineJob name. If not provided, an ID will be automatically generated.

            This value should be less than 128 characters, and valid characters are /[a-z][0-9]-/.
        :param retry: Designation of what errors, if any, should be retried.
        :param timeout: The timeout for this request.
        :param metadata: Strings which should be sent along with the request as metadata.
        """
        client = self.get_pipeline_service_client(region)
        parent = client.common_location_path(project_id, region)

        result = client.create_pipeline_job(
            request={
                "parent": parent,
                "pipeline_job": pipeline_job,
                "pipeline_job_id": pipeline_job_id,
            },
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return result

    @GoogleBaseHook.fallback_to_default_project_id
    def run_pipeline_job(
        self,
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
        # START: run param
        service_account: str | None = None,
        network: str | None = None,
        create_request_timeout: float | None = None,
        experiment: str | experiment_resources.Experiment | None = None,
        # END: run param
    ) -> PipelineJob:
        """
        Create and run a PipelineJob until its completion.

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
        :param experiment: Optional. The Vertex AI experiment name or instance to associate to this
            PipelineJob. Metrics produced by the PipelineJob as system.Metric Artifacts will be associated as
            metrics to the current Experiment Run. Pipeline parameters will be associated as parameters to
            the current Experiment Run.
        """
        self._pipeline_job = self.get_pipeline_job_object(
            display_name=display_name,
            template_path=template_path,
            job_id=job_id,
            pipeline_root=pipeline_root,
            parameter_values=parameter_values,
            input_artifacts=input_artifacts,
            enable_caching=enable_caching,
            encryption_spec_key_name=encryption_spec_key_name,
            labels=labels,
            project=project_id,
            location=region,
            failure_policy=failure_policy,
        )
        self._pipeline_job.submit(
            service_account=service_account,
            network=network,
            create_request_timeout=create_request_timeout,
            experiment=experiment,
        )
        self._pipeline_job.wait()

        return self._pipeline_job

    @GoogleBaseHook.fallback_to_default_project_id
    def submit_pipeline_job(
        self,
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
        # START: run param
        service_account: str | None = None,
        network: str | None = None,
        create_request_timeout: float | None = None,
        experiment: str | experiment_resources.Experiment | None = None,
        # END: run param
    ) -> PipelineJob:
        """
        Create and start a PipelineJob run.

        For more info about the client method please see:
        https://cloud.google.com/python/docs/reference/aiplatform/latest/google.cloud.aiplatform.PipelineJob#google_cloud_aiplatform_PipelineJob_submit

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
        """
        self._pipeline_job = self.get_pipeline_job_object(
            display_name=display_name,
            template_path=template_path,
            job_id=job_id,
            pipeline_root=pipeline_root,
            parameter_values=parameter_values,
            input_artifacts=input_artifacts,
            enable_caching=enable_caching,
            encryption_spec_key_name=encryption_spec_key_name,
            labels=labels,
            project=project_id,
            location=region,
            failure_policy=failure_policy,
        )
        self._pipeline_job.submit(
            service_account=service_account,
            network=network,
            create_request_timeout=create_request_timeout,
            experiment=experiment,
        )

        return self._pipeline_job

    @GoogleBaseHook.fallback_to_default_project_id
    def get_pipeline_job(
        self,
        project_id: str,
        region: str,
        pipeline_job_id: str,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> types.PipelineJob:
        """
        Get a PipelineJob.

        :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
        :param region: Required. The ID of the Google Cloud region that the service belongs to.
        :param pipeline_job_id: Required. The ID of the PipelineJob resource.
        :param retry: Designation of what errors, if any, should be retried.
        :param timeout: The timeout for this request.
        :param metadata: Strings which should be sent along with the request as metadata.
        """
        client = self.get_pipeline_service_client(region)
        name = client.pipeline_job_path(project_id, region, pipeline_job_id)

        result = client.get_pipeline_job(
            request={
                "name": name,
            },
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return result

    @GoogleBaseHook.fallback_to_default_project_id
    def list_pipeline_jobs(
        self,
        project_id: str,
        region: str,
        page_size: int | None = None,
        page_token: str | None = None,
        filter: str | None = None,
        order_by: str | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> ListPipelineJobsPager:
        """
        List PipelineJobs in a Location.

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
        """
        client = self.get_pipeline_service_client(region)
        parent = client.common_location_path(project_id, region)

        result = client.list_pipeline_jobs(
            request={
                "parent": parent,
                "page_size": page_size,
                "page_token": page_token,
                "filter": filter,
                "order_by": order_by,
            },
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return result

    @GoogleBaseHook.fallback_to_default_project_id
    def delete_pipeline_job(
        self,
        project_id: str,
        region: str,
        pipeline_job_id: str,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> Operation:
        """
        Delete a PipelineJob.

        :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
        :param region: Required. The ID of the Google Cloud region that the service belongs to.
        :param pipeline_job_id: Required. The ID of the PipelineJob resource to be deleted.
        :param retry: Designation of what errors, if any, should be retried.
        :param timeout: The timeout for this request.
        :param metadata: Strings which should be sent along with the request as metadata.
        """
        client = self.get_pipeline_service_client(region)
        name = client.pipeline_job_path(project_id, region, pipeline_job_id)

        result = client.delete_pipeline_job(
            request={
                "name": name,
            },
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return result

    @staticmethod
    def extract_pipeline_job_id(obj: dict) -> str:
        """Return unique id of a pipeline job from its name."""
        return obj["name"].rpartition("/")[-1]


class PipelineJobAsyncHook(GoogleBaseAsyncHook):
    """Asynchronous hook for Google Cloud Vertex AI Pipeline Job APIs."""

    sync_hook_class = PipelineJobHook
    PIPELINE_COMPLETE_STATES = (
        PipelineState.PIPELINE_STATE_CANCELLED,
        PipelineState.PIPELINE_STATE_FAILED,
        PipelineState.PIPELINE_STATE_PAUSED,
        PipelineState.PIPELINE_STATE_SUCCEEDED,
    )

    def __init__(
        self,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(
            gcp_conn_id=gcp_conn_id,
            impersonation_chain=impersonation_chain,
            **kwargs,
        )

    async def get_credentials(self) -> Credentials:
        sync_hook = await self.get_sync_hook()
        return sync_hook.get_credentials()

    async def get_project_id(self) -> str:
        sync_hook = await self.get_sync_hook()
        return sync_hook.project_id

    async def get_location(self) -> str:
        sync_hook = await self.get_sync_hook()
        return sync_hook.location

    async def get_pipeline_service_client(
        self,
        region: str | None = None,
    ) -> PipelineServiceAsyncClient:
        if region and region != "global":
            client_options = ClientOptions(api_endpoint=f"{region}-aiplatform.googleapis.com:443")
        else:
            client_options = ClientOptions()
        return PipelineServiceAsyncClient(
            credentials=await self.get_credentials(),
            client_info=CLIENT_INFO,
            client_options=client_options,
        )

    async def get_pipeline_job(
        self,
        project_id: str,
        location: str,
        job_id: str,
        retry: AsyncRetry | _MethodDefault = DEFAULT,
        timeout: float | _MethodDefault | None = DEFAULT,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> types.PipelineJob:
        """
        Get a PipelineJob proto message from PipelineServiceAsyncClient.

        :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
        :param location: Required. The ID of the Google Cloud region that the service belongs to.
        :param job_id: Required. The ID of the PipelineJob resource.
        :param retry: Designation of what errors, if any, should be retried.
        :param timeout: The timeout for this request.
        :param metadata: Strings which should be sent along with the request as metadata.
        """
        client = await self.get_pipeline_service_client(region=location)
        pipeline_job_name = client.pipeline_job_path(
            project=project_id,
            location=location,
            pipeline_job=job_id,
        )
        pipeline_job: types.PipelineJob = await client.get_pipeline_job(
            request={"name": pipeline_job_name},
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return pipeline_job

    async def wait_for_pipeline_job(
        self,
        project_id: str,
        location: str,
        job_id: str,
        retry: AsyncRetry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
        poll_interval: int = 10,
    ) -> types.PipelineJob:
        """Wait until the pipeline job is in a complete state and return it."""
        while True:
            try:
                self.log.info("Requesting a pipeline job with id %s", job_id)
                job: types.PipelineJob = await self.get_pipeline_job(
                    project_id=project_id,
                    location=location,
                    job_id=job_id,
                    retry=retry,
                    timeout=timeout,
                    metadata=metadata,
                )
            except Exception as ex:
                self.log.exception("Exception occurred while requesting pipeline job %s", job_id)
                raise AirflowException(ex)

            self.log.info("Status of the pipeline job %s is %s", job.name, job.state.name)
            if job.state in self.PIPELINE_COMPLETE_STATES:
                return job

            self.log.info("Sleeping for %s seconds.", poll_interval)
            await asyncio.sleep(poll_interval)
