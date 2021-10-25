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
#
"""This module contains a Google Cloud Vertex AI hook."""

from typing import Dict, Optional, Sequence, Tuple, Union

from airflow import AirflowException
from airflow.providers.google.common.hooks.base_google import GoogleBaseHook

from google.api_core.operation import Operation
from google.api_core.retry import Retry
from google.cloud.aiplatform_v1 import PipelineServiceClient
from google.cloud.aiplatform_v1.services.pipeline_service.pagers import (
    ListPipelineJobsPager, ListTrainingPipelinesPager)
from google.cloud.aiplatform_v1.types import (
    PipelineJob, TrainingPipeline)


class VertexAIHook(GoogleBaseHook):
    """Hook for Google Cloud Vertex AI APIs."""

    def get_pipeline_service_client(
        self,
        region: Optional[str] = None,
    ) -> PipelineServiceClient:
        """Returns PipelineServiceClient."""
        client_options = None
        if region and region != 'global':
            client_options = {'api_endpoint': f'{region}-aiplatform.googleapis.com:443'}

        return PipelineServiceClient(
            credentials=self._get_credentials(), client_info=self.client_info, client_options=client_options
        )

    def wait_for_operation(self, timeout: float, operation: Operation):
        """Waits for long-lasting operation to complete."""
        try:
            return operation.result(timeout=timeout)
        except Exception:
            error = operation.exception(timeout=timeout)
            raise AirflowException(error)

    @GoogleBaseHook.fallback_to_default_project_id
    def cancel_pipeline_job(
        self,
        project_id: str,
        region: str,
        pipeline_job: str,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = None,
    ) -> None:
        """
        Cancels a PipelineJob. Starts asynchronous cancellation on the PipelineJob. The server makes a best
        effort to cancel the pipeline, but success is not guaranteed. Clients can use
        [PipelineService.GetPipelineJob][google.cloud.aiplatform.v1.PipelineService.GetPipelineJob] or other
        methods to check whether the cancellation succeeded or whether the pipeline completed despite
        cancellation. On successful cancellation, the PipelineJob is not deleted; instead it becomes a
        pipeline with a [PipelineJob.error][google.cloud.aiplatform.v1.PipelineJob.error] value with a
        [google.rpc.Status.code][google.rpc.Status.code] of 1, corresponding to ``Code.CANCELLED``, and
        [PipelineJob.state][google.cloud.aiplatform.v1.PipelineJob.state] is set to ``CANCELLED``.

        :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
        :type project_id: str
        :param region: Required. The ID of the Google Cloud region that the service belongs to.
        :type region: str
        :param pipeline_job: The name of the PipelineJob to cancel.
        :type pipeline_job: str
        :param retry: Designation of what errors, if any, should be retried.
        :type retry: google.api_core.retry.Retry
        :param timeout: The timeout for this request.
        :type timeout: float
        :param metadata: Strings which should be sent along with the request as metadata.
        :type metadata: Sequence[Tuple[str, str]]
        """
        client = self.get_pipeline_service_client(region)
        name = client.pipeline_job_path(project_id, region, pipeline_job)

        client.cancel_pipeline_job(
            request={
                'name': name,
            },
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )

    @GoogleBaseHook.fallback_to_default_project_id
    def cancel_training_pipeline(
        self,
        project_id: str,
        region: str,
        training_pipeline: str,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = None,
    ) -> None:
        """
        Cancels a TrainingPipeline. Starts asynchronous cancellation on the TrainingPipeline. The server makes
        a best effort to cancel the pipeline, but success is not guaranteed. Clients can use
        [PipelineService.GetTrainingPipeline][google.cloud.aiplatform.v1.PipelineService.GetTrainingPipeline]
        or other methods to check whether the cancellation succeeded or whether the pipeline completed despite
        cancellation. On successful cancellation, the TrainingPipeline is not deleted; instead it becomes a
        pipeline with a [TrainingPipeline.error][google.cloud.aiplatform.v1.TrainingPipeline.error] value with
        a [google.rpc.Status.code][google.rpc.Status.code] of 1, corresponding to ``Code.CANCELLED``, and
        [TrainingPipeline.state][google.cloud.aiplatform.v1.TrainingPipeline.state] is set to ``CANCELLED``.

        :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
        :type project_id: str
        :param region: Required. The ID of the Google Cloud region that the service belongs to.
        :type region: str
        :param training_pipeline: Required. The name of the TrainingPipeline to cancel.
        :type training_pipeline: str
        :param retry: Designation of what errors, if any, should be retried.
        :type retry: google.api_core.retry.Retry
        :param timeout: The timeout for this request.
        :type timeout: float
        :param metadata: Strings which should be sent along with the request as metadata.
        :type metadata: Sequence[Tuple[str, str]]
        """
        client = self.get_pipeline_service_client(region)
        name = client.training_pipeline_path(project_id, region, training_pipeline)

        client.cancel_training_pipeline(
            request={
                'name': name,
            },
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )

    @GoogleBaseHook.fallback_to_default_project_id
    def create_pipeline_job(
        self,
        project_id: str,
        region: str,
        pipeline_job: PipelineJob,
        pipeline_job_id: str,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = None,
    ) -> PipelineJob:
        """
        Creates a PipelineJob. A PipelineJob will run immediately when created.

        :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
        :type project_id: str
        :param region: Required. The ID of the Google Cloud region that the service belongs to.
        :type region: str
        :param pipeline_job:  Required. The PipelineJob to create.
        :type pipeline_job: google.cloud.aiplatform_v1.types.PipelineJob
        :param pipeline_job_id:  The ID to use for the PipelineJob, which will become the final component of
            the PipelineJob name. If not provided, an ID will be automatically generated.

            This value should be less than 128 characters, and valid characters are /[a-z][0-9]-/.
        :type pipeline_job_id: str
        :param retry: Designation of what errors, if any, should be retried.
        :type retry: google.api_core.retry.Retry
        :param timeout: The timeout for this request.
        :type timeout: float
        :param metadata: Strings which should be sent along with the request as metadata.
        :type metadata: Sequence[Tuple[str, str]]
        """
        client = self.get_pipeline_service_client(region)
        parent = client.common_location_path(project_id, region)

        result = client.create_pipeline_job(
            request={
                'parent': parent,
                'pipeline_job': pipeline_job,
                'pipeline_job_id': pipeline_job_id,
            },
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return result

    @GoogleBaseHook.fallback_to_default_project_id
    def create_training_pipeline(
        self,
        project_id: str,
        region: str,
        training_pipeline: TrainingPipeline,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = None,
    ) -> TrainingPipeline:
        """
        Creates a TrainingPipeline. A created TrainingPipeline right away will be attempted to be run.

        :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
        :type project_id: str
        :param region: Required. The ID of the Google Cloud region that the service belongs to.
        :type region: str
        :param training_pipeline:  Required. The TrainingPipeline to create.
        :type training_pipeline: google.cloud.aiplatform_v1.types.TrainingPipeline
        :param retry: Designation of what errors, if any, should be retried.
        :type retry: google.api_core.retry.Retry
        :param timeout: The timeout for this request.
        :type timeout: float
        :param metadata: Strings which should be sent along with the request as metadata.
        :type metadata: Sequence[Tuple[str, str]]
        """
        client = self.get_pipeline_service_client(region)
        parent = client.common_location_path(project_id, region)

        result = client.create_training_pipeline(
            request={
                'parent': parent,
                'training_pipeline': training_pipeline,
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
        pipeline_job: str,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = None,
    ) -> Operation:
        """
        Deletes a PipelineJob.

        :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
        :type project_id: str
        :param region: Required. The ID of the Google Cloud region that the service belongs to.
        :type region: str
        :param pipeline_job: Required. The name of the PipelineJob resource to be deleted.
        :type pipeline_job: str
        :param retry: Designation of what errors, if any, should be retried.
        :type retry: google.api_core.retry.Retry
        :param timeout: The timeout for this request.
        :type timeout: float
        :param metadata: Strings which should be sent along with the request as metadata.
        :type metadata: Sequence[Tuple[str, str]]
        """
        client = self.get_pipeline_service_client(region)
        name = client.pipeline_job_path(project_id, region, pipeline_job)

        result = client.delete_pipeline_job(
            request={
                'name': name,
            },
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return result

    @GoogleBaseHook.fallback_to_default_project_id
    def delete_training_pipeline(
        self,
        project_id: str,
        region: str,
        training_pipeline: str,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = None,
    ) -> Operation:
        """
        Deletes a TrainingPipeline.

        :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
        :type project_id: str
        :param region: Required. The ID of the Google Cloud region that the service belongs to.
        :type region: str
        :param training_pipeline: Required. The name of the TrainingPipeline resource to be deleted.
        :type training_pipeline: str
        :param retry: Designation of what errors, if any, should be retried.
        :type retry: google.api_core.retry.Retry
        :param timeout: The timeout for this request.
        :type timeout: float
        :param metadata: Strings which should be sent along with the request as metadata.
        :type metadata: Sequence[Tuple[str, str]]
        """
        client = self.get_pipeline_service_client(region)
        name = client.training_pipeline_path(project_id, region, training_pipeline)

        result = client.delete_training_pipeline(
            request={
                'name': name,
            },
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return result

    @GoogleBaseHook.fallback_to_default_project_id
    def get_pipeline_job(
        self,
        project_id: str,
        region: str,
        pipeline_job: str,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = None,
    ) -> PipelineJob:
        """
        Gets a PipelineJob.

        :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
        :type project_id: str
        :param region: Required. The ID of the Google Cloud region that the service belongs to.
        :type region: str
        :param pipeline_job: Required. The name of the PipelineJob resource.
        :type pipeline_job: str
        :param retry: Designation of what errors, if any, should be retried.
        :type retry: google.api_core.retry.Retry
        :param timeout: The timeout for this request.
        :type timeout: float
        :param metadata: Strings which should be sent along with the request as metadata.
        :type metadata: Sequence[Tuple[str, str]]
        """
        client = self.get_pipeline_service_client(region)
        name = client.pipeline_job_path(project_id, region, pipeline_job)

        result = client.get_pipeline_job(
            request={
                'name': name,
            },
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return result

    @GoogleBaseHook.fallback_to_default_project_id
    def get_training_pipeline(
        self,
        project_id: str,
        region: str,
        training_pipeline: str,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = None,
    ) -> TrainingPipeline:
        """
        Gets a TrainingPipeline.

        :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
        :type project_id: str
        :param region: Required. The ID of the Google Cloud region that the service belongs to.
        :type region: str
        :param training_pipeline: Required. The name of the TrainingPipeline resource.
        :type training_pipeline: str
        :param retry: Designation of what errors, if any, should be retried.
        :type retry: google.api_core.retry.Retry
        :param timeout: The timeout for this request.
        :type timeout: float
        :param metadata: Strings which should be sent along with the request as metadata.
        :type metadata: Sequence[Tuple[str, str]]
        """
        client = self.get_pipeline_service_client(region)
        name = client.training_pipeline_path(project_id, region, training_pipeline)

        result = client.get_training_pipeline(
            request={
                'name': name,
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
        page_size: Optional[int] = None,
        page_token: Optional[str] = None,
        filter: Optional[str] = None,
        order_by: Optional[str] = None,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = None,
    ) -> ListPipelineJobsPager:
        """
        Lists PipelineJobs in a Location.

        :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
        :type project_id: str
        :param region: Required. The ID of the Google Cloud region that the service belongs to.
        :type region: str
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

            Examples:

            -  ``create_time>"2021-05-18T00:00:00Z" OR update_time>"2020-05-18T00:00:00Z"``
               PipelineJobs created or updated after 2020-05-18 00:00:00
               UTC.
            -  ``labels.env = "prod"`` PipelineJobs with label "env" set
               to "prod".
        :type filter: str
        :param page_size: Optional. The standard list page size.
        :type page_size: int
        :param page_token: Optional. The standard list page token. Typically obtained via
            [ListPipelineJobsResponse.next_page_token][google.cloud.aiplatform.v1.ListPipelineJobsResponse.next_page_token]
            of the previous
            [PipelineService.ListPipelineJobs][google.cloud.aiplatform.v1.PipelineService.ListPipelineJobs]
            call.
        :type page_token: str
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
        :type order_by: str
        :param retry: Designation of what errors, if any, should be retried.
        :type retry: google.api_core.retry.Retry
        :param timeout: The timeout for this request.
        :type timeout: float
        :param metadata: Strings which should be sent along with the request as metadata.
        :type metadata: Sequence[Tuple[str, str]]
        """
        client = self.get_pipeline_service_client(region)
        parent = client.common_location_path(project_id, region)

        result = client.list_pipeline_jobs(
            request={
                'parent': parent,
                'page_size': page_size,
                'page_token': page_token,
                'filter': filter,
                'order_by': order_by,
            },
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return result

    @GoogleBaseHook.fallback_to_default_project_id
    def list_training_pipelines(
        self,
        project_id: str,
        region: str,
        page_size: Optional[int] = None,
        page_token: Optional[str] = None,
        filter: Optional[str] = None,
        read_mask: Optional[str] = None,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = None,
    ) -> ListTrainingPipelinesPager:
        """
        Lists TrainingPipelines in a Location.

        :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
        :type project_id: str
        :param region: Required. The ID of the Google Cloud region that the service belongs to.
        :type region: str
        :param filter: Optional. The standard list filter. Supported fields:

            -  ``display_name`` supports = and !=.

            -  ``state`` supports = and !=.

            Some examples of using the filter are:

            -  ``state="PIPELINE_STATE_SUCCEEDED" AND display_name="my_pipeline"``

            -  ``state="PIPELINE_STATE_RUNNING" OR display_name="my_pipeline"``

            -  ``NOT display_name="my_pipeline"``

            -  ``state="PIPELINE_STATE_FAILED"``
        :type filter: str
        :param page_size: Optional. The standard list page size.
        :type page_size: int
        :param page_token: Optional. The standard list page token. Typically obtained via
            [ListTrainingPipelinesResponse.next_page_token][google.cloud.aiplatform.v1.ListTrainingPipelinesResponse.next_page_token]
            of the previous
            [PipelineService.ListTrainingPipelines][google.cloud.aiplatform.v1.PipelineService.ListTrainingPipelines]
            call.
        :type page_token: str
        :param read_mask: Optional. Mask specifying which fields to read.
        :type read_mask: google.protobuf.field_mask_pb2.FieldMask
        :param retry: Designation of what errors, if any, should be retried.
        :type retry: google.api_core.retry.Retry
        :param timeout: The timeout for this request.
        :type timeout: float
        :param metadata: Strings which should be sent along with the request as metadata.
        :type metadata: Sequence[Tuple[str, str]]
        """
        client = self.get_pipeline_service_client(region)
        parent = client.common_location_path(project_id, region)

        result = client.list_training_pipelines(
            request={
                'parent': parent,
                'page_size': page_size,
                'page_token': page_token,
                'filter': filter,
                'read_mask': read_mask,
            },
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return result
