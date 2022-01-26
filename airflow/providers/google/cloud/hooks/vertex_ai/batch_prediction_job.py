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

from typing import Dict, Optional, Sequence, Tuple

from google.api_core.operation import Operation
from google.api_core.retry import Retry
from google.cloud.aiplatform_v1 import JobServiceClient
from google.cloud.aiplatform_v1.services.job_service.pagers import ListBatchPredictionJobsPager
from google.cloud.aiplatform_v1.types import BatchPredictionJob

from airflow import AirflowException
from airflow.providers.google.common.hooks.base_google import GoogleBaseHook


class BatchPredictionJobHook(GoogleBaseHook):
    """Hook for Google Cloud Vertex AI Batch Prediction Job APIs."""

    def get_job_service_client(self, region: Optional[str] = None) -> JobServiceClient:
        """Returns JobServiceClient."""
        client_options = None
        if region and region != 'global':
            client_options = {'api_endpoint': f'{region}-aiplatform.googleapis.com:443'}

        return JobServiceClient(
            credentials=self._get_credentials(), client_info=self.client_info, client_options=client_options
        )

    def wait_for_operation(self, operation: Operation, timeout: Optional[float] = None):
        """Waits for long-lasting operation to complete."""
        try:
            return operation.result(timeout=timeout)
        except Exception:
            error = operation.exception(timeout=timeout)
            raise AirflowException(error)

    @staticmethod
    def extract_batch_prediction_job_id(obj: Dict) -> str:
        """Returns unique id of the batch_prediction_job."""
        return obj["name"].rpartition("/")[-1]

    @GoogleBaseHook.fallback_to_default_project_id
    def cancel_batch_prediction_job(
        self,
        project_id: str,
        region: str,
        batch_prediction_job: str,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = None,
    ) -> None:
        """
        Cancels a BatchPredictionJob.

        :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
        :param region: Required. The ID of the Google Cloud region that the service belongs to.
        :param batch_prediction_job: Required. The name of the BatchPredictionJob to cancel.
        :param retry: Designation of what errors, if any, should be retried.
        :param timeout: The timeout for this request.
        :param metadata: Strings which should be sent along with the request as metadata.
        """
        client = self.get_job_service_client(region)
        name = client.batch_prediction_job_path(project_id, region, batch_prediction_job)

        client.cancel_batch_prediction_job(
            request={
                'name': name,
            },
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )

    @GoogleBaseHook.fallback_to_default_project_id
    def create_batch_prediction_job(
        self,
        project_id: str,
        region: str,
        batch_prediction_job: BatchPredictionJob,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = "",
    ) -> BatchPredictionJob:
        """
        Creates a BatchPredictionJob. A BatchPredictionJob once created will right away be attempted to start.

        :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
        :param region: Required. The ID of the Google Cloud region that the service belongs to.
        :param batch_prediction_job:  Required. The BatchPredictionJob to create.
        :param retry: Designation of what errors, if any, should be retried.
        :param timeout: The timeout for this request.
        :param metadata: Strings which should be sent along with the request as metadata.
        """
        client = self.get_job_service_client(region)
        parent = client.common_location_path(project_id, region)

        result = client.create_batch_prediction_job(
            request={
                'parent': parent,
                'batch_prediction_job': batch_prediction_job,
            },
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return result

    @GoogleBaseHook.fallback_to_default_project_id
    def delete_batch_prediction_job(
        self,
        project_id: str,
        region: str,
        batch_prediction_job: str,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = "",
    ) -> Operation:
        """
        Deletes a BatchPredictionJob. Can only be called on jobs that already finished.

        :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
        :param region: Required. The ID of the Google Cloud region that the service belongs to.
        :param batch_prediction_job: The name of the BatchPredictionJob resource to be deleted.
        :param retry: Designation of what errors, if any, should be retried.
        :param timeout: The timeout for this request.
        :param metadata: Strings which should be sent along with the request as metadata.
        """
        client = self.get_job_service_client(region)
        name = client.batch_prediction_job_path(project_id, region, batch_prediction_job)

        result = client.delete_batch_prediction_job(
            request={
                'name': name,
            },
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return result

    @GoogleBaseHook.fallback_to_default_project_id
    def get_batch_prediction_job(
        self,
        project_id: str,
        region: str,
        batch_prediction_job: str,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = None,
    ) -> BatchPredictionJob:
        """
        Gets a BatchPredictionJob

        :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
        :param region: Required. The ID of the Google Cloud region that the service belongs to.
        :param batch_prediction_job: Required. The name of the BatchPredictionJob resource.
        :param retry: Designation of what errors, if any, should be retried.
        :param timeout: The timeout for this request.
        :param metadata: Strings which should be sent along with the request as metadata.
        """
        client = self.get_job_service_client(region)
        name = client.batch_prediction_job_path(project_id, region, batch_prediction_job)

        result = client.get_batch_prediction_job(
            request={
                'name': name,
            },
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return result

    @GoogleBaseHook.fallback_to_default_project_id
    def list_batch_prediction_jobs(
        self,
        project_id: str,
        region: str,
        filter: Optional[str] = None,
        page_size: Optional[int] = None,
        page_token: Optional[str] = None,
        read_mask: Optional[str] = None,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = "",
    ) -> ListBatchPredictionJobsPager:
        """
        Lists BatchPredictionJobs in a Location.

        :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
        :param region: Required. The ID of the Google Cloud region that the service belongs to.
        :param filter: The standard list filter.
            Supported fields:
                -  ``display_name`` supports = and !=.
                -  ``state`` supports = and !=.
                -  ``model_display_name`` supports = and !=
            Some examples of using the filter are:
                -  ``state="JOB_STATE_SUCCEEDED" AND display_name="my_job"``
                -  ``state="JOB_STATE_RUNNING" OR display_name="my_job"``
                -  ``NOT display_name="my_job"``
                -  ``state="JOB_STATE_FAILED"``
        :param page_size: The standard list page size.
        :param page_token: The standard list page token.
        :param read_mask: Mask specifying which fields to read.
        :param retry: Designation of what errors, if any, should be retried.
        :param timeout: The timeout for this request.
        :param metadata: Strings which should be sent along with the request as metadata.
        """
        client = self.get_job_service_client(region)
        parent = client.common_location_path(project_id, region)

        result = client.list_batch_prediction_jobs(
            request={
                'parent': parent,
                'filter': filter,
                'page_size': page_size,
                'page_token': page_token,
                'read_mask': read_mask,
            },
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return result
