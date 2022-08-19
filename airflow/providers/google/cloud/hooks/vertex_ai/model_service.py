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
"""This module contains a Google Cloud Vertex AI hook.

.. spelling::

    aiplatform
    camelCase
"""

from typing import Dict, Optional, Sequence, Tuple, Union

from google.api_core.client_options import ClientOptions
from google.api_core.gapic_v1.method import DEFAULT, _MethodDefault
from google.api_core.operation import Operation
from google.api_core.retry import Retry
from google.cloud.aiplatform_v1 import ModelServiceClient
from google.cloud.aiplatform_v1.services.model_service.pagers import ListModelsPager
from google.cloud.aiplatform_v1.types import Model, model_service

from airflow import AirflowException
from airflow.providers.google.common.hooks.base_google import GoogleBaseHook


class ModelServiceHook(GoogleBaseHook):
    """Hook for Google Cloud Vertex AI Endpoint Service APIs."""

    def get_model_service_client(self, region: Optional[str] = None) -> ModelServiceClient:
        """Returns ModelServiceClient."""
        if region and region != 'global':
            client_options = ClientOptions(api_endpoint=f'{region}-aiplatform.googleapis.com:443')
        else:
            client_options = ClientOptions()

        return ModelServiceClient(
            credentials=self.get_credentials(), client_info=self.client_info, client_options=client_options
        )

    @staticmethod
    def extract_model_id(obj: Dict) -> str:
        """Returns unique id of the model."""
        return obj["model"].rpartition("/")[-1]

    def wait_for_operation(self, operation: Operation, timeout: Optional[float] = None):
        """Waits for long-lasting operation to complete."""
        try:
            return operation.result(timeout=timeout)
        except Exception:
            error = operation.exception(timeout=timeout)
            raise AirflowException(error)

    @GoogleBaseHook.fallback_to_default_project_id
    def delete_model(
        self,
        project_id: str,
        region: str,
        model: str,
        retry: Union[Retry, _MethodDefault] = DEFAULT,
        timeout: Optional[float] = None,
        metadata: Sequence[Tuple[str, str]] = (),
    ) -> Operation:
        """
        Deletes a Model.

        :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
        :param region: Required. The ID of the Google Cloud region that the service belongs to.
        :param model: Required. The name of the Model resource to be deleted.
        :param retry: Designation of what errors, if any, should be retried.
        :param timeout: The timeout for this request.
        :param metadata: Strings which should be sent along with the request as metadata.
        """
        client = self.get_model_service_client(region)
        name = client.model_path(project_id, region, model)

        result = client.delete_model(
            request={
                'name': name,
            },
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return result

    @GoogleBaseHook.fallback_to_default_project_id
    def export_model(
        self,
        project_id: str,
        region: str,
        model: str,
        output_config: Union[model_service.ExportModelRequest.OutputConfig, Dict],
        retry: Union[Retry, _MethodDefault] = DEFAULT,
        timeout: Optional[float] = None,
        metadata: Sequence[Tuple[str, str]] = (),
    ) -> Operation:
        """
        Exports a trained, exportable Model to a location specified by the user.

        :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
        :param region: Required. The ID of the Google Cloud region that the service belongs to.
        :param model: Required. The resource name of the Model to export.
        :param output_config:  Required. The desired output location and configuration.
        :param retry: Designation of what errors, if any, should be retried.
        :param timeout: The timeout for this request.
        :param metadata: Strings which should be sent along with the request as metadata.
        """
        client = self.get_model_service_client(region)
        name = client.model_path(project_id, region, model)

        result = client.export_model(
            request={
                "name": name,
                "output_config": output_config,
            },
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return result

    @GoogleBaseHook.fallback_to_default_project_id
    def list_models(
        self,
        project_id: str,
        region: str,
        filter: Optional[str] = None,
        page_size: Optional[int] = None,
        page_token: Optional[str] = None,
        read_mask: Optional[str] = None,
        order_by: Optional[str] = None,
        retry: Union[Retry, _MethodDefault] = DEFAULT,
        timeout: Optional[float] = None,
        metadata: Sequence[Tuple[str, str]] = (),
    ) -> ListModelsPager:
        r"""
        Lists Models in a Location.

        :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
        :param region: Required. The ID of the Google Cloud region that the service belongs to.
        :param filter: An expression for filtering the results of the request. For field names both
            snake_case and camelCase are supported.
            -  ``model`` supports = and !=. ``model`` represents the Model ID, i.e. the last segment of the
            Model's [resource name][google.cloud.aiplatform.v1.Model.name].
            -  ``display_name`` supports = and !=
            -  ``labels`` supports general map functions that is:
            --  ``labels.key=value`` - key:value equality
            --  \`labels.key:\* or labels:key - key existence
            --  A key including a space must be quoted. ``labels."a key"``.
        :param page_size: The standard list page size.
        :param page_token: The standard list page token. Typically obtained via
            [ListModelsResponse.next_page_token][google.cloud.aiplatform.v1.ListModelsResponse.next_page_token]
            of the previous
            [ModelService.ListModels][google.cloud.aiplatform.v1.ModelService.ListModels]
            call.
        :param read_mask: Mask specifying which fields to read.
        :param order_by: A comma-separated list of fields to order by, sorted in ascending order. Use "desc"
            after a field name for descending.
        :param retry: Designation of what errors, if any, should be retried.
        :param timeout: The timeout for this request.
        :param metadata: Strings which should be sent along with the request as metadata.
        """
        client = self.get_model_service_client(region)
        parent = client.common_location_path(project_id, region)

        result = client.list_models(
            request={
                'parent': parent,
                'filter': filter,
                'page_size': page_size,
                'page_token': page_token,
                'read_mask': read_mask,
                'order_by': order_by,
            },
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return result

    @GoogleBaseHook.fallback_to_default_project_id
    def upload_model(
        self,
        project_id: str,
        region: str,
        model: Union[Model, Dict],
        retry: Union[Retry, _MethodDefault] = DEFAULT,
        timeout: Optional[float] = None,
        metadata: Sequence[Tuple[str, str]] = (),
    ) -> Operation:
        """
        Uploads a Model artifact into Vertex AI.

        :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
        :param region: Required. The ID of the Google Cloud region that the service belongs to.
        :param model:  Required. The Model to create.
        :param retry: Designation of what errors, if any, should be retried.
        :param timeout: The timeout for this request.
        :param metadata: Strings which should be sent along with the request as metadata.
        """
        client = self.get_model_service_client(region)
        parent = client.common_location_path(project_id, region)

        result = client.upload_model(
            request={
                "parent": parent,
                "model": model,
            },
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return result
