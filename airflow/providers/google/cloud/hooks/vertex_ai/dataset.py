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

from google.api_core.operation import Operation
from google.api_core.retry import Retry
from google.cloud.aiplatform_v1 import DatasetServiceClient
from google.cloud.aiplatform_v1.services.dataset_service.pagers import (
    ListAnnotationsPager,
    ListDataItemsPager,
    ListDatasetsPager,
)
from google.cloud.aiplatform_v1.types import AnnotationSpec, Dataset, ExportDataConfig, ImportDataConfig
from google.protobuf.field_mask_pb2 import FieldMask

from airflow import AirflowException
from airflow.providers.google.common.hooks.base_google import GoogleBaseHook


class DatasetHook(GoogleBaseHook):
    """Hook for Google Cloud Vertex AI Dataset APIs."""

    def get_dataset_service_client(self, region: Optional[str] = None) -> DatasetServiceClient:
        """Returns DatasetServiceClient."""
        client_options = None
        if region and region != 'global':
            client_options = {'api_endpoint': f'{region}-aiplatform.googleapis.com:443'}

        return DatasetServiceClient(
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
    def extract_dataset_id(obj: Dict) -> str:
        """Returns unique id of the dataset."""
        return obj["name"].rpartition("/")[-1]

    @GoogleBaseHook.fallback_to_default_project_id
    def create_dataset(
        self,
        project_id: str,
        region: str,
        dataset: Union[Dataset, Dict],
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Sequence[Tuple[str, str]] = (),
    ) -> Operation:
        """
        Creates a Dataset.

        :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
        :param region: Required. The ID of the Google Cloud region that the service belongs to.
        :param dataset:  Required. The Dataset to create.
        :param retry: Designation of what errors, if any, should be retried.
        :param timeout: The timeout for this request.
        :param metadata: Strings which should be sent along with the request as metadata.
        """
        client = self.get_dataset_service_client(region)
        parent = client.common_location_path(project_id, region)

        result = client.create_dataset(
            request={
                'parent': parent,
                'dataset': dataset,
            },
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return result

    @GoogleBaseHook.fallback_to_default_project_id
    def delete_dataset(
        self,
        project_id: str,
        region: str,
        dataset: str,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Sequence[Tuple[str, str]] = (),
    ) -> Operation:
        """
        Deletes a Dataset.

        :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
        :param region: Required. The ID of the Google Cloud region that the service belongs to.
        :param dataset: Required. The ID of the Dataset to delete.
        :param retry: Designation of what errors, if any, should be retried.
        :param timeout: The timeout for this request.
        :param metadata: Strings which should be sent along with the request as metadata.
        """
        client = self.get_dataset_service_client(region)
        name = client.dataset_path(project_id, region, dataset)

        result = client.delete_dataset(
            request={
                'name': name,
            },
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return result

    @GoogleBaseHook.fallback_to_default_project_id
    def export_data(
        self,
        project_id: str,
        region: str,
        dataset: str,
        export_config: Union[ExportDataConfig, Dict],
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Sequence[Tuple[str, str]] = (),
    ) -> Operation:
        """
        Exports data from a Dataset.

        :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
        :param region: Required. The ID of the Google Cloud region that the service belongs to.
        :param dataset: Required. The ID of the Dataset to export.
        :param export_config:  Required. The desired output location.
        :param retry: Designation of what errors, if any, should be retried.
        :param timeout: The timeout for this request.
        :param metadata: Strings which should be sent along with the request as metadata.
        """
        client = self.get_dataset_service_client(region)
        name = client.dataset_path(project_id, region, dataset)

        result = client.export_data(
            request={
                'name': name,
                'export_config': export_config,
            },
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return result

    @GoogleBaseHook.fallback_to_default_project_id
    def get_annotation_spec(
        self,
        project_id: str,
        region: str,
        dataset: str,
        annotation_spec: str,
        read_mask: Optional[str] = None,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Sequence[Tuple[str, str]] = (),
    ) -> AnnotationSpec:
        """
        Gets an AnnotationSpec.

        :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
        :param region: Required. The ID of the Google Cloud region that the service belongs to.
        :param dataset: Required. The ID of the Dataset.
        :param annotation_spec: The ID of the AnnotationSpec resource.
        :param read_mask: Optional. Mask specifying which fields to read.
        :param retry: Designation of what errors, if any, should be retried.
        :param timeout: The timeout for this request.
        :param metadata: Strings which should be sent along with the request as metadata.
        """
        client = self.get_dataset_service_client(region)
        name = client.annotation_spec_path(project_id, region, dataset, annotation_spec)

        result = client.get_annotation_spec(
            request={
                'name': name,
                'read_mask': read_mask,
            },
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return result

    @GoogleBaseHook.fallback_to_default_project_id
    def get_dataset(
        self,
        project_id: str,
        region: str,
        dataset: str,
        read_mask: Optional[str] = None,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Sequence[Tuple[str, str]] = (),
    ) -> Dataset:
        """
        Gets a Dataset.

        :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
        :param region: Required. The ID of the Google Cloud region that the service belongs to.
        :param dataset: Required. The ID of the Dataset to export.
        :param read_mask: Optional. Mask specifying which fields to read.
        :param retry: Designation of what errors, if any, should be retried.
        :param timeout: The timeout for this request.
        :param metadata: Strings which should be sent along with the request as metadata.
        """
        client = self.get_dataset_service_client(region)
        name = client.dataset_path(project_id, region, dataset)

        result = client.get_dataset(
            request={
                'name': name,
                'read_mask': read_mask,
            },
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return result

    @GoogleBaseHook.fallback_to_default_project_id
    def import_data(
        self,
        project_id: str,
        region: str,
        dataset: str,
        import_configs: Sequence[ImportDataConfig],
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Sequence[Tuple[str, str]] = (),
    ) -> Operation:
        """
        Imports data into a Dataset.

        :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
        :param region: Required. The ID of the Google Cloud region that the service belongs to.
        :param dataset: Required. The ID of the Dataset to import.
        :param import_configs:  Required. The desired input locations. The contents of all input locations
            will be imported in one batch.
        :param retry: Designation of what errors, if any, should be retried.
        :param timeout: The timeout for this request.
        :param metadata: Strings which should be sent along with the request as metadata.
        """
        client = self.get_dataset_service_client(region)
        name = client.dataset_path(project_id, region, dataset)

        result = client.import_data(
            request={
                'name': name,
                'import_configs': import_configs,
            },
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return result

    @GoogleBaseHook.fallback_to_default_project_id
    def list_annotations(
        self,
        project_id: str,
        region: str,
        dataset: str,
        data_item: str,
        filter: Optional[str] = None,
        page_size: Optional[int] = None,
        page_token: Optional[str] = None,
        read_mask: Optional[str] = None,
        order_by: Optional[str] = None,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Sequence[Tuple[str, str]] = (),
    ) -> ListAnnotationsPager:
        """
        Lists Annotations belongs to a data item

        :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
        :param region: Required. The ID of the Google Cloud region that the service belongs to.
        :param dataset: Required. The ID of the Dataset.
        :param data_item: Required. The ID of the DataItem to list Annotations from.
        :param filter: The standard list filter.
        :param page_size: The standard list page size.
        :param page_token: The standard list page token.
        :param read_mask: Mask specifying which fields to read.
        :param order_by: A comma-separated list of fields to order by, sorted in ascending order. Use "desc"
            after a field name for descending.
        :param retry: Designation of what errors, if any, should be retried.
        :param timeout: The timeout for this request.
        :param metadata: Strings which should be sent along with the request as metadata.
        """
        client = self.get_dataset_service_client(region)
        parent = client.data_item_path(project_id, region, dataset, data_item)

        result = client.list_annotations(
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
    def list_data_items(
        self,
        project_id: str,
        region: str,
        dataset: str,
        filter: Optional[str] = None,
        page_size: Optional[int] = None,
        page_token: Optional[str] = None,
        read_mask: Optional[str] = None,
        order_by: Optional[str] = None,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Sequence[Tuple[str, str]] = (),
    ) -> ListDataItemsPager:
        """
        Lists DataItems in a Dataset.

        :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
        :param region: Required. The ID of the Google Cloud region that the service belongs to.
        :param dataset: Required. The ID of the Dataset.
        :param filter: The standard list filter.
        :param page_size: The standard list page size.
        :param page_token: The standard list page token.
        :param read_mask: Mask specifying which fields to read.
        :param order_by: A comma-separated list of fields to order by, sorted in ascending order. Use "desc"
            after a field name for descending.
        :param retry: Designation of what errors, if any, should be retried.
        :param timeout: The timeout for this request.
        :param metadata: Strings which should be sent along with the request as metadata.
        """
        client = self.get_dataset_service_client(region)
        parent = client.dataset_path(project_id, region, dataset)

        result = client.list_data_items(
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
    def list_datasets(
        self,
        project_id: str,
        region: str,
        filter: Optional[str] = None,
        page_size: Optional[int] = None,
        page_token: Optional[str] = None,
        read_mask: Optional[str] = None,
        order_by: Optional[str] = None,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Sequence[Tuple[str, str]] = (),
    ) -> ListDatasetsPager:
        """
        Lists Datasets in a Location.

        :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
        :param region: Required. The ID of the Google Cloud region that the service belongs to.
        :param filter: The standard list filter.
        :param page_size: The standard list page size.
        :param page_token: The standard list page token.
        :param read_mask: Mask specifying which fields to read.
        :param order_by: A comma-separated list of fields to order by, sorted in ascending order. Use "desc"
            after a field name for descending.
        :param retry: Designation of what errors, if any, should be retried.
        :param timeout: The timeout for this request.
        :param metadata: Strings which should be sent along with the request as metadata.
        """
        client = self.get_dataset_service_client(region)
        parent = client.common_location_path(project_id, region)

        result = client.list_datasets(
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

    def update_dataset(
        self,
        project_id: str,
        region: str,
        dataset_id: str,
        dataset: Union[Dataset, Dict],
        update_mask: Union[FieldMask, Dict],
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Sequence[Tuple[str, str]] = (),
    ) -> Dataset:
        """
        Updates a Dataset.

        :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
        :param region: Required. The ID of the Google Cloud region that the service belongs to.
        :param dataset_id: Required. The ID of the Dataset.
        :param dataset:  Required. The Dataset which replaces the resource on the server.
        :param update_mask:  Required. The update mask applies to the resource.
        :param retry: Designation of what errors, if any, should be retried.
        :param timeout: The timeout for this request.
        :param metadata: Strings which should be sent along with the request as metadata.
        """
        client = self.get_dataset_service_client(region)
        dataset["name"] = client.dataset_path(project_id, region, dataset_id)

        result = client.update_dataset(
            request={
                'dataset': dataset,
                'update_mask': update_mask,
            },
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return result
