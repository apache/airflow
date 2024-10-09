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
"""This module contains a Google Cloud Vertex AI hook."""

from __future__ import annotations

from typing import TYPE_CHECKING, Sequence

from google.api_core.client_options import ClientOptions
from google.api_core.gapic_v1.method import DEFAULT, _MethodDefault
from google.cloud.aiplatform_v1 import DatasetServiceClient

from airflow.exceptions import AirflowException
from airflow.providers.google.common.consts import CLIENT_INFO
from airflow.providers.google.common.hooks.base_google import GoogleBaseHook

if TYPE_CHECKING:
    from google.api_core.operation import Operation
    from google.api_core.retry import Retry
    from google.cloud.aiplatform_v1.services.dataset_service.pagers import (
        ListAnnotationsPager,
        ListDataItemsPager,
        ListDatasetsPager,
    )
    from google.cloud.aiplatform_v1.types import AnnotationSpec, Dataset, ExportDataConfig, ImportDataConfig
    from google.protobuf.field_mask_pb2 import FieldMask


class DatasetHook(GoogleBaseHook):
    """Hook for Google Cloud Vertex AI Dataset APIs."""

    def __init__(self, **kwargs):
        if kwargs.get("delegate_to") is not None:
            raise RuntimeError(
                "The `delegate_to` parameter has been deprecated before and finally removed in this version"
                " of Google Provider. You MUST convert it to `impersonate_chain`"
            )
        super().__init__(**kwargs)

    def get_dataset_service_client(self, region: str | None = None) -> DatasetServiceClient:
        """Return DatasetServiceClient."""
        if region and region != "global":
            client_options = ClientOptions(api_endpoint=f"{region}-aiplatform.googleapis.com:443")
        else:
            client_options = ClientOptions()

        return DatasetServiceClient(
            credentials=self.get_credentials(), client_info=CLIENT_INFO, client_options=client_options
        )

    def wait_for_operation(self, operation: Operation, timeout: float | None = None):
        """Wait for long-lasting operation to complete."""
        try:
            return operation.result(timeout=timeout)
        except Exception:
            error = operation.exception(timeout=timeout)
            raise AirflowException(error)

    @staticmethod
    def extract_dataset_id(obj: dict) -> str:
        """Return unique id of the dataset."""
        return obj["name"].rpartition("/")[-1]

    @GoogleBaseHook.fallback_to_default_project_id
    def create_dataset(
        self,
        project_id: str,
        region: str,
        dataset: Dataset | dict,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> Operation:
        """
        Create a Dataset.

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
                "parent": parent,
                "dataset": dataset,
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
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> Operation:
        """
        Delete a Dataset.

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
                "name": name,
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
        export_config: ExportDataConfig | dict,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> Operation:
        """
        Export data from a Dataset.

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
                "name": name,
                "export_config": export_config,
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
        read_mask: str | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> AnnotationSpec:
        """
        Get an AnnotationSpec.

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
                "name": name,
                "read_mask": read_mask,
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
        read_mask: str | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> Dataset:
        """
        Get a Dataset.

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
                "name": name,
                "read_mask": read_mask,
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
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> Operation:
        """
        Import data into a Dataset.

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
                "name": name,
                "import_configs": import_configs,
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
        filter: str | None = None,
        page_size: int | None = None,
        page_token: str | None = None,
        read_mask: str | None = None,
        order_by: str | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> ListAnnotationsPager:
        """
        List Annotations belongs to a data item.

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
                "parent": parent,
                "filter": filter,
                "page_size": page_size,
                "page_token": page_token,
                "read_mask": read_mask,
                "order_by": order_by,
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
        filter: str | None = None,
        page_size: int | None = None,
        page_token: str | None = None,
        read_mask: str | None = None,
        order_by: str | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> ListDataItemsPager:
        """
        List DataItems in a Dataset.

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
                "parent": parent,
                "filter": filter,
                "page_size": page_size,
                "page_token": page_token,
                "read_mask": read_mask,
                "order_by": order_by,
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
        filter: str | None = None,
        page_size: int | None = None,
        page_token: str | None = None,
        read_mask: str | None = None,
        order_by: str | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> ListDatasetsPager:
        """
        List Datasets in a Location.

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
                "parent": parent,
                "filter": filter,
                "page_size": page_size,
                "page_token": page_token,
                "read_mask": read_mask,
                "order_by": order_by,
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
        dataset: Dataset | dict,
        update_mask: FieldMask | dict,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> Dataset:
        """
        Update a Dataset.

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
                "dataset": dataset,
                "update_mask": update_mask,
            },
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return result
