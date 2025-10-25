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
from typing import TYPE_CHECKING, Any

from google.api_core.exceptions import NotFound
from google.api_core.gapic_v1.method import DEFAULT, _MethodDefault
from google.cloud.aiplatform_v1.types import Dataset, ExportDataConfig, ImportDataConfig

from airflow.exceptions import AirflowException
from airflow.providers.google.cloud.hooks.vertex_ai.dataset import DatasetHook
from airflow.providers.google.cloud.links.vertex_ai import VertexAIDatasetLink, VertexAIDatasetListLink
from airflow.providers.google.cloud.operators.cloud_base import GoogleCloudBaseOperator

if TYPE_CHECKING:
    from google.api_core.retry import Retry
    from google.protobuf.field_mask_pb2 import FieldMask

    from airflow.providers.common.compat.sdk import Context


class CreateDatasetOperator(GoogleCloudBaseOperator):
    """
    Creates a Dataset.

    :param project_id: Required. The ID of the Google Cloud project the cluster belongs to.
    :param region: Required. The Cloud Dataproc region in which to handle the request.
    :param dataset:  Required. The Dataset to create. This corresponds to the ``dataset`` field on the
        ``request`` instance; if ``request`` is provided, this should not be set.
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

    template_fields = ("region", "project_id", "impersonation_chain")
    operator_extra_links = (VertexAIDatasetLink(),)

    def __init__(
        self,
        *,
        region: str,
        project_id: str,
        dataset: Dataset | dict,
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
        self.dataset = dataset
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
        hook = DatasetHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )

        self.log.info("Creating dataset")
        operation = hook.create_dataset(
            project_id=self.project_id,
            region=self.region,
            dataset=self.dataset,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )
        result = hook.wait_for_operation(timeout=self.timeout, operation=operation)

        dataset = Dataset.to_dict(result)
        dataset_id = hook.extract_dataset_id(dataset)
        self.log.info("Dataset was created. Dataset id: %s", dataset_id)

        context["ti"].xcom_push(key="dataset_id", value=dataset_id)
        VertexAIDatasetLink.persist(context=context, dataset_id=dataset_id)
        return dataset


class GetDatasetOperator(GoogleCloudBaseOperator):
    """
    Get a Dataset.

    :param project_id: Required. The ID of the Google Cloud project the cluster belongs to.
    :param region: Required. The Cloud Dataproc region in which to handle the request.
    :param dataset_id: Required. The ID of the Dataset to get.
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

    template_fields = ("region", "dataset_id", "project_id", "impersonation_chain")
    operator_extra_links = (VertexAIDatasetLink(),)

    def __init__(
        self,
        *,
        region: str,
        project_id: str,
        dataset_id: str,
        read_mask: str | None = None,
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
        self.dataset_id = dataset_id
        self.read_mask = read_mask
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
        hook = DatasetHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )

        try:
            self.log.info("Get dataset: %s", self.dataset_id)
            dataset_obj = hook.get_dataset(
                project_id=self.project_id,
                region=self.region,
                dataset=self.dataset_id,
                read_mask=self.read_mask,
                retry=self.retry,
                timeout=self.timeout,
                metadata=self.metadata,
            )
            VertexAIDatasetLink.persist(context=context, dataset_id=self.dataset_id)
            self.log.info("Dataset was gotten.")
            return Dataset.to_dict(dataset_obj)
        except NotFound:
            self.log.info("The Dataset ID %s does not exist.", self.dataset_id)


class DeleteDatasetOperator(GoogleCloudBaseOperator):
    """
    Deletes a Dataset.

    :param project_id: Required. The ID of the Google Cloud project the cluster belongs to.
    :param region: Required. The Cloud Dataproc region in which to handle the request.
    :param dataset_id: Required. The ID of the Dataset to delete.
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

    template_fields = ("region", "dataset_id", "project_id", "impersonation_chain")

    def __init__(
        self,
        *,
        region: str,
        project_id: str,
        dataset_id: str,
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
        self.dataset_id = dataset_id
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context):
        hook = DatasetHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )

        try:
            self.log.info("Deleting dataset: %s", self.dataset_id)
            operation = hook.delete_dataset(
                project_id=self.project_id,
                region=self.region,
                dataset=self.dataset_id,
                retry=self.retry,
                timeout=self.timeout,
                metadata=self.metadata,
            )
            hook.wait_for_operation(timeout=self.timeout, operation=operation)
            self.log.info("Dataset was deleted.")
        except NotFound:
            self.log.info("The Dataset ID %s does not exist.", self.dataset_id)


class ExportDataOperator(GoogleCloudBaseOperator):
    """
    Exports data from a Dataset.

    :param project_id: Required. The ID of the Google Cloud project the cluster belongs to.
    :param region: Required. The Cloud Dataproc region in which to handle the request.
    :param dataset_id: Required. The ID of the Dataset to delete.
    :param export_config:  Required. The desired output location.
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

    template_fields = ("region", "dataset_id", "project_id", "impersonation_chain")

    def __init__(
        self,
        *,
        region: str,
        project_id: str,
        dataset_id: str,
        export_config: ExportDataConfig | dict,
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
        self.dataset_id = dataset_id
        self.export_config = export_config
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context):
        hook = DatasetHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )

        self.log.info("Exporting data: %s", self.dataset_id)
        operation = hook.export_data(
            project_id=self.project_id,
            region=self.region,
            dataset=self.dataset_id,
            export_config=self.export_config,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )
        hook.wait_for_operation(timeout=self.timeout, operation=operation)
        self.log.info("Export was done successfully")


class DatasetImportDataResultsCheckHelper:
    """Helper utils to verify import dataset data results."""

    @staticmethod
    def _get_number_of_ds_items(dataset, total_key_name):
        number_of_items = type(dataset).to_dict(dataset).get(total_key_name, 0)
        return number_of_items

    @staticmethod
    def _raise_for_empty_import_result(dataset_id, initial_size, size_after_import):
        if int(size_after_import) - int(initial_size) <= 0:
            raise AirflowException(f"Empty results of data import for the dataset_id {dataset_id}.")


class ImportDataOperator(GoogleCloudBaseOperator, DatasetImportDataResultsCheckHelper):
    """
    Imports data into a Dataset.

    :param project_id: Required. The ID of the Google Cloud project the cluster belongs to.
    :param region: Required. The Cloud Dataproc region in which to handle the request.
    :param dataset_id: Required. The ID of the Dataset to delete.
    :param import_configs:  Required. The desired input locations. The contents of all input locations will be
        imported in one batch.
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
    :param raise_for_empty_result: Raise an error if no additional data has been populated after the import.
    """

    template_fields = ("region", "dataset_id", "project_id", "impersonation_chain")

    def __init__(
        self,
        *,
        region: str,
        project_id: str,
        dataset_id: str,
        import_configs: Sequence[ImportDataConfig] | list,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        raise_for_empty_result: bool = False,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.region = region
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.import_configs = import_configs
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain
        self.raise_for_empty_result = raise_for_empty_result

    def execute(self, context: Context):
        hook = DatasetHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )
        initial_dataset_size = self._get_number_of_ds_items(
            dataset=hook.get_dataset(
                dataset=self.dataset_id,
                project_id=self.project_id,
                region=self.region,
                retry=self.retry,
                timeout=self.timeout,
                metadata=self.metadata,
            ),
            total_key_name="data_item_count",
        )
        self.log.info("Importing data: %s", self.dataset_id)
        operation = hook.import_data(
            project_id=self.project_id,
            region=self.region,
            dataset=self.dataset_id,
            import_configs=self.import_configs,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )
        hook.wait_for_operation(timeout=self.timeout, operation=operation)
        result_dataset_size = self._get_number_of_ds_items(
            dataset=hook.get_dataset(
                dataset=self.dataset_id,
                project_id=self.project_id,
                region=self.region,
                retry=self.retry,
                timeout=self.timeout,
                metadata=self.metadata,
            ),
            total_key_name="data_item_count",
        )
        if self.raise_for_empty_result:
            self._raise_for_empty_import_result(self.dataset_id, initial_dataset_size, result_dataset_size)
        self.log.info("Import was done successfully")
        return {"total_data_items_imported": int(result_dataset_size) - int(initial_dataset_size)}


class ListDatasetsOperator(GoogleCloudBaseOperator):
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

    template_fields = ("region", "project_id", "impersonation_chain")
    operator_extra_links = (VertexAIDatasetListLink(),)

    def __init__(
        self,
        *,
        region: str,
        project_id: str,
        filter: str | None = None,
        page_size: int | None = None,
        page_token: str | None = None,
        read_mask: str | None = None,
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
        self.filter = filter
        self.page_size = page_size
        self.page_token = page_token
        self.read_mask = read_mask
        self.order_by = order_by
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    @property
    def extra_links_params(self) -> dict[str, Any]:
        return {
            "project_id": self.project_id,
        }

    def execute(self, context: Context):
        hook = DatasetHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )
        results = hook.list_datasets(
            project_id=self.project_id,
            region=self.region,
            filter=self.filter,
            page_size=self.page_size,
            page_token=self.page_token,
            read_mask=self.read_mask,
            order_by=self.order_by,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )
        VertexAIDatasetListLink.persist(context=context)
        return [Dataset.to_dict(result) for result in results]


class UpdateDatasetOperator(GoogleCloudBaseOperator):
    """
    Updates a Dataset.

    :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
    :param region: Required. The ID of the Google Cloud region that the service belongs to.
    :param dataset_id: Required. The ID of the Dataset to update.
    :param dataset:  Required. The Dataset which replaces the resource on the server.
    :param update_mask:  Required. The update mask applies to the resource.
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

    template_fields = ("region", "dataset_id", "project_id", "impersonation_chain")

    def __init__(
        self,
        *,
        project_id: str,
        region: str,
        dataset_id: str,
        dataset: Dataset | dict,
        update_mask: FieldMask | dict,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.project_id = project_id
        self.region = region
        self.dataset_id = dataset_id
        self.dataset = dataset
        self.update_mask = update_mask
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context):
        hook = DatasetHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )
        self.log.info("Updating dataset: %s", self.dataset_id)
        result = hook.update_dataset(
            project_id=self.project_id,
            region=self.region,
            dataset_id=self.dataset_id,
            dataset=self.dataset,
            update_mask=self.update_mask,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )
        self.log.info("Dataset was updated")
        return Dataset.to_dict(result)
