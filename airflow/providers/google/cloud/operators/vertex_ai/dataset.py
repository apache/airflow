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
"""This module contains Google Vertex AI operators."""

from typing import Dict, Optional, Sequence, Tuple, Union

from google.api_core.exceptions import NotFound
from google.api_core.retry import Retry
from google.cloud.aiplatform_v1.types import Dataset, ExportDataConfig, ImportDataConfig
from google.protobuf.field_mask_pb2 import FieldMask

from airflow.models import BaseOperator, BaseOperatorLink
from airflow.models.taskinstance import TaskInstance
from airflow.providers.google.cloud.hooks.vertex_ai.dataset import DatasetHook

VERTEX_AI_BASE_LINK = "https://console.cloud.google.com/vertex-ai"
VERTEX_AI_DATASET_LINK = (
    VERTEX_AI_BASE_LINK + "/locations/{region}/datasets/{dataset_id}/analyze?project={project_id}"
)
VERTEX_AI_DATASET_LIST_LINK = VERTEX_AI_BASE_LINK + "/datasets?project={project_id}"


class VertexAIDatasetLink(BaseOperatorLink):
    """Helper class for constructing Vertex AI Dataset link"""

    name = "Dataset"

    def get_link(self, operator, dttm):
        ti = TaskInstance(task=operator, execution_date=dttm)
        dataset_conf = ti.xcom_pull(task_ids=operator.task_id, key="dataset_conf")
        return (
            VERTEX_AI_DATASET_LINK.format(
                region=dataset_conf["region"],
                dataset_id=dataset_conf["dataset_id"],
                project_id=dataset_conf["project_id"],
            )
            if dataset_conf
            else ""
        )


class VertexAIDatasetListLink(BaseOperatorLink):
    """Helper class for constructing Vertex AI Datasets Link"""

    name = "Dataset List"

    def get_link(self, operator, dttm):
        ti = TaskInstance(task=operator, execution_date=dttm)
        project_id = ti.xcom_pull(task_ids=operator.task_id, key="project_id")
        return (
            VERTEX_AI_DATASET_LIST_LINK.format(
                project_id=project_id,
            )
            if project_id
            else ""
        )


class CreateDatasetOperator(BaseOperator):
    """
    Creates a Dataset.

    :param project_id: Required. The ID of the Google Cloud project the cluster belongs to.
    :type project_id: str
    :param region: Required. The Cloud Dataproc region in which to handle the request.
    :type region: str
    :param dataset:  Required. The Dataset to create. This corresponds to the ``dataset`` field on the
        ``request`` instance; if ``request`` is provided, this should not be set.
    :type dataset: google.cloud.aiplatform_v1.types.Dataset
    :param retry: Designation of what errors, if any, should be retried.
    :type retry: google.api_core.retry.Retry
    :param timeout: The timeout for this request.
    :type timeout: float
    :param metadata: Strings which should be sent along with the request as metadata.
    :type metadata: Sequence[Tuple[str, str]]
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud.
    :type gcp_conn_id: str
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :type impersonation_chain: Union[str, Sequence[str]]
    """

    template_fields = ("region", "project_id", "impersonation_chain")
    operator_extra_links = (VertexAIDatasetLink(),)

    def __init__(
        self,
        *,
        region: str,
        project_id: str,
        dataset: Dataset,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = "",
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
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

    def execute(self, context: Dict):
        hook = DatasetHook(gcp_conn_id=self.gcp_conn_id, impersonation_chain=self.impersonation_chain)

        self.log.info("Creating dataset")
        operation = hook.create_dataset(
            project_id=self.project_id,
            region=self.region,
            dataset=self.dataset,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )
        result = hook.wait_for_operation(self.timeout, operation)

        dataset = Dataset.to_dict(result)
        dataset_id = hook.extract_dataset_id(dataset)
        self.log.info("Dataset was created. Dataset id: %s", dataset_id)

        self.xcom_push(context, key="dataset_id", value=dataset_id)
        self.xcom_push(
            context,
            key="dataset_conf",
            value={
                "dataset_id": dataset_id,
                "region": self.region,
                "project_id": self.project_id,
            },
        )
        return dataset


class GetDatasetOperator(BaseOperator):
    """
    Get a Dataset.

    :param project_id: Required. The ID of the Google Cloud project the cluster belongs to.
    :type project_id: str
    :param region: Required. The Cloud Dataproc region in which to handle the request.
    :type region: str
    :param dataset_id: Required. The ID of the Dataset to get.
    :type dataset_id: str
    :param retry: Designation of what errors, if any, should be retried.
    :type retry: google.api_core.retry.Retry
    :param timeout: The timeout for this request.
    :type timeout: float
    :param metadata: Strings which should be sent along with the request as metadata.
    :type metadata: Sequence[Tuple[str, str]]
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud.
    :type gcp_conn_id: str
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :type impersonation_chain: Union[str, Sequence[str]]
    """

    template_fields = ("region", "dataset_id", "project_id", "impersonation_chain")
    operator_extra_links = (VertexAIDatasetLink(),)

    def __init__(
        self,
        *,
        region: str,
        project_id: str,
        dataset_id: str,
        read_mask: Optional[str] = None,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = "",
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
        **kwargs,
    ) -> Dataset:
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

    def execute(self, context):
        hook = DatasetHook(gcp_conn_id=self.gcp_conn_id, impersonation_chain=self.impersonation_chain)

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
            self.xcom_push(
                context,
                key="dataset_conf",
                value={
                    "dataset_id": self.dataset_id,
                    "project_id": self.project_id,
                    "region": self.region,
                },
            )
            self.log.info("Dataset was gotten.")
            return Dataset.to_dict(dataset_obj)
        except NotFound:
            self.log.info("The Dataset ID %s does not exist.", self.dataset_id)


class DeleteDatasetOperator(BaseOperator):
    """
    Deletes a Dataset.

    :param project_id: Required. The ID of the Google Cloud project the cluster belongs to.
    :type project_id: str
    :param region: Required. The Cloud Dataproc region in which to handle the request.
    :type region: str
    :param dataset_id: Required. The ID of the Dataset to delete.
    :type dataset_id: str
    :param retry: Designation of what errors, if any, should be retried.
    :type retry: google.api_core.retry.Retry
    :param timeout: The timeout for this request.
    :type timeout: float
    :param metadata: Strings which should be sent along with the request as metadata.
    :type metadata: Sequence[Tuple[str, str]]
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud.
    :type gcp_conn_id: str
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :type impersonation_chain: Union[str, Sequence[str]]
    """

    template_fields = ("region", "dataset_id", "project_id", "impersonation_chain")

    def __init__(
        self,
        *,
        region: str,
        project_id: str,
        dataset_id: str,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = "",
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
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

    def execute(self, context: Dict):
        hook = DatasetHook(gcp_conn_id=self.gcp_conn_id, impersonation_chain=self.impersonation_chain)

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


class ExportDataOperator(BaseOperator):
    """
    Exports data from a Dataset.

    :param project_id: Required. The ID of the Google Cloud project the cluster belongs to.
    :type project_id: str
    :param region: Required. The Cloud Dataproc region in which to handle the request.
    :type region: str
    :param dataset_id: Required. The ID of the Dataset to delete.
    :type dataset_id: str
    :param export_config:  Required. The desired output location.
    :type export_config: google.cloud.aiplatform_v1.types.ExportDataConfig
    :param retry: Designation of what errors, if any, should be retried.
    :type retry: google.api_core.retry.Retry
    :param timeout: The timeout for this request.
    :type timeout: float
    :param metadata: Strings which should be sent along with the request as metadata.
    :type metadata: Sequence[Tuple[str, str]]
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud.
    :type gcp_conn_id: str
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :type impersonation_chain: Union[str, Sequence[str]]
    """

    template_fields = ("region", "dataset_id", "project_id", "impersonation_chain")

    def __init__(
        self,
        *,
        region: str,
        project_id: str,
        dataset_id: str,
        export_config: ExportDataConfig,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = "",
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
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

    def execute(self, context: Dict):
        hook = DatasetHook(gcp_conn_id=self.gcp_conn_id, impersonation_chain=self.impersonation_chain)

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


class ImportDataOperator(BaseOperator):
    """
    Imports data into a Dataset.

    :param project_id: Required. The ID of the Google Cloud project the cluster belongs to.
    :type project_id: str
    :param region: Required. The Cloud Dataproc region in which to handle the request.
    :type region: str
    :param dataset_id: Required. The ID of the Dataset to delete.
    :type dataset_id: str
    :param import_configs:  Required. The desired input locations. The contents of all input locations will be
        imported in one batch.
    :type import_configs: Sequence[google.cloud.aiplatform_v1.types.ImportDataConfig]
    :param retry: Designation of what errors, if any, should be retried.
    :type retry: google.api_core.retry.Retry
    :param timeout: The timeout for this request.
    :type timeout: float
    :param metadata: Strings which should be sent along with the request as metadata.
    :type metadata: Sequence[Tuple[str, str]]
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud.
    :type gcp_conn_id: str
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :type impersonation_chain: Union[str, Sequence[str]]
    """

    template_fields = ("region", "dataset_id", "project_id", "impersonation_chain")

    def __init__(
        self,
        *,
        region: str,
        project_id: str,
        dataset_id: str,
        import_configs: Sequence[ImportDataConfig],
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = "",
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
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

    def execute(self, context: Dict):
        hook = DatasetHook(gcp_conn_id=self.gcp_conn_id, impersonation_chain=self.impersonation_chain)

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
        self.log.info("Import was done successfully")


class ListDatasetsOperator(BaseOperator):
    """
    Lists Datasets in a Location.

    :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
    :type project_id: str
    :param region: Required. The ID of the Google Cloud region that the service belongs to.
    :type region: str
    :param filter: The standard list filter.
    :type filter: str
    :param page_size: The standard list page size.
    :type page_size: int
    :param page_token: The standard list page token.
    :type page_token: str
    :param read_mask: Mask specifying which fields to read.
    :type read_mask: google.protobuf.field_mask_pb2.FieldMask
    :param order_by: A comma-separated list of fields to order by, sorted in ascending order. Use "desc"
        after a field name for descending.
    :type order_by: str
    :param retry: Designation of what errors, if any, should be retried.
    :type retry: google.api_core.retry.Retry
    :param timeout: The timeout for this request.
    :type timeout: float
    :param metadata: Strings which should be sent along with the request as metadata.
    :type metadata: Sequence[Tuple[str, str]]
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud.
    :type gcp_conn_id: str
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :type impersonation_chain: Union[str, Sequence[str]]
    """

    template_fields = ("region", "project_id", "impersonation_chain")
    operator_extra_links = (VertexAIDatasetListLink(),)

    def __init__(
        self,
        *,
        region: str,
        project_id: str,
        filter: Optional[str] = None,
        page_size: Optional[int] = None,
        page_token: Optional[str] = None,
        read_mask: Optional[str] = None,
        order_by: Optional[str] = None,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = "",
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
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

    def execute(self, context: Dict):
        hook = DatasetHook(gcp_conn_id=self.gcp_conn_id, impersonation_chain=self.impersonation_chain)
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
        self.xcom_push(
            context,
            key="project_id",
            value=self.project_id,
        )
        return [Dataset.to_dict(result) for result in results]


class UpdateDatasetOperator(BaseOperator):
    """
    Updates a Dataset.

    :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
    :type project_id: str
    :param region: Required. The ID of the Google Cloud region that the service belongs to.
    :type region: str
    :param dataset_id: Required. The ID of the Dataset to update.
    :type dataset_id: str
    :param dataset:  Required. The Dataset which replaces the resource on the server.
    :type dataset: google.cloud.aiplatform_v1.types.Dataset
    :param update_mask:  Required. The update mask applies to the resource. For the ``FieldMask`` definition,
        see [google.protobuf.FieldMask][google.protobuf.FieldMask]. Updatable fields:
            -  ``display_name``
            -  ``description``
            -  ``labels``
    :type update_mask: google.protobuf.field_mask_pb2.FieldMask
    :param retry: Designation of what errors, if any, should be retried.
    :type retry: google.api_core.retry.Retry
    :param timeout: The timeout for this request.
    :type timeout: float
    :param metadata: Strings which should be sent along with the request as metadata.
    :type metadata: Sequence[Tuple[str, str]]
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud.
    :type gcp_conn_id: str
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :type impersonation_chain: Union[str, Sequence[str]]
    """

    template_fields = ("region", "dataset_id", "project_id", "impersonation_chain")

    def __init__(
        self,
        *,
        project_id: str,
        region: str,
        dataset_id: str,
        dataset: Dataset,
        update_mask: FieldMask,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = "",
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
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

    def execute(self, context):
        hook = DatasetHook(gcp_conn_id=self.gcp_conn_id, impersonation_chain=self.impersonation_chain)
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
