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
"""This module contains Google Vertex AI operators.

.. spelling::

    aiplatform
    camelCase
"""
from __future__ import annotations

from typing import TYPE_CHECKING, Sequence

from google.api_core.exceptions import NotFound
from google.api_core.gapic_v1.method import DEFAULT, _MethodDefault
from google.api_core.retry import Retry
from google.cloud.aiplatform_v1.types import Model, model_service

from airflow.providers.google.cloud.hooks.vertex_ai.model_service import ModelServiceHook
from airflow.providers.google.cloud.links.vertex_ai import (
    VertexAIModelExportLink,
    VertexAIModelLink,
    VertexAIModelListLink,
)
from airflow.providers.google.cloud.operators.cloud_base import GoogleCloudBaseOperator

if TYPE_CHECKING:
    from airflow.utils.context import Context


class DeleteModelOperator(GoogleCloudBaseOperator):
    """
    Deletes a Model.

    :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
    :param region: Required. The ID of the Google Cloud region that the service belongs to.
    :param model_id: Required. The name of the Model resource to be deleted.
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

    template_fields = ("region", "model_id", "project_id", "impersonation_chain")

    def __init__(
        self,
        *,
        region: str,
        project_id: str,
        model_id: str,
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
        self.model_id = model_id
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context):
        hook = ModelServiceHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )

        try:
            self.log.info("Deleting model: %s", self.model_id)
            operation = hook.delete_model(
                project_id=self.project_id,
                region=self.region,
                model=self.model_id,
                retry=self.retry,
                timeout=self.timeout,
                metadata=self.metadata,
            )
            hook.wait_for_operation(timeout=self.timeout, operation=operation)
            self.log.info("Model was deleted.")
        except NotFound:
            self.log.info("The Model ID %s does not exist.", self.model_id)


class ExportModelOperator(GoogleCloudBaseOperator):
    """
    Exports a trained, exportable Model to a location specified by the user.

    :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
    :param region: Required. The ID of the Google Cloud region that the service belongs to.
    :param model_id: Required. The resource name of the Model to export.
    :param output_config:  Required. The desired output location and configuration.
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

    template_fields = ("region", "model_id", "project_id", "impersonation_chain")
    operator_extra_links = (VertexAIModelExportLink(),)

    def __init__(
        self,
        *,
        project_id: str,
        region: str,
        model_id: str,
        output_config: model_service.ExportModelRequest.OutputConfig | dict,
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
        self.model_id = model_id
        self.output_config = output_config
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context):
        hook = ModelServiceHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )

        try:
            self.log.info("Exporting model: %s", self.model_id)
            operation = hook.export_model(
                project_id=self.project_id,
                region=self.region,
                model=self.model_id,
                output_config=self.output_config,
                retry=self.retry,
                timeout=self.timeout,
                metadata=self.metadata,
            )
            hook.wait_for_operation(timeout=self.timeout, operation=operation)
            VertexAIModelExportLink.persist(context=context, task_instance=self)
            self.log.info("Model was exported.")
        except NotFound:
            self.log.info("The Model ID %s does not exist.", self.model_id)


class ListModelsOperator(GoogleCloudBaseOperator):
    r"""
    Lists Models in a Location.

    :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
    :param region: Required. The ID of the Google Cloud region that the service belongs to.
    :param retry: Designation of what errors, if any, should be retried.
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
    operator_extra_links = (VertexAIModelListLink(),)

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

    def execute(self, context: Context):
        hook = ModelServiceHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )
        results = hook.list_models(
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
        VertexAIModelListLink.persist(context=context, task_instance=self)
        return [Model.to_dict(result) for result in results]


class UploadModelOperator(GoogleCloudBaseOperator):
    """
    Uploads a Model artifact into Vertex AI.

    :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
    :param region: Required. The ID of the Google Cloud region that the service belongs to.
    :param model:  Required. The Model to create.
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

    template_fields = ("region", "project_id", "model", "impersonation_chain")
    operator_extra_links = (VertexAIModelLink(),)

    def __init__(
        self,
        *,
        project_id: str,
        region: str,
        model: Model | dict,
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
        self.model = model
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context):
        hook = ModelServiceHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )
        self.log.info("Upload model")
        operation = hook.upload_model(
            project_id=self.project_id,
            region=self.region,
            model=self.model,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )
        result = hook.wait_for_operation(timeout=self.timeout, operation=operation)

        model_resp = model_service.UploadModelResponse.to_dict(result)
        model_id = hook.extract_model_id(model_resp)
        self.log.info("Model was uploaded. Model ID: %s", model_id)

        self.xcom_push(context, key="model_id", value=model_id)
        VertexAIModelLink.persist(context=context, task_instance=self, model_id=model_id)
        return model_resp
