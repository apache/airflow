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

from collections.abc import Sequence
from typing import TYPE_CHECKING

from google.api_core.client_options import ClientOptions
from google.api_core.gapic_v1.method import DEFAULT, _MethodDefault
from google.cloud.aiplatform_v1 import ModelServiceClient

from airflow.providers.common.compat.sdk import AirflowException
from airflow.providers.google.common.consts import CLIENT_INFO
from airflow.providers.google.common.hooks.base_google import GoogleBaseHook

if TYPE_CHECKING:
    from google.api_core.operation import Operation
    from google.api_core.retry import Retry
    from google.cloud.aiplatform_v1.services.model_service.pagers import (
        ListModelsPager,
        ListModelVersionsPager,
    )
    from google.cloud.aiplatform_v1.types import Model, model_service

from airflow.providers.google.common.hooks.operation_helpers import OperationHelper


class ModelServiceHook(GoogleBaseHook, OperationHelper):
    """Hook for Google Cloud Vertex AI Endpoint Service APIs."""

    def get_model_service_client(self, region: str | None = None) -> ModelServiceClient:
        """Return ModelServiceClient object."""
        if region and region != "global":
            client_options = ClientOptions(api_endpoint=f"{region}-aiplatform.googleapis.com:443")
        else:
            client_options = ClientOptions()

        return ModelServiceClient(
            credentials=self.get_credentials(), client_info=CLIENT_INFO, client_options=client_options
        )

    @staticmethod
    def extract_model_id(obj: dict) -> str:
        """Return unique id of the model."""
        return obj["model"].rpartition("/")[-1]

    @GoogleBaseHook.fallback_to_default_project_id
    def delete_model(
        self,
        project_id: str,
        region: str,
        model: str,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> Operation:
        """
        Delete a Model.

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
                "name": name,
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
        output_config: model_service.ExportModelRequest.OutputConfig | dict,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> Operation:
        """
        Export a trained, exportable Model to a location specified by the user.

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
        filter: str | None = None,
        page_size: int | None = None,
        page_token: str | None = None,
        read_mask: str | None = None,
        order_by: str | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> ListModelsPager:
        r"""
        List Models in a Location.

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
    def upload_model(
        self,
        project_id: str,
        region: str,
        model: Model | dict,
        parent_model: str | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> Operation:
        """
        Upload a Model artifact into Vertex AI.

        :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
        :param region: Required. The ID of the Google Cloud region that the service belongs to.
        :param model:  Required. The Model to create.
        :param parent_model: The ID of the parent model to create a new version under.
        :param retry: Designation of what errors, if any, should be retried.
        :param timeout: The timeout for this request.
        :param metadata: Strings which should be sent along with the request as metadata.
        """
        client = self.get_model_service_client(region)
        parent = client.common_location_path(project_id, region)

        request = {
            "parent": parent,
            "model": model,
        }

        if parent_model:
            request["parent_model"] = client.model_path(project_id, region, parent_model)

        result = client.upload_model(
            request=request,
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return result

    @GoogleBaseHook.fallback_to_default_project_id
    def list_model_versions(
        self,
        region: str,
        project_id: str,
        model_id: str,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> ListModelVersionsPager:
        """
        List all versions of the existing Model.

        :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
        :param region: Required. The ID of the Google Cloud region that the service belongs to.
        :param model_id: Required. The ID of the Model to output versions for.
        :param retry: Designation of what errors, if any, should be retried.
        :param timeout: The timeout for this request.
        :param metadata: Strings which should be sent along with the request as metadata.
        """
        client = self.get_model_service_client(region)
        name = client.model_path(project_id, region, model_id)

        result = client.list_model_versions(
            request={
                "name": name,
            },
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return result

    @GoogleBaseHook.fallback_to_default_project_id
    def delete_model_version(
        self,
        region: str,
        project_id: str,
        model_id: str,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> Operation:
        """
        Delete version of the Model. The version could not be deleted if this version is default.

        :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
        :param region: Required. The ID of the Google Cloud region that the service belongs to.
        :param model_id: Required. The ID of the Model in which to delete version.
        :param retry: Designation of what errors, if any, should be retried.
        :param timeout: The timeout for this request.
        :param metadata: Strings which should be sent along with the request as metadata.
        """
        client = self.get_model_service_client(region)
        name = client.model_path(project_id, region, model_id)

        result = client.delete_model_version(
            request={
                "name": name,
            },
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return result

    @GoogleBaseHook.fallback_to_default_project_id
    def get_model(
        self,
        region: str,
        project_id: str,
        model_id: str,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> Model:
        """
        Retrieve Model of specific name and version. If version is not specified, the default is retrieved.

        :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
        :param region: Required. The ID of the Google Cloud region that the service belongs to.
        :param model_id: Required. The ID of the Model to retrieve.
        :param retry: Designation of what errors, if any, should be retried.
        :param timeout: The timeout for this request.
        :param metadata: Strings which should be sent along with the request as metadata.
        """
        client = self.get_model_service_client(region)
        name = client.model_path(project_id, region, model_id)

        result = client.get_model(
            request={
                "name": name,
            },
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return result

    @GoogleBaseHook.fallback_to_default_project_id
    def set_version_as_default(
        self,
        region: str,
        model_id: str,
        project_id: str,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> Model:
        """
        Set current version of the Model as default.

        :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
        :param region: Required. The ID of the Google Cloud region that the service belongs to.
        :param model_id: Required. The ID of the Model to set as default.
        :param retry: Designation of what errors, if any, should be retried.
        :param timeout: The timeout for this request.
        :param metadata: Strings which should be sent along with the request as metadata.
        """
        client = self.get_model_service_client(region)
        name = client.model_path(project_id, region, model_id)

        result = client.merge_version_aliases(
            request={
                "name": name,
                "version_aliases": ["default"],
            },
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return result

    @GoogleBaseHook.fallback_to_default_project_id
    def add_version_aliases(
        self,
        region: str,
        model_id: str,
        project_id: str,
        version_aliases: Sequence[str],
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> Model:
        """
        Add list of version aliases to specific version of Model.

        :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
        :param region: Required. The ID of the Google Cloud region that the service belongs to.
        :param model_id: Required. The ID of the Model to add aliases to.
        :param version_aliases: Required. List of version aliases to be added for specific version.
        :param retry: Designation of what errors, if any, should be retried.
        :param timeout: The timeout for this request.
        :param metadata: Strings which should be sent along with the request as metadata.
        """
        client = self.get_model_service_client(region)
        name = client.model_path(project_id, region, model_id)

        for alias in version_aliases:
            if alias.startswith("-"):
                raise AirflowException("Name of the alias can't start with '-'")

        result = client.merge_version_aliases(
            request={
                "name": name,
                "version_aliases": version_aliases,
            },
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return result

    @GoogleBaseHook.fallback_to_default_project_id
    def delete_version_aliases(
        self,
        region: str,
        model_id: str,
        project_id: str,
        version_aliases: Sequence[str],
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> Model:
        """
        Delete list of version aliases of specific version of Model.

        :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
        :param region: Required. The ID of the Google Cloud region that the service belongs to.
        :param model_id: Required. The ID of the Model to delete aliases from.
        :param version_aliases: Required. List of version aliases to be deleted from specific version.
        :param retry: Designation of what errors, if any, should be retried.
        :param timeout: The timeout for this request.
        :param metadata: Strings which should be sent along with the request as metadata.
        """
        client = self.get_model_service_client(region)
        name = client.model_path(project_id, region, model_id)
        if "default" in version_aliases:
            raise AirflowException(
                "Default alias can't be deleted. "
                "Make sure to assign this alias to another version before deletion"
            )
        aliases_for_delete = ["-" + alias for alias in version_aliases]

        result = client.merge_version_aliases(
            request={
                "name": name,
                "version_aliases": aliases_for_delete,
            },
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return result
