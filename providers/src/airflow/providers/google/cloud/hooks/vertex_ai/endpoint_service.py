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
from google.cloud.aiplatform_v1 import EndpointServiceClient

from airflow.exceptions import AirflowException
from airflow.providers.google.common.hooks.base_google import GoogleBaseHook

if TYPE_CHECKING:
    from google.api_core.operation import Operation
    from google.api_core.retry import Retry
    from google.cloud.aiplatform_v1.services.endpoint_service.pagers import (
        ListEndpointsPager,
    )
    from google.cloud.aiplatform_v1.types import DeployedModel, Endpoint
    from google.protobuf.field_mask_pb2 import FieldMask


class EndpointServiceHook(GoogleBaseHook):
    """Hook for Google Cloud Vertex AI Endpoint Service APIs."""

    def __init__(self, **kwargs):
        if kwargs.get("delegate_to") is not None:
            raise RuntimeError(
                "The `delegate_to` parameter has been deprecated before and finally removed in this version"
                " of Google Provider. You MUST convert it to `impersonate_chain`"
            )
        super().__init__(**kwargs)

    def get_endpoint_service_client(
        self, region: str | None = None
    ) -> EndpointServiceClient:
        """Return EndpointServiceClient."""
        if region and region != "global":
            client_options = ClientOptions(
                api_endpoint=f"{region}-aiplatform.googleapis.com:443"
            )
        else:
            client_options = ClientOptions()

        return EndpointServiceClient(
            credentials=self.get_credentials(),
            client_info=self.client_info,
            client_options=client_options,
        )

    def wait_for_operation(self, operation: Operation, timeout: float | None = None):
        """Wait for long-lasting operation to complete."""
        try:
            return operation.result(timeout=timeout)
        except Exception:
            error = operation.exception(timeout=timeout)
            raise AirflowException(error)

    @staticmethod
    def extract_endpoint_id(obj: dict) -> str:
        """Return unique id of the endpoint."""
        return obj["name"].rpartition("/")[-1]

    @staticmethod
    def extract_deployed_model_id(obj: dict) -> str:
        """Return unique id of the deploy model."""
        return obj["deployed_model"]["id"]

    @GoogleBaseHook.fallback_to_default_project_id
    def create_endpoint(
        self,
        project_id: str,
        region: str,
        endpoint: Endpoint | dict,
        endpoint_id: str | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> Operation:
        """
        Create an Endpoint.

        :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
        :param region: Required. The ID of the Google Cloud region that the service belongs to.
        :param endpoint: Required. The Endpoint to create.
        :param endpoint_id: The ID of Endpoint. This value should be 1-10 characters, and valid characters
            are /[0-9]/. If not provided, Vertex AI will generate a value for this ID.
        :param retry: Designation of what errors, if any, should be retried.
        :param timeout: The timeout for this request.
        :param metadata: Strings which should be sent along with the request as metadata.
        """
        client = self.get_endpoint_service_client(region)
        parent = client.common_location_path(project_id, region)

        result = client.create_endpoint(
            request={
                "parent": parent,
                "endpoint": endpoint,
                "endpoint_id": endpoint_id,
            },
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return result

    @GoogleBaseHook.fallback_to_default_project_id
    def delete_endpoint(
        self,
        project_id: str,
        region: str,
        endpoint: str,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> Operation:
        """
        Delete an Endpoint.

        :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
        :param region: Required. The ID of the Google Cloud region that the service belongs to.
        :param endpoint: Required. The Endpoint to delete.
        :param retry: Designation of what errors, if any, should be retried.
        :param timeout: The timeout for this request.
        :param metadata: Strings which should be sent along with the request as metadata.
        """
        client = self.get_endpoint_service_client(region)
        name = client.endpoint_path(project_id, region, endpoint)

        result = client.delete_endpoint(
            request={
                "name": name,
            },
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return result

    @GoogleBaseHook.fallback_to_default_project_id
    def deploy_model(
        self,
        project_id: str,
        region: str,
        endpoint: str,
        deployed_model: DeployedModel | dict,
        traffic_split: Sequence | dict | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> Operation:
        """
        Deploys a Model into this Endpoint, creating a DeployedModel within it.

        :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
        :param region: Required. The ID of the Google Cloud region that the service belongs to.
        :param endpoint:  Required. The name of the Endpoint resource into which to deploy a Model. Format:
            ``projects/{project}/locations/{location}/endpoints/{endpoint}``
        :param deployed_model:  Required. The DeployedModel to be created within the Endpoint. Note that
            [Endpoint.traffic_split][google.cloud.aiplatform.v1.Endpoint.traffic_split] must be updated for
            the DeployedModel to start receiving traffic, either as part of this call, or via
            [EndpointService.UpdateEndpoint][google.cloud.aiplatform.v1.EndpointService.UpdateEndpoint].
        :param traffic_split:  A map from a DeployedModel's ID to the percentage of this Endpoint's traffic
            that should be forwarded to that DeployedModel.

            If this field is non-empty, then the Endpoint's
            [traffic_split][google.cloud.aiplatform.v1.Endpoint.traffic_split] will be overwritten with it. To
            refer to the ID of the just being deployed Model, a "0" should be used, and the actual ID of the
            new DeployedModel will be filled in its place by this method. The traffic percentage values must
            add up to 100.

            If this field is empty, then the Endpoint's
            [traffic_split][google.cloud.aiplatform.v1.Endpoint.traffic_split] is not updated.
        :param retry: Designation of what errors, if any, should be retried.
        :param timeout: The timeout for this request.
        :param metadata: Strings which should be sent along with the request as metadata.
        """
        client = self.get_endpoint_service_client(region)
        endpoint_path = client.endpoint_path(project_id, region, endpoint)

        result = client.deploy_model(
            request={
                "endpoint": endpoint_path,
                "deployed_model": deployed_model,
                "traffic_split": traffic_split,
            },
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return result

    @GoogleBaseHook.fallback_to_default_project_id
    def get_endpoint(
        self,
        project_id: str,
        region: str,
        endpoint: str,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> Endpoint:
        """
        Get an Endpoint.

        :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
        :param region: Required. The ID of the Google Cloud region that the service belongs to.
        :param endpoint: Required. The Endpoint to get.
        :param retry: Designation of what errors, if any, should be retried.
        :param timeout: The timeout for this request.
        :param metadata: Strings which should be sent along with the request as metadata.
        """
        client = self.get_endpoint_service_client(region)
        name = client.endpoint_path(project_id, region, endpoint)

        result = client.get_endpoint(
            request={
                "name": name,
            },
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return result

    @GoogleBaseHook.fallback_to_default_project_id
    def list_endpoints(
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
    ) -> ListEndpointsPager:
        """
        List Endpoints in a Location.

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
        :param order_by: A comma-separated list of fields to order by, sorted in
            ascending order. Use "desc" after a field name for
            descending. Supported fields:

            -  ``display_name``
            -  ``create_time``
            -  ``update_time``

            Example: ``display_name, create_time desc``.
        :param retry: Designation of what errors, if any, should be retried.
        :param timeout: The timeout for this request.
        :param metadata: Strings which should be sent along with the request as metadata.
        """
        client = self.get_endpoint_service_client(region)
        parent = client.common_location_path(project_id, region)

        result = client.list_endpoints(
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
    def undeploy_model(
        self,
        project_id: str,
        region: str,
        endpoint: str,
        deployed_model_id: str,
        traffic_split: Sequence | dict | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> Operation:
        """
        Undeploys a Model from an Endpoint, removing a DeployedModel from it, and freeing all used resources.

        :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
        :param region: Required. The ID of the Google Cloud region that the service belongs to.
        :param endpoint:  Required. The name of the Endpoint resource from which to undeploy a Model.
        :param deployed_model_id:  Required. The ID of the DeployedModel to be undeployed from the Endpoint.
        :param traffic_split:  If this field is provided, then the Endpoint's
            [traffic_split][google.cloud.aiplatform.v1.Endpoint.traffic_split] will be overwritten with it. If
            last DeployedModel is being undeployed from the Endpoint, the [Endpoint.traffic_split] will always
            end up empty when this call returns. A DeployedModel will be successfully undeployed only if it
            doesn't have any traffic assigned to it when this method executes, or if this field unassigns any
            traffic to it.
        :param retry: Designation of what errors, if any, should be retried.
        :param timeout: The timeout for this request.
        :param metadata: Strings which should be sent along with the request as metadata.
        """
        client = self.get_endpoint_service_client(region)
        endpoint_path = client.endpoint_path(project_id, region, endpoint)

        result = client.undeploy_model(
            request={
                "endpoint": endpoint_path,
                "deployed_model_id": deployed_model_id,
                "traffic_split": traffic_split,
            },
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return result

    @GoogleBaseHook.fallback_to_default_project_id
    def update_endpoint(
        self,
        project_id: str,
        region: str,
        endpoint_id: str,
        endpoint: Endpoint | dict,
        update_mask: FieldMask | dict,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> Endpoint:
        """
        Update an Endpoint.

        :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
        :param region: Required. The ID of the Google Cloud region that the service belongs to.
        :param endpoint:  Required. The Endpoint which replaces the resource on the server.
        :param update_mask:  Required. The update mask applies to the resource. See
            [google.protobuf.FieldMask][google.protobuf.FieldMask].
        :param retry: Designation of what errors, if any, should be retried.
        :param timeout: The timeout for this request.
        :param metadata: Strings which should be sent along with the request as metadata.
        """
        client = self.get_endpoint_service_client(region)
        endpoint["name"] = client.endpoint_path(project_id, region, endpoint_id)

        result = client.update_endpoint(
            request={
                "endpoint": endpoint,
                "update_mask": update_mask,
            },
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return result
