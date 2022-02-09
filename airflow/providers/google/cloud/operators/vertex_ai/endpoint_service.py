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

from typing import TYPE_CHECKING, Dict, Optional, Sequence, Tuple, Union

from google.api_core.exceptions import NotFound
from google.api_core.retry import Retry
from google.cloud.aiplatform_v1.types import DeployedModel, Endpoint, endpoint_service
from google.protobuf.field_mask_pb2 import FieldMask

from airflow.models import BaseOperator, BaseOperatorLink
from airflow.models.xcom import XCom
from airflow.providers.google.cloud.hooks.vertex_ai.endpoint_service import EndpointServiceHook

if TYPE_CHECKING:
    from airflow.utils.context import Context

VERTEX_AI_BASE_LINK = "https://console.cloud.google.com/vertex-ai"
VERTEX_AI_ENDPOINT_LINK = (
    VERTEX_AI_BASE_LINK + "/locations/{region}/endpoints/{endpoint_id}?project={project_id}"
)
VERTEX_AI_ENDPOINT_LIST_LINK = VERTEX_AI_BASE_LINK + "/endpoints?project={project_id}"


class VertexAIEndpointLink(BaseOperatorLink):
    """Helper class for constructing Vertex AI Endpoint link"""

    name = "Endpoint"

    def get_link(self, operator, dttm):
        endpoint_conf = XCom.get_one(
            key='endpoint_conf', dag_id=operator.dag.dag_id, task_id=operator.task_id, execution_date=dttm
        )
        return (
            VERTEX_AI_ENDPOINT_LINK.format(
                region=endpoint_conf["region"],
                endpoint_id=endpoint_conf["endpoint_id"],
                project_id=endpoint_conf["project_id"],
            )
            if endpoint_conf
            else ""
        )


class VertexAIEndpointListLink(BaseOperatorLink):
    """Helper class for constructing Vertex AI EndpointList link"""

    name = "Endpoint List"

    def get_link(self, operator, dttm):
        project_id = XCom.get_one(
            key='project_id', dag_id=operator.dag.dag_id, task_id=operator.task_id, execution_date=dttm
        )
        return (
            VERTEX_AI_ENDPOINT_LIST_LINK.format(
                project_id=project_id,
            )
            if project_id
            else ""
        )


class CreateEndpointOperator(BaseOperator):
    """
    Creates an Endpoint.

    :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
    :param region: Required. The ID of the Google Cloud region that the service belongs to.
    :param endpoint: Required. The Endpoint to create.
    :param retry: Designation of what errors, if any, should be retried.
    :param timeout: The timeout for this request.
    :param metadata: Strings which should be sent along with the request as metadata.
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud.
    :param delegate_to: The account to impersonate using domain-wide delegation of authority,
        if any. For this to work, the service account making the request must have
        domain-wide delegation enabled.
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
    operator_extra_links = (VertexAIEndpointLink(),)

    def __init__(
        self,
        *,
        region: str,
        project_id: str,
        endpoint: Endpoint,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = "",
        gcp_conn_id: str = "google_cloud_default",
        delegate_to: Optional[str] = None,
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.region = region
        self.project_id = project_id
        self.endpoint = endpoint
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to
        self.impersonation_chain = impersonation_chain

    def execute(self, context: 'Context'):
        hook = EndpointServiceHook(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to,
            impersonation_chain=self.impersonation_chain,
        )

        self.log.info("Creating endpoint")
        operation = hook.create_endpoint(
            project_id=self.project_id,
            region=self.region,
            endpoint=self.endpoint,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )
        result = hook.wait_for_operation(timeout=self.timeout, operation=operation)

        endpoint = Endpoint.to_dict(result)
        endpoint_id = hook.extract_endpoint_id(endpoint)
        self.log.info("Endpoint was created. Endpoint ID: %s", endpoint_id)

        self.xcom_push(context, key="endpoint_id", value=endpoint_id)
        self.xcom_push(
            context,
            key="endpoint_conf",
            value={
                "endpoint_id": endpoint_id,
                "region": self.region,
                "project_id": self.project_id,
            },
        )
        return endpoint


class DeleteEndpointOperator(BaseOperator):
    """
    Deletes an Endpoint.

    :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
    :param region: Required. The ID of the Google Cloud region that the service belongs to.
    :param endpoint_id: Required. The Endpoint ID to delete.
    :param retry: Designation of what errors, if any, should be retried.
    :param timeout: The timeout for this request.
    :param metadata: Strings which should be sent along with the request as metadata.
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud.
    :param delegate_to: The account to impersonate using domain-wide delegation of authority,
        if any. For this to work, the service account making the request must have
        domain-wide delegation enabled.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    template_fields = ("region", "endpoint_id", "project_id", "impersonation_chain")

    def __init__(
        self,
        *,
        region: str,
        project_id: str,
        endpoint_id: str,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = "",
        gcp_conn_id: str = "google_cloud_default",
        delegate_to: Optional[str] = None,
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.region = region
        self.project_id = project_id
        self.endpoint_id = endpoint_id
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to
        self.impersonation_chain = impersonation_chain

    def execute(self, context: 'Context'):
        hook = EndpointServiceHook(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to,
            impersonation_chain=self.impersonation_chain,
        )

        try:
            self.log.info("Deleting endpoint: %s", self.endpoint_id)
            operation = hook.delete_endpoint(
                project_id=self.project_id,
                region=self.region,
                endpoint=self.endpoint_id,
                retry=self.retry,
                timeout=self.timeout,
                metadata=self.metadata,
            )
            hook.wait_for_operation(timeout=self.timeout, operation=operation)
            self.log.info("Endpoint was deleted.")
        except NotFound:
            self.log.info("The Endpoint ID %s does not exist.", self.endpoint_id)


class DeployModelOperator(BaseOperator):
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
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud.
    :param delegate_to: The account to impersonate using domain-wide delegation of authority,
        if any. For this to work, the service account making the request must have
        domain-wide delegation enabled.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    template_fields = ("region", "endpoint_id", "project_id", "impersonation_chain")

    def __init__(
        self,
        *,
        region: str,
        project_id: str,
        endpoint_id: str,
        deployed_model: DeployedModel,
        traffic_split: Sequence[str] = None,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = "",
        gcp_conn_id: str = "google_cloud_default",
        delegate_to: Optional[str] = None,
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.region = region
        self.project_id = project_id
        self.endpoint_id = endpoint_id
        self.deployed_model = deployed_model
        self.traffic_split = traffic_split
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to
        self.impersonation_chain = impersonation_chain

    def execute(self, context: 'Context'):
        hook = EndpointServiceHook(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to,
            impersonation_chain=self.impersonation_chain,
        )

        self.log.info("Deploying model")
        operation = hook.deploy_model(
            project_id=self.project_id,
            region=self.region,
            endpoint=self.endpoint_id,
            deployed_model=self.deployed_model,
            traffic_split=self.traffic_split,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )
        hook.wait_for_operation(timeout=self.timeout, operation=operation)
        self.log.info("Deploy was done successfully")


class GetEndpointOperator(BaseOperator):
    """
    Gets an Endpoint.

    :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
    :param region: Required. The ID of the Google Cloud region that the service belongs to.
    :param endpoint_id: Required. The Endpoint ID to get.
    :param retry: Designation of what errors, if any, should be retried.
    :param timeout: The timeout for this request.
    :param metadata: Strings which should be sent along with the request as metadata.
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud.
    :param delegate_to: The account to impersonate using domain-wide delegation of authority,
        if any. For this to work, the service account making the request must have
        domain-wide delegation enabled.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    template_fields = ("region", "endpoint_id", "project_id", "impersonation_chain")
    operator_extra_links = (VertexAIEndpointLink(),)

    def __init__(
        self,
        *,
        region: str,
        project_id: str,
        endpoint_id: str,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Sequence[Tuple[str, str]] = (),
        gcp_conn_id: str = "google_cloud_default",
        delegate_to: Optional[str] = None,
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.region = region
        self.project_id = project_id
        self.endpoint_id = endpoint_id
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to
        self.impersonation_chain = impersonation_chain

    def execute(self, context: 'Context'):
        hook = EndpointServiceHook(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to,
            impersonation_chain=self.impersonation_chain,
        )

        try:
            self.log.info("Get endpoint: %s", self.endpoint_id)
            endpoint_obj = hook.get_endpoint(
                project_id=self.project_id,
                region=self.region,
                endpoint=self.endpoint_id,
                retry=self.retry,
                timeout=self.timeout,
                metadata=self.metadata,
            )
            self.xcom_push(
                context,
                key="endpoint_conf",
                value={
                    "endpoint_id": self.endpoint_id,
                    "project_id": self.project_id,
                    "region": self.region,
                },
            )
            self.log.info("Endpoint was gotten.")
            return Endpoint.to_dict(endpoint_obj)
        except NotFound:
            self.log.info("The Endpoint ID %s does not exist.", self.endpoint_id)


class ListEndpointsOperator(BaseOperator):
    """
    Lists Endpoints in a Location.

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
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud.
    :param delegate_to: The account to impersonate using domain-wide delegation of authority,
        if any. For this to work, the service account making the request must have
        domain-wide delegation enabled.
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
    operator_extra_links = (VertexAIEndpointListLink(),)

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
        metadata: Sequence[Tuple[str, str]] = (),
        gcp_conn_id: str = "google_cloud_default",
        delegate_to: Optional[str] = None,
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
        self.delegate_to = delegate_to
        self.impersonation_chain = impersonation_chain

    def execute(self, context: 'Context'):
        hook = EndpointServiceHook(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to,
            impersonation_chain=self.impersonation_chain,
        )
        results = hook.list_endpoints(
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
        return [Endpoint.to_dict(result) for result in results]


class EndpointServiceJobUndeployModelOperator(BaseOperator):
    """
    Undeploys a Model from an Endpoint, removing a DeployedModel from it, and freeing all resources it's
    using.

    :param request:  The request object. Request message for
        [EndpointService.UndeployModel][google.cloud.aiplatform.v1.EndpointService.UndeployModel].
    :type request: Union[google.cloud.aiplatform_v1.types.UndeployModelRequest, Dict]
    :param endpoint:  Required. The name of the Endpoint resource from which to undeploy a Model. Format:
        ``projects/{project}/locations/{location}/endpoints/{endpoint}``

        This corresponds to the ``endpoint`` field on the ``request`` instance; if ``request`` is provided,
        this should not be set.
    :type endpoint: str
    :param deployed_model_id:  Required. The ID of the DeployedModel to be undeployed from the Endpoint.

        This corresponds to the ``deployed_model_id`` field on the ``request`` instance; if ``request`` is
        provided, this should not be set.
    :type deployed_model_id: str
    :param traffic_split:  If this field is provided, then the Endpoint's
        [traffic_split][google.cloud.aiplatform.v1.Endpoint.traffic_split] will be overwritten with it. If
        last DeployedModel is being undeployed from the Endpoint, the [Endpoint.traffic_split] will always end
        up empty when this call returns. A DeployedModel will be successfully undeployed only if it doesn't
        have any traffic assigned to it when this method executes, or if this field unassigns any traffic to
        it.

        This corresponds to the ``traffic_split`` field on the ``request`` instance; if ``request`` is
        provided, this should not be set.
    :type traffic_split: Sequence[google.cloud.aiplatform_v1.types.UndeployModelRequest.TrafficSplitEntry]
    :param retry: Designation of what errors, if any, should be retried.
    :type retry: google.api_core.retry.Retry
    :param timeout: The timeout for this request.
    :type timeout: float
    :param metadata: Strings which should be sent along with the request as metadata.
    :type metadata: Sequence[Tuple[str, str]]
    :param gcp_conn_id:
    :type gcp_conn_id: str
    """

    def __init__(
        self,
        *,
        region: str,
        project_id: str,
        endpoint: str,
        deployed_model_id: str,
        traffic_split: Sequence[endpoint_service.UndeployModelRequest.TrafficSplitEntry],
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = "",
        gcp_conn_id: str = "google_cloud_default",
        delegate_to: Optional[str] = None,
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.request = request
        self.endpoint = endpoint
        self.deployed_model_id = deployed_model_id
        self.traffic_split = traffic_split
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id

    def execute(self, context: Dict):
        hook = EndpointServiceHook(gcp_conn_id=self.gcp_conn_id)
        hook.undeploy_model(
            request=self.request,
            endpoint=self.endpoint,
            deployed_model_id=self.deployed_model_id,
            traffic_split=self.traffic_split,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )


class UpdateEndpointOperator(BaseOperator):
    """
    Updates an Endpoint.

    :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
    :param region: Required. The ID of the Google Cloud region that the service belongs to.
    :param endpoint_id: Required. The ID of the Endpoint to update.
    :param endpoint:  Required. The Endpoint which replaces the resource on the server.
    :param update_mask:  Required. The update mask applies to the resource. See
        [google.protobuf.FieldMask][google.protobuf.FieldMask].
    :param retry: Designation of what errors, if any, should be retried.
    :param timeout: The timeout for this request.
    :param metadata: Strings which should be sent along with the request as metadata.
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud.
    :param delegate_to: The account to impersonate using domain-wide delegation of authority,
        if any. For this to work, the service account making the request must have
        domain-wide delegation enabled.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    template_fields = ("region", "endpoint_id", "project_id", "impersonation_chain")

    def __init__(
        self,
        *,
        project_id: str,
        region: str,
        endpoint_id: str,
        endpoint: Union[Endpoint, Dict],
        update_mask: Union[FieldMask, Dict],
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Sequence[Tuple[str, str]] = (),
        gcp_conn_id: str = "google_cloud_default",
        delegate_to: Optional[str] = None,
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.project_id = project_id
        self.region = region
        self.endpoint_id = endpoint_id
        self.endpoint = endpoint
        self.update_mask = update_mask
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to
        self.impersonation_chain = impersonation_chain

    def execute(self, context: 'Context'):
        hook = EndpointServiceHook(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to,
            impersonation_chain=self.impersonation_chain,
        )
        self.log.info("Updating endpoint: %s", self.endpoint_id)
        result = hook.update_endpoint(
            project_id=self.project_id,
            region=self.region,
            endpoint_id=self.endpoint_id,
            endpoint=self.endpoint,
            update_mask=self.update_mask,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )
        self.log.info("Endpoint was updated")
        return Endpoint.to_dict(result)
