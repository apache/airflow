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
"""This module contains Managed Service for Apache Kafka operators."""

from __future__ import annotations

from collections.abc import Sequence
from functools import cached_property
from typing import TYPE_CHECKING, Any

from google.api_core.exceptions import AlreadyExists, NotFound
from google.api_core.gapic_v1.method import DEFAULT, _MethodDefault
from google.cloud.managedkafka_v1 import types

from airflow.exceptions import AirflowException
from airflow.providers.google.cloud.hooks.managed_kafka import ManagedKafkaHook
from airflow.providers.google.cloud.links.managed_kafka import (
    ApacheKafkaClusterLink,
    ApacheKafkaClusterListLink,
    ApacheKafkaConsumerGroupLink,
    ApacheKafkaTopicLink,
)
from airflow.providers.google.cloud.operators.cloud_base import GoogleCloudBaseOperator

if TYPE_CHECKING:
    from google.api_core.retry import Retry
    from google.protobuf.field_mask_pb2 import FieldMask

    from airflow.providers.common.compat.sdk import Context


class ManagedKafkaBaseOperator(GoogleCloudBaseOperator):
    """
    Base class for Managed Kafka operators.

    :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
    :param location: Required. The ID of the Google Cloud region that the service belongs to.
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

    template_fields: Sequence[str] = (
        "location",
        "gcp_conn_id",
        "project_id",
        "impersonation_chain",
    )

    def __init__(
        self,
        project_id: str,
        location: str,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.location = location
        self.project_id = project_id
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    @cached_property
    def hook(self) -> ManagedKafkaHook:
        return ManagedKafkaHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )


class ManagedKafkaCreateClusterOperator(ManagedKafkaBaseOperator):
    """
    Create a new Apache Kafka cluster.

    :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
    :param location: Required. The ID of the Google Cloud region that the service belongs to.
    :param cluster: Required. Configuration of the cluster to create. Its ``name`` field is ignored.
    :param cluster_id: Required. The ID to use for the cluster, which will become the final component of
        the cluster's name. The ID must be 1-63 characters long, and match the regular expression
        ``[a-z]([-a-z0-9]*[a-z0-9])?`` to comply with RFC 1035. This value is structured like: ``my-cluster-id``.
    :param request_id: Optional. An optional request ID to identify requests. Specify a unique request ID
        to avoid duplication of requests. If a request times out or fails, retrying with the same ID
        allows the server to recognize the previous attempt. For at least 60 minutes, the server ignores
        duplicate requests bearing the same ID. For example, consider a situation where you make an
        initial request and the request times out. If you make the request again with the same request ID
        within 60 minutes of the last request, the server checks if an original operation with the same
        request ID was received. If so, the server ignores the second request. The request ID must be a
        valid UUID. A zero UUID is not supported (00000000-0000-0000-0000-000000000000).
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

    template_fields: Sequence[str] = tuple(
        {"cluster", "cluster_id"} | set(ManagedKafkaBaseOperator.template_fields)
    )
    operator_extra_links = (ApacheKafkaClusterLink(),)

    def __init__(
        self,
        cluster: types.Cluster | dict,
        cluster_id: str,
        request_id: str | None = None,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.cluster = cluster
        self.cluster_id = cluster_id
        self.request_id = request_id

    @property
    def extra_links_params(self) -> dict[str, Any]:
        return {
            "location": self.location,
            "cluster_id": self.cluster_id,
            "project_id": self.project_id,
        }

    def execute(self, context: Context):
        self.log.info("Creating an Apache Kafka cluster.")
        ApacheKafkaClusterLink.persist(context=context)
        try:
            operation = self.hook.create_cluster(
                project_id=self.project_id,
                location=self.location,
                cluster=self.cluster,
                cluster_id=self.cluster_id,
                request_id=self.request_id,
                retry=self.retry,
                timeout=self.timeout,
                metadata=self.metadata,
            )
            self.log.info("Waiting for operation to complete...")
            cluster = self.hook.wait_for_operation(operation=operation, timeout=self.timeout)
            self.log.info("Apache Kafka cluster was created.")
            return types.Cluster.to_dict(cluster)
        except AlreadyExists:
            self.log.info("Apache Kafka cluster %s already exists.", self.cluster_id)
            cluster = self.hook.get_cluster(
                project_id=self.project_id,
                location=self.location,
                cluster_id=self.cluster_id,
                retry=self.retry,
                timeout=self.timeout,
                metadata=self.metadata,
            )
            return types.Cluster.to_dict(cluster)


class ManagedKafkaListClustersOperator(ManagedKafkaBaseOperator):
    """
    List the clusters in a given project and location.

    :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
    :param location: Required. The ID of the Google Cloud region that the service belongs to.
    :param page_size: Optional. The maximum number of clusters to return. The service may return fewer
        than this value. If unspecified, server will pick an appropriate default.
    :param page_token: Optional. A page token, received from a previous ``ListClusters`` call. Provide
        this to retrieve the subsequent page.
        When paginating, all other parameters provided to ``ListClusters`` must match the call that
        provided the page token.
    :param filter: Optional. Filter expression for the result.
    :param order_by: Optional. Order by fields for the result.
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

    template_fields: Sequence[str] = tuple({"page_token"} | set(ManagedKafkaBaseOperator.template_fields))
    operator_extra_links = (ApacheKafkaClusterListLink(),)

    def __init__(
        self,
        page_size: int | None = None,
        page_token: str | None = None,
        filter: str | None = None,
        order_by: str | None = None,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.page_size = page_size
        self.page_token = page_token
        self.filter = filter
        self.order_by = order_by

    @property
    def extra_links_params(self) -> dict[str, Any]:
        return {
            "project_id": self.project_id,
        }

    def execute(self, context: Context):
        ApacheKafkaClusterListLink.persist(context=context)
        self.log.info("Listing Clusters from location %s.", self.location)
        try:
            cluster_list_pager = self.hook.list_clusters(
                project_id=self.project_id,
                location=self.location,
                page_size=self.page_size,
                page_token=self.page_token,
                filter=self.filter,
                order_by=self.order_by,
                retry=self.retry,
                timeout=self.timeout,
                metadata=self.metadata,
            )
            context["ti"].xcom_push(
                key="cluster_page",
                value=types.ListClustersResponse.to_dict(cluster_list_pager._response),
            )
        except Exception as error:
            raise AirflowException(error)
        return [types.Cluster.to_dict(cluster) for cluster in cluster_list_pager]


class ManagedKafkaGetClusterOperator(ManagedKafkaBaseOperator):
    """
    Get an Apache Kafka cluster.

    :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
    :param location: Required. The ID of the Google Cloud region that the service belongs to.
    :param cluster_id: Required. The ID of the cluster whose configuration to return.
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

    template_fields: Sequence[str] = tuple({"cluster_id"} | set(ManagedKafkaBaseOperator.template_fields))
    operator_extra_links = (ApacheKafkaClusterLink(),)

    def __init__(
        self,
        cluster_id: str,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.cluster_id = cluster_id

    @property
    def extra_links_params(self) -> dict[str, Any]:
        return {
            "location": self.location,
            "cluster_id": self.cluster_id,
            "project_id": self.project_id,
        }

    def execute(self, context: Context):
        ApacheKafkaClusterLink.persist(context=context)
        self.log.info("Getting Cluster: %s", self.cluster_id)
        try:
            cluster = self.hook.get_cluster(
                project_id=self.project_id,
                location=self.location,
                cluster_id=self.cluster_id,
                retry=self.retry,
                timeout=self.timeout,
                metadata=self.metadata,
            )
            self.log.info("Cluster was gotten.")
            return types.Cluster.to_dict(cluster)
        except NotFound as not_found_err:
            self.log.info("The Cluster %s does not exist.", self.cluster_id)
            raise AirflowException(not_found_err)


class ManagedKafkaUpdateClusterOperator(ManagedKafkaBaseOperator):
    """
    Update the properties of a single cluster.

    :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
    :param location: Required. The ID of the Google Cloud region that the service belongs to.
    :param cluster_id: Required. The ID of the cluster whose configuration to update.
    :param cluster: Required. The cluster to update.
    :param update_mask: Required. Field mask is used to specify the fields to be overwritten in the
        cluster resource by the update. The fields specified in the update_mask are relative to the
        resource, not the full request. A field will be overwritten if it is in the mask.
    :param request_id: Optional. An optional request ID to identify requests. Specify a unique request ID
        to avoid duplication of requests. If a request times out or fails, retrying with the same ID
        allows the server to recognize the previous attempt. For at least 60 minutes, the server ignores
        duplicate requests bearing the same ID.
        For example, consider a situation where you make an initial request and the request times out. If
        you make the request again with the same request ID within 60 minutes of the last request, the
        server checks if an original operation with the same request ID was received. If so, the server
        ignores the second request.
        The request ID must be a valid UUID. A zero UUID is not supported (00000000-0000-0000-0000-000000000000).
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

    template_fields: Sequence[str] = tuple(
        {"cluster_id", "cluster", "update_mask"} | set(ManagedKafkaBaseOperator.template_fields)
    )
    operator_extra_links = (ApacheKafkaClusterLink(),)

    def __init__(
        self,
        cluster_id: str,
        cluster: types.Cluster | dict,
        update_mask: FieldMask | dict,
        request_id: str | None = None,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.cluster_id = cluster_id
        self.cluster = cluster
        self.update_mask = update_mask
        self.request_id = request_id

    @property
    def extra_links_params(self) -> dict[str, Any]:
        return {
            "location": self.location,
            "cluster_id": self.cluster_id,
            "project_id": self.project_id,
        }

    def execute(self, context: Context):
        ApacheKafkaClusterLink.persist(context=context)
        self.log.info("Updating an Apache Kafka cluster.")
        try:
            operation = self.hook.update_cluster(
                project_id=self.project_id,
                location=self.location,
                cluster_id=self.cluster_id,
                cluster=self.cluster,
                update_mask=self.update_mask,
                request_id=self.request_id,
                retry=self.retry,
                timeout=self.timeout,
                metadata=self.metadata,
            )
            self.log.info("Waiting for operation to complete...")
            cluster = self.hook.wait_for_operation(operation=operation, timeout=self.timeout)
            self.log.info("Apache Kafka cluster %s was updated.", self.cluster_id)
            return types.Cluster.to_dict(cluster)
        except NotFound as not_found_err:
            self.log.info("The Cluster %s does not exist.", self.cluster_id)
            raise AirflowException(not_found_err)
        except Exception as error:
            raise AirflowException(error)


class ManagedKafkaDeleteClusterOperator(ManagedKafkaBaseOperator):
    """
    Delete an Apache Kafka cluster.

    :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
    :param location: Required. The ID of the Google Cloud region that the service belongs to.
    :param cluster_id: Required. The ID of the cluster to delete.
    :param request_id: Optional. An optional request ID to identify requests. Specify a unique request ID
        to avoid duplication of requests. If a request times out or fails, retrying with the same ID
        allows the server to recognize the previous attempt. For at least 60 minutes, the server ignores
        duplicate requests bearing the same ID.
        For example, consider a situation where you make an initial request and the request times out. If
        you make the request again with the same request ID within 60 minutes of the last request, the
        server checks if an original operation with the same request ID was received. If so, the server
        ignores the second request.
        The request ID must be a valid UUID. A zero UUID is not supported (00000000-0000-0000-0000-000000000000).
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

    template_fields: Sequence[str] = tuple({"cluster_id"} | set(ManagedKafkaBaseOperator.template_fields))

    def __init__(
        self,
        cluster_id: str,
        request_id: str | None = None,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.cluster_id = cluster_id
        self.request_id = request_id

    def execute(self, context: Context):
        try:
            self.log.info("Deleting Apache Kafka cluster: %s", self.cluster_id)
            operation = self.hook.delete_cluster(
                project_id=self.project_id,
                location=self.location,
                cluster_id=self.cluster_id,
                request_id=self.request_id,
                retry=self.retry,
                timeout=self.timeout,
                metadata=self.metadata,
            )
            self.log.info("Waiting for operation to complete...")
            self.hook.wait_for_operation(timeout=self.timeout, operation=operation)
            self.log.info("Apache Kafka cluster was deleted.")
        except NotFound as not_found_err:
            self.log.info("The Apache Kafka cluster ID %s does not exist.", self.cluster_id)
            raise AirflowException(not_found_err)


class ManagedKafkaCreateTopicOperator(ManagedKafkaBaseOperator):
    """
    Create a new topic in a given project and location.

    :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
    :param location: Required. The ID of the Google Cloud region that the service belongs to.
    :param cluster_id: Required. The ID of the cluster in which to create the topic.
    :param topic_id: Required. The ID to use for the topic, which will become the final component of the
        topic's name.
    :param topic: Required. Configuration of the topic to create.
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

    template_fields: Sequence[str] = tuple(
        {"cluster_id", "topic_id", "topic"} | set(ManagedKafkaBaseOperator.template_fields)
    )
    operator_extra_links = (ApacheKafkaTopicLink(),)

    def __init__(
        self,
        cluster_id: str,
        topic_id: str,
        topic: types.Topic | dict,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.cluster_id = cluster_id
        self.topic_id = topic_id
        self.topic = topic

    @property
    def extra_links_params(self) -> dict[str, Any]:
        return {
            "location": self.location,
            "cluster_id": self.cluster_id,
            "topic_id": self.topic_id,
            "project_id": self.project_id,
        }

    def execute(self, context: Context):
        self.log.info("Creating an Apache Kafka topic.")
        ApacheKafkaTopicLink.persist(context=context)
        try:
            topic_obj = self.hook.create_topic(
                project_id=self.project_id,
                location=self.location,
                cluster_id=self.cluster_id,
                topic_id=self.topic_id,
                topic=self.topic,
                retry=self.retry,
                timeout=self.timeout,
                metadata=self.metadata,
            )
            self.log.info("Apache Kafka topic for %s cluster was created.", self.cluster_id)
            return types.Topic.to_dict(topic_obj)
        except AlreadyExists:
            self.log.info("Apache Kafka topic %s already exists.", self.topic_id)
            topic_obj = self.hook.get_topic(
                project_id=self.project_id,
                location=self.location,
                cluster_id=self.cluster_id,
                topic_id=self.topic_id,
                retry=self.retry,
                timeout=self.timeout,
                metadata=self.metadata,
            )
            return types.Topic.to_dict(topic_obj)


class ManagedKafkaListTopicsOperator(ManagedKafkaBaseOperator):
    """
    List the topics in a given cluster.

    :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
    :param location: Required. The ID of the Google Cloud region that the service belongs to.
    :param cluster_id: Required. The ID of the cluster whose topics are to be listed.
    :param page_size: Optional. The maximum number of topics to return. The service may return fewer than
        this value. If unset or zero, all topics for the parent is returned.
    :param page_token: Optional. A page token, received from a previous ``ListTopics`` call. Provide this
        to retrieve the subsequent page. When paginating, all other parameters provided to ``ListTopics``
        must match the call that provided the page token.
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

    template_fields: Sequence[str] = tuple({"cluster_id"} | set(ManagedKafkaBaseOperator.template_fields))
    operator_extra_links = (ApacheKafkaClusterLink(),)

    def __init__(
        self,
        cluster_id: str,
        page_size: int | None = None,
        page_token: str | None = None,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.cluster_id = cluster_id
        self.page_size = page_size
        self.page_token = page_token

    @property
    def extra_links_params(self) -> dict[str, Any]:
        return {
            "location": self.location,
            "cluster_id": self.cluster_id,
            "project_id": self.project_id,
        }

    def execute(self, context: Context):
        ApacheKafkaClusterLink.persist(context=context)
        self.log.info("Listing Topics for cluster %s.", self.cluster_id)
        try:
            topic_list_pager = self.hook.list_topics(
                project_id=self.project_id,
                location=self.location,
                cluster_id=self.cluster_id,
                page_size=self.page_size,
                page_token=self.page_token,
                retry=self.retry,
                timeout=self.timeout,
                metadata=self.metadata,
            )
            context["ti"].xcom_push(
                key="topic_page",
                value=types.ListTopicsResponse.to_dict(topic_list_pager._response),
            )
        except Exception as error:
            raise AirflowException(error)
        return [types.Topic.to_dict(topic) for topic in topic_list_pager]


class ManagedKafkaGetTopicOperator(ManagedKafkaBaseOperator):
    """
    Return the properties of a single topic.

    :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
    :param location: Required. The ID of the Google Cloud region that the service belongs to.
    :param cluster_id: Required. The ID of the cluster whose topic is to be returned.
    :param topic_id: Required. The ID of the topic whose configuration to return.
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

    template_fields: Sequence[str] = tuple(
        {"cluster_id", "topic_id"} | set(ManagedKafkaBaseOperator.template_fields)
    )
    operator_extra_links = (ApacheKafkaTopicLink(),)

    def __init__(
        self,
        cluster_id: str,
        topic_id: str,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.cluster_id = cluster_id
        self.topic_id = topic_id

    @property
    def extra_links_params(self) -> dict[str, Any]:
        return {
            "location": self.location,
            "cluster_id": self.cluster_id,
            "topic_id": self.topic_id,
            "project_id": self.project_id,
        }

    def execute(self, context: Context):
        ApacheKafkaTopicLink.persist(context=context)
        self.log.info("Getting Topic: %s", self.topic_id)
        try:
            topic = self.hook.get_topic(
                project_id=self.project_id,
                location=self.location,
                cluster_id=self.cluster_id,
                topic_id=self.topic_id,
                retry=self.retry,
                timeout=self.timeout,
                metadata=self.metadata,
            )
            self.log.info("The topic %s from cluster %s was retrieved.", self.topic_id, self.cluster_id)
            return types.Topic.to_dict(topic)
        except NotFound as not_found_err:
            self.log.info("The Topic %s does not exist.", self.topic_id)
            raise AirflowException(not_found_err)


class ManagedKafkaUpdateTopicOperator(ManagedKafkaBaseOperator):
    """
    Update the properties of a single topic.

    :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
    :param location: Required. The ID of the Google Cloud region that the service belongs to.
    :param cluster_id: Required. The ID of the cluster whose topic is to be updated.
    :param topic_id: Required. The ID of the topic whose configuration to update.
    :param topic: Required. The topic to update. Its ``name`` field must be populated.
    :param update_mask: Required. Field mask is used to specify the fields to be overwritten in the Topic
        resource by the update. The fields specified in the update_mask are relative to the resource, not
        the full request. A field will be overwritten if it is in the mask.
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

    template_fields: Sequence[str] = tuple(
        {"cluster_id", "topic_id", "topic", "update_mask"} | set(ManagedKafkaBaseOperator.template_fields)
    )
    operator_extra_links = (ApacheKafkaTopicLink(),)

    def __init__(
        self,
        cluster_id: str,
        topic_id: str,
        topic: types.Topic | dict,
        update_mask: FieldMask | dict,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.cluster_id = cluster_id
        self.topic_id = topic_id
        self.topic = topic
        self.update_mask = update_mask

    @property
    def extra_links_params(self) -> dict[str, Any]:
        return {
            "location": self.location,
            "cluster_id": self.cluster_id,
            "topic_id": self.topic_id,
            "project_id": self.project_id,
        }

    def execute(self, context: Context):
        ApacheKafkaTopicLink.persist(context=context)
        self.log.info("Updating an Apache Kafka topic.")
        try:
            topic_obj = self.hook.update_topic(
                project_id=self.project_id,
                location=self.location,
                cluster_id=self.cluster_id,
                topic_id=self.topic_id,
                topic=self.topic,
                update_mask=self.update_mask,
                retry=self.retry,
                timeout=self.timeout,
                metadata=self.metadata,
            )
            self.log.info("Apache Kafka topic %s was updated.", self.topic_id)
            return types.Topic.to_dict(topic_obj)
        except NotFound as not_found_err:
            self.log.info("The Topic %s does not exist.", self.topic_id)
            raise AirflowException(not_found_err)
        except Exception as error:
            raise AirflowException(error)


class ManagedKafkaDeleteTopicOperator(ManagedKafkaBaseOperator):
    """
    Delete a single topic.

    :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
    :param location: Required. The ID of the Google Cloud region that the service belongs to.
    :param cluster_id: Required. The ID of the cluster whose topic is to be deleted.
    :param topic_id: Required. The ID of the topic to delete.
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

    template_fields: Sequence[str] = tuple(
        {"cluster_id", "topic_id"} | set(ManagedKafkaBaseOperator.template_fields)
    )

    def __init__(
        self,
        cluster_id: str,
        topic_id: str,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.cluster_id = cluster_id
        self.topic_id = topic_id

    def execute(self, context: Context):
        try:
            self.log.info("Deleting Apache Kafka topic: %s", self.topic_id)
            self.hook.delete_topic(
                project_id=self.project_id,
                location=self.location,
                cluster_id=self.cluster_id,
                topic_id=self.topic_id,
                retry=self.retry,
                timeout=self.timeout,
                metadata=self.metadata,
            )
            self.log.info("Apache Kafka topic was deleted.")
        except NotFound as not_found_err:
            self.log.info("The Apache Kafka topic ID %s does not exist.", self.topic_id)
            raise AirflowException(not_found_err)


class ManagedKafkaListConsumerGroupsOperator(ManagedKafkaBaseOperator):
    """
    List the consumer groups in a given cluster.

    :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
    :param location: Required. The ID of the Google Cloud region that the service belongs to.
    :param cluster_id: Required. The ID of the cluster whose consumer groups are to be listed.
    :param page_size: Optional. The maximum number of consumer groups to return. The service may return
        fewer than this value. If unset or zero, all consumer groups for the parent is returned.
    :param page_token: Optional. A page token, received from a previous ``ListConsumerGroups`` call.
        Provide this to retrieve the subsequent page. When paginating, all other parameters provided to
        ``ListConsumerGroups`` must match the call that provided the page token.
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

    template_fields: Sequence[str] = tuple({"cluster_id"} | set(ManagedKafkaBaseOperator.template_fields))
    operator_extra_links = (ApacheKafkaClusterLink(),)

    def __init__(
        self,
        cluster_id: str,
        page_size: int | None = None,
        page_token: str | None = None,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.cluster_id = cluster_id
        self.page_size = page_size
        self.page_token = page_token

    @property
    def extra_links_params(self) -> dict[str, Any]:
        return {
            "location": self.location,
            "cluster_id": self.cluster_id,
            "project_id": self.project_id,
        }

    def execute(self, context: Context):
        ApacheKafkaClusterLink.persist(context=context)
        self.log.info("Listing Consumer Groups for cluster %s.", self.cluster_id)
        try:
            consumer_group_list_pager = self.hook.list_consumer_groups(
                project_id=self.project_id,
                location=self.location,
                cluster_id=self.cluster_id,
                page_size=self.page_size,
                page_token=self.page_token,
                retry=self.retry,
                timeout=self.timeout,
                metadata=self.metadata,
            )
            context["ti"].xcom_push(
                key="consumer_group_page",
                value=types.ListConsumerGroupsResponse.to_dict(consumer_group_list_pager._response),
            )
        except Exception as error:
            raise AirflowException(error)
        return [types.ConsumerGroup.to_dict(consumer_group) for consumer_group in consumer_group_list_pager]


class ManagedKafkaGetConsumerGroupOperator(ManagedKafkaBaseOperator):
    """
    Return the properties of a single consumer group.

    :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
    :param location: Required. The ID of the Google Cloud region that the service belongs to.
    :param cluster_id: Required. The ID of the cluster whose consumer group is to be returned.
    :param consumer_group_id: Required. The ID of the consumer group whose configuration to return.
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

    template_fields: Sequence[str] = tuple(
        {"cluster_id", "consumer_group_id"} | set(ManagedKafkaBaseOperator.template_fields)
    )
    operator_extra_links = (ApacheKafkaConsumerGroupLink(),)

    def __init__(
        self,
        cluster_id: str,
        consumer_group_id: str,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.cluster_id = cluster_id
        self.consumer_group_id = consumer_group_id

    @property
    def extra_links_params(self) -> dict[str, Any]:
        return {
            "location": self.location,
            "cluster_id": self.cluster_id,
            "consumer_group_id": self.consumer_group_id,
            "project_id": self.project_id,
        }

    def execute(self, context: Context):
        ApacheKafkaConsumerGroupLink.persist(context=context)
        self.log.info("Getting Consumer Group: %s", self.consumer_group_id)
        try:
            consumer_group = self.hook.get_consumer_group(
                project_id=self.project_id,
                location=self.location,
                cluster_id=self.cluster_id,
                consumer_group_id=self.consumer_group_id,
                retry=self.retry,
                timeout=self.timeout,
                metadata=self.metadata,
            )
            self.log.info(
                "The consumer group %s from cluster %s was retrieved.",
                self.consumer_group_id,
                self.cluster_id,
            )
            return types.ConsumerGroup.to_dict(consumer_group)
        except NotFound as not_found_err:
            self.log.info("The Consumer Group %s does not exist.", self.consumer_group_id)
            raise AirflowException(not_found_err)


class ManagedKafkaUpdateConsumerGroupOperator(ManagedKafkaBaseOperator):
    """
    Update the properties of a single consumer group.

    :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
    :param location: Required. The ID of the Google Cloud region that the service belongs to.
    :param cluster_id: Required. The ID of the cluster whose topic is to be updated.
    :param consumer_group_id: Required. The ID of the consumer group whose configuration to update.
    :param consumer_group: Required. The consumer_group to update. Its ``name`` field must be populated.
    :param update_mask: Required. Field mask is used to specify the fields to be overwritten in the
        ConsumerGroup resource by the update. The fields specified in the update_mask are relative to the
        resource, not the full request. A field will be overwritten if it is in the mask.
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

    template_fields: Sequence[str] = tuple(
        {"cluster_id", "consumer_group_id", "consumer_group", "update_mask"}
        | set(ManagedKafkaBaseOperator.template_fields)
    )
    operator_extra_links = (ApacheKafkaConsumerGroupLink(),)

    def __init__(
        self,
        cluster_id: str,
        consumer_group_id: str,
        consumer_group: types.Topic | dict,
        update_mask: FieldMask | dict,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.cluster_id = cluster_id
        self.consumer_group_id = consumer_group_id
        self.consumer_group = consumer_group
        self.update_mask = update_mask

    @property
    def extra_links_params(self) -> dict[str, Any]:
        return {
            "location": self.location,
            "cluster_id": self.cluster_id,
            "consumer_group_id": self.consumer_group_id,
            "project_id": self.project_id,
        }

    def execute(self, context: Context):
        ApacheKafkaConsumerGroupLink.persist(context=context)
        self.log.info("Updating an Apache Kafka consumer group.")
        try:
            consumer_group_obj = self.hook.update_consumer_group(
                project_id=self.project_id,
                location=self.location,
                cluster_id=self.cluster_id,
                consumer_group_id=self.consumer_group_id,
                consumer_group=self.consumer_group,
                update_mask=self.update_mask,
                retry=self.retry,
                timeout=self.timeout,
                metadata=self.metadata,
            )
            self.log.info("Apache Kafka consumer group %s was updated.", self.consumer_group_id)
            return types.ConsumerGroup.to_dict(consumer_group_obj)
        except NotFound as not_found_err:
            self.log.info("The Consumer Group %s does not exist.", self.consumer_group_id)
            raise AirflowException(not_found_err)
        except Exception as error:
            raise AirflowException(error)


class ManagedKafkaDeleteConsumerGroupOperator(ManagedKafkaBaseOperator):
    """
    Delete a single consumer group.

    :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
    :param location: Required. The ID of the Google Cloud region that the service belongs to.
    :param cluster_id: Required. The ID of the cluster whose consumer group is to be deleted.
    :param consumer_group_id: Required. The ID of the consumer group to delete.
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

    template_fields: Sequence[str] = tuple(
        {"cluster_id", "consumer_group_id"} | set(ManagedKafkaBaseOperator.template_fields)
    )

    def __init__(
        self,
        cluster_id: str,
        consumer_group_id: str,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.cluster_id = cluster_id
        self.consumer_group_id = consumer_group_id

    def execute(self, context: Context):
        try:
            self.log.info("Deleting Apache Kafka consumer group: %s", self.consumer_group_id)
            self.hook.delete_consumer_group(
                project_id=self.project_id,
                location=self.location,
                cluster_id=self.cluster_id,
                consumer_group_id=self.consumer_group_id,
                retry=self.retry,
                timeout=self.timeout,
                metadata=self.metadata,
            )
            self.log.info("Apache Kafka consumer group was deleted.")
        except NotFound as not_found_err:
            self.log.info("The Apache Kafka consumer group ID %s does not exist.", self.consumer_group_id)
            raise AirflowException(not_found_err)
