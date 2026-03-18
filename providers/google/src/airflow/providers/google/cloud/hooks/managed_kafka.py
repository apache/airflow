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
"""This module contains a Managed Service for Apache Kafka hook."""

from __future__ import annotations

import base64
import datetime
import json
import time
from collections.abc import Sequence
from copy import deepcopy
from typing import TYPE_CHECKING

from google.api_core.gapic_v1.method import DEFAULT, _MethodDefault
from google.auth.transport import requests as google_requests
from google.cloud.managedkafka_v1 import Cluster, ConsumerGroup, ManagedKafkaClient, Topic, types

from airflow.providers.common.compat.sdk import AirflowException
from airflow.providers.google.common.consts import CLIENT_INFO
from airflow.providers.google.common.hooks.base_google import GoogleBaseHook

if TYPE_CHECKING:
    from google.api_core.operation import Operation
    from google.api_core.retry import Retry
    from google.auth.credentials import Credentials
    from google.cloud.managedkafka_v1.services.managed_kafka.pagers import (
        ListClustersPager,
        ListConsumerGroupsPager,
        ListTopicsPager,
    )
    from google.protobuf.field_mask_pb2 import FieldMask


class ManagedKafkaTokenProvider:
    """Helper for providing authentication token for establishing connection via confluent to Apache Kafka cluster managed by Google Cloud."""

    def __init__(
        self,
        credentials: Credentials,
    ):
        self._credentials = credentials
        self._header = json.dumps(dict(typ="JWT", alg="GOOG_OAUTH2_TOKEN"))

    def _valid_credentials(self):
        if not self._credentials.valid:
            self._credentials.refresh(google_requests.Request())
            return self._credentials

    def _get_jwt(self, credentials):
        return json.dumps(
            dict(
                exp=credentials.expiry.timestamp(),
                iss="Google",
                iat=datetime.datetime.now(datetime.timezone.utc).timestamp(),
                scope="kafka",
                sub=credentials.service_account_email,
            )
        )

    def _b64_encode(self, source):
        return base64.urlsafe_b64encode(source.encode("utf-8")).decode("utf-8").rstrip("=")

    def _get_kafka_access_token(self, credentials):
        return ".".join(
            [
                self._b64_encode(self._header),
                self._b64_encode(self._get_jwt(credentials)),
                self._b64_encode(credentials.token),
            ]
        )

    def confluent_token(self):
        credentials = self._valid_credentials()

        utc_expiry = credentials.expiry.replace(tzinfo=datetime.timezone.utc)
        expiry_seconds = (utc_expiry - datetime.datetime.now(datetime.timezone.utc)).total_seconds()

        return self._get_kafka_access_token(credentials), time.time() + expiry_seconds


class ManagedKafkaHook(GoogleBaseHook):
    """Hook for Managed Service for Apache Kafka APIs."""

    def __init__(
        self,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(gcp_conn_id, impersonation_chain, **kwargs)

    def get_managed_kafka_client(self) -> ManagedKafkaClient:
        """Return ManagedKafkaClient object."""
        return ManagedKafkaClient(
            credentials=self.get_credentials(),
            client_info=CLIENT_INFO,
        )

    def wait_for_operation(self, operation: Operation, timeout: float | None = None):
        """Wait for long-lasting operation to complete."""
        try:
            return operation.result(timeout=timeout)
        except Exception:
            error = operation.exception(timeout=timeout)
            raise AirflowException(error)

    def get_confluent_token(self, config_str: str):
        """Get the authentication token for confluent client."""
        token_provider = ManagedKafkaTokenProvider(credentials=self.get_credentials())
        token = token_provider.confluent_token()
        return token

    @GoogleBaseHook.fallback_to_default_project_id
    def create_cluster(
        self,
        project_id: str,
        location: str,
        cluster: types.Cluster | dict,
        cluster_id: str,
        request_id: str | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> Operation:
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
        """
        client = self.get_managed_kafka_client()
        parent = client.common_location_path(project_id, location)

        operation = client.create_cluster(
            request={
                "parent": parent,
                "cluster_id": cluster_id,
                "cluster": cluster,
                "request_id": request_id,
            },
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return operation

    @GoogleBaseHook.fallback_to_default_project_id
    def list_clusters(
        self,
        project_id: str,
        location: str,
        page_size: int | None = None,
        page_token: str | None = None,
        filter: str | None = None,
        order_by: str | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> ListClustersPager:
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
        """
        client = self.get_managed_kafka_client()
        parent = client.common_location_path(project_id, location)

        result = client.list_clusters(
            request={
                "parent": parent,
                "page_size": page_size,
                "page_token": page_token,
                "filter": filter,
                "order_by": order_by,
            },
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return result

    @GoogleBaseHook.fallback_to_default_project_id
    def get_cluster(
        self,
        project_id: str,
        location: str,
        cluster_id: str,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> types.Cluster:
        """
        Return the properties of a single cluster.

        :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
        :param location: Required. The ID of the Google Cloud region that the service belongs to.
        :param cluster_id: Required. The ID of the cluster whose configuration to return.
        :param retry: Designation of what errors, if any, should be retried.
        :param timeout: The timeout for this request.
        :param metadata: Strings which should be sent along with the request as metadata.
        """
        client = self.get_managed_kafka_client()
        name = client.cluster_path(project_id, location, cluster_id)

        result = client.get_cluster(
            request={
                "name": name,
            },
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return result

    @GoogleBaseHook.fallback_to_default_project_id
    def update_cluster(
        self,
        project_id: str,
        location: str,
        cluster_id: str,
        cluster: types.Cluster | dict,
        update_mask: FieldMask | dict,
        request_id: str | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> Operation:
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
        """
        client = self.get_managed_kafka_client()
        _cluster = deepcopy(cluster) if isinstance(cluster, dict) else Cluster.to_dict(cluster)
        _cluster["name"] = client.cluster_path(project_id, location, cluster_id)

        operation = client.update_cluster(
            request={
                "update_mask": update_mask,
                "cluster": _cluster,
                "request_id": request_id,
            },
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return operation

    @GoogleBaseHook.fallback_to_default_project_id
    def delete_cluster(
        self,
        project_id: str,
        location: str,
        cluster_id: str,
        request_id: str | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> Operation:
        """
        Delete a single cluster.

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
        """
        client = self.get_managed_kafka_client()
        name = client.cluster_path(project_id, location, cluster_id)

        operation = client.delete_cluster(
            request={
                "name": name,
                "request_id": request_id,
            },
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return operation

    @GoogleBaseHook.fallback_to_default_project_id
    def create_topic(
        self,
        project_id: str,
        location: str,
        cluster_id: str,
        topic_id: str,
        topic: types.Topic | dict,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> types.Topic:
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
        """
        client = self.get_managed_kafka_client()
        parent = client.cluster_path(project_id, location, cluster_id)

        result = client.create_topic(
            request={
                "parent": parent,
                "topic_id": topic_id,
                "topic": topic,
            },
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return result

    @GoogleBaseHook.fallback_to_default_project_id
    def list_topics(
        self,
        project_id: str,
        location: str,
        cluster_id: str,
        page_size: int | None = None,
        page_token: str | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> ListTopicsPager:
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
        """
        client = self.get_managed_kafka_client()
        parent = client.cluster_path(project_id, location, cluster_id)

        result = client.list_topics(
            request={
                "parent": parent,
                "page_size": page_size,
                "page_token": page_token,
            },
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return result

    @GoogleBaseHook.fallback_to_default_project_id
    def get_topic(
        self,
        project_id: str,
        location: str,
        cluster_id: str,
        topic_id: str,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> types.Topic:
        """
        Return the properties of a single topic.

        :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
        :param location: Required. The ID of the Google Cloud region that the service belongs to.
        :param cluster_id: Required. The ID of the cluster whose topic is to be returned.
        :param topic_id: Required. The ID of the topic whose configuration to return.
        :param retry: Designation of what errors, if any, should be retried.
        :param timeout: The timeout for this request.
        :param metadata: Strings which should be sent along with the request as metadata.
        """
        client = self.get_managed_kafka_client()
        name = client.topic_path(project_id, location, cluster_id, topic_id)

        result = client.get_topic(
            request={
                "name": name,
            },
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return result

    @GoogleBaseHook.fallback_to_default_project_id
    def update_topic(
        self,
        project_id: str,
        location: str,
        cluster_id: str,
        topic_id: str,
        topic: types.Topic | dict,
        update_mask: FieldMask | dict,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> types.Topic:
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
        """
        client = self.get_managed_kafka_client()
        _topic = deepcopy(topic) if isinstance(topic, dict) else Topic.to_dict(topic)
        _topic["name"] = client.topic_path(project_id, location, cluster_id, topic_id)

        result = client.update_topic(
            request={
                "update_mask": update_mask,
                "topic": _topic,
            },
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return result

    @GoogleBaseHook.fallback_to_default_project_id
    def delete_topic(
        self,
        project_id: str,
        location: str,
        cluster_id: str,
        topic_id: str,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> None:
        """
        Delete a single topic.

        :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
        :param location: Required. The ID of the Google Cloud region that the service belongs to.
        :param cluster_id: Required. The ID of the cluster whose topic is to be deleted.
        :param topic_id: Required. The ID of the topic to delete.
        :param retry: Designation of what errors, if any, should be retried.
        :param timeout: The timeout for this request.
        :param metadata: Strings which should be sent along with the request as metadata.
        """
        client = self.get_managed_kafka_client()
        name = client.topic_path(project_id, location, cluster_id, topic_id)

        client.delete_topic(
            request={
                "name": name,
            },
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )

    @GoogleBaseHook.fallback_to_default_project_id
    def list_consumer_groups(
        self,
        project_id: str,
        location: str,
        cluster_id: str,
        page_size: int | None = None,
        page_token: str | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> ListConsumerGroupsPager:
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
        """
        client = self.get_managed_kafka_client()
        parent = client.cluster_path(project_id, location, cluster_id)

        result = client.list_consumer_groups(
            request={
                "parent": parent,
                "page_size": page_size,
                "page_token": page_token,
            },
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return result

    @GoogleBaseHook.fallback_to_default_project_id
    def get_consumer_group(
        self,
        project_id: str,
        location: str,
        cluster_id: str,
        consumer_group_id: str,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> types.ConsumerGroup:
        """
        Return the properties of a single consumer group.

        :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
        :param location: Required. The ID of the Google Cloud region that the service belongs to.
        :param cluster_id: Required. The ID of the cluster whose consumer group is to be returned.
        :param consumer_group_id: Required. The ID of the consumer group whose configuration to return.
        :param retry: Designation of what errors, if any, should be retried.
        :param timeout: The timeout for this request.
        :param metadata: Strings which should be sent along with the request as metadata.
        """
        client = self.get_managed_kafka_client()
        name = client.consumer_group_path(project_id, location, cluster_id, consumer_group_id)

        result = client.get_consumer_group(
            request={
                "name": name,
            },
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return result

    @GoogleBaseHook.fallback_to_default_project_id
    def update_consumer_group(
        self,
        project_id: str,
        location: str,
        cluster_id: str,
        consumer_group_id: str,
        consumer_group: types.ConsumerGroup | dict,
        update_mask: FieldMask | dict,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> types.ConsumerGroup:
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
        """
        client = self.get_managed_kafka_client()
        _consumer_group = (
            deepcopy(consumer_group)
            if isinstance(consumer_group, dict)
            else ConsumerGroup.to_dict(consumer_group)
        )
        _consumer_group["name"] = client.consumer_group_path(
            project_id, location, cluster_id, consumer_group_id
        )

        result = client.update_consumer_group(
            request={
                "update_mask": update_mask,
                "consumer_group": _consumer_group,
            },
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return result

    @GoogleBaseHook.fallback_to_default_project_id
    def delete_consumer_group(
        self,
        project_id: str,
        location: str,
        cluster_id: str,
        consumer_group_id: str,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
    ) -> None:
        """
        Delete a single consumer group.

        :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
        :param location: Required. The ID of the Google Cloud region that the service belongs to.
        :param cluster_id: Required. The ID of the cluster whose consumer group is to be deleted.
        :param consumer_group_id: Required. The ID of the consumer group to delete.
        :param retry: Designation of what errors, if any, should be retried.
        :param timeout: The timeout for this request.
        :param metadata: Strings which should be sent along with the request as metadata.
        """
        client = self.get_managed_kafka_client()
        name = client.consumer_group_path(project_id, location, cluster_id, consumer_group_id)

        client.delete_consumer_group(
            request={
                "name": name,
            },
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
