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
"""This module contains Amazon Managed Streaming for Apache Kafka hook."""

from __future__ import annotations

from typing import Any

from botocore.credentials import CredentialProvider, Credentials, ReadOnlyCredentials

from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.providers.common.compat.sdk import AirflowOptionalProviderFeatureException
from airflow.utils.helpers import prune_dict


def _get_msk_auth_token_provider():
    try:
        from aws_msk_iam_sasl_signer import MSKAuthTokenProvider
    except ImportError as e:
        raise AirflowOptionalProviderFeatureException(
            "The `aws-msk-iam-sasl-signer-python` package is required to generate Amazon MSK "
            "IAM OAuth tokens. Install it to use `MskHook.get_confluent_token`."
        ) from e
    return MSKAuthTokenProvider


class _StaticCredentialsProvider(CredentialProvider):
    """Credentials provider wrapper accepted by aws-msk-iam-sasl-signer-python."""

    def __init__(self, credentials: Credentials | ReadOnlyCredentials) -> None:
        self.credentials = credentials

    def load(self) -> Credentials:
        if isinstance(self.credentials, Credentials):
            return self.credentials
        return Credentials(
            access_key=self.credentials.access_key,
            secret_key=self.credentials.secret_key,
            token=self.credentials.token,
        )


class MskHook(AwsBaseHook):
    """
    Interact with Amazon Managed Streaming for Apache Kafka.

    Provide thin wrappers around :external+boto3:py:class:`boto3.client("kafka") <Kafka.Client>`.

    """

    def __init__(self, *args, **kwargs) -> None:
        kwargs["client_type"] = "kafka"
        super().__init__(*args, **kwargs)

    def _get_region_name(self) -> str:
        region_name = self.region_name or self.get_session().region_name
        if not region_name:
            raise ValueError("AWS region name is required to generate an Amazon MSK IAM OAuth token.")
        return region_name

    def _get_credentials_provider(
        self,
        credentials: CredentialProvider | Credentials | ReadOnlyCredentials | None = None,
        region_name: str | None = None,
    ) -> CredentialProvider:
        if isinstance(credentials, CredentialProvider):
            return credentials
        return _StaticCredentialsProvider(credentials or self.get_credentials(region_name=region_name))

    def _confluent_token(
        self,
        credentials: CredentialProvider | Credentials | ReadOnlyCredentials | None = None,
    ) -> tuple[str, float]:
        """
        Generate an Amazon MSK IAM OAuth token for ``confluent-kafka``.

        :param credentials: AWS credentials, or a botocore credentials provider, used to sign the token.
            When omitted, credentials are resolved from this hook's AWS connection.
        """
        region_name = self._get_region_name()
        credentials_provider = self._get_credentials_provider(credentials, region_name=region_name)
        token, expiration_ms = _get_msk_auth_token_provider().generate_auth_token_from_credentials_provider(
            region_name, credentials_provider
        )
        return token, expiration_ms / 1000

    def get_confluent_token(
        self,
        _oauth_config: str | None = None,
        *,
        credentials: CredentialProvider | Credentials | ReadOnlyCredentials | None = None,
    ) -> tuple[str, float]:
        """
        Get the authentication token for a ``confluent-kafka`` OAuth callback.

        :param credentials: AWS credentials, or a botocore credentials provider, used to sign the token.
            When omitted, credentials are resolved from this hook's AWS connection.
        """
        return self._confluent_token(credentials=credentials)

    def create_cluster(
        self,
        cluster_name: str,
        kafka_version: str,
        number_of_broker_nodes: int,
        broker_node_group_info: dict[str, Any],
        **kwargs: Any,
    ) -> dict[str, Any]:
        """
        Create a provisioned Amazon MSK cluster.

        .. seealso::
            - :external+boto3:py:meth:`Kafka.Client.create_cluster`

        :param cluster_name: Name of the cluster.
        :param kafka_version: Apache Kafka version for the cluster.
        :param number_of_broker_nodes: Number of broker nodes in the cluster.
        :param broker_node_group_info: Broker node group configuration.
        :param kwargs: Additional arguments passed to ``create_cluster``.
        """
        return self.conn.create_cluster(
            ClusterName=cluster_name,
            KafkaVersion=kafka_version,
            NumberOfBrokerNodes=number_of_broker_nodes,
            BrokerNodeGroupInfo=broker_node_group_info,
            **kwargs,
        )

    def list_clusters(
        self,
        cluster_name_filter: str | None = None,
        max_results: int | None = None,
        next_token: str | None = None,
    ) -> dict[str, Any]:
        """
        List provisioned Amazon MSK clusters.

        .. seealso::
            - :external+boto3:py:meth:`Kafka.Client.list_clusters`

        :param cluster_name_filter: Prefix filter for cluster names.
        :param max_results: Maximum number of clusters to return.
        :param next_token: Pagination token from a previous call.
        """
        return self.conn.list_clusters(
            **prune_dict(
                {
                    "ClusterNameFilter": cluster_name_filter,
                    "MaxResults": max_results,
                    "NextToken": next_token,
                }
            )
        )

    def get_cluster(self, cluster_arn: str) -> dict[str, Any]:
        """
        Get details for a provisioned Amazon MSK cluster.

        .. seealso::
            - :external+boto3:py:meth:`Kafka.Client.describe_cluster`

        :param cluster_arn: ARN that uniquely identifies the cluster.
        """
        return self.conn.describe_cluster(ClusterArn=cluster_arn)

    def update_cluster_configuration(
        self,
        cluster_arn: str,
        current_version: str,
        configuration_info: dict[str, Any],
    ) -> dict[str, Any]:
        """
        Update the configuration for a provisioned Amazon MSK cluster.

        .. seealso::
            - :external+boto3:py:meth:`Kafka.Client.update_cluster_configuration`

        :param cluster_arn: ARN that uniquely identifies the cluster.
        :param current_version: Current version of the cluster.
        :param configuration_info: Configuration to use for the brokers in the cluster.
        """
        return self.conn.update_cluster_configuration(
            ClusterArn=cluster_arn,
            CurrentVersion=current_version,
            ConfigurationInfo=configuration_info,
        )

    def delete_cluster(
        self,
        cluster_arn: str,
        current_version: str | None = None,
    ) -> dict[str, Any]:
        """
        Delete a provisioned Amazon MSK cluster.

        .. seealso::
            - :external+boto3:py:meth:`Kafka.Client.delete_cluster`

        :param cluster_arn: ARN that uniquely identifies the cluster.
        :param current_version: Current version of the cluster.
        """
        return self.conn.delete_cluster(
            **prune_dict(
                {
                    "ClusterArn": cluster_arn,
                    "CurrentVersion": current_version,
                }
            )
        )

    def create_topic(
        self,
        cluster_arn: str,
        topic_name: str,
        partition_count: int,
        replication_factor: int,
        configs: str | None = None,
    ) -> dict[str, Any]:
        """
        Create a topic in an Amazon MSK cluster.

        .. seealso::
            - :external+boto3:py:meth:`Kafka.Client.create_topic`

        :param cluster_arn: ARN that uniquely identifies the cluster.
        :param topic_name: Name of the topic to create.
        :param partition_count: Number of partitions for the topic.
        :param replication_factor: Replication factor for the topic.
        :param configs: Topic configurations encoded as a Base64 string.
        """
        create_topic_kwargs = {
            "ClusterArn": cluster_arn,
            "TopicName": topic_name,
            "PartitionCount": partition_count,
            "ReplicationFactor": replication_factor,
        }
        if configs is not None:
            create_topic_kwargs["Configs"] = configs

        return self.conn.create_topic(**create_topic_kwargs)

    def get_topic(
        self,
        cluster_arn: str,
        topic_name: str,
    ) -> dict[str, Any]:
        """
        Get details for a topic in an Amazon MSK cluster.

        .. seealso::
            - :external+boto3:py:meth:`Kafka.Client.describe_topic`

        :param cluster_arn: ARN that uniquely identifies the cluster.
        :param topic_name: Name of the topic to describe.
        """
        return self.conn.describe_topic(ClusterArn=cluster_arn, TopicName=topic_name)

    def update_topic(
        self,
        cluster_arn: str,
        topic_name: str,
        configs: str | None = None,
        partition_count: int | None = None,
    ) -> dict[str, Any]:
        """
        Update a topic in an Amazon MSK cluster.

        .. seealso::
            - :external+boto3:py:meth:`Kafka.Client.update_topic`

        :param cluster_arn: ARN that uniquely identifies the cluster.
        :param topic_name: Name of the topic to update.
        :param configs: New topic configurations encoded as a Base64 string.
        :param partition_count: New total number of partitions for the topic.
        """
        return self.conn.update_topic(
            **prune_dict(
                {
                    "ClusterArn": cluster_arn,
                    "TopicName": topic_name,
                    "Configs": configs,
                    "PartitionCount": partition_count,
                }
            )
        )

    def delete_topic(
        self,
        cluster_arn: str,
        topic_name: str,
    ) -> dict[str, Any]:
        """
        Delete a topic from an Amazon MSK cluster.

        .. seealso::
            - :external+boto3:py:meth:`Kafka.Client.delete_topic`

        :param cluster_arn: ARN that uniquely identifies the cluster.
        :param topic_name: Name of the topic to delete.
        """
        return self.conn.delete_topic(ClusterArn=cluster_arn, TopicName=topic_name)
