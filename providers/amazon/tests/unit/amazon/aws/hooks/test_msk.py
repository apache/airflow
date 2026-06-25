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
from __future__ import annotations

from unittest import mock

from botocore.credentials import Credentials, ReadOnlyCredentials

from airflow.providers.amazon.aws.hooks.msk import (
    MskHook,
)

MSK_STRING = "airflow.providers.amazon.aws.hooks.msk.{}"

CLUSTER_ARN = "arn:aws:kafka:us-east-1:123456789012:cluster/test-cluster/uuid"
TEST_CONFIGS = "Y2xlYW51cC5wb2xpY3k9ZGVsZXRl"
BROKER_NODE_GROUP_INFO = {
    "ClientSubnets": ["subnet-1", "subnet-2"],
    "InstanceType": "kafka.m5.large",
}
CONFIGURATION_INFO = {
    "Arn": "arn:aws:kafka:us-east-1:123456789012:configuration/test-configuration/uuid",
    "Revision": 1,
}


class TestMskHook:
    def setup_method(self):
        self.hook = MskHook(region_name="us-east-1")

    def test_init(self):
        assert self.hook.client_type == "kafka"

    @mock.patch.object(MskHook, "conn")
    def test_create_cluster(self, mock_conn):
        result = self.hook.create_cluster(
            cluster_name="test-cluster",
            kafka_version="3.7.x",
            number_of_broker_nodes=2,
            broker_node_group_info=BROKER_NODE_GROUP_INFO,
            ClientAuthentication={"Sasl": {"Iam": {"Enabled": True}}},
        )

        assert mock_conn.mock_calls == [
            mock.call.create_cluster(
                ClusterName="test-cluster",
                KafkaVersion="3.7.x",
                NumberOfBrokerNodes=2,
                BrokerNodeGroupInfo=BROKER_NODE_GROUP_INFO,
                ClientAuthentication={"Sasl": {"Iam": {"Enabled": True}}},
            )
        ]
        assert result == mock_conn.create_cluster.return_value

    @mock.patch.object(MskHook, "conn")
    def test_list_clusters(self, mock_conn):
        self.hook.list_clusters(cluster_name_filter="test", max_results=25)

        assert mock_conn.mock_calls == [mock.call.list_clusters(ClusterNameFilter="test", MaxResults=25)]

    @mock.patch.object(MskHook, "conn")
    def test_get_cluster(self, mock_conn):
        result = self.hook.get_cluster(cluster_arn=CLUSTER_ARN)

        assert mock_conn.mock_calls == [mock.call.describe_cluster(ClusterArn=CLUSTER_ARN)]
        assert result == mock_conn.describe_cluster.return_value

    @mock.patch.object(MskHook, "conn")
    def test_update_cluster_configuration(self, mock_conn):
        result = self.hook.update_cluster_configuration(
            cluster_arn=CLUSTER_ARN,
            current_version="test_version",
            configuration_info=CONFIGURATION_INFO,
        )

        assert mock_conn.mock_calls == [
            mock.call.update_cluster_configuration(
                ClusterArn=CLUSTER_ARN,
                CurrentVersion="test_version",
                ConfigurationInfo=CONFIGURATION_INFO,
            )
        ]
        assert result == mock_conn.update_cluster_configuration.return_value

    @mock.patch.object(MskHook, "conn")
    def test_delete_cluster(self, mock_conn):
        result = self.hook.delete_cluster(cluster_arn=CLUSTER_ARN, current_version="test_version")

        assert mock_conn.mock_calls == [
            mock.call.delete_cluster(ClusterArn=CLUSTER_ARN, CurrentVersion="test_version")
        ]
        assert result == mock_conn.delete_cluster.return_value

    @mock.patch(MSK_STRING.format("_get_msk_auth_token_provider"))
    @mock.patch.object(MskHook, "get_credentials")
    def test_get_confluent_token_uses_hook_credentials(self, mock_get_credentials, mock_token_provider):
        mock_get_credentials.return_value = ReadOnlyCredentials("access-key", "secret-key", "session-token")
        mock_token_provider.return_value.generate_auth_token_from_credentials_provider.return_value = (
            "token",
            1234567890,
        )

        result = self.hook.get_confluent_token("oauth-config")

        assert result == ("token", 1234567.89)
        assert mock_get_credentials.mock_calls == [mock.call(region_name="us-east-1")]
        mock_generate = mock_token_provider.return_value.generate_auth_token_from_credentials_provider
        assert mock_generate.mock_calls == [mock.call("us-east-1", mock.ANY)]
        region_name, credentials_provider = mock_generate.call_args.args
        assert region_name == "us-east-1"
        credentials = credentials_provider.load()
        assert credentials.access_key == "access-key"
        assert credentials.secret_key == "secret-key"
        assert credentials.token == "session-token"

    @mock.patch(MSK_STRING.format("_get_msk_auth_token_provider"))
    @mock.patch.object(MskHook, "get_credentials")
    def test_get_confluent_token_uses_parameter_credentials(self, mock_get_credentials, mock_token_provider):
        credentials = Credentials("access-key", "secret-key", "session-token")
        mock_token_provider.return_value.generate_auth_token_from_credentials_provider.return_value = (
            "token",
            1234567890,
        )

        self.hook.get_confluent_token(credentials=credentials)

        mock_get_credentials.assert_not_called()
        credentials_provider = (
            mock_token_provider.return_value.generate_auth_token_from_credentials_provider.call_args.args[1]
        )
        assert credentials_provider.load() is credentials

    @mock.patch.object(MskHook, "conn")
    def test_create_topic(self, mock_conn):
        result = self.hook.create_topic(
            cluster_arn=CLUSTER_ARN,
            topic_name="test-topic",
            partition_count=3,
            replication_factor=2,
            configs=TEST_CONFIGS,
        )

        assert mock_conn.mock_calls == [
            mock.call.create_topic(
                ClusterArn=CLUSTER_ARN,
                TopicName="test-topic",
                PartitionCount=3,
                ReplicationFactor=2,
                Configs=TEST_CONFIGS,
            )
        ]
        assert result == mock_conn.create_topic.return_value

    @mock.patch.object(MskHook, "conn")
    def test_create_topic_without_configs(self, mock_conn):
        result = self.hook.create_topic(
            cluster_arn=CLUSTER_ARN,
            topic_name="test-topic",
            partition_count=3,
            replication_factor=2,
        )

        assert mock_conn.mock_calls == [
            mock.call.create_topic(
                ClusterArn=CLUSTER_ARN,
                TopicName="test-topic",
                PartitionCount=3,
                ReplicationFactor=2,
            )
        ]
        assert result == mock_conn.create_topic.return_value

    @mock.patch.object(MskHook, "conn")
    def test_get_topic(self, mock_conn):
        result = self.hook.get_topic(cluster_arn=CLUSTER_ARN, topic_name="test-topic")

        assert mock_conn.mock_calls == [
            mock.call.describe_topic(ClusterArn=CLUSTER_ARN, TopicName="test-topic")
        ]
        assert result == mock_conn.describe_topic.return_value

    @mock.patch.object(MskHook, "conn")
    def test_update_topic(self, mock_conn):
        result = self.hook.update_topic(
            cluster_arn=CLUSTER_ARN,
            topic_name="test-topic",
            configs=TEST_CONFIGS,
            partition_count=5,
        )

        assert mock_conn.mock_calls == [
            mock.call.update_topic(
                ClusterArn=CLUSTER_ARN,
                TopicName="test-topic",
                Configs=TEST_CONFIGS,
                PartitionCount=5,
            )
        ]
        assert result == mock_conn.update_topic.return_value

    @mock.patch.object(MskHook, "conn")
    def test_update_topic_without_optional_parameters(self, mock_conn):
        result = self.hook.update_topic(cluster_arn=CLUSTER_ARN, topic_name="test-topic")

        assert mock_conn.mock_calls == [
            mock.call.update_topic(ClusterArn=CLUSTER_ARN, TopicName="test-topic")
        ]
        assert result == mock_conn.update_topic.return_value

    @mock.patch.object(MskHook, "conn")
    def test_delete_topic(self, mock_conn):
        self.hook.delete_topic(cluster_arn=CLUSTER_ARN, topic_name="test-topic")

        assert mock_conn.mock_calls == [
            mock.call.delete_topic(ClusterArn=CLUSTER_ARN, TopicName="test-topic")
        ]
