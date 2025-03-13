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

from google.api_core.retry import Retry

from airflow.providers.google.cloud.operators.managed_kafka import (
    ManagedKafkaCreateClusterOperator,
    ManagedKafkaCreateTopicOperator,
    ManagedKafkaDeleteClusterOperator,
    ManagedKafkaDeleteConsumerGroupOperator,
    ManagedKafkaDeleteTopicOperator,
    ManagedKafkaGetClusterOperator,
    ManagedKafkaGetConsumerGroupOperator,
    ManagedKafkaGetTopicOperator,
    ManagedKafkaListClustersOperator,
    ManagedKafkaListConsumerGroupsOperator,
    ManagedKafkaListTopicsOperator,
    ManagedKafkaUpdateClusterOperator,
    ManagedKafkaUpdateConsumerGroupOperator,
    ManagedKafkaUpdateTopicOperator,
)

MANAGED_KAFKA_PATH = "airflow.providers.google.cloud.operators.managed_kafka.{}"
TIMEOUT = 120
RETRY = mock.MagicMock(Retry)
METADATA = [("key", "value")]

TASK_ID = "test_task_id"
GCP_PROJECT = "test-project"
GCP_LOCATION = "test-location"
GCP_CONN_ID = "test-conn"
IMPERSONATION_CHAIN = ["ACCOUNT_1", "ACCOUNT_2", "ACCOUNT_3"]

TEST_CLUSTER_ID: str = "test-cluster-id"
TEST_CLUSTER: dict = {
    "gcp_config": {
        "access_config": {
            "network_configs": {
                "subnet": "subnet_value",
            },
        },
    },
    "capacity_config": {
        "vcpu_count": 1094,
        "memory_bytes": 1311,
    },
}
TEST_CLUSTER_UPDATE_MASK: dict = {"paths": ["gcp_config.access_config.network_configs.subnet"]}
TEST_UPDATED_CLUSTER: dict = {
    "gcp_config": {
        "access_config": {
            "network_configs": {
                "subnet": "new_subnet_value",
            },
        },
    },
}

TEST_TOPIC_ID: str = "test-topic-id"
TEST_TOPIC: dict = {
    "partition_count": 1634,
    "replication_factor": 1912,
}
TEST_TOPIC_UPDATE_MASK: dict = {"paths": ["partition_count"]}
TEST_UPDATED_TOPIC: dict = {
    "partition_count": 2000,
    "replication_factor": 1912,
}

TEST_CONSUMER_GROUP_ID: str = "test-consumer-group-id"


class TestManagedKafkaCreateClusterOperator:
    @mock.patch(MANAGED_KAFKA_PATH.format("types.Cluster.to_dict"))
    @mock.patch(MANAGED_KAFKA_PATH.format("ManagedKafkaHook"))
    def test_execute(self, mock_hook, to_dict_mock):
        op = ManagedKafkaCreateClusterOperator(
            task_id=TASK_ID,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
            location=GCP_LOCATION,
            project_id=GCP_PROJECT,
            cluster=TEST_CLUSTER,
            cluster_id=TEST_CLUSTER_ID,
            request_id=None,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )
        op.execute(context={"ti": mock.MagicMock()})
        mock_hook.assert_called_once_with(gcp_conn_id=GCP_CONN_ID, impersonation_chain=IMPERSONATION_CHAIN)
        mock_hook.return_value.create_cluster.assert_called_once_with(
            location=GCP_LOCATION,
            project_id=GCP_PROJECT,
            cluster=TEST_CLUSTER,
            cluster_id=TEST_CLUSTER_ID,
            request_id=None,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )


class TestManagedKafkaListClustersOperator:
    @mock.patch(MANAGED_KAFKA_PATH.format("types.ListClustersResponse.to_dict"))
    @mock.patch(MANAGED_KAFKA_PATH.format("types.Cluster.to_dict"))
    @mock.patch(MANAGED_KAFKA_PATH.format("ManagedKafkaHook"))
    def test_execute(self, mock_hook, to_cluster_dict_mock, to_clusters_dict_mock):
        page_token = "page_token"
        page_size = 42
        filter = "filter"
        order_by = "order_by"

        op = ManagedKafkaListClustersOperator(
            task_id=TASK_ID,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
            location=GCP_LOCATION,
            project_id=GCP_PROJECT,
            page_size=page_size,
            page_token=page_token,
            filter=filter,
            order_by=order_by,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )
        op.execute(context={"ti": mock.MagicMock()})
        mock_hook.assert_called_once_with(gcp_conn_id=GCP_CONN_ID, impersonation_chain=IMPERSONATION_CHAIN)
        mock_hook.return_value.list_clusters.assert_called_once_with(
            location=GCP_LOCATION,
            project_id=GCP_PROJECT,
            page_size=page_size,
            page_token=page_token,
            filter=filter,
            order_by=order_by,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )


class TestManagedKafkaGetClusterOperator:
    @mock.patch(MANAGED_KAFKA_PATH.format("types.Cluster.to_dict"))
    @mock.patch(MANAGED_KAFKA_PATH.format("ManagedKafkaHook"))
    def test_execute(self, mock_hook, to_dict_mock):
        op = ManagedKafkaGetClusterOperator(
            task_id=TASK_ID,
            cluster_id=TEST_CLUSTER_ID,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
            location=GCP_LOCATION,
            project_id=GCP_PROJECT,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )
        op.execute(context={"ti": mock.MagicMock()})
        mock_hook.assert_called_once_with(gcp_conn_id=GCP_CONN_ID, impersonation_chain=IMPERSONATION_CHAIN)
        mock_hook.return_value.get_cluster.assert_called_once_with(
            location=GCP_LOCATION,
            project_id=GCP_PROJECT,
            cluster_id=TEST_CLUSTER_ID,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )


class TestManagedKafkaUpdateClusterOperator:
    @mock.patch(MANAGED_KAFKA_PATH.format("types.Cluster.to_dict"))
    @mock.patch(MANAGED_KAFKA_PATH.format("ManagedKafkaHook"))
    def test_execute(self, mock_hook, to_dict_mock):
        op = ManagedKafkaUpdateClusterOperator(
            task_id=TASK_ID,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
            project_id=GCP_PROJECT,
            location=GCP_LOCATION,
            cluster_id=TEST_CLUSTER_ID,
            cluster=TEST_UPDATED_CLUSTER,
            update_mask=TEST_CLUSTER_UPDATE_MASK,
            request_id=None,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )
        op.execute(context={"ti": mock.MagicMock()})
        mock_hook.assert_called_once_with(gcp_conn_id=GCP_CONN_ID, impersonation_chain=IMPERSONATION_CHAIN)
        mock_hook.return_value.update_cluster.assert_called_once_with(
            project_id=GCP_PROJECT,
            location=GCP_LOCATION,
            cluster_id=TEST_CLUSTER_ID,
            cluster=TEST_UPDATED_CLUSTER,
            update_mask=TEST_CLUSTER_UPDATE_MASK,
            request_id=None,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )


class TestManagedKafkaDeleteClusterOperator:
    @mock.patch(MANAGED_KAFKA_PATH.format("ManagedKafkaHook"))
    def test_execute(self, mock_hook):
        op = ManagedKafkaDeleteClusterOperator(
            task_id=TASK_ID,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
            location=GCP_LOCATION,
            project_id=GCP_PROJECT,
            cluster_id=TEST_CLUSTER_ID,
            request_id=None,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )
        op.execute(context={})
        mock_hook.assert_called_once_with(gcp_conn_id=GCP_CONN_ID, impersonation_chain=IMPERSONATION_CHAIN)
        mock_hook.return_value.delete_cluster.assert_called_once_with(
            location=GCP_LOCATION,
            project_id=GCP_PROJECT,
            cluster_id=TEST_CLUSTER_ID,
            request_id=None,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )


class TestManagedKafkaCreateTopicOperator:
    @mock.patch(MANAGED_KAFKA_PATH.format("types.Topic.to_dict"))
    @mock.patch(MANAGED_KAFKA_PATH.format("ManagedKafkaHook"))
    def test_execute(self, mock_hook, to_dict_mock):
        op = ManagedKafkaCreateTopicOperator(
            task_id=TASK_ID,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
            location=GCP_LOCATION,
            project_id=GCP_PROJECT,
            cluster_id=TEST_CLUSTER_ID,
            topic_id=TEST_TOPIC_ID,
            topic=TEST_TOPIC,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )
        op.execute(context={"ti": mock.MagicMock()})
        mock_hook.assert_called_once_with(gcp_conn_id=GCP_CONN_ID, impersonation_chain=IMPERSONATION_CHAIN)
        mock_hook.return_value.create_topic.assert_called_once_with(
            location=GCP_LOCATION,
            project_id=GCP_PROJECT,
            cluster_id=TEST_CLUSTER_ID,
            topic_id=TEST_TOPIC_ID,
            topic=TEST_TOPIC,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )


class TestManagedKafkaListTopicsOperator:
    @mock.patch(MANAGED_KAFKA_PATH.format("types.ListTopicsResponse.to_dict"))
    @mock.patch(MANAGED_KAFKA_PATH.format("types.Topic.to_dict"))
    @mock.patch(MANAGED_KAFKA_PATH.format("ManagedKafkaHook"))
    def test_execute(self, mock_hook, to_cluster_dict_mock, to_clusters_dict_mock):
        page_token = "page_token"
        page_size = 42

        op = ManagedKafkaListTopicsOperator(
            task_id=TASK_ID,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
            location=GCP_LOCATION,
            project_id=GCP_PROJECT,
            cluster_id=TEST_CLUSTER_ID,
            page_size=page_size,
            page_token=page_token,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )
        op.execute(context={"ti": mock.MagicMock()})
        mock_hook.assert_called_once_with(gcp_conn_id=GCP_CONN_ID, impersonation_chain=IMPERSONATION_CHAIN)
        mock_hook.return_value.list_topics.assert_called_once_with(
            location=GCP_LOCATION,
            project_id=GCP_PROJECT,
            cluster_id=TEST_CLUSTER_ID,
            page_size=page_size,
            page_token=page_token,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )


class TestManagedKafkaGetTopicOperator:
    @mock.patch(MANAGED_KAFKA_PATH.format("types.Topic.to_dict"))
    @mock.patch(MANAGED_KAFKA_PATH.format("ManagedKafkaHook"))
    def test_execute(self, mock_hook, to_dict_mock):
        op = ManagedKafkaGetTopicOperator(
            task_id=TASK_ID,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
            location=GCP_LOCATION,
            project_id=GCP_PROJECT,
            cluster_id=TEST_CLUSTER_ID,
            topic_id=TEST_TOPIC_ID,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )
        op.execute(context={"ti": mock.MagicMock()})
        mock_hook.assert_called_once_with(gcp_conn_id=GCP_CONN_ID, impersonation_chain=IMPERSONATION_CHAIN)
        mock_hook.return_value.get_topic.assert_called_once_with(
            location=GCP_LOCATION,
            project_id=GCP_PROJECT,
            cluster_id=TEST_CLUSTER_ID,
            topic_id=TEST_TOPIC_ID,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )


class TestManagedKafkaUpdateTopicOperator:
    @mock.patch(MANAGED_KAFKA_PATH.format("types.Topic.to_dict"))
    @mock.patch(MANAGED_KAFKA_PATH.format("ManagedKafkaHook"))
    def test_execute(self, mock_hook, to_dict_mock):
        op = ManagedKafkaUpdateTopicOperator(
            task_id=TASK_ID,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
            project_id=GCP_PROJECT,
            location=GCP_LOCATION,
            cluster_id=TEST_CLUSTER_ID,
            topic_id=TEST_TOPIC_ID,
            topic=TEST_UPDATED_TOPIC,
            update_mask=TEST_TOPIC_UPDATE_MASK,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )
        op.execute(context={"ti": mock.MagicMock()})
        mock_hook.assert_called_once_with(gcp_conn_id=GCP_CONN_ID, impersonation_chain=IMPERSONATION_CHAIN)
        mock_hook.return_value.update_topic.assert_called_once_with(
            project_id=GCP_PROJECT,
            location=GCP_LOCATION,
            cluster_id=TEST_CLUSTER_ID,
            topic_id=TEST_TOPIC_ID,
            topic=TEST_UPDATED_TOPIC,
            update_mask=TEST_TOPIC_UPDATE_MASK,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )


class TestManagedKafkaDeleteTopicOperator:
    @mock.patch(MANAGED_KAFKA_PATH.format("ManagedKafkaHook"))
    def test_execute(self, mock_hook):
        op = ManagedKafkaDeleteTopicOperator(
            task_id=TASK_ID,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
            location=GCP_LOCATION,
            project_id=GCP_PROJECT,
            cluster_id=TEST_CLUSTER_ID,
            topic_id=TEST_TOPIC_ID,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )
        op.execute(context={})
        mock_hook.assert_called_once_with(gcp_conn_id=GCP_CONN_ID, impersonation_chain=IMPERSONATION_CHAIN)
        mock_hook.return_value.delete_topic.assert_called_once_with(
            location=GCP_LOCATION,
            project_id=GCP_PROJECT,
            cluster_id=TEST_CLUSTER_ID,
            topic_id=TEST_TOPIC_ID,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )


class TestManagedKafkaListConsumerGroupsOperator:
    @mock.patch(MANAGED_KAFKA_PATH.format("types.ListConsumerGroupsResponse.to_dict"))
    @mock.patch(MANAGED_KAFKA_PATH.format("types.ConsumerGroup.to_dict"))
    @mock.patch(MANAGED_KAFKA_PATH.format("ManagedKafkaHook"))
    def test_execute(self, mock_hook, to_cluster_dict_mock, to_clusters_dict_mock):
        page_token = "page_token"
        page_size = 42

        op = ManagedKafkaListConsumerGroupsOperator(
            task_id=TASK_ID,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
            location=GCP_LOCATION,
            project_id=GCP_PROJECT,
            cluster_id=TEST_CLUSTER_ID,
            page_size=page_size,
            page_token=page_token,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )
        op.execute(context={"ti": mock.MagicMock()})
        mock_hook.assert_called_once_with(gcp_conn_id=GCP_CONN_ID, impersonation_chain=IMPERSONATION_CHAIN)
        mock_hook.return_value.list_consumer_groups.assert_called_once_with(
            location=GCP_LOCATION,
            project_id=GCP_PROJECT,
            cluster_id=TEST_CLUSTER_ID,
            page_size=page_size,
            page_token=page_token,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )


class TestManagedKafkaGetConsumerGroupOperator:
    @mock.patch(MANAGED_KAFKA_PATH.format("types.ConsumerGroup.to_dict"))
    @mock.patch(MANAGED_KAFKA_PATH.format("ManagedKafkaHook"))
    def test_execute(self, mock_hook, to_dict_mock):
        op = ManagedKafkaGetConsumerGroupOperator(
            task_id=TASK_ID,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
            location=GCP_LOCATION,
            project_id=GCP_PROJECT,
            cluster_id=TEST_CLUSTER_ID,
            consumer_group_id=TEST_CONSUMER_GROUP_ID,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )
        op.execute(context={"ti": mock.MagicMock()})
        mock_hook.assert_called_once_with(gcp_conn_id=GCP_CONN_ID, impersonation_chain=IMPERSONATION_CHAIN)
        mock_hook.return_value.get_consumer_group.assert_called_once_with(
            location=GCP_LOCATION,
            project_id=GCP_PROJECT,
            cluster_id=TEST_CLUSTER_ID,
            consumer_group_id=TEST_CONSUMER_GROUP_ID,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )


class TestManagedKafkaUpdateConsumerGroupOperator:
    @mock.patch(MANAGED_KAFKA_PATH.format("types.ConsumerGroup.to_dict"))
    @mock.patch(MANAGED_KAFKA_PATH.format("ManagedKafkaHook"))
    def test_execute(self, mock_hook, to_dict_mock):
        op = ManagedKafkaUpdateConsumerGroupOperator(
            task_id=TASK_ID,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
            project_id=GCP_PROJECT,
            location=GCP_LOCATION,
            cluster_id=TEST_CLUSTER_ID,
            consumer_group_id=TEST_CONSUMER_GROUP_ID,
            consumer_group={},
            update_mask={},
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )
        op.execute(context={"ti": mock.MagicMock()})
        mock_hook.assert_called_once_with(gcp_conn_id=GCP_CONN_ID, impersonation_chain=IMPERSONATION_CHAIN)
        mock_hook.return_value.update_consumer_group.assert_called_once_with(
            project_id=GCP_PROJECT,
            location=GCP_LOCATION,
            cluster_id=TEST_CLUSTER_ID,
            consumer_group_id=TEST_CONSUMER_GROUP_ID,
            consumer_group={},
            update_mask={},
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )


class TestManagedKafkaDeleteConsumerGroupOperator:
    @mock.patch(MANAGED_KAFKA_PATH.format("ManagedKafkaHook"))
    def test_execute(self, mock_hook):
        op = ManagedKafkaDeleteConsumerGroupOperator(
            task_id=TASK_ID,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
            location=GCP_LOCATION,
            project_id=GCP_PROJECT,
            cluster_id=TEST_CLUSTER_ID,
            consumer_group_id=TEST_CONSUMER_GROUP_ID,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )
        op.execute(context={})
        mock_hook.assert_called_once_with(gcp_conn_id=GCP_CONN_ID, impersonation_chain=IMPERSONATION_CHAIN)
        mock_hook.return_value.delete_consumer_group.assert_called_once_with(
            location=GCP_LOCATION,
            project_id=GCP_PROJECT,
            cluster_id=TEST_CLUSTER_ID,
            consumer_group_id=TEST_CONSUMER_GROUP_ID,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )
