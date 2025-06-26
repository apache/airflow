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

from google.api_core.gapic_v1.method import DEFAULT

from airflow.providers.google.cloud.hooks.managed_kafka import ManagedKafkaHook

from unit.google.cloud.utils.base_gcp_mock import (
    mock_base_gcp_hook_default_project_id,
    mock_base_gcp_hook_no_default_project_id,
)

TEST_GCP_CONN_ID: str = "test-gcp-conn-id"
TEST_LOCATION: str = "test-location"
TEST_PROJECT_ID: str = "test-project-id"
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

BASE_STRING = "airflow.providers.google.common.hooks.base_google.{}"
MANAGED_KAFKA_STRING = "airflow.providers.google.cloud.hooks.managed_kafka.{}"


class TestManagedKafkaWithDefaultProjectIdHook:
    def setup_method(self):
        with mock.patch(
            BASE_STRING.format("GoogleBaseHook.__init__"), new=mock_base_gcp_hook_default_project_id
        ):
            self.hook = ManagedKafkaHook(gcp_conn_id=TEST_GCP_CONN_ID)

    @mock.patch(MANAGED_KAFKA_STRING.format("ManagedKafkaHook.get_managed_kafka_client"))
    def test_create_cluster(self, mock_client) -> None:
        self.hook.create_cluster(
            project_id=TEST_PROJECT_ID,
            location=TEST_LOCATION,
            cluster=TEST_CLUSTER,
            cluster_id=TEST_CLUSTER_ID,
        )
        mock_client.assert_called_once()
        mock_client.return_value.create_cluster.assert_called_once_with(
            request=dict(
                parent=mock_client.return_value.common_location_path.return_value,
                cluster=TEST_CLUSTER,
                cluster_id=TEST_CLUSTER_ID,
                request_id=None,
            ),
            metadata=(),
            retry=DEFAULT,
            timeout=None,
        )
        mock_client.return_value.common_location_path.assert_called_once_with(TEST_PROJECT_ID, TEST_LOCATION)

    @mock.patch(MANAGED_KAFKA_STRING.format("ManagedKafkaHook.get_managed_kafka_client"))
    def test_delete_cluster(self, mock_client) -> None:
        self.hook.delete_cluster(
            project_id=TEST_PROJECT_ID,
            location=TEST_LOCATION,
            cluster_id=TEST_CLUSTER_ID,
        )
        mock_client.assert_called_once()
        mock_client.return_value.delete_cluster.assert_called_once_with(
            request=dict(name=mock_client.return_value.cluster_path.return_value, request_id=None),
            metadata=(),
            retry=DEFAULT,
            timeout=None,
        )
        mock_client.return_value.cluster_path.assert_called_once_with(
            TEST_PROJECT_ID, TEST_LOCATION, TEST_CLUSTER_ID
        )

    @mock.patch(MANAGED_KAFKA_STRING.format("ManagedKafkaHook.get_managed_kafka_client"))
    def test_get_cluster(self, mock_client) -> None:
        self.hook.get_cluster(
            project_id=TEST_PROJECT_ID,
            location=TEST_LOCATION,
            cluster_id=TEST_CLUSTER_ID,
        )
        mock_client.assert_called_once()
        mock_client.return_value.get_cluster.assert_called_once_with(
            request=dict(
                name=mock_client.return_value.cluster_path.return_value,
            ),
            metadata=(),
            retry=DEFAULT,
            timeout=None,
        )
        mock_client.return_value.cluster_path.assert_called_once_with(
            TEST_PROJECT_ID, TEST_LOCATION, TEST_CLUSTER_ID
        )

    @mock.patch(MANAGED_KAFKA_STRING.format("ManagedKafkaHook.get_managed_kafka_client"))
    def test_update_cluster(self, mock_client) -> None:
        self.hook.update_cluster(
            project_id=TEST_PROJECT_ID,
            location=TEST_LOCATION,
            cluster=TEST_UPDATED_CLUSTER,
            cluster_id=TEST_CLUSTER_ID,
            update_mask=TEST_CLUSTER_UPDATE_MASK,
        )
        mock_client.assert_called_once()
        mock_client.return_value.update_cluster.assert_called_once_with(
            request=dict(
                update_mask=TEST_CLUSTER_UPDATE_MASK,
                cluster={
                    "name": mock_client.return_value.cluster_path.return_value,
                    **TEST_UPDATED_CLUSTER,
                },
                request_id=None,
            ),
            metadata=(),
            retry=DEFAULT,
            timeout=None,
        )
        mock_client.return_value.cluster_path.assert_called_once_with(
            TEST_PROJECT_ID, TEST_LOCATION, TEST_CLUSTER_ID
        )

    @mock.patch(MANAGED_KAFKA_STRING.format("ManagedKafkaHook.get_managed_kafka_client"))
    def test_list_clusters(self, mock_client) -> None:
        self.hook.list_clusters(
            project_id=TEST_PROJECT_ID,
            location=TEST_LOCATION,
        )
        mock_client.assert_called_once()
        mock_client.return_value.list_clusters.assert_called_once_with(
            request=dict(
                parent=mock_client.return_value.common_location_path.return_value,
                page_size=None,
                page_token=None,
                filter=None,
                order_by=None,
            ),
            metadata=(),
            retry=DEFAULT,
            timeout=None,
        )
        mock_client.return_value.common_location_path.assert_called_once_with(TEST_PROJECT_ID, TEST_LOCATION)

    @mock.patch(MANAGED_KAFKA_STRING.format("ManagedKafkaHook.get_managed_kafka_client"))
    def test_create_topic(self, mock_client) -> None:
        self.hook.create_topic(
            project_id=TEST_PROJECT_ID,
            location=TEST_LOCATION,
            cluster_id=TEST_CLUSTER_ID,
            topic_id=TEST_TOPIC_ID,
            topic=TEST_TOPIC,
        )
        mock_client.assert_called_once()
        mock_client.return_value.create_topic.assert_called_once_with(
            request=dict(
                parent=mock_client.return_value.cluster_path.return_value,
                topic_id=TEST_TOPIC_ID,
                topic=TEST_TOPIC,
            ),
            metadata=(),
            retry=DEFAULT,
            timeout=None,
        )
        mock_client.return_value.cluster_path.assert_called_once_with(
            TEST_PROJECT_ID, TEST_LOCATION, TEST_CLUSTER_ID
        )

    @mock.patch(MANAGED_KAFKA_STRING.format("ManagedKafkaHook.get_managed_kafka_client"))
    def test_delete_topic(self, mock_client) -> None:
        self.hook.delete_topic(
            project_id=TEST_PROJECT_ID,
            location=TEST_LOCATION,
            cluster_id=TEST_CLUSTER_ID,
            topic_id=TEST_TOPIC_ID,
        )
        mock_client.assert_called_once()
        mock_client.return_value.delete_topic.assert_called_once_with(
            request=dict(name=mock_client.return_value.topic_path.return_value),
            metadata=(),
            retry=DEFAULT,
            timeout=None,
        )
        mock_client.return_value.topic_path.assert_called_once_with(
            TEST_PROJECT_ID, TEST_LOCATION, TEST_CLUSTER_ID, TEST_TOPIC_ID
        )

    @mock.patch(MANAGED_KAFKA_STRING.format("ManagedKafkaHook.get_managed_kafka_client"))
    def test_get_topic(self, mock_client) -> None:
        self.hook.get_topic(
            project_id=TEST_PROJECT_ID,
            location=TEST_LOCATION,
            cluster_id=TEST_CLUSTER_ID,
            topic_id=TEST_TOPIC_ID,
        )
        mock_client.assert_called_once()
        mock_client.return_value.get_topic.assert_called_once_with(
            request=dict(
                name=mock_client.return_value.topic_path.return_value,
            ),
            metadata=(),
            retry=DEFAULT,
            timeout=None,
        )
        mock_client.return_value.topic_path.assert_called_once_with(
            TEST_PROJECT_ID,
            TEST_LOCATION,
            TEST_CLUSTER_ID,
            TEST_TOPIC_ID,
        )

    @mock.patch(MANAGED_KAFKA_STRING.format("ManagedKafkaHook.get_managed_kafka_client"))
    def test_update_topic(self, mock_client) -> None:
        self.hook.update_topic(
            project_id=TEST_PROJECT_ID,
            location=TEST_LOCATION,
            cluster_id=TEST_CLUSTER_ID,
            topic_id=TEST_TOPIC_ID,
            topic=TEST_UPDATED_TOPIC,
            update_mask=TEST_TOPIC_UPDATE_MASK,
        )
        mock_client.assert_called_once()
        mock_client.return_value.update_topic.assert_called_once_with(
            request=dict(
                update_mask=TEST_TOPIC_UPDATE_MASK,
                topic={
                    "name": mock_client.return_value.topic_path.return_value,
                    **TEST_UPDATED_TOPIC,
                },
            ),
            metadata=(),
            retry=DEFAULT,
            timeout=None,
        )
        mock_client.return_value.topic_path.assert_called_once_with(
            TEST_PROJECT_ID, TEST_LOCATION, TEST_CLUSTER_ID, TEST_TOPIC_ID
        )

    @mock.patch(MANAGED_KAFKA_STRING.format("ManagedKafkaHook.get_managed_kafka_client"))
    def test_list_topics(self, mock_client) -> None:
        self.hook.list_topics(
            project_id=TEST_PROJECT_ID,
            location=TEST_LOCATION,
            cluster_id=TEST_CLUSTER_ID,
        )
        mock_client.assert_called_once()
        mock_client.return_value.list_topics.assert_called_once_with(
            request=dict(
                parent=mock_client.return_value.cluster_path.return_value,
                page_size=None,
                page_token=None,
            ),
            metadata=(),
            retry=DEFAULT,
            timeout=None,
        )
        mock_client.return_value.cluster_path.assert_called_once_with(
            TEST_PROJECT_ID, TEST_LOCATION, TEST_CLUSTER_ID
        )

    @mock.patch(MANAGED_KAFKA_STRING.format("ManagedKafkaHook.get_managed_kafka_client"))
    def test_delete_consumer_group(self, mock_client) -> None:
        self.hook.delete_consumer_group(
            project_id=TEST_PROJECT_ID,
            location=TEST_LOCATION,
            cluster_id=TEST_CLUSTER_ID,
            consumer_group_id=TEST_CONSUMER_GROUP_ID,
        )
        mock_client.assert_called_once()
        mock_client.return_value.delete_consumer_group.assert_called_once_with(
            request=dict(name=mock_client.return_value.consumer_group_path.return_value),
            metadata=(),
            retry=DEFAULT,
            timeout=None,
        )
        mock_client.return_value.consumer_group_path.assert_called_once_with(
            TEST_PROJECT_ID, TEST_LOCATION, TEST_CLUSTER_ID, TEST_CONSUMER_GROUP_ID
        )

    @mock.patch(MANAGED_KAFKA_STRING.format("ManagedKafkaHook.get_managed_kafka_client"))
    def test_get_consumer_group(self, mock_client) -> None:
        self.hook.get_consumer_group(
            project_id=TEST_PROJECT_ID,
            location=TEST_LOCATION,
            cluster_id=TEST_CLUSTER_ID,
            consumer_group_id=TEST_CONSUMER_GROUP_ID,
        )
        mock_client.assert_called_once()
        mock_client.return_value.get_consumer_group.assert_called_once_with(
            request=dict(
                name=mock_client.return_value.consumer_group_path.return_value,
            ),
            metadata=(),
            retry=DEFAULT,
            timeout=None,
        )
        mock_client.return_value.consumer_group_path.assert_called_once_with(
            TEST_PROJECT_ID,
            TEST_LOCATION,
            TEST_CLUSTER_ID,
            TEST_CONSUMER_GROUP_ID,
        )

    @mock.patch(MANAGED_KAFKA_STRING.format("ManagedKafkaHook.get_managed_kafka_client"))
    def test_update_consumer_group(self, mock_client) -> None:
        self.hook.update_consumer_group(
            project_id=TEST_PROJECT_ID,
            location=TEST_LOCATION,
            cluster_id=TEST_CLUSTER_ID,
            consumer_group_id=TEST_CONSUMER_GROUP_ID,
            consumer_group={},
            update_mask={},
        )
        mock_client.assert_called_once()
        mock_client.return_value.update_consumer_group.assert_called_once_with(
            request=dict(
                update_mask={},
                consumer_group={
                    "name": mock_client.return_value.consumer_group_path.return_value,
                    **{},
                },
            ),
            metadata=(),
            retry=DEFAULT,
            timeout=None,
        )
        mock_client.return_value.consumer_group_path.assert_called_once_with(
            TEST_PROJECT_ID, TEST_LOCATION, TEST_CLUSTER_ID, TEST_CONSUMER_GROUP_ID
        )

    @mock.patch(MANAGED_KAFKA_STRING.format("ManagedKafkaHook.get_managed_kafka_client"))
    def test_list_consumer_groups(self, mock_client) -> None:
        self.hook.list_consumer_groups(
            project_id=TEST_PROJECT_ID,
            location=TEST_LOCATION,
            cluster_id=TEST_CLUSTER_ID,
        )
        mock_client.assert_called_once()
        mock_client.return_value.list_consumer_groups.assert_called_once_with(
            request=dict(
                parent=mock_client.return_value.cluster_path.return_value,
                page_size=None,
                page_token=None,
            ),
            metadata=(),
            retry=DEFAULT,
            timeout=None,
        )
        mock_client.return_value.cluster_path.assert_called_once_with(
            TEST_PROJECT_ID, TEST_LOCATION, TEST_CLUSTER_ID
        )


class TestManagedKafkaWithoutDefaultProjectIdHook:
    def setup_method(self):
        with mock.patch(
            BASE_STRING.format("GoogleBaseHook.__init__"), new=mock_base_gcp_hook_no_default_project_id
        ):
            self.hook = ManagedKafkaHook(gcp_conn_id=TEST_GCP_CONN_ID)

    @mock.patch(MANAGED_KAFKA_STRING.format("ManagedKafkaHook.get_managed_kafka_client"))
    def test_create_cluster(self, mock_client) -> None:
        self.hook.create_cluster(
            project_id=TEST_PROJECT_ID,
            location=TEST_LOCATION,
            cluster=TEST_CLUSTER,
            cluster_id=TEST_CLUSTER_ID,
        )
        mock_client.assert_called_once()
        mock_client.return_value.create_cluster.assert_called_once_with(
            request=dict(
                parent=mock_client.return_value.common_location_path.return_value,
                cluster=TEST_CLUSTER,
                cluster_id=TEST_CLUSTER_ID,
                request_id=None,
            ),
            metadata=(),
            retry=DEFAULT,
            timeout=None,
        )
        mock_client.return_value.common_location_path.assert_called_once_with(TEST_PROJECT_ID, TEST_LOCATION)

    @mock.patch(MANAGED_KAFKA_STRING.format("ManagedKafkaHook.get_managed_kafka_client"))
    def test_delete_cluster(self, mock_client) -> None:
        self.hook.delete_cluster(
            project_id=TEST_PROJECT_ID,
            location=TEST_LOCATION,
            cluster_id=TEST_CLUSTER_ID,
        )
        mock_client.assert_called_once()
        mock_client.return_value.delete_cluster.assert_called_once_with(
            request=dict(name=mock_client.return_value.cluster_path.return_value, request_id=None),
            metadata=(),
            retry=DEFAULT,
            timeout=None,
        )
        mock_client.return_value.cluster_path.assert_called_once_with(
            TEST_PROJECT_ID, TEST_LOCATION, TEST_CLUSTER_ID
        )

    @mock.patch(MANAGED_KAFKA_STRING.format("ManagedKafkaHook.get_managed_kafka_client"))
    def test_get_cluster(self, mock_client) -> None:
        self.hook.get_cluster(
            project_id=TEST_PROJECT_ID,
            location=TEST_LOCATION,
            cluster_id=TEST_CLUSTER_ID,
        )
        mock_client.assert_called_once()
        mock_client.return_value.get_cluster.assert_called_once_with(
            request=dict(
                name=mock_client.return_value.cluster_path.return_value,
            ),
            metadata=(),
            retry=DEFAULT,
            timeout=None,
        )
        mock_client.return_value.cluster_path.assert_called_once_with(
            TEST_PROJECT_ID, TEST_LOCATION, TEST_CLUSTER_ID
        )

    @mock.patch(MANAGED_KAFKA_STRING.format("ManagedKafkaHook.get_managed_kafka_client"))
    def test_update_cluster(self, mock_client) -> None:
        self.hook.update_cluster(
            project_id=TEST_PROJECT_ID,
            location=TEST_LOCATION,
            cluster=TEST_UPDATED_CLUSTER,
            cluster_id=TEST_CLUSTER_ID,
            update_mask=TEST_CLUSTER_UPDATE_MASK,
        )
        mock_client.assert_called_once()
        mock_client.return_value.update_cluster.assert_called_once_with(
            request=dict(
                update_mask=TEST_CLUSTER_UPDATE_MASK,
                cluster={
                    "name": mock_client.return_value.cluster_path.return_value,
                    **TEST_UPDATED_CLUSTER,
                },
                request_id=None,
            ),
            metadata=(),
            retry=DEFAULT,
            timeout=None,
        )
        mock_client.return_value.cluster_path.assert_called_once_with(
            TEST_PROJECT_ID, TEST_LOCATION, TEST_CLUSTER_ID
        )

    @mock.patch(MANAGED_KAFKA_STRING.format("ManagedKafkaHook.get_managed_kafka_client"))
    def test_list_clusters(self, mock_client) -> None:
        self.hook.list_clusters(
            project_id=TEST_PROJECT_ID,
            location=TEST_LOCATION,
        )
        mock_client.assert_called_once()
        mock_client.return_value.list_clusters.assert_called_once_with(
            request=dict(
                parent=mock_client.return_value.common_location_path.return_value,
                page_size=None,
                page_token=None,
                filter=None,
                order_by=None,
            ),
            metadata=(),
            retry=DEFAULT,
            timeout=None,
        )
        mock_client.return_value.common_location_path.assert_called_once_with(TEST_PROJECT_ID, TEST_LOCATION)

    @mock.patch(MANAGED_KAFKA_STRING.format("ManagedKafkaHook.get_managed_kafka_client"))
    def test_create_topic(self, mock_client) -> None:
        self.hook.create_topic(
            project_id=TEST_PROJECT_ID,
            location=TEST_LOCATION,
            cluster_id=TEST_CLUSTER_ID,
            topic_id=TEST_TOPIC_ID,
            topic=TEST_TOPIC,
        )
        mock_client.assert_called_once()
        mock_client.return_value.create_topic.assert_called_once_with(
            request=dict(
                parent=mock_client.return_value.cluster_path.return_value,
                topic_id=TEST_TOPIC_ID,
                topic=TEST_TOPIC,
            ),
            metadata=(),
            retry=DEFAULT,
            timeout=None,
        )
        mock_client.return_value.cluster_path.assert_called_once_with(
            TEST_PROJECT_ID, TEST_LOCATION, TEST_CLUSTER_ID
        )

    @mock.patch(MANAGED_KAFKA_STRING.format("ManagedKafkaHook.get_managed_kafka_client"))
    def test_delete_topic(self, mock_client) -> None:
        self.hook.delete_topic(
            project_id=TEST_PROJECT_ID,
            location=TEST_LOCATION,
            cluster_id=TEST_CLUSTER_ID,
            topic_id=TEST_TOPIC_ID,
        )
        mock_client.assert_called_once()
        mock_client.return_value.delete_topic.assert_called_once_with(
            request=dict(name=mock_client.return_value.topic_path.return_value),
            metadata=(),
            retry=DEFAULT,
            timeout=None,
        )
        mock_client.return_value.topic_path.assert_called_once_with(
            TEST_PROJECT_ID,
            TEST_LOCATION,
            TEST_CLUSTER_ID,
            TEST_TOPIC_ID,
        )

    @mock.patch(MANAGED_KAFKA_STRING.format("ManagedKafkaHook.get_managed_kafka_client"))
    def test_get_topic(self, mock_client) -> None:
        self.hook.get_topic(
            project_id=TEST_PROJECT_ID,
            location=TEST_LOCATION,
            cluster_id=TEST_CLUSTER_ID,
            topic_id=TEST_TOPIC_ID,
        )
        mock_client.assert_called_once()
        mock_client.return_value.get_topic.assert_called_once_with(
            request=dict(
                name=mock_client.return_value.topic_path.return_value,
            ),
            metadata=(),
            retry=DEFAULT,
            timeout=None,
        )
        mock_client.return_value.topic_path.assert_called_once_with(
            TEST_PROJECT_ID,
            TEST_LOCATION,
            TEST_CLUSTER_ID,
            TEST_TOPIC_ID,
        )

    @mock.patch(MANAGED_KAFKA_STRING.format("ManagedKafkaHook.get_managed_kafka_client"))
    def test_update_topic(self, mock_client) -> None:
        self.hook.update_topic(
            project_id=TEST_PROJECT_ID,
            location=TEST_LOCATION,
            cluster_id=TEST_CLUSTER_ID,
            topic_id=TEST_TOPIC_ID,
            topic=TEST_UPDATED_TOPIC,
            update_mask=TEST_TOPIC_UPDATE_MASK,
        )
        mock_client.assert_called_once()
        mock_client.return_value.update_topic.assert_called_once_with(
            request=dict(
                update_mask=TEST_TOPIC_UPDATE_MASK,
                topic={
                    "name": mock_client.return_value.topic_path.return_value,
                    **TEST_UPDATED_TOPIC,
                },
            ),
            metadata=(),
            retry=DEFAULT,
            timeout=None,
        )
        mock_client.return_value.topic_path.assert_called_once_with(
            TEST_PROJECT_ID, TEST_LOCATION, TEST_CLUSTER_ID, TEST_TOPIC_ID
        )

    @mock.patch(MANAGED_KAFKA_STRING.format("ManagedKafkaHook.get_managed_kafka_client"))
    def test_list_topics(self, mock_client) -> None:
        self.hook.list_topics(
            project_id=TEST_PROJECT_ID,
            location=TEST_LOCATION,
            cluster_id=TEST_CLUSTER_ID,
        )
        mock_client.assert_called_once()
        mock_client.return_value.list_topics.assert_called_once_with(
            request=dict(
                parent=mock_client.return_value.cluster_path.return_value,
                page_size=None,
                page_token=None,
            ),
            metadata=(),
            retry=DEFAULT,
            timeout=None,
        )
        mock_client.return_value.cluster_path.assert_called_once_with(
            TEST_PROJECT_ID, TEST_LOCATION, TEST_CLUSTER_ID
        )

    @mock.patch(MANAGED_KAFKA_STRING.format("ManagedKafkaHook.get_managed_kafka_client"))
    def test_delete_consumer_group(self, mock_client) -> None:
        self.hook.delete_consumer_group(
            project_id=TEST_PROJECT_ID,
            location=TEST_LOCATION,
            cluster_id=TEST_CLUSTER_ID,
            consumer_group_id=TEST_CONSUMER_GROUP_ID,
        )
        mock_client.assert_called_once()
        mock_client.return_value.delete_consumer_group.assert_called_once_with(
            request=dict(name=mock_client.return_value.consumer_group_path.return_value),
            metadata=(),
            retry=DEFAULT,
            timeout=None,
        )
        mock_client.return_value.consumer_group_path.assert_called_once_with(
            TEST_PROJECT_ID,
            TEST_LOCATION,
            TEST_CLUSTER_ID,
            TEST_CONSUMER_GROUP_ID,
        )

    @mock.patch(MANAGED_KAFKA_STRING.format("ManagedKafkaHook.get_managed_kafka_client"))
    def test_get_consumer_group(self, mock_client) -> None:
        self.hook.get_consumer_group(
            project_id=TEST_PROJECT_ID,
            location=TEST_LOCATION,
            cluster_id=TEST_CLUSTER_ID,
            consumer_group_id=TEST_CONSUMER_GROUP_ID,
        )
        mock_client.assert_called_once()
        mock_client.return_value.get_consumer_group.assert_called_once_with(
            request=dict(
                name=mock_client.return_value.consumer_group_path.return_value,
            ),
            metadata=(),
            retry=DEFAULT,
            timeout=None,
        )
        mock_client.return_value.consumer_group_path.assert_called_once_with(
            TEST_PROJECT_ID,
            TEST_LOCATION,
            TEST_CLUSTER_ID,
            TEST_CONSUMER_GROUP_ID,
        )

    @mock.patch(MANAGED_KAFKA_STRING.format("ManagedKafkaHook.get_managed_kafka_client"))
    def test_update_consumer_group(self, mock_client) -> None:
        self.hook.update_consumer_group(
            project_id=TEST_PROJECT_ID,
            location=TEST_LOCATION,
            cluster_id=TEST_CLUSTER_ID,
            consumer_group_id=TEST_CONSUMER_GROUP_ID,
            consumer_group={},
            update_mask={},
        )
        mock_client.assert_called_once()
        mock_client.return_value.update_consumer_group.assert_called_once_with(
            request=dict(
                update_mask={},
                consumer_group={
                    "name": mock_client.return_value.consumer_group_path.return_value,
                    **{},
                },
            ),
            metadata=(),
            retry=DEFAULT,
            timeout=None,
        )
        mock_client.return_value.consumer_group_path.assert_called_once_with(
            TEST_PROJECT_ID, TEST_LOCATION, TEST_CLUSTER_ID, TEST_CONSUMER_GROUP_ID
        )

    @mock.patch(MANAGED_KAFKA_STRING.format("ManagedKafkaHook.get_managed_kafka_client"))
    def test_list_consumer_groups(self, mock_client) -> None:
        self.hook.list_consumer_groups(
            project_id=TEST_PROJECT_ID,
            location=TEST_LOCATION,
            cluster_id=TEST_CLUSTER_ID,
        )
        mock_client.assert_called_once()
        mock_client.return_value.list_consumer_groups.assert_called_once_with(
            request=dict(
                parent=mock_client.return_value.cluster_path.return_value,
                page_size=None,
                page_token=None,
            ),
            metadata=(),
            retry=DEFAULT,
            timeout=None,
        )
        mock_client.return_value.cluster_path.assert_called_once_with(
            TEST_PROJECT_ID, TEST_LOCATION, TEST_CLUSTER_ID
        )
