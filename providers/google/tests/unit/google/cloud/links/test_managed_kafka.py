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

from airflow.providers.google.cloud.links.managed_kafka import (
    ApacheKafkaClusterLink,
    ApacheKafkaClusterListLink,
    ApacheKafkaConsumerGroupLink,
    ApacheKafkaTopicLink,
)

TEST_LOCATION = "test-location"
TEST_CLUSTER_ID = "test-cluster-id"
TEST_PROJECT_ID = "test-project-id"
TEST_TOPIC_ID = "test-topic-id"
TEST_CONSUMER_GROUP_ID = "test-consumer-group-id"
EXPECTED_MANAGED_KAFKA_CLUSTER_LINK_NAME = "Apache Kafka Cluster"
EXPECTED_MANAGED_KAFKA_CLUSTER_LINK_KEY = "cluster_conf"
EXPECTED_MANAGED_KAFKA_CLUSTER_LINK_FORMAT_STR = (
    "/managedkafka/{location}/clusters/{cluster_id}?project={project_id}"
)
EXPECTED_MANAGED_KAFKA_CLUSTER_LIST_LINK_NAME = "Apache Kafka Cluster List"
EXPECTED_MANAGED_KAFKA_CLUSTER_LIST_LINK_KEY = "cluster_list_conf"
EXPECTED_MANAGED_KAFKA_CLUSTER_LIST_LINK_FORMAT_STR = "/managedkafka/clusters?project={project_id}"
EXPECTED_MANAGED_KAFKA_TOPIC_LINK_NAME = "Apache Kafka Topic"
EXPECTED_MANAGED_KAFKA_TOPIC_LINK_KEY = "topic_conf"
EXPECTED_MANAGED_KAFKA_TOPIC_LINK_FORMAT_STR = (
    "/managedkafka/{location}/clusters/{cluster_id}/topics/{topic_id}?project={project_id}"
)
EXPECTED_MANAGED_KAFKA_CONSUMER_GROUP_LINK_NAME = "Apache Kafka Consumer Group"
EXPECTED_MANAGED_KAFKA_CONSUMER_GROUP_LINK_KEY = "consumer_group_conf"
EXPECTED_MANAGED_KAFKA_CONSUMER_GROUP_LINK_FORMAT_STR = (
    "/managedkafka/{location}/clusters/{cluster_id}/consumer_groups/{consumer_group_id}?project={project_id}"
)


class TestApacheKafkaClusterLink:
    def test_class_attributes(self):
        assert ApacheKafkaClusterLink.key == EXPECTED_MANAGED_KAFKA_CLUSTER_LINK_KEY
        assert ApacheKafkaClusterLink.name == EXPECTED_MANAGED_KAFKA_CLUSTER_LINK_NAME
        assert ApacheKafkaClusterLink.format_str == EXPECTED_MANAGED_KAFKA_CLUSTER_LINK_FORMAT_STR

    def test_persist(self):
        mock_context, mock_task_instance = (
            mock.MagicMock(),
            mock.MagicMock(location=TEST_LOCATION, project_id=TEST_PROJECT_ID),
        )

        ApacheKafkaClusterLink.persist(
            context=mock_context,
            task_instance=mock_task_instance,
            cluster_id=TEST_CLUSTER_ID,
        )

        mock_task_instance.xcom_push.assert_called_once_with(
            context=mock_context,
            key=EXPECTED_MANAGED_KAFKA_CLUSTER_LINK_KEY,
            value={
                "location": TEST_LOCATION,
                "cluster_id": TEST_CLUSTER_ID,
                "project_id": TEST_PROJECT_ID,
            },
        )


class TestApacheKafkaClusterListLink:
    def test_class_attributes(self):
        assert ApacheKafkaClusterListLink.key == EXPECTED_MANAGED_KAFKA_CLUSTER_LIST_LINK_KEY
        assert ApacheKafkaClusterListLink.name == EXPECTED_MANAGED_KAFKA_CLUSTER_LIST_LINK_NAME
        assert ApacheKafkaClusterListLink.format_str == EXPECTED_MANAGED_KAFKA_CLUSTER_LIST_LINK_FORMAT_STR

    def test_persist(self):
        mock_context, mock_task_instance = mock.MagicMock(), mock.MagicMock(project_id=TEST_PROJECT_ID)

        ApacheKafkaClusterListLink.persist(
            context=mock_context,
            task_instance=mock_task_instance,
        )

        mock_task_instance.xcom_push.assert_called_once_with(
            context=mock_context,
            key=EXPECTED_MANAGED_KAFKA_CLUSTER_LIST_LINK_KEY,
            value={
                "project_id": TEST_PROJECT_ID,
            },
        )


class TestApacheKafkaTopicLink:
    def test_class_attributes(self):
        assert ApacheKafkaTopicLink.key == EXPECTED_MANAGED_KAFKA_TOPIC_LINK_KEY
        assert ApacheKafkaTopicLink.name == EXPECTED_MANAGED_KAFKA_TOPIC_LINK_NAME
        assert ApacheKafkaTopicLink.format_str == EXPECTED_MANAGED_KAFKA_TOPIC_LINK_FORMAT_STR

    def test_persist(self):
        mock_context, mock_task_instance = (
            mock.MagicMock(),
            mock.MagicMock(location=TEST_LOCATION, project_id=TEST_PROJECT_ID),
        )

        ApacheKafkaTopicLink.persist(
            context=mock_context,
            task_instance=mock_task_instance,
            cluster_id=TEST_CLUSTER_ID,
            topic_id=TEST_TOPIC_ID,
        )

        mock_task_instance.xcom_push.assert_called_once_with(
            context=mock_context,
            key=EXPECTED_MANAGED_KAFKA_TOPIC_LINK_KEY,
            value={
                "location": TEST_LOCATION,
                "cluster_id": TEST_CLUSTER_ID,
                "topic_id": TEST_TOPIC_ID,
                "project_id": TEST_PROJECT_ID,
            },
        )


class TestApacheKafkaConsumerGroupLink:
    def test_class_attributes(self):
        assert ApacheKafkaConsumerGroupLink.key == EXPECTED_MANAGED_KAFKA_CONSUMER_GROUP_LINK_KEY
        assert ApacheKafkaConsumerGroupLink.name == EXPECTED_MANAGED_KAFKA_CONSUMER_GROUP_LINK_NAME
        assert (
            ApacheKafkaConsumerGroupLink.format_str == EXPECTED_MANAGED_KAFKA_CONSUMER_GROUP_LINK_FORMAT_STR
        )

    def test_persist(self):
        mock_context, mock_task_instance = (
            mock.MagicMock(),
            mock.MagicMock(location=TEST_LOCATION, project_id=TEST_PROJECT_ID),
        )

        ApacheKafkaConsumerGroupLink.persist(
            context=mock_context,
            task_instance=mock_task_instance,
            cluster_id=TEST_CLUSTER_ID,
            consumer_group_id=TEST_CONSUMER_GROUP_ID,
        )

        mock_task_instance.xcom_push.assert_called_once_with(
            context=mock_context,
            key=EXPECTED_MANAGED_KAFKA_CONSUMER_GROUP_LINK_KEY,
            value={
                "location": TEST_LOCATION,
                "cluster_id": TEST_CLUSTER_ID,
                "consumer_group_id": TEST_CONSUMER_GROUP_ID,
                "project_id": TEST_PROJECT_ID,
            },
        )
