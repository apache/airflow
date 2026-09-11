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

"""Tests for Pub/Sub links."""

from __future__ import annotations

from unittest import mock

from airflow.providers.google.cloud.links.base import BASE_LINK
from airflow.providers.google.cloud.links.pubsub import (
    PUBSUB_SUBSCRIPTION_LINK,
    PUBSUB_TOPIC_LINK,
    PubSubSubscriptionLink,
    PubSubTopicLink,
)

TEST_PROJECT_ID = "test-project-id"
TEST_SUBSCRIPTION_ID = "test-subscription-id"
TEST_TOPIC_ID = "test-topic-id"


class TestPubSubTopicLink:
    def test_class_attributes(self):
        assert PubSubTopicLink.key == "pubsub_topic"
        assert PubSubTopicLink.name == "Pub/Sub Topic"
        assert PubSubTopicLink.format_str == PUBSUB_TOPIC_LINK

    def test_persist(self):
        mock_context = mock.MagicMock()
        mock_context["ti"] = mock.MagicMock()
        mock_context["task"] = mock.MagicMock(spec=[])

        PubSubTopicLink.persist(
            context=mock_context,
            project_id=TEST_PROJECT_ID,
            topic_id=TEST_TOPIC_ID,
        )

        mock_context["ti"].xcom_push.assert_called_once_with(
            key="pubsub_topic",
            value={"project_id": TEST_PROJECT_ID, "topic_id": TEST_TOPIC_ID},
        )

    def test_format_link(self):
        link = PubSubTopicLink()

        result = link._format_link(project_id=TEST_PROJECT_ID, topic_id=TEST_TOPIC_ID)

        assert result == BASE_LINK + PUBSUB_TOPIC_LINK.format(
            project_id=TEST_PROJECT_ID, topic_id=TEST_TOPIC_ID
        )


class TestPubSubSubscriptionLink:
    def test_class_attributes(self):
        assert PubSubSubscriptionLink.key == "pubsub_subscription"
        assert PubSubSubscriptionLink.name == "Pub/Sub Subscription"
        assert PubSubSubscriptionLink.format_str == PUBSUB_SUBSCRIPTION_LINK

    def test_persist(self):
        mock_context = mock.MagicMock()
        mock_context["ti"] = mock.MagicMock()
        mock_context["task"] = mock.MagicMock(spec=[])

        PubSubSubscriptionLink.persist(
            context=mock_context,
            project_id=TEST_PROJECT_ID,
            subscription_id=TEST_SUBSCRIPTION_ID,
        )

        mock_context["ti"].xcom_push.assert_called_once_with(
            key="pubsub_subscription",
            value={"project_id": TEST_PROJECT_ID, "subscription_id": TEST_SUBSCRIPTION_ID},
        )

    def test_format_link(self):
        link = PubSubSubscriptionLink()

        result = link._format_link(project_id=TEST_PROJECT_ID, subscription_id=TEST_SUBSCRIPTION_ID)

        assert result == BASE_LINK + PUBSUB_SUBSCRIPTION_LINK.format(
            project_id=TEST_PROJECT_ID, subscription_id=TEST_SUBSCRIPTION_ID
        )
