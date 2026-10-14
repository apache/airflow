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

from airflow.providers.google.cloud.links.pubsub import (
    PubSubSubscriptionLink,
    PubSubTopicLink,
)

EXPECTED_PUB_SUB_TOPIC_LINK_NAME = "Pub/Sub Topic"
EXPECTED_PUB_SUB_TOPIC_LINK_KEY = "pubsub_topic"
EXPECTED_PUB_SUB_TOPIC_LINK_FORMAT_STR = "/cloudpubsub/topic/detail/{topic_id}?project={project_id}"
EXPECTED_PUB_SUB_SUBSCRIPTION_LINK_NAME = "Pub/Sub Subscription"
EXPECTED_PUB_SUB_SUBSCRIPTION_LINK_KEY = "pubsub_subscription"
EXPECTED_PUB_SUB_SUBSCRIPTION_LINK_FORMAT_STR = (
    "/cloudpubsub/subscription/detail/{subscription_id}?project={project_id}"
)


class TestPubSubTopicLink:
    def test_class_attributes(self):
        assert PubSubTopicLink.name == EXPECTED_PUB_SUB_TOPIC_LINK_NAME
        assert PubSubTopicLink.key == EXPECTED_PUB_SUB_TOPIC_LINK_KEY
        assert PubSubTopicLink.format_str == EXPECTED_PUB_SUB_TOPIC_LINK_FORMAT_STR


class TestPubSubSubscriptionLink:
    def test_class_attributes(self):
        assert PubSubSubscriptionLink.name == EXPECTED_PUB_SUB_SUBSCRIPTION_LINK_NAME
        assert PubSubSubscriptionLink.key == EXPECTED_PUB_SUB_SUBSCRIPTION_LINK_KEY
        assert PubSubSubscriptionLink.format_str == EXPECTED_PUB_SUB_SUBSCRIPTION_LINK_FORMAT_STR
