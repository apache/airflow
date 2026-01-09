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
"""This module contains Google Pub/Sub links."""

from __future__ import annotations

from airflow.providers.google.cloud.links.base import BaseGoogleLink

PUBSUB_BASE_LINK = "/cloudpubsub"
PUBSUB_TOPIC_LINK = PUBSUB_BASE_LINK + "/topic/detail/{topic_id}?project={project_id}"
PUBSUB_SUBSCRIPTION_LINK = PUBSUB_BASE_LINK + "/subscription/detail/{subscription_id}?project={project_id}"


class PubSubTopicLink(BaseGoogleLink):
    """Helper class for constructing Pub/Sub Topic Link."""

    name = "Pub/Sub Topic"
    key = "pubsub_topic"
    format_str = PUBSUB_TOPIC_LINK


class PubSubSubscriptionLink(BaseGoogleLink):
    """Helper class for constructing Pub/Sub Subscription Link."""

    name = "Pub/Sub Subscription"
    key = "pubsub_subscription"
    format_str = PUBSUB_SUBSCRIPTION_LINK
