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

import pytest

from airflow.providers.google.cloud.triggers.pubsub import PubsubPullTrigger

pytest.importorskip("airflow.providers.common.messaging.providers.base_provider")


def test_message_pubsub_queue_create():
    from airflow.providers.common.messaging.providers.base_provider import BaseMessageQueueProvider
    from airflow.providers.google.cloud.queues.pubsub import PubsubMessageQueueProvider

    provider = PubsubMessageQueueProvider()
    assert isinstance(provider, BaseMessageQueueProvider)


def test_message_pubsub_queue_matches():
    from airflow.providers.google.cloud.queues.pubsub import PubsubMessageQueueProvider

    provider = PubsubMessageQueueProvider()
    assert provider.queue_matches("projects/test-project/subscriptions/my-subscription")
    assert not provider.queue_matches("projects/test-project/subscriptions/goog-my-subscription")
    assert not provider.queue_matches("projects/test-project/subscriptions")
    assert not provider.queue_matches("projects/test-project/subscriptions/")
    assert not provider.queue_matches("projects/test-project")


def test_message_pubsub_queue_trigger_class():
    from airflow.providers.google.cloud.queues.pubsub import PubsubMessageQueueProvider

    provider = PubsubMessageQueueProvider()
    assert provider.trigger_class() == PubsubPullTrigger


def test_message_pubsub_queue_trigger_kwargs():
    from airflow.providers.google.cloud.queues.pubsub import PubsubMessageQueueProvider

    provider = PubsubMessageQueueProvider()
    assert provider.trigger_kwargs("projects/test-project/subscriptions/my-subscription") == {
        "project_id": "test-project",
        "subscription": "my-subscription",
        "ack_messages": True,
        "max_messages": 1,
        "gcp_conn_id": "google_cloud_default",
    }


@pytest.mark.parametrize(
    "queue, extra_kwargs, expected_error, error_match",
    [
        pytest.param(
            "projects/test-project/subscriptions/my-subscription",
            {"project_id": "my-project"},
            ValueError,
            "project_id or subscription cannot be provided in kwargs, use the queue param instead",
            id="project_id_in_kwargs",
        ),
        pytest.param(
            "projects/test-project/subscriptions/my-subscription",
            {"subscription": "my-subscription"},
            ValueError,
            "project_id or subscription cannot be provided in kwargs, use the queue param instead",
            id="subscription_in_kwargs",
        ),
        pytest.param(
            "projects/test-project/subscriptions/my-subscription",
            {"project_id": "my-project", "subscription": "my-subscription"},
            ValueError,
            "project_id or subscription cannot be provided in kwargs, use the queue param instead",
            id="both_in_kwargs",
        ),
    ],
)
def test_message_pubsub_queue_trigger_kwargs_invalid_cases(queue, extra_kwargs, expected_error, error_match):
    from airflow.providers.google.cloud.queues.pubsub import PubsubMessageQueueProvider

    provider = PubsubMessageQueueProvider()
    with pytest.raises(expected_error, match=error_match):
        provider.trigger_kwargs(queue, **extra_kwargs)
