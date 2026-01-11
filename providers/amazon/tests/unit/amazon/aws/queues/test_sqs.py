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

from airflow.providers.amazon.aws.triggers.sqs import SqsSensorTrigger

pytest.importorskip("airflow.providers.common.messaging.providers.base_provider")


def test_message_sqs_queue_create():
    from airflow.providers.amazon.aws.queues.sqs import SqsMessageQueueProvider
    from airflow.providers.common.messaging.providers.base_provider import BaseMessageQueueProvider

    provider = SqsMessageQueueProvider()
    assert isinstance(provider, BaseMessageQueueProvider)


def test_message_sqs_queue_matches():
    from airflow.providers.amazon.aws.queues.sqs import SqsMessageQueueProvider

    provider = SqsMessageQueueProvider()
    assert provider.queue_matches("https://sqs.us-east-1.amazonaws.com/123456789012/my-queue")
    assert not provider.queue_matches("https://sqs.us-east-1.amazonaws.com/123456789012")
    assert not provider.queue_matches("https://sqs.us-east-1.amazonaws.com/123456789012/")
    assert not provider.queue_matches("https://sqs.us-east-1.amazonaws.com/")


@pytest.mark.parametrize(
    ("scheme", "expected_result"),
    [
        pytest.param("sqs", True, id="sqs_scheme"),
        pytest.param("kafka", False, id="kafka_scheme"),
        pytest.param("redis+pubsub", False, id="redis_scheme"),
        pytest.param("unknown", False, id="unknown_scheme"),
    ],
)
def test_message_sqs_scheme_matches(scheme, expected_result):
    """Test the scheme_matches method with various schemes."""
    from airflow.providers.amazon.aws.queues.sqs import SqsMessageQueueProvider

    provider = SqsMessageQueueProvider()
    assert provider.scheme_matches(scheme) == expected_result


def test_message_sqs_queue_trigger_class():
    from airflow.providers.amazon.aws.queues.sqs import SqsMessageQueueProvider

    provider = SqsMessageQueueProvider()
    assert provider.trigger_class() == SqsSensorTrigger


def test_message_sqs_queue_trigger_kwargs():
    from airflow.providers.amazon.aws.queues.sqs import SqsMessageQueueProvider

    provider = SqsMessageQueueProvider()
    assert provider.trigger_kwargs("https://sqs.us-east-1.amazonaws.com/123456789012/my-queue") == {
        "sqs_queue": "https://sqs.us-east-1.amazonaws.com/123456789012/my-queue",
    }
