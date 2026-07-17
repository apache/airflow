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

from airflow.providers.apache.kafka.triggers.await_message import AwaitMessageTrigger

pytest.importorskip("airflow.providers.common.messaging.providers.base_provider")

MOCK_KAFKA_TRIGGER_APPLY_FUNCTION = "mock_kafka_trigger_apply_function"


class TestKafkaMessageQueueProvider:
    """Tests for KafkaMessageQueueProvider."""

    def setup_method(self):
        """Set up the test environment."""
        from airflow.providers.apache.kafka.queues.kafka import KafkaMessageQueueProvider

        self.provider = KafkaMessageQueueProvider()

    def test_queue_create(self):
        """Test the creation of the KafkaMessageQueueProvider."""
        from airflow.providers.common.messaging.providers.base_provider import BaseMessageQueueProvider

        assert isinstance(self.provider, BaseMessageQueueProvider)

    @pytest.mark.parametrize(
        ("queue_uri", "expected_result"),
        [
            pytest.param("kafka://localhost:9092/topic1", True, id="single_broker_single_topic"),
            pytest.param(
                "kafka://broker1:9092,broker2:9092/topic1,topic2", True, id="multiple_brokers_multiple_topics"
            ),
            pytest.param("http://example.com", False, id="http_url"),
            pytest.param("not-a-url", False, id="invalid_url"),
        ],
    )
    def test_queue_matches(self, queue_uri, expected_result):
        """Test the queue_matches method with various URLs."""
        assert self.provider.queue_matches(queue_uri) == expected_result

    @pytest.mark.parametrize(
        ("scheme", "expected_result"),
        [
            pytest.param("kafka", True, id="kafka_scheme"),
            pytest.param("redis+pubsub", False, id="redis_scheme"),
            pytest.param("sqs", False, id="sqs_scheme"),
            pytest.param("unknown", False, id="unknown_scheme"),
        ],
    )
    def test_scheme_matches(self, scheme, expected_result):
        """Test the scheme_matches method with various schemes."""
        assert self.provider.scheme_matches(scheme) == expected_result

    def test_trigger_class(self):
        """Test the trigger_class method."""
        assert self.provider.trigger_class() == AwaitMessageTrigger

    @pytest.mark.parametrize(
        ("queue_uri", "extra_kwargs", "expected_result"),
        [
            pytest.param(
                "kafka://broker:9092/topic1,topic2",
                {"apply_function": MOCK_KAFKA_TRIGGER_APPLY_FUNCTION},
                {"topics": ["topic1", "topic2"]},
                id="topics_from_uri",
            ),
            pytest.param(
                "kafka://broker:9092/",
                {"topics": ["topic1", "topic2"], "apply_function": MOCK_KAFKA_TRIGGER_APPLY_FUNCTION},
                {},
                id="topics_from_kwargs",
            ),
        ],
    )
    def test_trigger_kwargs_valid_cases(self, queue_uri, extra_kwargs, expected_result):
        """Test the trigger_kwargs method with valid parameters."""
        kwargs = self.provider.trigger_kwargs(queue_uri, **extra_kwargs)
        assert kwargs == expected_result

    @pytest.mark.parametrize(
        ("queue_uri", "extra_kwargs", "expected_error", "error_match"),
        [
            pytest.param(
                "kafka://broker:9092/topic1",
                {},
                ValueError,
                "apply_function is required in KafkaMessageQueueProvider kwargs",
                id="missing_apply_function",
            ),
            pytest.param(
                "kafka://broker:9092/",
                {"apply_function": MOCK_KAFKA_TRIGGER_APPLY_FUNCTION},
                ValueError,
                "topics is required in KafkaMessageQueueProvider kwargs or provide them in the queue URI",
                id="missing_topics",
            ),
        ],
    )
    def test_trigger_kwargs_error_cases(self, queue_uri, extra_kwargs, expected_error, error_match):
        """Test that trigger_kwargs raises appropriate errors with invalid parameters."""
        with pytest.raises(expected_error, match=error_match):
            self.provider.trigger_kwargs(queue_uri, **extra_kwargs)
