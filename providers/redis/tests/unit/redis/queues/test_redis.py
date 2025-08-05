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

from airflow.providers.redis.triggers.redis_await_message import AwaitMessageTrigger

pytest.importorskip("airflow.providers.common.messaging.providers.base_provider")


class TestRedisPubSubMessageQueueProvider:
    """Tests for RedisPubSubMessageQueueProvider."""

    def setup_method(self):
        """Set up the test environment."""
        from airflow.providers.redis.queues.redis import RedisPubSubMessageQueueProvider

        self.provider = RedisPubSubMessageQueueProvider()

    def test_queue_create(self):
        """Test the creation of the RedisPubSubMessageQueueProvider."""
        from airflow.providers.common.messaging.providers.base_provider import BaseMessageQueueProvider

        assert isinstance(self.provider, BaseMessageQueueProvider)

    @pytest.mark.parametrize(
        "queue_uri, expected_result",
        [
            pytest.param("redis+pubsub://localhost:6379/channel1", True, id="single_channel"),
            pytest.param("redis+pubsub://localhost:6379/channel1,channel2", True, id="multiple_channels"),
            pytest.param("http://example.com", False, id="http_url"),
            pytest.param("not-a-url", False, id="invalid_url"),
        ],
    )
    def test_queue_matches(self, queue_uri, expected_result):
        """Test the queue_matches method with various URLs."""
        assert self.provider.queue_matches(queue_uri) == expected_result

    def test_trigger_class(self):
        """Test the trigger_class method."""
        assert self.provider.trigger_class() == AwaitMessageTrigger

    @pytest.mark.parametrize(
        "queue_uri, extra_kwargs, expected_result",
        [
            pytest.param(
                "redis+pubsub://localhost:6379/channel1,channel2",
                {"channels": ["channel1", "channel2"]},
                {},
                id="channels_from_uri",
            ),
            pytest.param(
                "redis+pubsub://localhost:6379/",
                {"channels": ["channel1", "channel2"]},
                {},
                id="channels_from_kwargs",
            ),
        ],
    )
    def test_trigger_kwargs_valid_cases(self, queue_uri, extra_kwargs, expected_result):
        """Test the trigger_kwargs method with valid parameters."""
        kwargs = self.provider.trigger_kwargs(queue_uri, **extra_kwargs)
        assert kwargs == expected_result

    @pytest.mark.parametrize(
        "queue_uri, extra_kwargs, expected_error, error_match",
        [
            pytest.param(
                "redis+pubsub://localhost:6379/",
                {},
                ValueError,
                "channels is required in RedisPubSubMessageQueueProvider kwargs or provide them in the queue URI",
                id="missing_channels",
            ),
        ],
    )
    def test_trigger_kwargs_error_cases(self, queue_uri, extra_kwargs, expected_error, error_match):
        """Test that trigger_kwargs raises appropriate errors with invalid parameters."""
        with pytest.raises(expected_error, match=error_match):
            self.provider.trigger_kwargs(queue_uri, **extra_kwargs)
