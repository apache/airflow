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

from airflow.providers.ibm.mq.triggers.mq import AwaitMessageTrigger

pytest.importorskip("airflow.providers.common.messaging.providers.base_provider")


class TestIBMMQMessageQueueProvider:
    """Tests for IBMMQMessageQueueProvider."""

    def setup_method(self):
        """Set up the test environment."""
        from airflow.providers.ibm.mq.queues.mq import IBMMQMessageQueueProvider

        self.provider = IBMMQMessageQueueProvider()

    def test_queue_create(self):
        """Test the creation of the provider."""
        from airflow.providers.common.messaging.providers.base_provider import BaseMessageQueueProvider

        assert isinstance(self.provider, BaseMessageQueueProvider)

    @pytest.mark.parametrize(
        ("queue_uri", "expected_result"),
        [
            pytest.param("mq://mq_conn/QUEUE1", True, id="valid_mq_uri"),
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
            pytest.param("kafka", False, id="kafka_scheme"),
            pytest.param("mq", True, id="mq_scheme"),
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
                "mq://my_conn/QUEUE1",
                {},
                {
                    "mq_conn_id": "my_conn",
                    "queue_name": "QUEUE1",
                    "poll_interval": 5,
                },
                id="default_poll_interval",
            ),
            pytest.param(
                "mq://my_conn/QUEUE1",
                {"poll_interval": 60},
                {
                    "mq_conn_id": "my_conn",
                    "queue_name": "QUEUE1",
                    "poll_interval": 60,
                },
                id="override_poll_interval",
            ),
        ],
    )
    def test_trigger_kwargs_valid_cases(self, queue_uri, extra_kwargs, expected_result):
        """Test the trigger_kwargs method with valid parameters."""
        kwargs = self.provider.trigger_kwargs(queue_uri, **extra_kwargs)
        assert kwargs == expected_result

    @pytest.mark.parametrize(
        ("queue_uri", "expected_error", "error_match"),
        [
            pytest.param(
                "mq:///QUEUE1",
                ValueError,
                "MQ URI must contain connection id",
                id="missing_conn_id",
            ),
            pytest.param(
                "mq://my_conn/",
                ValueError,
                "MQ URI must contain queue name",
                id="missing_queue_name",
            ),
        ],
    )
    def test_trigger_kwargs_error_cases(self, queue_uri, expected_error, error_match):
        """Test that trigger_kwargs raises appropriate errors with invalid parameters."""
        with pytest.raises(expected_error, match=error_match):
            self.provider.trigger_kwargs(queue_uri)
