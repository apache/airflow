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
            pytest.param("ibmmq://mq_default/MY.QUEUE.NAME", True, id="valid_mq_uri"),
            pytest.param("ibmmq://mq_default", False, id="invalid_mq_url"),
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
            pytest.param("ibmmq", True, id="mq_scheme"),
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
                "ibmmq://my_conn/QUEUE1",
                {},
                {
                    "mq_conn_id": "my_conn",
                    "queue_name": "QUEUE1",
                    "poll_interval": 5,
                },
                id="default_poll_interval",
            ),
            pytest.param(
                "ibmmq://my_conn/QUEUE1",
                {"poll_interval": 60},
                {
                    "mq_conn_id": "my_conn",
                    "queue_name": "QUEUE1",
                },
                id="override_poll_interval",
            ),
            pytest.param(
                "ibmmq://my_conn/QUEUE1?open_options=8192",
                {},
                {
                    "mq_conn_id": "my_conn",
                    "queue_name": "QUEUE1",
                    "poll_interval": 5,
                    "open_options": 8192,
                },
                id="open_options_from_uri_query",
            ),
            pytest.param(
                "ibmmq://my_conn/QUEUE1?open_options=MQOO_INPUT_SHARED",
                {},
                {
                    "mq_conn_id": "my_conn",
                    "queue_name": "QUEUE1",
                    "poll_interval": 5,
                    "open_options": 2,
                },
                id="open_options_symbolic_name_from_uri_query",
            ),
            pytest.param(
                "ibmmq://my_conn/QUEUE1",
                {"open_options": 32},
                {
                    "mq_conn_id": "my_conn",
                    "queue_name": "QUEUE1",
                    "poll_interval": 5,
                },
                id="open_options_in_kwargs_is_not_re_emitted",
            ),
            pytest.param(
                "ibmmq://my_conn/QUEUE1?open_options=8192",
                {"open_options": 32},
                {
                    "mq_conn_id": "my_conn",
                    "queue_name": "QUEUE1",
                    "poll_interval": 5,
                },
                id="uri_open_options_not_re_emitted_when_kwargs_has_open_options",
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
                "ibmmq:///QUEUE1",
                ValueError,
                "MQ URI must contain connection id",
                id="missing_conn_id",
            ),
            pytest.param(
                "ibmmq://my_conn/",
                ValueError,
                "MQ URI must contain queue name",
                id="missing_queue_name",
            ),
            pytest.param(
                "ibmmq://my_conn/QUEUE1?open_options=not_a_number",
                ValueError,
                "Invalid MQ URI open_options value",
                id="invalid_open_options_value",
            ),
            pytest.param(
                "ibmmq://my_conn/QUEUE1?open_options=",
                ValueError,
                "MQ URI open_options query parameter values must be numeric or CMQC constant names",
                id="empty_open_options_value",
            ),
            pytest.param(
                "ibmmq://my_conn/QUEUE1?open_options=MQGMO_NO_WAIT",
                ValueError,
                "Invalid MQ URI open_options value",
                id="non_mqoo_symbolic_open_options_value",
            ),
        ],
    )
    def test_trigger_kwargs_error_cases(self, queue_uri, expected_error, error_match):
        """Test that trigger_kwargs raises appropriate errors with invalid parameters."""
        with pytest.raises(expected_error, match=error_match):
            self.provider.trigger_kwargs(queue_uri)

    @pytest.mark.parametrize(
        ("queue_uri", "expected_open_options"),
        [
            pytest.param(
                "ibmmq://my_conn/QUEUE1?open_options=MQOO_INPUT_SHARED",
                "MQOO_INPUT_SHARED",
                id="symbolic_name",
            ),
            pytest.param(
                "ibmmq://my_conn/QUEUE1?open_options=MQOO_INPUT_SHARED&open_options=MQOO_FAIL_IF_QUIESCING",
                "MQOO_INPUT_SHARED|MQOO_FAIL_IF_QUIESCING",
                id="multiple_query_values",
            ),
            pytest.param(
                "ibmmq://my_conn/QUEUE1?open_options=MQOO_INPUT_SHARED|MQOO_FAIL_IF_QUIESCING",
                "MQOO_INPUT_SHARED|MQOO_FAIL_IF_QUIESCING",
                id="pipe_delimited",
            ),
            pytest.param(
                "ibmmq://my_conn/QUEUE1?open_options=MQOO_INPUT_SHARED,MQOO_FAIL_IF_QUIESCING",
                "MQOO_INPUT_SHARED|MQOO_FAIL_IF_QUIESCING",
                id="comma_delimited",
            ),
            pytest.param(
                "ibmmq://my_conn/QUEUE1?open_options=2|MQOO_FAIL_IF_QUIESCING",
                "MQOO_INPUT_SHARED|MQOO_FAIL_IF_QUIESCING",
                id="mixed_numeric_and_symbolic",
            ),
        ],
    )
    def test_trigger_kwargs_parses_open_options_formats(self, queue_uri, expected_open_options):
        import ibmmq

        expected = 0
        for flag in expected_open_options.split("|"):
            expected |= getattr(ibmmq.CMQC, flag)

        kwargs = self.provider.trigger_kwargs(queue_uri)
        assert kwargs["open_options"] == expected

    def test_message_queue_trigger_with_scheme(self):
        from airflow.providers.common.messaging.triggers.msg_queue import MessageQueueTrigger

        trigger = MessageQueueTrigger(
            scheme="ibmmq",
            mq_conn_id="mq_default",
            queue_name="MY.QUEUE.NAME",
        )
        assert trigger.queue is None
        assert trigger.scheme == "ibmmq"
        assert isinstance(trigger.trigger, AwaitMessageTrigger)
        assert trigger.trigger.mq_conn_id == "mq_default"
        assert trigger.trigger.queue_name == "MY.QUEUE.NAME"
        assert trigger.trigger.poll_interval == 5

    @pytest.mark.filterwarnings("ignore::airflow.exceptions.AirflowProviderDeprecationWarning")
    def test_message_queue_trigger_with_deprecated_queue(self):
        from airflow.providers.common.messaging.triggers.msg_queue import MessageQueueTrigger

        trigger = MessageQueueTrigger(queue="ibmmq://mq_default/MY.QUEUE.NAME", open_options=32)
        assert trigger.scheme is None
        assert trigger.queue == "ibmmq://mq_default/MY.QUEUE.NAME"
        assert isinstance(trigger.trigger, AwaitMessageTrigger)
        assert trigger.trigger.mq_conn_id == "mq_default"
        assert trigger.trigger.queue_name == "MY.QUEUE.NAME"
        assert trigger.trigger.poll_interval == 5
        assert trigger.trigger.open_options == 32
