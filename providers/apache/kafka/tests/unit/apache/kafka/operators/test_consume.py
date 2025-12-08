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

import json
import logging
from typing import Any
from unittest import mock

import pytest

from airflow.exceptions import AirflowException
from airflow.models import Connection

# Import Operator
from airflow.providers.apache.kafka.operators.consume import VALID_COMMIT_CADENCE, ConsumeFromTopicOperator

log = logging.getLogger(__name__)


def _no_op(*args, **kwargs) -> Any:
    """no_op A function that returns its arguments

    :return: whatever was passed in
    :rtype: Any
    """
    return args, kwargs


def create_mock_kafka_consumer(
    message_count: int = 1001, message_content: Any = "test_message", track_consumed_messages: bool = False
) -> tuple[mock.MagicMock, mock.MagicMock, list[int] | None]:
    """
    Creates a mock Kafka consumer with configurable behavior.

    :param message_count: Number of messages to generate
    :param message_content: Content of each message
    :param track_consumed_messages: Whether to track total consumed messages
    :return: Tuple of (mock_consumer, mock_get_consumer, total_consumed_messages)
    """
    # Initialize messages and tracking variables
    mocked_messages = [message_content for _ in range(message_count)]
    total_consumed_count = [0] if track_consumed_messages else None

    # Define the mock consume behavior
    def mock_consume(num_messages=0, timeout=-1):
        nonlocal mocked_messages
        if num_messages < 0:
            raise Exception("Number of messages needs to be positive")

        msg_count = min(num_messages, len(mocked_messages))
        returned_messages = mocked_messages[:msg_count]
        mocked_messages = mocked_messages[msg_count:]

        if track_consumed_messages:
            total_consumed_count[0] += msg_count

        return returned_messages

    # Create mock objects
    mock_consumer = mock.MagicMock()
    mock_consumer.consume = mock_consume

    mock_get_consumer = mock.patch(
        "airflow.providers.apache.kafka.hooks.consume.KafkaConsumerHook.get_consumer",
        return_value=mock_consumer,
    )

    #
    return mock_consumer, mock_get_consumer, total_consumed_count  # type: ignore[return-value]


class TestConsumeFromTopic:
    """
    Test ConsumeFromTopic
    """

    @pytest.fixture(autouse=True)
    def setup_connections(self, create_connection_without_db):
        create_connection_without_db(
            Connection(
                conn_id="kafka_d",
                conn_type="kafka",
                extra=json.dumps(
                    {"socket.timeout.ms": 10, "bootstrap.servers": "localhost:9092", "group.id": "test_group"}
                ),
            )
        )

    def test_operator(self):
        operator = ConsumeFromTopicOperator(
            kafka_config_id="kafka_d",
            topics=["test"],
            apply_function="unit.apache.kafka.operators.test_consume._no_op",
            task_id="test",
            poll_timeout=0.0001,
        )

        # execute the operator (this is essentially a no op as the broker isn't setup)
        operator.execute(context={})

    def test_operator_callable(self):
        operator = ConsumeFromTopicOperator(
            kafka_config_id="kafka_d",
            topics=["test"],
            apply_function=_no_op,
            task_id="test",
            poll_timeout=0.0001,
        )

        # execute the operator (this is essentially a no op as the broker isn't setup)
        operator.execute(context={})

    @pytest.mark.parametrize(
        ("max_messages", "expected_consumed_messages"),
        [
            [None, 1001],  # Consume all messages
            [100, 1000],  # max_messages < max_batch_size -> max_messages is set to default max_batch_size
            [2000, 1001],  # max_messages > max_batch_size
        ],
    )
    def test_operator_consume(self, max_messages, expected_consumed_messages):
        # Create mock consumer with tracking of consumed messages
        _, mock_get_consumer, consumed_messages = create_mock_kafka_consumer(
            message_count=1001, message_content="test_messages", track_consumed_messages=True
        )

        # Use the mock
        with mock_get_consumer:
            operator = ConsumeFromTopicOperator(
                kafka_config_id="kafka_d",
                topics=["test"],
                task_id="test",
                poll_timeout=0.0001,
                max_messages=max_messages,
            )

            # execute the operator (this is essentially a no op as we're mocking the consumer)
            operator.execute(context={})
            assert consumed_messages[0] == expected_consumed_messages

    @pytest.mark.parametrize(
        "commit_cadence",
        [
            # will raise AirflowException for invalid commit_cadence
            ("invalid_cadence"),
            ("end_of_operator"),
            ("end_of_batch"),
            ("never"),
        ],
    )
    def test__validate_commit_cadence_on_construct(self, commit_cadence):
        operator_kwargs = {
            "kafka_config_id": "kafka_d",
            "topics": ["test"],
            "task_id": "test",
            "commit_cadence": commit_cadence,
        }
        # early return for invalid commit_cadence
        if commit_cadence == "invalid_cadence":
            with pytest.raises(
                AirflowException,
                match=f"commit_cadence must be one of {VALID_COMMIT_CADENCE}. Got invalid_cadence",
            ):
                ConsumeFromTopicOperator(**operator_kwargs)
            return

        # should not raise AirflowException for valid commit_cadence
        ConsumeFromTopicOperator(**operator_kwargs)

    @pytest.mark.parametrize(
        ("commit_cadence", "enable_auto_commit", "expected_warning"),
        [
            # will not log warning if set 'enable.auto.commit' to false
            ("end_of_operator", "false", False),
            ("end_of_batch", "false", False),
            ("never", "false", False),
            # will log warning if set 'enable.auto.commit' to true
            ("end_of_operator", "true", True),
            ("end_of_batch", "true", True),
            ("never", "true", True),
            # will log warning if 'enable.auto.commit' is not set
            ("end_of_operator", None, True),
            ("end_of_batch", None, True),
            ("never", None, True),
            # will not log warning if commit_cadence is None, no matter the value of 'enable.auto.commit'
            (None, None, False),
            (None, "true", False),
            (None, "false", False),
        ],
    )
    def test__validate_commit_cadence_before_execute(
        self, commit_cadence, enable_auto_commit, expected_warning
    ):
        # mock connection and hook
        mocked_hook = mock.MagicMock()
        mocked_hook.get_connection.return_value.extra_dejson = (
            {} if enable_auto_commit is None else {"enable.auto.commit": enable_auto_commit}
        )

        with (
            mock.patch(
                "airflow.providers.apache.kafka.operators.consume.ConsumeFromTopicOperator.hook",
                new_callable=mock.PropertyMock,
                return_value=mocked_hook,
            ),
            mock.patch(
                "airflow.providers.apache.kafka.operators.consume.ConsumeFromTopicOperator.log"
            ) as mock_log,
        ):
            operator = ConsumeFromTopicOperator(
                kafka_config_id="kafka_d",
                topics=["test"],
                task_id="test",
                commit_cadence=commit_cadence,
            )
            operator._validate_commit_cadence_before_execute()
            if expected_warning:
                expected_warning_template = (
                    "To respect commit_cadence='%s', "
                    "'enable.auto.commit' should be set to 'false' in the Kafka connection configuration. "
                    "Currently, 'enable.auto.commit' is not explicitly set, so it defaults to 'true', which causes "
                    "the consumer to auto-commit offsets every 5 seconds. "
                    "See: https://kafka.apache.org/documentation/#consumerconfigs_enable.auto.commit for more information"
                )
                mock_log.warning.assert_called_with(expected_warning_template, commit_cadence)
            else:
                mock_log.warning.assert_not_called()

    @pytest.mark.parametrize(
        ("commit_cadence", "max_messages", "expected_commit_calls"),
        [
            # end_of_operator: should call commit once at the end
            ("end_of_operator", 1500, 1),
            # end_of_batch: should call commit after each batch (2 batches for 1500 messages with default batch size 1000)
            # and a final commit at the end of execute (since commit_cadence is not 'never')
            ("end_of_batch", 1500, 3),
            # never: should never call commit
            ("never", 1500, 0),
        ],
    )
    def test_commit_cadence_behavior(self, commit_cadence, max_messages, expected_commit_calls):
        # Create mock consumer with 1500 messages (will use 1001 for the first batch)
        mock_consumer, mock_get_consumer, _ = create_mock_kafka_consumer(
            message_count=1001,  # Only need to create 1001 messages for the first batch
        )

        # Use the mocks
        with mock_get_consumer:
            # Create and execute the operator
            operator = ConsumeFromTopicOperator(
                kafka_config_id="kafka_d",
                topics=["test"],
                task_id="test",
                poll_timeout=0.0001,
                max_messages=max_messages,
                commit_cadence=commit_cadence,
                apply_function=_no_op,
            )

            operator.execute(context={})

            # Verify commit was called the expected number of times
            assert mock_consumer.commit.call_count == expected_commit_calls

            # Verify consumer was closed
            mock_consumer.close.assert_called_once()
