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
        ["max_messages", "expected_consumed_messages"],
        [
            [None, 1001],  # Consume all messages
            [100, 1000],  # max_messages < max_batch_size -> max_messages is set to default max_batch_size
            [2000, 1001],  # max_messages > max_batch_size
        ],
    )
    def test_operator_consume(self, max_messages, expected_consumed_messages):
        total_consumed_messages = 0
        mocked_messages = ["test_messages" for i in range(1001)]

        def mock_consume(num_messages=0, timeout=-1):
            nonlocal mocked_messages
            nonlocal total_consumed_messages
            if num_messages < 0:
                raise Exception("Number of messages needs to be positive")
            msg_count = min(num_messages, len(mocked_messages))
            returned_messages = mocked_messages[:msg_count]
            mocked_messages = mocked_messages[msg_count:]
            total_consumed_messages += msg_count
            return returned_messages

        mock_consumer = mock.MagicMock()
        mock_consumer.consume = mock_consume

        with mock.patch(
            "airflow.providers.apache.kafka.hooks.consume.KafkaConsumerHook.get_consumer"
        ) as mock_get_consumer:
            mock_get_consumer.return_value = mock_consumer

            operator = ConsumeFromTopicOperator(
                kafka_config_id="kafka_d",
                topics=["test"],
                task_id="test",
                poll_timeout=0.0001,
                max_messages=max_messages,
            )

            # execute the operator (this is essentially a no op as we're mocking the consumer)
            operator.execute(context={})
            assert total_consumed_messages == expected_consumed_messages

    @pytest.mark.parametrize(
        "commit_cadence, enable_auto_commit, expected_warning",
        [
            # will raise AirflowException for invalid commit_cadence
            ("invalid_cadence", "false", False),
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
    def test__validate_commit_cadence(self, commit_cadence, enable_auto_commit, expected_warning):
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
            ConsumeFromTopicOperator(**operator_kwargs)
            if expected_warning:
                expected_warning_template = (
                    "To respect commit_cadence='%s', "
                    "'enable.auto.commit' should be set to 'false' in the Kafka connection configuration. "
                    "Currently, 'enable.auto.commit' is not explicitly set, so it defaults to 'true', which causes "
                    "the consumer to auto-commit offsets every 5 seconds. "
                    "See: https://kafka.apache.org/documentation/#consumerconfigs_enable.auto.commit"
                )
                mock_log.warning.assert_called_with(expected_warning_template, commit_cadence)
            else:
                mock_log.warning.assert_not_called()
