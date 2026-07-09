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
from confluent_kafka import KafkaError

from airflow.models import Connection
from airflow.providers.apache.kafka.operators.produce import (
    KafkaMessageDeliveryError,
    ProduceToTopicOperator,
)

log = logging.getLogger(__name__)

GET_PRODUCER_PATH = "airflow.providers.apache.kafka.hooks.produce.KafkaProducerHook.get_producer"


def _simple_producer(key, value) -> list[tuple[Any, Any]]:
    """simple_producer A function that returns the key,value passed
    in for production via "KafkaProducerOperator"

    :param key: the key for the message
    :param value: the value for the message
    :return: The Key / Value pair for production via the operator
    :rtype: list[tuple[Any, Any]]
    """
    return [(key, value)]


_custom_delivery_callback_calls: list[Any] = []


def _custom_delivery_callback(err, msg) -> None:
    """A delivery callback recording the errors it was invoked with."""
    _custom_delivery_callback_calls.append(err)


class TestProduceToTopic:
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
                    {
                        "socket.timeout.ms": 10,
                        "message.timeout.ms": 10,
                        "bootstrap.servers": "localhost:9092",
                        "group.id": "test_group",
                    }
                ),
            )
        )

    def test_operator_string(self):
        operator = ProduceToTopicOperator(
            kafka_config_id="kafka_d",
            topic="test_1",
            producer_function="unit.apache.kafka.operators.test_produce._simple_producer",
            producer_function_args=(b"test", b"test"),
            task_id="test",
            synchronous=False,
        )

        operator.execute(context={})

    def test_operator_callable(self):
        operator = ProduceToTopicOperator(
            kafka_config_id="kafka_d",
            topic="test_1",
            producer_function=_simple_producer,
            producer_function_args=(b"test", b"test"),
            task_id="test",
            synchronous=False,
        )

        operator.execute(context={})

    @mock.patch(GET_PRODUCER_PATH)
    def test_operator_raise_on_delivery_failure(self, mock_get_producer):
        mock_producer = mock_get_producer.return_value
        error = KafkaError(KafkaError.MSG_SIZE_TOO_LARGE)
        mock_producer.produce.side_effect = lambda topic, key, value, on_delivery: on_delivery(error, None)

        operator = ProduceToTopicOperator(
            kafka_config_id="kafka_d",
            topic="test_1",
            producer_function=_simple_producer,
            producer_function_args=(b"test", b"test"),
            task_id="test",
            synchronous=False,
            raise_on_delivery_failure=True,
        )

        with pytest.raises(KafkaMessageDeliveryError, match="Failed to deliver 1 message"):
            operator.execute(context={})

    @mock.patch(GET_PRODUCER_PATH)
    def test_operator_raise_on_delivery_failure_successful_delivery(self, mock_get_producer):
        mock_producer = mock_get_producer.return_value
        mock_producer.produce.side_effect = lambda topic, key, value, on_delivery: on_delivery(
            None, mock.MagicMock()
        )

        operator = ProduceToTopicOperator(
            kafka_config_id="kafka_d",
            topic="test_1",
            producer_function=_simple_producer,
            producer_function_args=(b"test", b"test"),
            task_id="test",
            synchronous=False,
            raise_on_delivery_failure=True,
        )

        operator.execute(context={})

    @mock.patch(GET_PRODUCER_PATH)
    def test_operator_delivery_failure_ignored_by_default(self, mock_get_producer):
        mock_producer = mock_get_producer.return_value
        error = KafkaError(KafkaError.MSG_SIZE_TOO_LARGE)
        mock_producer.produce.side_effect = lambda topic, key, value, on_delivery: on_delivery(error, None)

        operator = ProduceToTopicOperator(
            kafka_config_id="kafka_d",
            topic="test_1",
            producer_function=_simple_producer,
            producer_function_args=(b"test", b"test"),
            task_id="test",
            synchronous=False,
        )

        operator.execute(context={})

    @mock.patch(GET_PRODUCER_PATH)
    def test_operator_raise_on_delivery_failure_custom_callback(self, mock_get_producer):
        _custom_delivery_callback_calls.clear()
        mock_producer = mock_get_producer.return_value
        error = KafkaError(KafkaError.MSG_SIZE_TOO_LARGE)
        mock_producer.produce.side_effect = lambda topic, key, value, on_delivery: on_delivery(error, None)

        operator = ProduceToTopicOperator(
            kafka_config_id="kafka_d",
            topic="test_1",
            producer_function=_simple_producer,
            producer_function_args=(b"test", b"test"),
            task_id="test",
            synchronous=False,
            delivery_callback="unit.apache.kafka.operators.test_produce._custom_delivery_callback",
            raise_on_delivery_failure=True,
        )

        with pytest.raises(KafkaMessageDeliveryError, match="Failed to deliver 1 message"):
            operator.execute(context={})

        assert _custom_delivery_callback_calls == [error]
