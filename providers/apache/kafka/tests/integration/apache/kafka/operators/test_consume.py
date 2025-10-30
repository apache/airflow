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

import logging
from typing import Any

import pytest
from confluent_kafka import Producer

# Import Operator
from airflow.models.connection import Connection
from airflow.providers.apache.kafka.operators.consume import ConsumeFromTopicOperator

log = logging.getLogger(__name__)


def _batch_tester(messages, test_string=None):
    assert test_string
    assert len(messages) == 10

    for x in messages:
        # Confluent Kafka converts messages to bytes
        assert x.value().decode(encoding="utf-8") == test_string


def _basic_message_tester(message, test=None) -> Any:
    """a function that tests the message received"""

    assert test
    # Confluent Kafka converts messages to bytes
    assert message.value().decode(encoding="utf-8") == test


@pytest.fixture(autouse=True)
def kafka_consumer_connections(create_connection_without_db):
    """Create Kafka consumer connections for testing purpose."""
    connections = [
        Connection(
            conn_id="operator.consumer.test.integration.test_1",
            uri="kafka://broker:29092?socket.timeout.ms=10&bootstrap.servers=broker:29092&group.id=operator.consumer.test.integration.test_1&enable.auto.commit=False&auto.offset.reset=beginning",
        ),
        Connection(
            conn_id="operator.consumer.test.integration.test_2",
            uri="kafka://broker:29092?socket.timeout.ms=10&bootstrap.servers=broker:29092&group.id=operator.consumer.test.integration.test_2&enable.auto.commit=False&auto.offset.reset=beginning",
        ),
        Connection(
            conn_id="operator.consumer.test.integration.test_3",
            uri="kafka://broker:29092?socket.timeout.ms=10&bootstrap.servers=broker:29092&group.id=operator.consumer.test.integration.test_3&enable.auto.commit=False&auto.offset.reset=beginning",
        ),
    ]

    for conn in connections:
        create_connection_without_db(conn)


@pytest.mark.integration("kafka")
class TestConsumeFromTopic:
    """
    test ConsumeFromTopicOperator
    """

    def test_consumer_operator_test_1(self):
        """test consumer works with string import"""

        TOPIC = "operator.consumer.test.integration.test_1"

        p = Producer(**{"bootstrap.servers": "broker:29092"})
        p.produce(TOPIC, TOPIC)
        assert len(p) == 1
        x = p.flush()
        assert x == 0

        operator = ConsumeFromTopicOperator(
            kafka_config_id=TOPIC,
            topics=[TOPIC],
            apply_function="integration.apache.kafka.operators.test_consume._basic_message_tester",
            apply_function_kwargs={"test": TOPIC},
            task_id="test",
            poll_timeout=10,
        )

        x = operator.execute(context={})

    def test_consumer_operator_test_2(self):
        """test consumer works with direct binding"""

        TOPIC = "operator.consumer.test.integration.test_2"

        p = Producer(**{"bootstrap.servers": "broker:29092"})
        p.produce(TOPIC, TOPIC)
        assert len(p) == 1
        x = p.flush()
        assert x == 0

        operator = ConsumeFromTopicOperator(
            kafka_config_id=TOPIC,
            topics=[TOPIC],
            apply_function=_basic_message_tester,
            apply_function_kwargs={"test": TOPIC},
            task_id="test",
            poll_timeout=10,
        )

        x = operator.execute(context={})

    def test_consumer_operator_test_3(self):
        """test consumer works in batch mode"""

        TOPIC = "operator.consumer.test.integration.test_3"

        p = Producer(**{"bootstrap.servers": "broker:29092"})

        for _ in range(20):
            p.produce(TOPIC, TOPIC)

        assert len(p) == 20
        x = p.flush()
        assert x == 0

        operator = ConsumeFromTopicOperator(
            kafka_config_id=TOPIC,
            topics=[TOPIC],
            apply_function_batch=_batch_tester,
            apply_function_kwargs={"test_string": TOPIC},
            task_id="test",
            poll_timeout=10,
            commit_cadence="end_of_batch",
            max_messages=30,
            max_batch_size=10,
        )

        x = operator.execute(context={})
