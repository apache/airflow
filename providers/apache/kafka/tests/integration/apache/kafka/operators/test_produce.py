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

import pytest
from confluent_kafka import Consumer

from airflow.models.connection import Connection
from airflow.providers.apache.kafka.operators.produce import ProduceToTopicOperator

log = logging.getLogger(__name__)


def _producer_function():
    for i in range(20):
        yield (json.dumps(i), json.dumps(i + 1))


@pytest.fixture(autouse=True)
def kafka_connections(create_connection_without_db):
    """Create Kafka producer connections for testing purpose."""
    connections = [
        Connection(
            conn_id="kafka_default_test_1",
            uri="kafka://broker:29092?socket.timeout.ms=10&message.timeout.ms=10&group.id=operator.producer.test.integration.test_1",
        ),
        Connection(
            conn_id="kafka_default_test_2",
            uri="kafka://broker:29092?socket.timeout.ms=10&message.timeout.ms=10&group.id=operator.producer.test.integration.test_2",
        ),
    ]

    for conn in connections:
        create_connection_without_db(conn)


@pytest.mark.integration("kafka")
class TestProduceToTopic:
    """
    test ProduceToTopicOperator
    """

    def test_producer_operator_test_1(self):
        GROUP = "operator.producer.test.integration.test_1"
        TOPIC = "operator.producer.test.integration.test_1"

        t = ProduceToTopicOperator(
            kafka_config_id="kafka_default",
            task_id="produce_to_topic",
            topic=TOPIC,
            producer_function="integration.apache.kafka.operators.test_produce._producer_function",
        )

        t.execute(context={})

        config = {
            "bootstrap.servers": "broker:29092",
            "group.id": GROUP,
            "enable.auto.commit": False,
            "auto.offset.reset": "beginning",
        }

        c = Consumer(config)
        c.subscribe([TOPIC])
        msg = c.consume()

        assert msg[0].key() == b"0"
        assert msg[0].value() == b"1"

    def test_producer_operator_test_2(self):
        GROUP = "operator.producer.test.integration.test_2"
        TOPIC = "operator.producer.test.integration.test_2"

        t = ProduceToTopicOperator(
            kafka_config_id="kafka_default",
            task_id="produce_to_topic",
            topic=TOPIC,
            producer_function=_producer_function,
        )

        t.execute(context={})

        config = {
            "bootstrap.servers": "broker:29092",
            "group.id": GROUP,
            "enable.auto.commit": False,
            "auto.offset.reset": "beginning",
        }

        c = Consumer(config)
        c.subscribe([TOPIC])
        msg = c.consume()

        assert msg[0].key() == b"0"
        assert msg[0].value() == b"1"
