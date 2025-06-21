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
import os

import pytest
from confluent_kafka import Consumer

from airflow.models import Connection
from airflow.providers.apache.kafka.operators.produce import ProduceToTopicOperator

log = logging.getLogger(__name__)


def _producer_function():
    for i in range(20):
        yield (json.dumps(i), json.dumps(i + 1))


@pytest.mark.integration("kafka")
class TestProduceToTopic:
    """
    test ProduceToTopicOperator
    """

    def setup_method(self):
        """Set up connections for each test method."""
        # Create separate connections for each test
        for num in (1, 2):
            group = f"operator.producer.test.integration.test_{num}"
            conn = Connection(
                conn_id=f"kafka_default_test_{num}",
                conn_type="kafka",
                extra=json.dumps(
                    {
                        "socket.timeout.ms": 10,
                        "message.timeout.ms": 10,
                        "bootstrap.servers": "broker:29092",
                        "group.id": group,
                    }
                ),
            )

            # Set environment variable directly (like create_connection_without_db does)
            env_var_name = f"AIRFLOW_CONN_{conn.conn_id.upper()}"
            os.environ[env_var_name] = conn.get_uri()

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
