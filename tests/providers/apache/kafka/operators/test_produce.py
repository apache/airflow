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

import pytest

from airflow.models import Connection
from airflow.providers.apache.kafka.operators.produce import ProduceToTopicOperator
from airflow.utils import db

pytestmark = pytest.mark.db_test


log = logging.getLogger(__name__)


def _simple_producer(key, value) -> list[tuple[Any, Any]]:
    """simple_producer A function that returns the key,value passed
    in for production via "KafkaProducerOperator"

    :param key: the key for the message
    :param value: the value for the message
    :return: The Key / Value pair for production via the operator
    :rtype: List[Tuple[Any, Any]]
    """
    return [(key, value)]


class TestProduceToTopic:
    """
    Test ConsumeFromTopic
    """

    def setup_method(self):
        db.merge_conn(
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
            producer_function="tests.providers.apache.kafka.operators.test_produce._simple_producer",
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
