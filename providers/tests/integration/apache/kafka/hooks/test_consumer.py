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

import pytest
from confluent_kafka import Producer

from airflow.models import Connection

# Import Hook
from airflow.providers.apache.kafka.hooks.client import KafkaAdminClientHook
from airflow.providers.apache.kafka.hooks.consume import KafkaConsumerHook
from airflow.utils import db

TOPIC = "consumer_hook_test_1"

config = {
    "bootstrap.servers": "broker:29092",
    "group.id": "hook.consumer.integration.test",
    "enable.auto.commit": False,
    "auto.offset.reset": "beginning",
}


@pytest.mark.integration("kafka")
class TestConsumerHook:
    """
    Test consumer hook.
    """

    def setup_method(self):
        db.merge_conn(
            Connection(
                conn_id="kafka_d",
                conn_type="kafka",
                extra=json.dumps(config),
            )
        )

    def test_consume_messages(self):
        """test initialization of AdminClientHook"""

        # Standard Init
        p = Producer(**{"bootstrap.servers": "broker:29092"})
        p.produce(TOPIC, "test_message")
        assert len(p) == 1
        x = p.flush()
        assert x == 0

        c = KafkaConsumerHook([TOPIC], kafka_config_id="kafka_d")
        consumer = c.get_consumer()

        msg = consumer.consume()

        assert msg[0].value() == b"test_message"
        hook = KafkaAdminClientHook(kafka_config_id="kafka_d")
        hook.delete_topic(topics=[TOPIC])
