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

from airflow.models import Connection
from airflow.providers.apache.kafka.hooks.client import KafkaAdminClientHook
from airflow.providers.apache.kafka.hooks.produce import KafkaProducerHook
from airflow.utils import db

log = logging.getLogger(__name__)
config = {
    "bootstrap.servers": "broker:29092",
    "group.id": "hook.producer.integration.test",
}


@pytest.mark.integration("kafka")
class TestProducerHook:
    """
    Test consumer hook.
    """

    def setup_method(self):
        db.merge_conn(
            Connection(
                conn_id="kafka_default",
                conn_type="kafka",
                extra=json.dumps(config),
            )
        )

    def test_produce(self):
        """test producer hook functionality"""

        topic = "producer_hook_integration_test"

        def acked(err, msg):
            if err is not None:
                raise Exception(f"{err}")
            else:
                assert msg.topic() == topic
                assert msg.partition() == 0
                assert msg.offset() == 0

        # Standard Init
        p_hook = KafkaProducerHook(kafka_config_id="kafka_default")

        producer = p_hook.get_producer()
        producer.produce(topic, key="p1", value="p2", on_delivery=acked)
        producer.poll(0)
        producer.flush()
        hook = KafkaAdminClientHook(kafka_config_id="kafka_default")
        hook.delete_topic(topics=[topic])
