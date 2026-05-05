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
from unittest.mock import MagicMock, patch

import pytest
from confluent_kafka.admin import AdminClient

from airflow.models import Connection
from airflow.providers.apache.kafka.hooks.produce import KafkaProducerHook

log = logging.getLogger(__name__)


class TestProducerHook:
    """
    Test consumer hook.
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

        create_connection_without_db(
            Connection(
                conn_id="kafka_bad",
                conn_type="kafka",
                extra=json.dumps({}),
            )
        )
        self.hook = KafkaProducerHook(kafka_config_id="kafka_d")

    @patch("airflow.providers.apache.kafka.hooks.base.AdminClient")
    def test_get_producer(self, mock_client):
        mock_client_spec = MagicMock(spec=AdminClient)
        mock_client.return_value = mock_client_spec
        assert self.hook.get_producer() == self.hook.get_conn

    @patch("airflow.providers.apache.kafka.hooks.produce.Producer")
    def test_config_dict_overrides_connection_config(self, mock_producer):
        hook = KafkaProducerHook(
            kafka_config_id="kafka_d", config_dict={"bootstrap.servers": "override:9092"}
        )
        producer = hook.get_producer()
        mock_producer.assert_called_once_with(
            {"socket.timeout.ms": 10, "bootstrap.servers": "override:9092", "group.id": "test_group"}
        )
        assert producer == mock_producer.return_value

    @patch("airflow.providers.apache.kafka.hooks.produce.Producer")
    def test_config_dict_standalone_without_connection(self, mock_producer):
        hook = KafkaProducerHook(
            kafka_config_id="kafka_missing", config_dict={"bootstrap.servers": "standalone:9092"}
        )
        producer = hook.get_producer()
        mock_producer.assert_called_once_with({"bootstrap.servers": "standalone:9092"})
        assert producer == mock_producer.return_value
