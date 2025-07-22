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
from unittest.mock import MagicMock, patch

import pytest
from confluent_kafka.admin import AdminClient

from airflow.models import Connection

# Import Hook
from airflow.providers.apache.kafka.hooks.consume import KafkaConsumerHook


class TestConsumerHook:
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
        self.hook = KafkaConsumerHook(["test_1"], kafka_config_id="kafka_d")

    @patch("airflow.providers.apache.kafka.hooks.base.AdminClient")
    def test_get_consumer(self, mock_client):
        mock_client_spec = MagicMock(spec=AdminClient)
        mock_client.return_value = mock_client_spec
        assert self.hook.get_consumer() == self.hook.get_conn
