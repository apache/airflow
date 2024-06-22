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
from confluent_kafka import KafkaException
from confluent_kafka.admin import AdminClient, NewTopic

from airflow.models import Connection
from airflow.providers.apache.kafka.hooks.client import KafkaAdminClientHook
from airflow.utils import db

pytestmark = pytest.mark.db_test

log = logging.getLogger(__name__)


class TestKafkaAdminClientHook:
    def setup_method(self):
        db.merge_conn(
            Connection(
                conn_id="kafka_d",
                conn_type="kafka",
                extra=json.dumps(
                    {"socket.timeout.ms": 10, "bootstrap.servers": "localhost:9092", "group.id": "test_group"}
                ),
            )
        )

        db.merge_conn(
            Connection(
                conn_id="kafka_bad",
                conn_type="kafka",
                extra=json.dumps({"socket.timeout.ms": 10}),
            )
        )
        self.hook = KafkaAdminClientHook(kafka_config_id="kafka_d")

    def test_get_conn(self):
        assert isinstance(self.hook.get_conn, AdminClient)

    @patch(
        "airflow.providers.apache.kafka.hooks.base.AdminClient",
    )
    def test_create_topic(self, admin_client):
        mock_f = MagicMock()
        admin_client.return_value.create_topics.return_value = {"topic_name": mock_f}
        self.hook.create_topic(topics=[("topic_name", 0, 1)])
        admin_client.return_value.create_topics.assert_called_with([NewTopic("topic_name", 0, 1)])
        mock_f.result.assert_called_once()

    @patch(
        "airflow.providers.apache.kafka.hooks.base.AdminClient",
    )
    def test_create_topic_error(self, admin_client):
        mock_f = MagicMock()
        kafka_exception = KafkaException()
        mock_arg = MagicMock()
        # mock_arg.name = "TOPIC_ALREADY_EXISTS"
        kafka_exception.args = [mock_arg]
        mock_f.result.side_effect = [kafka_exception]
        admin_client.return_value.create_topics.return_value = {"topic_name": mock_f}
        with pytest.raises(KafkaException):
            self.hook.create_topic(topics=[("topic_name", 0, 1)])

    @patch(
        "airflow.providers.apache.kafka.hooks.base.AdminClient",
    )
    def test_create_topic_warning(self, admin_client, caplog):
        mock_f = MagicMock()
        kafka_exception = KafkaException()
        mock_arg = MagicMock()
        mock_arg.name = "TOPIC_ALREADY_EXISTS"
        kafka_exception.args = [mock_arg]
        mock_f.result.side_effect = [kafka_exception]
        admin_client.return_value.create_topics.return_value = {"topic_name": mock_f}
        with caplog.at_level(
            logging.WARNING, logger="airflow.providers.apache.kafka.hooks.client.KafkaAdminClientHook"
        ):
            self.hook.create_topic(topics=[("topic_name", 0, 1)])
            assert "The topic topic_name already exists" in caplog.text

    @patch(
        "airflow.providers.apache.kafka.hooks.base.AdminClient",
    )
    def test_delete_topic(self, admin_client):
        mock_f = MagicMock()
        admin_client.return_value.delete_topics.return_value = {"topic_name": mock_f}
        self.hook.delete_topic(topics=["topic_name"])
        admin_client.return_value.delete_topics.assert_called_with(["topic_name"])
        mock_f.result.assert_called_once()
