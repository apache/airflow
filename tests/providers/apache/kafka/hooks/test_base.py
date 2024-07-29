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

from unittest import mock
from unittest.mock import MagicMock

import pytest

from airflow.providers.apache.kafka.hooks.base import KafkaBaseHook


class SomeKafkaHook(KafkaBaseHook):
    def _get_client(self, config):
        return config


@pytest.fixture
def hook():
    return SomeKafkaHook()


TIMEOUT = 10


class TestKafkaBaseHook:
    @mock.patch("airflow.hooks.base.BaseHook.get_connection")
    def test_get_conn(self, mock_get_connection, hook):
        config = {"bootstrap.servers": MagicMock()}
        mock_get_connection.return_value.extra_dejson = config
        assert hook.get_conn == config

    @mock.patch("airflow.hooks.base.BaseHook.get_connection")
    def test_get_conn_value_error(self, mock_get_connection, hook):
        mock_get_connection.return_value.extra_dejson = {}
        with pytest.raises(ValueError, match="must be provided"):
            hook.get_conn()

    @mock.patch("airflow.providers.apache.kafka.hooks.base.AdminClient")
    @mock.patch("airflow.hooks.base.BaseHook.get_connection")
    def test_test_connection(self, mock_get_connection, admin_client, hook):
        config = {"bootstrap.servers": MagicMock()}
        mock_get_connection.return_value.extra_dejson = config
        connection = hook.test_connection()
        admin_client.assert_called_once_with(config, timeout=10)
        assert connection == (True, "Connection successful.")

    @mock.patch(
        "airflow.providers.apache.kafka.hooks.base.AdminClient",
        return_value=MagicMock(list_topics=MagicMock(return_value=[])),
    )
    @mock.patch("airflow.hooks.base.BaseHook.get_connection")
    def test_test_connection_no_topics(self, mock_get_connection, admin_client, hook):
        config = {"bootstrap.servers": MagicMock()}
        mock_get_connection.return_value.extra_dejson = config
        connection = hook.test_connection()
        admin_client.assert_called_once_with(config, timeout=TIMEOUT)
        assert connection == (False, "Failed to establish connection.")

    @mock.patch("airflow.providers.apache.kafka.hooks.base.AdminClient")
    @mock.patch("airflow.hooks.base.BaseHook.get_connection")
    def test_test_connection_exception(self, mock_get_connection, admin_client, hook):
        config = {"bootstrap.servers": MagicMock()}
        mock_get_connection.return_value.extra_dejson = config
        admin_client.return_value.list_topics.side_effect = [ValueError("some error")]
        connection = hook.test_connection()
        assert connection == (False, "some error")
