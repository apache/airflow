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
from unittest import mock
from unittest.mock import MagicMock

import pytest

from airflow.providers.apache.kafka.hooks.base import KafkaBaseHook

try:
    import importlib.util

    if not importlib.util.find_spec("airflow.sdk.bases.hook"):
        raise ImportError

    BASEHOOK_PATCH_PATH = "airflow.sdk.bases.hook.BaseHook"
except ImportError:
    BASEHOOK_PATCH_PATH = "airflow.hooks.base.BaseHook"


class SomeKafkaHook(KafkaBaseHook):
    def _get_client(self, config):
        return config


@pytest.fixture
def hook():
    return SomeKafkaHook()


TIMEOUT = 10


class TestKafkaBaseHook:
    @mock.patch(f"{BASEHOOK_PATCH_PATH}.get_connection")
    def test_get_conn(self, mock_get_connection, hook):
        config = {"bootstrap.servers": "localhost:9092"}
        mock_get_connection.return_value.extra_dejson = config
        assert hook.get_conn == config

    @mock.patch(f"{BASEHOOK_PATCH_PATH}.get_connection")
    def test_get_conn_value_error(self, mock_get_connection, hook):
        mock_get_connection.return_value.extra_dejson = {}
        with pytest.raises(ValueError, match="must be provided"):
            hook.get_conn()

    @mock.patch(f"{BASEHOOK_PATCH_PATH}.get_connection")
    def test_config_dict_overrides_connection_config(self, mock_get_connection):
        connection_config = {"bootstrap.servers": "conn:9092", "group.id": "from_conn"}
        mock_get_connection.return_value.extra_dejson = connection_config

        hook = SomeKafkaHook(config_dict={"bootstrap.servers": "override:9092"})

        assert hook.get_conn == {"bootstrap.servers": "override:9092", "group.id": "from_conn"}
        # The connection's own config must not be mutated by the merge.
        assert connection_config == {"bootstrap.servers": "conn:9092", "group.id": "from_conn"}

    @mock.patch(
        f"{BASEHOOK_PATCH_PATH}.get_connection",
        side_effect=RuntimeError("connection lookup failed"),
    )
    def test_config_dict_standalone_when_connection_unavailable(self, mock_get_connection):
        hook = SomeKafkaHook(config_dict={"bootstrap.servers": "standalone:9092"})
        assert hook.get_conn == {"bootstrap.servers": "standalone:9092"}

    @mock.patch(
        f"{BASEHOOK_PATCH_PATH}.get_connection",
        side_effect=RuntimeError("connection lookup failed"),
    )
    def test_connection_unavailable_without_bootstrap_servers(self, mock_get_connection):
        hook = SomeKafkaHook(config_dict={"group.id": "no_brokers"})
        with pytest.raises(RuntimeError, match="connection lookup failed"):
            hook.get_conn()

    @mock.patch(f"{BASEHOOK_PATCH_PATH}.get_connection")
    def test_callbacks_resolved_from_dotted_path(self, mock_get_connection):
        mock_get_connection.return_value.extra_dejson = {
            "bootstrap.servers": "localhost:9092",
            "error_cb": "json.loads",
        }
        stats_cb = MagicMock()
        hook = SomeKafkaHook(config_dict={"oauth_cb": "json.dumps", "stats_cb": stats_cb})

        config = hook.get_conn
        assert config["error_cb"] is json.loads
        assert config["oauth_cb"] is json.dumps
        # Already-callable values are passed through unchanged.
        assert config["stats_cb"] is stats_cb

    @mock.patch("airflow.providers.apache.kafka.hooks.base.AdminClient")
    @mock.patch(f"{BASEHOOK_PATCH_PATH}.get_connection")
    def test_test_connection(self, mock_get_connection, admin_client, hook):
        config = {"bootstrap.servers": MagicMock()}
        mock_get_connection.return_value.extra_dejson = config
        connection = hook.test_connection()
        admin_client.assert_called_once_with(config)
        mock_admin_instance = admin_client.return_value
        mock_admin_instance.list_topics.assert_called_once_with(timeout=TIMEOUT)
        assert connection == (True, "Connection successful.")

    @mock.patch(
        "airflow.providers.apache.kafka.hooks.base.AdminClient",
        return_value=MagicMock(list_topics=MagicMock(return_value=[])),
    )
    @mock.patch(f"{BASEHOOK_PATCH_PATH}.get_connection")
    def test_test_connection_no_topics(self, mock_get_connection, admin_client, hook):
        config = {"bootstrap.servers": MagicMock()}
        mock_get_connection.return_value.extra_dejson = config
        connection = hook.test_connection()
        admin_client.assert_called_once_with(config)
        mock_admin_instance = admin_client.return_value
        mock_admin_instance.list_topics.assert_called_once_with(timeout=TIMEOUT)
        assert connection == (False, "Failed to establish connection.")

    @mock.patch("airflow.providers.apache.kafka.hooks.base.AdminClient")
    @mock.patch(f"{BASEHOOK_PATCH_PATH}.get_connection")
    def test_test_connection_with_config_dict(self, mock_get_connection, admin_client):
        mock_get_connection.return_value.extra_dejson = {"bootstrap.servers": "conn:9092"}
        hook = SomeKafkaHook(config_dict={"bootstrap.servers": "override:9092"})
        connection = hook.test_connection()
        admin_client.assert_called_once_with({"bootstrap.servers": "override:9092"})
        assert connection == (True, "Connection successful.")

    @mock.patch("airflow.providers.apache.kafka.hooks.base.AdminClient")
    @mock.patch(f"{BASEHOOK_PATCH_PATH}.get_connection")
    def test_test_connection_exception(self, mock_get_connection, admin_client, hook):
        config = {"bootstrap.servers": MagicMock()}
        mock_get_connection.return_value.extra_dejson = config
        admin_client.return_value.list_topics.side_effect = [ValueError("some error")]
        connection = hook.test_connection()
        assert connection == (False, "some error")
