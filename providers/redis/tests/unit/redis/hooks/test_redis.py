#
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

import pytest

from airflow.models import Connection
from airflow.providers.redis.hooks.redis import RedisHook


class TestRedisHook:
    @pytest.mark.db_test
    def test_get_conn(self):
        hook = RedisHook(redis_conn_id="redis_default")
        assert hook.redis is None

        assert hook.host is None, "host initialised as None."
        assert hook.port is None, "port initialised as None."
        assert hook.password is None, "password initialised as None."
        assert hook.db is None, "db initialised as None."
        assert hook.get_conn() is hook.get_conn(), "Connection initialized only if None."

    @mock.patch("airflow.providers.redis.hooks.redis.Redis")
    @mock.patch(
        "airflow.providers.redis.hooks.redis.RedisHook.get_connection",
    )
    def test_get_conn_with_extra_config(self, mock_get_connection, mock_redis):
        connection = Connection(
            login="user",
            password="password",
            host="remote_host",
            port=1234,
        )
        connection.set_extra(
            """{
                "db": 2,
                "ssl": true,
                "ssl_cert_reqs": "required",
                "ssl_ca_certs": "/path/to/custom/ca-cert",
                "ssl_keyfile": "/path/to/key-file",
                "ssl_certfile": "/path/to/cert-file",
                "ssl_check_hostname": true
            }"""
        )
        mock_get_connection.return_value = connection
        hook = RedisHook()

        hook.get_conn()

        mock_redis.assert_called_once()
        call_kwargs = mock_redis.call_args[1]
        assert call_kwargs["host"] == connection.host
        assert call_kwargs["username"] == connection.login
        assert call_kwargs["password"] == connection.password
        assert call_kwargs["port"] == connection.port
        assert call_kwargs["db"] == connection.extra_dejson["db"]
        assert call_kwargs["ssl"] == connection.extra_dejson["ssl"]
        assert call_kwargs["ssl_cert_reqs"] == connection.extra_dejson["ssl_cert_reqs"]
        assert call_kwargs["ssl_ca_certs"] == connection.extra_dejson["ssl_ca_certs"]
        assert call_kwargs["ssl_keyfile"] == connection.extra_dejson["ssl_keyfile"]
        assert call_kwargs["ssl_certfile"] == connection.extra_dejson["ssl_certfile"]
        assert call_kwargs["ssl_check_hostname"] == connection.extra_dejson["ssl_check_hostname"]

    @mock.patch("airflow.providers.redis.hooks.redis.Redis")
    @mock.patch("airflow.providers.redis.hooks.redis.RedisHook.get_connection")
    def test_client_identification_with_driver_info(self, mock_get_connection, mock_redis):
        """When DriverInfo is available, the Redis client is created with a driver_info kwarg."""
        mock_get_connection.return_value = Connection(host="h", port=1, login="u", password="p")
        fake_driver_info = mock.MagicMock()
        fake_driver_info.add_upstream_driver.return_value = fake_driver_info
        with mock.patch(
            "airflow.providers.redis.hooks.redis.DriverInfo", return_value=fake_driver_info
        ) as mock_driver_info_cls:
            RedisHook().get_conn()

        mock_driver_info_cls.assert_called_once_with()
        fake_driver_info.add_upstream_driver.assert_called_once()
        args, _ = fake_driver_info.add_upstream_driver.call_args
        assert args[0] == "apache-airflow-providers-redis"
        call_kwargs = mock_redis.call_args[1]
        assert call_kwargs["driver_info"] is fake_driver_info
        assert "lib_name" not in call_kwargs

    @mock.patch("airflow.providers.redis.hooks.redis.Redis")
    @mock.patch("airflow.providers.redis.hooks.redis.RedisHook.get_connection")
    @mock.patch("airflow.providers.redis.hooks.redis.DriverInfo", None)
    @mock.patch("airflow.providers.redis.hooks.redis._SUPPORTS_LIB_NAME", True)
    def test_client_identification_with_lib_name(self, mock_get_connection, mock_redis):
        """When DriverInfo is unavailable but lib_name is supported, lib_name kwarg is passed."""
        mock_get_connection.return_value = Connection(host="h", port=1, login="u", password="p")
        RedisHook().get_conn()

        call_kwargs = mock_redis.call_args[1]
        assert "driver_info" not in call_kwargs
        assert "apache-airflow-providers-redis" in call_kwargs["lib_name"]

    @mock.patch("airflow.providers.redis.hooks.redis.Redis")
    @mock.patch("airflow.providers.redis.hooks.redis.RedisHook.get_connection")
    @mock.patch("airflow.providers.redis.hooks.redis.DriverInfo", None)
    @mock.patch("airflow.providers.redis.hooks.redis._SUPPORTS_LIB_NAME", False)
    def test_client_identification_unsupported(self, mock_get_connection, mock_redis):
        """When neither DriverInfo nor lib_name is supported, no identification kwarg is passed."""
        mock_get_connection.return_value = Connection(host="h", port=1, login="u", password="p")
        RedisHook().get_conn()

        call_kwargs = mock_redis.call_args[1]
        assert "driver_info" not in call_kwargs
        assert "lib_name" not in call_kwargs

    @pytest.mark.db_test
    def test_get_conn_password_stays_none(self):
        hook = RedisHook(redis_conn_id="redis_default")
        hook.get_conn()
        assert hook.password is None

    @mock.patch("airflow.providers.redis.hooks.redis.RedisCluster")
    @mock.patch("airflow.providers.redis.hooks.redis.Redis")
    @mock.patch("airflow.providers.redis.hooks.redis.RedisHook.get_connection")
    def test_get_conn_defaults_to_single_node_client(
        self, mock_get_connection, mock_redis, mock_redis_cluster
    ):
        mock_get_connection.return_value = Connection(host="remote_host", port=1234)

        RedisHook().get_conn()

        mock_redis.assert_called_once()
        mock_redis_cluster.assert_not_called()

    @mock.patch("airflow.providers.redis.hooks.redis.Redis")
    @mock.patch("airflow.providers.redis.hooks.redis.RedisCluster")
    @mock.patch("airflow.providers.redis.hooks.redis.RedisHook.get_connection")
    def test_get_conn_cluster_mode(self, mock_get_connection, mock_redis_cluster, mock_redis):
        connection = Connection(login="user", password="password", host="node-1", port=6379)
        connection.set_extra('{"cluster": true, "ssl": true, "ssl_cert_reqs": "required"}')
        mock_get_connection.return_value = connection

        RedisHook().get_conn()

        mock_redis.assert_not_called()
        mock_redis_cluster.assert_called_once()
        call_kwargs = mock_redis_cluster.call_args[1]
        assert call_kwargs["host"] == connection.host
        assert call_kwargs["port"] == connection.port
        assert call_kwargs["username"] == connection.login
        assert call_kwargs["password"] == connection.password
        assert call_kwargs["ssl"] is True
        assert call_kwargs["ssl_cert_reqs"] == "required"
        # RedisCluster raises RedisClusterException when handed a `db` kwarg at all, even `db=0`.
        assert "db" not in call_kwargs

    @pytest.mark.parametrize("db", [1, 2])
    @mock.patch("airflow.providers.redis.hooks.redis.RedisCluster")
    @mock.patch("airflow.providers.redis.hooks.redis.RedisHook.get_connection")
    def test_get_conn_cluster_mode_rejects_non_zero_db(self, mock_get_connection, mock_redis_cluster, db):
        connection = Connection(host="node-1", port=6379)
        connection.set_extra(f'{{"cluster": true, "db": {db}}}')
        mock_get_connection.return_value = connection

        with pytest.raises(ValueError, match="only supports database 0"):
            RedisHook().get_conn()

        mock_redis_cluster.assert_not_called()

    @mock.patch("airflow.providers.redis.hooks.redis.RedisCluster")
    @mock.patch("airflow.providers.redis.hooks.redis.RedisHook.get_connection")
    def test_get_conn_cluster_mode_accepts_db_zero(self, mock_get_connection, mock_redis_cluster):
        """`db` defaults to 0 in the connection form, so that value must not be treated as a conflict."""
        connection = Connection(host="node-1", port=6379)
        connection.set_extra('{"cluster": true, "db": 0}')
        mock_get_connection.return_value = connection

        RedisHook().get_conn()

        mock_redis_cluster.assert_called_once()
        assert "db" not in mock_redis_cluster.call_args[1]

    @mock.patch("airflow.providers.redis.hooks.redis.RedisCluster")
    @mock.patch("airflow.providers.redis.hooks.redis.RedisHook.get_connection")
    def test_get_conn_cluster_mode_passes_startup_nodes(self, mock_get_connection, mock_redis_cluster):
        connection = Connection(host="node-1", port=6379)
        connection.set_extra('{"cluster": true, "startup_nodes": "node-2:6379,node-3:6380"}')
        mock_get_connection.return_value = connection

        RedisHook().get_conn()

        startup_nodes = mock_redis_cluster.call_args[1]["startup_nodes"]
        assert [(node.host, node.port) for node in startup_nodes] == [("node-2", 6379), ("node-3", 6380)]

    @pytest.mark.parametrize(
        ("raw_nodes", "expected"),
        [
            pytest.param("node-2:6379,node-3:6380", [("node-2", 6379), ("node-3", 6380)], id="csv-string"),
            pytest.param("node-2 , node-3:6380", [("node-2", 6379), ("node-3", 6380)], id="default-port"),
            pytest.param(None, [], id="unset"),
            pytest.param("", [], id="empty"),
        ],
    )
    def test_build_startup_nodes(self, raw_nodes, expected):
        nodes = RedisHook(startup_nodes=raw_nodes)._build_startup_nodes()

        assert [(node.host, node.port) for node in nodes] == expected

    @pytest.mark.parametrize(
        ("raw_nodes", "expected_error"),
        [
            pytest.param("node-2:not-a-port", "Invalid port", id="non-numeric-port"),
            pytest.param(":6379", "Missing host", id="missing-host"),
            pytest.param(["node-2:6379"], "must be a comma-separated string", id="list"),
            pytest.param({"host": "node-2"}, "must be a comma-separated string", id="mapping"),
            pytest.param(6379, "must be a comma-separated string", id="not-a-string"),
        ],
    )
    def test_build_startup_nodes_rejects_invalid_entries(self, raw_nodes, expected_error):
        with pytest.raises(ValueError, match=expected_error):
            RedisHook(startup_nodes=raw_nodes)._build_startup_nodes()

    @mock.patch("airflow.providers.redis.hooks.redis.RedisCluster")
    @mock.patch("airflow.providers.redis.hooks.redis.RedisHook.get_connection")
    @mock.patch("airflow.providers.redis.hooks.redis.DriverInfo", None)
    @mock.patch("airflow.providers.redis.hooks.redis._SUPPORTS_LIB_NAME", True)
    def test_cluster_mode_passes_client_identification(self, mock_get_connection, mock_redis_cluster):
        connection = Connection(host="node-1", port=6379)
        connection.set_extra('{"cluster": true}')
        mock_get_connection.return_value = connection

        RedisHook().get_conn()

        assert "apache-airflow-providers-redis" in mock_redis_cluster.call_args[1]["lib_name"]
