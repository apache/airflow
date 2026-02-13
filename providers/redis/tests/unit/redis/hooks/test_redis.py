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

        # Verify driver info is present with correct value
        # Check for either driver_info or lib_name parameter
        assert "driver_info" in call_kwargs or "lib_name" in call_kwargs, (
            "Expected either 'driver_info' or 'lib_name' in Redis client call"
        )

        if "driver_info" in call_kwargs:
            # Uses DriverInfo class
            driver_info = call_kwargs["driver_info"]
            assert hasattr(driver_info, "formatted_name"), "DriverInfo should have formatted_name attribute"
            assert "apache-airflow" in driver_info.formatted_name
        elif "lib_name" in call_kwargs:
            # Uses lib_name parameter
            assert "apache-airflow" in call_kwargs["lib_name"]

    @pytest.mark.db_test
    def test_get_conn_password_stays_none(self):
        hook = RedisHook(redis_conn_id="redis_default")
        hook.get_conn()
        assert hook.password is None
