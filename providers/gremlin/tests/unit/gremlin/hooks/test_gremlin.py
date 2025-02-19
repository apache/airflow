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

import pytest

from airflow.models import Connection
from airflow.providers.gremlin.hooks.gremlin import GremlinHook


class TestGremlinHook:
    @pytest.mark.parametrize(
        "conn_extra, host, port, expected_uri",
        [
            ({}, "host", None, "wss://host:443"),
            ({"development-graphdb": True}, "myhost", 1234, "wss://myhost:1234"),
            ({"development-graphdb": False}, "localhost", 8888, "wss://localhost:8888"),
        ],
    )
    def test_get_uri(self, conn_extra, host, port, expected_uri):
        """
        Test that get_uri builds the expected URI from the connection.
        """
        conn = Connection(conn_id="gremlin_default", host=host, port=port)
        # Instead of set_extra_json, assign extra directly.
        conn.extra = json.dumps(conn_extra)
        hook = GremlinHook()
        uri = hook.get_uri(conn)
        assert uri == expected_uri

    @mock.patch("airflow.providers.gremlin.hooks.gremlin.DriverRemoteConnection")
    def test_get_conn(self, mock_driver):
        """
        Test that get_conn() retrieves the connection and creates a driver correctly.
        """
        hook = GremlinHook()
        conn = Connection(
            conn_id="gremlin_default",
            host="host",
            port=1234,
            schema="mydb",
            login="mylogin",
            password="mypassword",
        )
        # Set the extra attribute using json.dumps.
        conn.extra = json.dumps({"development-graphdb": True})
        # Override get_connection so that it returns our dummy connection.
        hook.get_connection = lambda conn_id: conn

        hook.get_conn()
        expected_uri = "wss://host:1234"
        # Expected username is built from login and schema in that order.
        expected_username = "/dbs/mylogin/colls/mydb"

        mock_driver.assert_called_once()
        call_args = mock_driver.call_args.kwargs
        assert call_args["url"] == expected_uri
        assert call_args["traversal_source"] == hook.traversal_source
        assert call_args["username"] == expected_username
        assert call_args["password"] == "mypassword"

    @pytest.mark.parametrize(
        "side_effect, expected_exception, expected_result",
        [
            (None, None, ["dummy_result"]),
            (Exception("Test error"), Exception, None),
        ],
    )
    @mock.patch("airflow.providers.gremlin.hooks.gremlin.traversal")
    @mock.patch("airflow.providers.gremlin.hooks.gremlin.DriverRemoteConnection")
    def test_get_traversal(
        self, mock_driver, mock_traversal, side_effect, expected_exception, expected_result
    ):
        """
        Test that get_traversal() returns the expected result or propagates an exception.
        """
        # Create a dummy driver from the patched DriverRemoteConnection.
        dummy_driver = mock_driver.return_value

        # Configure the traversal chain.
        # When traversal() is called, it returns a mock on which with_remote(driver) is called.
        if side_effect is None:
            mock_traversal.return_value.with_remote.return_value = expected_result
        else:
            mock_traversal.return_value.with_remote.side_effect = side_effect

        hook = GremlinHook()
        # Patch get_conn so that it returns our dummy driver.
        hook.get_conn = lambda: dummy_driver

        if expected_exception:
            with pytest.raises(expected_exception, match="Test error"):
                hook.get_traversal()
        else:
            result = hook.get_traversal()
            assert result == expected_result
