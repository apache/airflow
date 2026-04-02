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
from airflow.providers.apache.tinkerpop.hooks.gremlin import GremlinHook


@pytest.fixture
def gremlin_hook():
    """Fixture to provide a GremlinHook instance with proper teardown."""
    hook = GremlinHook()
    yield hook
    # Teardown: Ensure the client is closed if it exists
    if hook.client:
        hook.client.close()


class TestGremlinHook:
    @pytest.mark.parametrize(
        ("host", "port", "expected_uri"),
        [
            ("host", None, "ws://host:443/gremlin"),
            ("myhost", 1234, "ws://myhost:1234/gremlin"),
            ("localhost", 8888, "ws://localhost:8888/gremlin"),
        ],
    )
    def test_get_uri(self, host, port, expected_uri, gremlin_hook):
        """
        Test that get_uri builds the expected URI from the connection.
        """
        conn = Connection(conn_id="gremlin_default", host=host, port=port)
        with mock.patch.dict("os.environ", AIRFLOW_CONN_GREMLIN_DEFAULT=conn.get_uri()):
            uri = gremlin_hook.get_uri(conn)

            assert uri == expected_uri

    def test_get_conn(self, gremlin_hook):
        """
        Test that get_conn() retrieves the connection and creates a client correctly.
        """
        conn = Connection(
            conn_type="gremlin",
            conn_id="gremlin_default",
            host="host",
            port=1234,
            schema="mydb",
            login="login",
            password="mypassword",
        )
        gremlin_hook.get_connection = lambda conn_id: conn

        with mock.patch("airflow.providers.apache.tinkerpop.hooks.gremlin.Client") as mock_client:
            gremlin_hook.get_conn()
            expected_uri = "wss://host:1234/"
            expected_username = "/dbs/login/colls/mydb"

            mock_client.assert_called_once_with(
                url=expected_uri,
                traversal_source=gremlin_hook.traversal_source,
                username=expected_username,
                password="mypassword",
            )

    @pytest.mark.parametrize(
        ("serializer", "should_include"),
        [
            (None, False),
            ("dummy_serializer", True),
        ],
    )
    def test_get_client_message_serializer(self, serializer, should_include, gremlin_hook):
        """
        Test that get_client() includes message_serializer only when provided.
        """
        conn = Connection(
            conn_id="gremlin_default",
            host="host",
            port=1234,
            schema="mydb",
            login="login",
            password="mypassword",
        )
        uri = "wss://test.uri"
        traversal_source = "g"

        with mock.patch("airflow.providers.apache.tinkerpop.hooks.gremlin.Client") as mock_client:
            gremlin_hook.get_client(conn, traversal_source, uri, message_serializer=serializer)
            call_args = mock_client.call_args.kwargs
            if should_include:
                assert "message_serializer" in call_args
                assert call_args["message_serializer"] == serializer
            else:
                assert "message_serializer" not in call_args

    @pytest.mark.parametrize(
        ("side_effect", "expected_exception", "expected_result"),
        [
            (None, None, ["dummy_result"]),
            (Exception("Test error"), Exception, None),
        ],
    )
    def test_run(self, side_effect, expected_exception, expected_result, gremlin_hook):
        """
        Test that run() returns the expected result or propagates an exception, with proper cleanup.
        """
        query = "g.V().limit(1)"

        # Mock the client instance
        with mock.patch("airflow.providers.apache.tinkerpop.hooks.gremlin.Client") as mock_client:
            instance = mock_client.return_value
            if side_effect is None:
                instance.submit.return_value.all.return_value.result.return_value = expected_result
            else:
                instance.submit.return_value.all.return_value.result.side_effect = side_effect

            # Mock get_connection to simplify setup
            conn = Connection(
                conn_id="gremlin_default",
                host="host",
                port=1234,
                schema="mydb",
                login="login",
                password="mypassword",
            )
            gremlin_hook.get_connection = lambda conn_id: conn

            if expected_exception:
                with pytest.raises(expected_exception, match="Test error"):
                    gremlin_hook.run(query)
            else:
                result = gremlin_hook.run(query)
                assert result == expected_result
