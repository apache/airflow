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
from airflow.providers.neo4j.hooks.neo4j import Neo4jHook


class TestNeo4jHookConn:
    @pytest.mark.parametrize(
        "conn_extra, expected_uri",
        [
            ({}, "bolt://host:7687"),
            ({"neo4j_scheme": False}, "bolt://host:7687"),
            ({"certs_self_signed": True, "neo4j_scheme": False}, "bolt+ssc://host:7687"),
            ({"certs_trusted_ca": True, "neo4j_scheme": False}, "bolt+s://host:7687"),
            ({"certs_self_signed": True, "neo4j_scheme": True}, "neo4j+ssc://host:7687"),
            ({"certs_trusted_ca": True, "neo4j_scheme": True}, "neo4j+s://host:7687"),
        ],
    )
    def test_get_uri_neo4j_scheme(self, conn_extra, expected_uri):
        connection = Connection(
            conn_type="neo4j",
            login="login",
            password="password",
            host="host",
            schema="schema",
            extra=conn_extra,
        )

        # Use the environment variable mocking to test saving the configuration as a URI and
        # to avoid mocking Airflow models class
        with mock.patch.dict("os.environ", AIRFLOW_CONN_NEO4J_DEFAULT=connection.get_uri()):
            neo4j_hook = Neo4jHook()
            uri = neo4j_hook.get_uri(connection)

            assert uri == expected_uri

    @mock.patch("airflow.providers.neo4j.hooks.neo4j.GraphDatabase")
    def test_run_with_schema(self, mock_graph_database):
        connection = Connection(
            conn_type="neo4j", login="login", password="password", host="host", schema="schema"
        )
        mock_sql = mock.MagicMock(name="sql")

        # Use the environment variable mocking to test saving the configuration as a URI and
        # to avoid mocking Airflow models class
        with mock.patch.dict("os.environ", AIRFLOW_CONN_NEO4J_DEFAULT=connection.get_uri()):
            neo4j_hook = Neo4jHook()
            op_result = neo4j_hook.run(mock_sql)
            mock_graph_database.assert_has_calls(
                [
                    mock.call.driver("bolt://host:7687", auth=("login", "password"), encrypted=False),
                    mock.call.driver().session(database="schema"),
                    mock.call.driver().session().__enter__(),
                    mock.call.driver().session().__enter__().run(mock_sql),
                    mock.call.driver().session().__enter__().run().data(),
                    mock.call.driver().session().__exit__(None, None, None),
                ]
            )
            session = mock_graph_database.driver.return_value.session.return_value.__enter__.return_value
            assert op_result == session.run.return_value.data.return_value

    @mock.patch("airflow.providers.neo4j.hooks.neo4j.GraphDatabase")
    def test_run_without_schema(self, mock_graph_database):
        connection = Connection(
            conn_type="neo4j", login="login", password="password", host="host", schema=None
        )
        mock_sql = mock.MagicMock(name="sql")

        # Use the environment variable mocking to test saving the configuration as a URI and
        # to avoid mocking Airflow models class
        with mock.patch.dict("os.environ", AIRFLOW_CONN_NEO4J_DEFAULT=connection.get_uri()):
            neo4j_hook = Neo4jHook()
            op_result = neo4j_hook.run(mock_sql)
            mock_graph_database.assert_has_calls(
                [
                    mock.call.driver("bolt://host:7687", auth=("login", "password"), encrypted=False),
                    mock.call.driver().session(),
                    mock.call.driver().session().__enter__(),
                    mock.call.driver().session().__enter__().run(mock_sql),
                    mock.call.driver().session().__enter__().run().data(),
                    mock.call.driver().session().__exit__(None, None, None),
                ]
            )
            session = mock_graph_database.driver.return_value.session.return_value.__enter__.return_value
            assert op_result == session.run.return_value.data.return_value

    @pytest.mark.parametrize(
        "conn_extra, should_provide_encrypted, expected_encrypted",
        [
            ({}, True, False),
            ({"neo4j_scheme": False, "encrypted": True}, True, True),
            ({"certs_self_signed": False, "neo4j_scheme": False, "encrypted": False}, True, False),
            ({"certs_trusted_ca": False, "neo4j_scheme": False, "encrypted": False}, True, False),
            ({"certs_self_signed": False, "neo4j_scheme": True, "encrypted": False}, True, False),
            ({"certs_trusted_ca": False, "neo4j_scheme": True, "encrypted": False}, True, False),
            ({"certs_self_signed": True, "neo4j_scheme": False, "encrypted": False}, False, None),
            ({"certs_trusted_ca": True, "neo4j_scheme": False, "encrypted": False}, False, None),
            ({"certs_self_signed": True, "neo4j_scheme": True, "encrypted": False}, False, None),
            ({"certs_trusted_ca": True, "neo4j_scheme": True, "encrypted": False}, False, None),
        ],
    )
    @mock.patch("airflow.providers.neo4j.hooks.neo4j.GraphDatabase.driver")
    def test_encrypted_provided(
        self, mock_graph_database, conn_extra, should_provide_encrypted, expected_encrypted
    ):
        connection = Connection(
            conn_type="neo4j",
            login="login",
            password="password",
            host="host",
            schema="schema",
            extra=conn_extra,
        )
        with mock.patch.dict("os.environ", AIRFLOW_CONN_NEO4J_DEFAULT=connection.get_uri()):
            neo4j_hook = Neo4jHook()
            with neo4j_hook.get_conn():
                if should_provide_encrypted:
                    assert "encrypted" in mock_graph_database.call_args.kwargs
                    assert mock_graph_database.call_args.kwargs["encrypted"] == expected_encrypted
                else:
                    assert "encrypted" not in mock_graph_database.call_args.kwargs
