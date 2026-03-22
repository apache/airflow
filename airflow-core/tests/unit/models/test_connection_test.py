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

from airflow.models.connection import Connection
from airflow.models.connection_test import (
    ConnectionTestRequest,
    ConnectionTestState,
    run_connection_test,
)

from tests_common.test_utils.db import clear_db_connection_tests, clear_db_connections

pytestmark = pytest.mark.db_test


class TestConnectionTestRequestModel:
    def test_token_is_generated(self):
        ct = ConnectionTestRequest(connection_id="test_conn", conn_type="postgres")
        assert ct.token is not None
        assert len(ct.token) > 0

    def test_initial_state_is_pending(self):
        ct = ConnectionTestRequest(connection_id="test_conn", conn_type="postgres")
        assert ct.state == ConnectionTestState.PENDING

    def test_tokens_are_unique(self):
        ct1 = ConnectionTestRequest(connection_id="test_conn", conn_type="postgres")
        ct2 = ConnectionTestRequest(connection_id="test_conn", conn_type="postgres")
        assert ct1.token != ct2.token

    def test_repr(self):
        ct = ConnectionTestRequest(connection_id="test_conn", conn_type="postgres")
        r = repr(ct)
        assert "test_conn" in r
        assert "pending" in r

    def test_executor_parameter(self):
        ct = ConnectionTestRequest(connection_id="test_conn", conn_type="postgres", executor="my_executor")
        assert ct.executor == "my_executor"

    def test_executor_defaults_to_none(self):
        ct = ConnectionTestRequest(connection_id="test_conn", conn_type="postgres")
        assert ct.executor is None

    def test_queue_parameter(self):
        ct = ConnectionTestRequest(connection_id="test_conn", conn_type="postgres", queue="my_queue")
        assert ct.queue == "my_queue"

    def test_queue_defaults_to_none(self):
        ct = ConnectionTestRequest(connection_id="test_conn", conn_type="postgres")
        assert ct.queue is None

    def test_connection_fields_stored(self):
        ct = ConnectionTestRequest(
            connection_id="test_conn",
            conn_type="postgres",
            host="db.example.com",
            login="user",
            password="secret",
            schema="mydb",
            port=5432,
            extra='{"key": "value"}',
        )
        assert ct.conn_type == "postgres"
        assert ct.host == "db.example.com"
        assert ct.login == "user"
        assert ct.password == "secret"
        assert ct.schema == "mydb"
        assert ct.port == 5432
        assert ct.extra == '{"key": "value"}'

    def test_password_is_encrypted(self):
        ct = ConnectionTestRequest(connection_id="test_conn", conn_type="postgres", password="secret")
        assert ct._password is not None
        assert ct._password != "secret"
        assert ct.password == "secret"

    def test_extra_is_encrypted(self):
        ct = ConnectionTestRequest(connection_id="test_conn", conn_type="postgres", extra='{"key": "val"}')
        assert ct._extra is not None
        assert ct._extra != '{"key": "val"}'
        assert ct.extra == '{"key": "val"}'

    def test_null_password_and_extra(self):
        ct = ConnectionTestRequest(connection_id="test_conn", conn_type="http")
        assert ct._password is None
        assert ct._extra is None

    def test_commit_on_success_default(self):
        ct = ConnectionTestRequest(connection_id="test_conn", conn_type="postgres")
        assert ct.commit_on_success is False

    def test_commit_on_success_true(self):
        ct = ConnectionTestRequest(connection_id="test_conn", conn_type="postgres", commit_on_success=True)
        assert ct.commit_on_success is True


class TestToConnection:
    def test_to_connection_returns_transient_connection(self):
        ct = ConnectionTestRequest(
            connection_id="test_conn",
            conn_type="postgres",
            host="db.example.com",
            login="user",
            password="secret",
            schema="mydb",
            port=5432,
            extra='{"key": "value"}',
        )
        conn = ct.to_connection()
        assert isinstance(conn, Connection)
        assert conn.conn_id == "test_conn"
        assert conn.conn_type == "postgres"
        assert conn.host == "db.example.com"
        assert conn.login == "user"
        assert conn.password == "secret"
        assert conn.schema == "mydb"
        assert conn.port == 5432
        assert conn.extra == '{"key": "value"}'


class TestCommitToConnectionTable:
    @pytest.fixture(autouse=True)
    def setup_teardown(self):
        clear_db_connections(add_default_connections_back=False)
        clear_db_connection_tests()
        yield
        clear_db_connections(add_default_connections_back=False)
        clear_db_connection_tests()

    def test_creates_new_connection(self, session):
        ct = ConnectionTestRequest(
            connection_id="new_conn",
            conn_type="postgres",
            host="db.example.com",
            login="user",
            password="secret",
            schema="mydb",
            port=5432,
        )
        session.add(ct)
        session.flush()

        ct.commit_to_connection_table(session=session)
        session.flush()

        from sqlalchemy import select

        conn = session.scalar(select(Connection).filter_by(conn_id="new_conn"))
        assert conn is not None
        assert conn.conn_type == "postgres"
        assert conn.host == "db.example.com"
        assert conn.password == "secret"

    def test_updates_existing_connection(self, session):
        conn = Connection(conn_id="existing_conn", conn_type="http", host="old-host.example.com")
        session.add(conn)
        session.flush()

        ct = ConnectionTestRequest(
            connection_id="existing_conn",
            conn_type="postgres",
            host="new-host.example.com",
            login="new_user",
            password="new_secret",
        )
        session.add(ct)
        session.flush()

        ct.commit_to_connection_table(session=session)
        session.flush()
        session.refresh(conn)

        assert conn.conn_type == "postgres"
        assert conn.host == "new-host.example.com"
        assert conn.login == "new_user"
        assert conn.password == "new_secret"


class TestRunConnectionTest:
    def test_successful_connection_test(self):
        conn = mock.MagicMock(spec=Connection)
        conn.conn_id = "test_conn"
        conn.test_connection.return_value = (True, "Connection OK")

        success, message = run_connection_test(conn=conn)

        assert success is True
        assert message == "Connection OK"

    def test_failed_connection_test(self):
        conn = mock.MagicMock(spec=Connection)
        conn.conn_id = "test_conn"
        conn.test_connection.return_value = (False, "Connection failed")

        success, message = run_connection_test(conn=conn)

        assert success is False
        assert message == "Connection failed"

    def test_exception_during_connection_test(self):
        conn = mock.MagicMock(spec=Connection)
        conn.conn_id = "test_conn"
        conn.test_connection.side_effect = Exception("Could not resolve host: db.example.com")

        success, message = run_connection_test(conn=conn)

        assert success is False
        assert "Could not resolve host" in message
