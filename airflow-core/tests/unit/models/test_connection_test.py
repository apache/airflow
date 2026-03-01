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
    ConnectionTest,
    ConnectionTestState,
    attempt_revert,
    run_connection_test,
    snapshot_connection,
)

from tests_common.test_utils.db import clear_db_connection_tests, clear_db_connections

pytestmark = pytest.mark.db_test


class TestConnectionTestModel:
    def test_token_is_generated(self):
        ct = ConnectionTest(connection_id="test_conn")
        assert ct.token is not None
        assert len(ct.token) > 0

    def test_initial_state_is_pending(self):
        ct = ConnectionTest(connection_id="test_conn")
        assert ct.state == ConnectionTestState.PENDING

    def test_tokens_are_unique(self):
        ct1 = ConnectionTest(connection_id="test_conn")
        ct2 = ConnectionTest(connection_id="test_conn")
        assert ct1.token != ct2.token

    def test_repr(self):
        ct = ConnectionTest(connection_id="test_conn")
        r = repr(ct)
        assert "test_conn" in r
        assert "pending" in r

    def test_queue_parameter(self):
        ct = ConnectionTest(connection_id="test_conn", queue="my_queue")
        assert ct.queue == "my_queue"

    def test_queue_defaults_to_none(self):
        ct = ConnectionTest(connection_id="test_conn")
        assert ct.queue is None


class TestRunConnectionTest:
    def test_successful_connection_test(self):
        """Pure function returns (True, message) on successful test."""
        with mock.patch.object(
            Connection, "get_connection_from_secrets", return_value=mock.MagicMock(spec=Connection)
        ) as mock_get_conn:
            mock_get_conn.return_value.test_connection.return_value = (True, "Connection OK")
            success, message = run_connection_test(connection_id="test_conn")

        assert success is True
        assert message == "Connection OK"

    def test_failed_connection_test(self):
        """Pure function returns (False, message) when test_connection returns False."""
        with mock.patch.object(
            Connection, "get_connection_from_secrets", return_value=mock.MagicMock(spec=Connection)
        ) as mock_get_conn:
            mock_get_conn.return_value.test_connection.return_value = (False, "Connection failed")
            success, message = run_connection_test(connection_id="test_conn")

        assert success is False
        assert message == "Connection failed"

    def test_exception_during_connection_test(self):
        """Pure function returns (False, error_str) on exception."""
        with mock.patch.object(
            Connection,
            "get_connection_from_secrets",
            side_effect=Exception("Could not resolve host: db.example.com"),
        ):
            success, message = run_connection_test(connection_id="test_conn")

        assert success is False
        assert "Could not resolve host" in message


class TestSnapshotConnection:
    def test_snapshot_captures_all_fields(self):
        """snapshot_connection captures all expected fields including encrypted ones."""
        conn = Connection(
            conn_id="snap_test",
            conn_type="postgres",
            host="db.example.com",
            login="user",
            password="secret",
            schema="mydb",
            port=5432,
            extra='{"key": "value"}',
        )
        snap = snapshot_connection(conn)
        assert snap["conn_type"] == "postgres"
        assert snap["host"] == "db.example.com"
        assert snap["login"] == "user"
        assert snap["schema"] == "mydb"
        assert snap["port"] == 5432
        assert snap["_password"] is not None
        assert snap["_password"] != "secret"  # Should be encrypted
        assert snap["_extra"] is not None
        assert snap["is_encrypted"] is True
        assert snap["is_extra_encrypted"] is True

    def test_snapshot_with_null_password_and_extra(self):
        """snapshot_connection handles None password and extra."""
        conn = Connection(conn_id="snap_test", conn_type="http")
        snap = snapshot_connection(conn)
        assert snap["_password"] is None
        assert snap["_extra"] is None
        assert not snap["is_encrypted"]
        assert not snap["is_extra_encrypted"]


class TestAttemptRevert:
    @pytest.fixture(autouse=True)
    def setup_teardown(self):
        clear_db_connections(add_default_connections_back=False)
        clear_db_connection_tests()
        yield
        clear_db_connections(add_default_connections_back=False)
        clear_db_connection_tests()

    def test_attempt_revert_success(self, session):
        """attempt_revert restores all connection fields and sets reverted=True."""
        conn = Connection(
            conn_id="revert_conn",
            conn_type="postgres",
            host="old-host.example.com",
            login="old_user",
            password="old_secret",
            schema="mydb",
            port=5432,
            extra='{"key": "old_value"}',
        )
        session.add(conn)
        session.flush()

        pre_snap = snapshot_connection(conn)

        conn.host = "new-host.example.com"
        conn.login = "new_user"
        conn.password = "new_secret"
        conn.port = 9999
        conn.extra = '{"key": "new_value"}'

        post_snap = snapshot_connection(conn)

        ct = ConnectionTest(connection_id="revert_conn")
        ct.state = ConnectionTestState.FAILED
        ct.result_message = "Connection refused"
        ct.connection_snapshot = {"pre": pre_snap, "post": post_snap}
        session.add(ct)
        session.flush()

        attempt_revert(ct, session=session)
        session.flush()

        assert ct.reverted is True
        session.refresh(conn)
        assert conn.host == "old-host.example.com"
        assert conn.login == "old_user"
        assert conn.password == "old_secret"
        assert conn.schema == "mydb"
        assert conn.port == 5432
        assert conn.extra == '{"key": "old_value"}'

    def test_attempt_revert_skipped_concurrent_edit(self, session):
        """attempt_revert skips revert when connection was modified by another user."""
        conn = Connection(
            conn_id="concurrent_conn",
            conn_type="postgres",
            host="old-host.example.com",
        )
        session.add(conn)
        session.flush()

        pre_snap = snapshot_connection(conn)
        conn.host = "new-host.example.com"
        post_snap = snapshot_connection(conn)

        conn.host = "third-party-host.example.com"
        session.flush()

        ct = ConnectionTest(connection_id="concurrent_conn")
        ct.state = ConnectionTestState.FAILED
        ct.result_message = "Connection refused"
        ct.connection_snapshot = {"pre": pre_snap, "post": post_snap}
        session.add(ct)
        session.flush()

        attempt_revert(ct, session=session)

        assert ct.reverted is False
        assert "modified by another user" in ct.result_message
        assert conn.host == "third-party-host.example.com"

    def test_attempt_revert_skipped_concurrent_password_edit(self, session):
        """attempt_revert skips revert when password was changed concurrently."""
        conn = Connection(
            conn_id="pw_conn",
            conn_type="postgres",
            host="host.example.com",
            password="original_secret",
        )
        session.add(conn)
        session.flush()

        pre_snap = snapshot_connection(conn)
        conn.password = "new_secret"
        post_snap = snapshot_connection(conn)

        conn.password = "third_party_secret"
        session.flush()

        ct = ConnectionTest(connection_id="pw_conn")
        ct.state = ConnectionTestState.FAILED
        ct.result_message = "Connection refused"
        ct.connection_snapshot = {"pre": pre_snap, "post": post_snap}
        session.add(ct)
        session.flush()

        attempt_revert(ct, session=session)

        assert ct.reverted is False
        assert "modified by another user" in ct.result_message
        session.refresh(conn)
        assert conn.password == "third_party_secret"

    def test_attempt_revert_skipped_connection_deleted(self, session):
        """attempt_revert skips revert when connection no longer exists."""
        conn = Connection(
            conn_id="deleted_conn",
            conn_type="postgres",
            host="old-host.example.com",
        )
        session.add(conn)
        session.flush()

        pre_snap = snapshot_connection(conn)
        conn.host = "new-host.example.com"
        post_snap = snapshot_connection(conn)

        ct = ConnectionTest(connection_id="deleted_conn")
        ct.state = ConnectionTestState.FAILED
        ct.result_message = "Connection refused"
        ct.connection_snapshot = {"pre": pre_snap, "post": post_snap}
        session.add(ct)

        session.delete(conn)
        session.flush()

        attempt_revert(ct, session=session)

        assert ct.reverted is False
        assert "no longer exists" in ct.result_message
