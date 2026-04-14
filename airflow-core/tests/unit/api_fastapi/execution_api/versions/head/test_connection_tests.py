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

import pytest
from sqlalchemy import select

from airflow.models.connection import Connection
from airflow.models.connection_test import ConnectionTestRequest, ConnectionTestState

from tests_common.test_utils.db import clear_db_connection_tests, clear_db_connections

pytestmark = pytest.mark.db_test


class TestPatchConnectionTest:
    @pytest.fixture(autouse=True)
    def setup_teardown(self):
        clear_db_connection_tests()
        yield
        clear_db_connection_tests()

    def test_patch_updates_result(self, client, session):
        """PATCH sets the state and result fields."""
        ct = ConnectionTestRequest(connection_id="test_conn", conn_type="postgres")
        ct.state = ConnectionTestState.RUNNING
        session.add(ct)
        session.commit()

        response = client.patch(
            f"/execution/connection-tests/{ct.id}",
            json={
                "state": "success",
                "result_message": "Connection successfully tested",
            },
        )
        assert response.status_code == 204

        session.expire_all()
        ct = session.get(ConnectionTestRequest, ct.id)
        assert ct.state == "success"
        assert ct.result_message == "Connection successfully tested"

    def test_patch_returns_404_for_nonexistent(self, client):
        """PATCH with unknown id returns 404."""
        response = client.patch(
            "/execution/connection-tests/00000000-0000-0000-0000-000000000000",
            json={"state": "success", "result_message": "ok"},
        )
        assert response.status_code == 404

    def test_patch_returns_422_for_invalid_uuid(self, client):
        """PATCH with invalid uuid returns 422."""
        response = client.patch(
            "/execution/connection-tests/not-a-uuid",
            json={"state": "success", "result_message": "ok"},
        )
        assert response.status_code == 422

    def test_patch_returns_409_for_terminal_state(self, client, session):
        """PATCH on a test already in terminal state returns 409."""
        ct = ConnectionTestRequest(connection_id="test_conn", conn_type="postgres")
        ct.state = ConnectionTestState.SUCCESS
        ct.result_message = "Already done"
        session.add(ct)
        session.commit()

        response = client.patch(
            f"/execution/connection-tests/{ct.id}",
            json={"state": "failed", "result_message": "retry"},
        )
        assert response.status_code == 409
        assert "terminal state" in response.json()["detail"]["message"]


class TestPatchConnectionTestCommitOnSuccess:
    """Tests for the commit_on_success behavior in the execution API."""

    @pytest.fixture(autouse=True)
    def setup_teardown(self):
        clear_db_connections(add_default_connections_back=False)
        clear_db_connection_tests()
        yield
        clear_db_connections(add_default_connections_back=False)
        clear_db_connection_tests()

    def test_success_with_commit_creates_connection(self, client, session):
        """PATCH with state=success and commit_on_success creates a new connection."""
        ct = ConnectionTestRequest(
            connection_id="new_conn",
            conn_type="postgres",
            host="db.example.com",
            login="user",
            password="secret",
            commit_on_success=True,
        )
        ct.state = ConnectionTestState.RUNNING
        session.add(ct)
        session.commit()

        response = client.patch(
            f"/execution/connection-tests/{ct.id}",
            json={"state": "success", "result_message": "Connection OK"},
        )
        assert response.status_code == 204

        conn = session.scalar(select(Connection).filter_by(conn_id="new_conn"))
        assert conn is not None
        assert conn.conn_type == "postgres"
        assert conn.host == "db.example.com"

    def test_success_with_commit_updates_existing(self, client, session):
        """PATCH with state=success and commit_on_success updates an existing connection."""
        conn = Connection(conn_id="existing_conn", conn_type="http", host="old-host.example.com")
        session.add(conn)
        session.flush()

        ct = ConnectionTestRequest(
            connection_id="existing_conn",
            conn_type="postgres",
            host="new-host.example.com",
            login="new_user",
            commit_on_success=True,
        )
        ct.state = ConnectionTestState.RUNNING
        session.add(ct)
        session.commit()

        response = client.patch(
            f"/execution/connection-tests/{ct.id}",
            json={"state": "success", "result_message": "Connection OK"},
        )
        assert response.status_code == 204

        session.expire_all()
        conn = session.scalar(select(Connection).filter_by(conn_id="existing_conn"))
        assert conn.conn_type == "postgres"
        assert conn.host == "new-host.example.com"

    def test_success_without_commit_does_not_create(self, client, session):
        """PATCH with state=success but commit_on_success=False does not create a connection."""
        ct = ConnectionTestRequest(
            connection_id="no_commit_conn",
            conn_type="postgres",
            host="db.example.com",
            commit_on_success=False,
        )
        ct.state = ConnectionTestState.RUNNING
        session.add(ct)
        session.commit()

        response = client.patch(
            f"/execution/connection-tests/{ct.id}",
            json={"state": "success", "result_message": "Connection OK"},
        )
        assert response.status_code == 204

        conn = session.scalar(select(Connection).filter_by(conn_id="no_commit_conn"))
        assert conn is None

    def test_failed_with_commit_does_not_create(self, client, session):
        """PATCH with state=failed and commit_on_success=True does NOT create a connection."""
        ct = ConnectionTestRequest(
            connection_id="fail_conn",
            conn_type="postgres",
            host="db.example.com",
            commit_on_success=True,
        )
        ct.state = ConnectionTestState.RUNNING
        session.add(ct)
        session.commit()

        response = client.patch(
            f"/execution/connection-tests/{ct.id}",
            json={"state": "failed", "result_message": "Connection refused"},
        )
        assert response.status_code == 204

        conn = session.scalar(select(Connection).filter_by(conn_id="fail_conn"))
        assert conn is None


class TestGetConnectionTestConnection:
    """Tests for the GET /{connection_test_id}/connection endpoint."""

    @pytest.fixture(autouse=True)
    def setup_teardown(self):
        clear_db_connection_tests()
        yield
        clear_db_connection_tests()

    def test_get_connection_returns_data(self, client, session):
        """GET returns decrypted connection data from the test request."""
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
        session.add(ct)
        session.commit()

        response = client.get(f"/execution/connection-tests/{ct.id}/connection")
        assert response.status_code == 200

        data = response.json()
        assert data["conn_id"] == "test_conn"
        assert data["conn_type"] == "postgres"
        assert data["host"] == "db.example.com"
        assert data["login"] == "user"
        assert data["password"] == "secret"
        assert data["schema"] == "mydb"
        assert data["port"] == 5432
        assert data["extra"] == '{"key": "value"}'

    def test_get_connection_returns_404_for_nonexistent(self, client):
        """GET with unknown id returns 404."""
        response = client.get("/execution/connection-tests/00000000-0000-0000-0000-000000000000/connection")
        assert response.status_code == 404
