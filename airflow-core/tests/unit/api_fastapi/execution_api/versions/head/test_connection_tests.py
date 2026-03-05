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
from airflow.models.connection_test import ConnectionTest, ConnectionTestState, snapshot_connection

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
        ct = ConnectionTest(connection_id="test_conn")
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
        ct = session.get(ConnectionTest, ct.id)
        assert ct.state == "success"
        assert ct.result_message == "Connection successfully tested"
        assert ct.connection_snapshot is None

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
        ct = ConnectionTest(connection_id="test_conn")
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


class TestPatchConnectionTestRevert:
    """Tests for the revert-on-failure behavior in the execution API."""

    @pytest.fixture(autouse=True)
    def setup_teardown(self):
        clear_db_connections(add_default_connections_back=False)
        clear_db_connection_tests()
        yield
        clear_db_connections(add_default_connections_back=False)
        clear_db_connection_tests()

    def test_patch_failed_with_snapshot_reverts_connection(self, client, session):
        """PATCH with state=failed and snapshot triggers revert."""
        conn = Connection(
            conn_id="revert_conn",
            conn_type="postgres",
            host="old-host.example.com",
            login="old_user",
        )
        session.add(conn)
        session.flush()

        pre_snap = snapshot_connection(conn)
        conn.host = "new-host.example.com"
        conn.login = "new_user"
        post_snap = snapshot_connection(conn)
        session.flush()

        ct = ConnectionTest(connection_id="revert_conn")
        ct.state = ConnectionTestState.RUNNING
        ct.connection_snapshot = {"pre": pre_snap, "post": post_snap}
        session.add(ct)
        session.commit()

        response = client.patch(
            f"/execution/connection-tests/{ct.id}",
            json={"state": "failed", "result_message": "Connection refused"},
        )
        assert response.status_code == 204

        session.expire_all()
        ct = session.get(ConnectionTest, ct.id)
        assert ct.reverted is True
        assert ct.connection_snapshot is None
        conn = session.scalar(select(Connection).filter_by(conn_id="revert_conn"))
        assert conn.host == "old-host.example.com"
        assert conn.login == "old_user"

    def test_patch_success_with_snapshot_no_revert(self, client, session):
        """PATCH with state=success does not trigger revert even with snapshot."""
        conn = Connection(
            conn_id="no_revert_conn",
            conn_type="postgres",
            host="old-host.example.com",
        )
        session.add(conn)
        session.flush()

        pre_snap = snapshot_connection(conn)
        conn.host = "new-host.example.com"
        post_snap = snapshot_connection(conn)
        session.flush()

        ct = ConnectionTest(connection_id="no_revert_conn")
        ct.state = ConnectionTestState.RUNNING
        ct.connection_snapshot = {"pre": pre_snap, "post": post_snap}
        session.add(ct)
        session.commit()

        response = client.patch(
            f"/execution/connection-tests/{ct.id}",
            json={"state": "success", "result_message": "Connection OK"},
        )
        assert response.status_code == 204

        session.expire_all()
        ct = session.get(ConnectionTest, ct.id)
        assert ct.reverted is False
        assert ct.connection_snapshot is None
        conn = session.scalar(select(Connection).filter_by(conn_id="no_revert_conn"))
        assert conn.host == "new-host.example.com"

    def test_patch_failed_without_snapshot_no_revert(self, client, session):
        """PATCH with state=failed but no snapshot does not trigger revert."""
        ct = ConnectionTest(connection_id="test_conn")
        ct.state = ConnectionTestState.RUNNING
        session.add(ct)
        session.commit()

        response = client.patch(
            f"/execution/connection-tests/{ct.id}",
            json={"state": "failed", "result_message": "Connection refused"},
        )
        assert response.status_code == 204

        session.expire_all()
        ct = session.get(ConnectionTest, ct.id)
        assert ct.reverted is False

    def test_patch_failed_concurrent_edit_skips_revert(self, client, session):
        """PATCH with state=failed skips revert when connection was modified concurrently."""
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
        ct.state = ConnectionTestState.RUNNING
        ct.connection_snapshot = {"pre": pre_snap, "post": post_snap}
        session.add(ct)
        session.commit()

        response = client.patch(
            f"/execution/connection-tests/{ct.id}",
            json={"state": "failed", "result_message": "Connection refused"},
        )
        assert response.status_code == 204

        session.expire_all()
        ct = session.get(ConnectionTest, ct.id)
        assert ct.reverted is False
        assert ct.connection_snapshot is None
        assert "modified by another user" in ct.result_message
        conn = session.scalar(select(Connection).filter_by(conn_id="concurrent_conn"))
        assert conn.host == "third-party-host.example.com"
