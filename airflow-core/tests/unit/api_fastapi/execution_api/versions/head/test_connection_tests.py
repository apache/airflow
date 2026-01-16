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

from airflow.models.connection_test import ConnectionTestRequest, ConnectionTestState

from tests_common.test_utils.db import clear_db_connection_tests

pytestmark = pytest.mark.db_test

TEST_ENCRYPTED_URI = "gAAAAABfakeencrypteddata..."
TEST_CONN_TYPE = "postgres"


@pytest.fixture(autouse=True)
def clean_connection_tests():
    """Clean up connection test requests after tests."""
    yield
    clear_db_connection_tests()


class TestGetPendingConnectionTests:
    """Tests for GET /execution/connection-tests/pending."""

    def test_get_pending_no_requests(self, client, session):
        """Test getting pending requests when none exist."""
        response = client.get(
            "/execution/connection-tests/pending",
            params={"hostname": "worker-1.example.com"},
        )

        assert response.status_code == 200
        data = response.json()
        assert data["requests"] == []

    def test_get_pending_returns_pending_requests(self, client, session):
        """Test that pending requests are returned."""
        request1 = ConnectionTestRequest.create_request(
            encrypted_connection_uri=TEST_ENCRYPTED_URI,
            conn_type=TEST_CONN_TYPE,
            session=session,
        )
        request2 = ConnectionTestRequest.create_request(
            encrypted_connection_uri=TEST_ENCRYPTED_URI,
            conn_type="mysql",
            session=session,
        )
        session.commit()

        response = client.get(
            "/execution/connection-tests/pending",
            params={"hostname": "worker-1.example.com"},
        )

        assert response.status_code == 200
        data = response.json()
        assert len(data["requests"]) == 2

        # Requests should contain the expected fields
        request_ids = {r["request_id"] for r in data["requests"]}
        assert request1.id in request_ids
        assert request2.id in request_ids

        for req in data["requests"]:
            assert "encrypted_connection_uri" in req
            assert "conn_type" in req
            assert "timeout" in req

    def test_get_pending_marks_requests_as_running(self, client, session):
        """Test that fetched requests are marked as running."""
        request = ConnectionTestRequest.create_request(
            encrypted_connection_uri=TEST_ENCRYPTED_URI,
            conn_type=TEST_CONN_TYPE,
            session=session,
        )
        session.commit()

        response = client.get(
            "/execution/connection-tests/pending",
            params={"hostname": "worker-1.example.com"},
        )

        assert response.status_code == 200

        # Refresh from database
        session.expire_all()
        updated_request = session.get(ConnectionTestRequest, request.id)
        assert updated_request.state == ConnectionTestState.RUNNING.value
        assert updated_request.worker_hostname == "worker-1.example.com"
        assert updated_request.started_at is not None

    def test_get_pending_excludes_running_requests(self, client, session):
        """Test that running requests are not returned."""
        pending = ConnectionTestRequest.create_request(
            encrypted_connection_uri=TEST_ENCRYPTED_URI,
            conn_type="pending_type",
            session=session,
        )
        running = ConnectionTestRequest.create_request(
            encrypted_connection_uri=TEST_ENCRYPTED_URI,
            conn_type="running_type",
            session=session,
        )
        running.mark_running("other-worker")
        session.commit()

        response = client.get(
            "/execution/connection-tests/pending",
            params={"hostname": "worker-1.example.com"},
        )

        assert response.status_code == 200
        data = response.json()
        assert len(data["requests"]) == 1
        assert data["requests"][0]["request_id"] == pending.id

    def test_get_pending_respects_limit(self, client, session):
        """Test that limit parameter is respected."""
        for i in range(5):
            ConnectionTestRequest.create_request(
                encrypted_connection_uri=TEST_ENCRYPTED_URI,
                conn_type=f"type_{i}",
                session=session,
            )
        session.commit()

        response = client.get(
            "/execution/connection-tests/pending",
            params={"hostname": "worker-1.example.com", "limit": 2},
        )

        assert response.status_code == 200
        data = response.json()
        assert len(data["requests"]) == 2

    def test_get_pending_requires_hostname(self, client):
        """Test that hostname parameter is required."""
        response = client.get("/execution/connection-tests/pending")

        assert response.status_code == 422


class TestUpdateConnectionTestState:
    """Tests for PATCH /execution/connection-tests/{request_id}/state."""

    def test_update_to_success(self, client, session):
        """Test marking a connection test as successful."""
        request = ConnectionTestRequest.create_request(
            encrypted_connection_uri=TEST_ENCRYPTED_URI,
            conn_type=TEST_CONN_TYPE,
            session=session,
        )
        request.mark_running("worker-1")
        session.commit()

        response = client.patch(
            f"/execution/connection-tests/{request.id}/state",
            json={
                "state": "success",
                "result_status": True,
                "result_message": "Connection successfully tested",
            },
        )

        assert response.status_code == 204

        # Verify database state
        session.expire_all()
        updated = session.get(ConnectionTestRequest, request.id)
        assert updated.state == ConnectionTestState.SUCCESS.value
        assert updated.result_status is True
        assert updated.result_message == "Connection successfully tested"
        assert updated.completed_at is not None

    def test_update_to_failed(self, client, session):
        """Test marking a connection test as failed."""
        request = ConnectionTestRequest.create_request(
            encrypted_connection_uri=TEST_ENCRYPTED_URI,
            conn_type=TEST_CONN_TYPE,
            session=session,
        )
        request.mark_running("worker-1")
        session.commit()

        response = client.patch(
            f"/execution/connection-tests/{request.id}/state",
            json={
                "state": "failed",
                "result_status": False,
                "result_message": "Connection refused: timeout",
            },
        )

        assert response.status_code == 204

        # Verify database state
        session.expire_all()
        updated = session.get(ConnectionTestRequest, request.id)
        assert updated.state == ConnectionTestState.FAILED.value
        assert updated.result_status is False
        assert updated.result_message == "Connection refused: timeout"

    def test_update_to_running(self, client, session):
        """Test updating state to running."""
        request = ConnectionTestRequest.create_request(
            encrypted_connection_uri=TEST_ENCRYPTED_URI,
            conn_type=TEST_CONN_TYPE,
            session=session,
        )
        session.commit()

        response = client.patch(
            f"/execution/connection-tests/{request.id}/state",
            json={
                "state": "running",
                "hostname": "worker-2.example.com",
            },
        )

        assert response.status_code == 204

        # Verify database state
        session.expire_all()
        updated = session.get(ConnectionTestRequest, request.id)
        assert updated.state == ConnectionTestState.RUNNING.value
        assert updated.worker_hostname == "worker-2.example.com"

    def test_update_not_found(self, client):
        """Test updating a non-existent request."""
        response = client.patch(
            "/execution/connection-tests/non-existent-id/state",
            json={
                "state": "success",
                "result_status": True,
                "result_message": "OK",
            },
        )

        assert response.status_code == 404
        assert "was not found" in response.json()["detail"]

    def test_update_invalid_state_transition_result_from_pending(self, client, session):
        """Test that result cannot be reported from pending state."""
        request = ConnectionTestRequest.create_request(
            encrypted_connection_uri=TEST_ENCRYPTED_URI,
            conn_type=TEST_CONN_TYPE,
            session=session,
        )
        session.commit()

        # Try to report result from pending state (should fail)
        response = client.patch(
            f"/execution/connection-tests/{request.id}/state",
            json={
                "state": "success",
                "result_status": True,
                "result_message": "OK",
            },
        )

        assert response.status_code == 409
        assert "Expected `running`" in response.json()["detail"]

    def test_update_invalid_state_transition_from_completed(self, client, session):
        """Test that state cannot be changed after completion."""
        request = ConnectionTestRequest.create_request(
            encrypted_connection_uri=TEST_ENCRYPTED_URI,
            conn_type=TEST_CONN_TYPE,
            session=session,
        )
        request.mark_running("worker-1")
        request.mark_success("OK")
        session.commit()

        # Try to update state from completed (should fail)
        response = client.patch(
            f"/execution/connection-tests/{request.id}/state",
            json={
                "state": "running",
                "hostname": "worker-2",
            },
        )

        assert response.status_code == 409
