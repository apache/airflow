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

from airflow.models.connection_test import (
    TERMINAL_STATES,
    VALID_STATE_TRANSITIONS,
    ConnectionTestRequest,
    ConnectionTestState,
)
from airflow.utils.session import create_session

from tests_common.test_utils.db import clear_db_connection_tests

pytestmark = [pytest.mark.db_test]

TEST_ENCRYPTED_URI = "gAAAAABfakeencrypteddata..."
TEST_CONN_TYPE = "postgres"


@pytest.fixture
def session():
    """Fixture that provides a SQLAlchemy session"""
    with create_session() as session:
        yield session


@pytest.fixture(autouse=True)
def clean_db():
    """Clean up connection test requests before and after each test."""
    clear_db_connection_tests()
    yield
    clear_db_connection_tests()


class TestConnectionTestState:
    def test_state_values(self):
        """Test that ConnectionTestState has expected values."""
        assert ConnectionTestState.PENDING.value == "pending"
        assert ConnectionTestState.RUNNING.value == "running"
        assert ConnectionTestState.SUCCESS.value == "success"
        assert ConnectionTestState.FAILED.value == "failed"

    def test_terminal_states(self):
        """Test that terminal states are correctly defined."""
        assert ConnectionTestState.SUCCESS in TERMINAL_STATES
        assert ConnectionTestState.FAILED in TERMINAL_STATES
        assert ConnectionTestState.PENDING not in TERMINAL_STATES
        assert ConnectionTestState.RUNNING not in TERMINAL_STATES

    def test_state_str(self):
        """Test that state __str__ returns the value."""
        assert str(ConnectionTestState.PENDING) == "pending"
        assert str(ConnectionTestState.SUCCESS) == "success"


class TestConnectionTestRequest:
    def test_create_request(self, session):
        """Test creating a connection test request."""
        request = ConnectionTestRequest.create_request(
            encrypted_connection_uri=TEST_ENCRYPTED_URI,
            conn_type=TEST_CONN_TYPE,
            timeout=120,
            session=session,
        )
        session.commit()

        assert request.id is not None
        assert request.state == ConnectionTestState.PENDING
        assert request.encrypted_connection_uri == TEST_ENCRYPTED_URI
        assert request.conn_type == TEST_CONN_TYPE
        assert request.timeout == 120
        assert request.result_status is None
        assert request.result_message is None
        assert request.created_at is not None
        assert request.started_at is None
        assert request.completed_at is None
        assert request.worker_hostname is None

    def test_create_request_default_timeout(self, session):
        """Test that default timeout is 60 seconds."""
        request = ConnectionTestRequest.create_request(
            encrypted_connection_uri=TEST_ENCRYPTED_URI,
            conn_type=TEST_CONN_TYPE,
            session=session,
        )
        session.commit()

        assert request.timeout == 60

    def test_create_request_without_session(self):
        """Test creating a request without adding to session."""
        request = ConnectionTestRequest.create_request(
            encrypted_connection_uri=TEST_ENCRYPTED_URI,
            conn_type=TEST_CONN_TYPE,
        )

        assert request.id is not None
        assert request.state == ConnectionTestState.PENDING

    def test_mark_running(self, session):
        """Test marking a request as running."""
        request = ConnectionTestRequest.create_request(
            encrypted_connection_uri=TEST_ENCRYPTED_URI,
            conn_type=TEST_CONN_TYPE,
            session=session,
        )
        session.commit()

        request.mark_running("worker-1.example.com")

        assert request.state == ConnectionTestState.RUNNING
        assert request.worker_hostname == "worker-1.example.com"
        assert request.started_at is not None

    def test_mark_success(self, session):
        """Test marking a request as successful."""
        request = ConnectionTestRequest.create_request(
            encrypted_connection_uri=TEST_ENCRYPTED_URI,
            conn_type=TEST_CONN_TYPE,
            session=session,
        )
        session.commit()
        request.mark_running("worker-1")

        request.mark_success("Connection successfully tested")

        assert request.state == ConnectionTestState.SUCCESS
        assert request.result_status is True
        assert request.result_message == "Connection successfully tested"
        assert request.completed_at is not None

    def test_mark_failed(self, session):
        """Test marking a request as failed."""
        request = ConnectionTestRequest.create_request(
            encrypted_connection_uri=TEST_ENCRYPTED_URI,
            conn_type=TEST_CONN_TYPE,
            session=session,
        )
        session.commit()
        request.mark_running("worker-1")

        request.mark_failed("Connection refused: timeout")

        assert request.state == ConnectionTestState.FAILED
        assert request.result_status is False
        assert request.result_message == "Connection refused: timeout"
        assert request.completed_at is not None

    def test_is_terminal(self, session):
        """Test the is_terminal property."""
        request = ConnectionTestRequest.create_request(
            encrypted_connection_uri=TEST_ENCRYPTED_URI,
            conn_type=TEST_CONN_TYPE,
            session=session,
        )
        session.commit()

        # Initially pending - not terminal
        assert request.is_terminal is False

        # Running - not terminal
        request.mark_running("worker-1")
        assert request.is_terminal is False

        # Success - terminal
        request.mark_success("OK")
        assert request.is_terminal is True

    def test_is_terminal_failed(self, session):
        """Test that failed state is terminal."""
        request = ConnectionTestRequest.create_request(
            encrypted_connection_uri=TEST_ENCRYPTED_URI,
            conn_type=TEST_CONN_TYPE,
            session=session,
        )
        session.commit()

        request.mark_running("worker-1")
        request.mark_failed("Error")

        assert request.is_terminal is True

    def test_get_pending_requests(self, session):
        """Test getting pending requests."""
        # Create multiple requests in different states
        pending1 = ConnectionTestRequest.create_request(
            encrypted_connection_uri=TEST_ENCRYPTED_URI,
            conn_type=TEST_CONN_TYPE,
            session=session,
        )
        pending2 = ConnectionTestRequest.create_request(
            encrypted_connection_uri=TEST_ENCRYPTED_URI,
            conn_type="mysql",
            session=session,
        )
        running = ConnectionTestRequest.create_request(
            encrypted_connection_uri=TEST_ENCRYPTED_URI,
            conn_type="sqlite",
            session=session,
        )
        session.commit()

        running.mark_running("worker-1")
        session.commit()

        # Get pending requests
        pending_requests = ConnectionTestRequest.get_pending_requests(session, limit=10)

        # Should only return pending requests
        assert len(pending_requests) == 2
        pending_ids = {r.id for r in pending_requests}
        assert pending1.id in pending_ids
        assert pending2.id in pending_ids
        assert running.id not in pending_ids

    def test_get_pending_requests_limit(self, session):
        """Test that get_pending_requests respects the limit."""
        # Create 5 pending requests
        for i in range(5):
            ConnectionTestRequest.create_request(
                encrypted_connection_uri=TEST_ENCRYPTED_URI,
                conn_type=f"type_{i}",
                session=session,
            )
        session.commit()

        # Get only 2
        pending_requests = ConnectionTestRequest.get_pending_requests(session, limit=2)
        assert len(pending_requests) == 2

    def test_get_pending_requests_ordered_by_created_at(self, session):
        """Test that pending requests are ordered by created_at."""
        import time

        first = ConnectionTestRequest.create_request(
            encrypted_connection_uri=TEST_ENCRYPTED_URI,
            conn_type="first",
            session=session,
        )
        session.commit()

        time.sleep(0.01)  # Small delay to ensure different timestamps

        second = ConnectionTestRequest.create_request(
            encrypted_connection_uri=TEST_ENCRYPTED_URI,
            conn_type="second",
            session=session,
        )
        session.commit()

        pending_requests = ConnectionTestRequest.get_pending_requests(session, limit=10)

        assert len(pending_requests) == 2
        assert pending_requests[0].id == first.id
        assert pending_requests[1].id == second.id

    def test_repr(self, session):
        """Test the __repr__ method."""
        request = ConnectionTestRequest.create_request(
            encrypted_connection_uri=TEST_ENCRYPTED_URI,
            conn_type=TEST_CONN_TYPE,
            session=session,
        )
        session.commit()

        repr_str = repr(request)
        assert "ConnectionTestRequest" in repr_str
        assert str(request.id) in repr_str
        assert "pending" in repr_str
        assert TEST_CONN_TYPE in repr_str

    def test_persistence(self, session):
        """Test that request is correctly persisted and retrieved."""
        request = ConnectionTestRequest.create_request(
            encrypted_connection_uri=TEST_ENCRYPTED_URI,
            conn_type=TEST_CONN_TYPE,
            timeout=90,
            session=session,
        )
        session.commit()

        request_id = request.id

        # Clear session and retrieve
        session.expire_all()
        retrieved = session.get(ConnectionTestRequest, request_id)

        assert retrieved is not None
        assert retrieved.id == request_id
        assert retrieved.encrypted_connection_uri == TEST_ENCRYPTED_URI
        assert retrieved.conn_type == TEST_CONN_TYPE
        assert retrieved.timeout == 90
        assert retrieved.state == "pending"

    def test_valid_state_transitions_defined(self):
        """Test that valid state transitions are correctly defined."""
        # PENDING can go to RUNNING or FAILED
        assert ConnectionTestState.RUNNING in VALID_STATE_TRANSITIONS[ConnectionTestState.PENDING]
        assert ConnectionTestState.FAILED in VALID_STATE_TRANSITIONS[ConnectionTestState.PENDING]

        # RUNNING can go to SUCCESS or FAILED
        assert ConnectionTestState.SUCCESS in VALID_STATE_TRANSITIONS[ConnectionTestState.RUNNING]
        assert ConnectionTestState.FAILED in VALID_STATE_TRANSITIONS[ConnectionTestState.RUNNING]

        # Terminal states have no valid transitions
        assert len(VALID_STATE_TRANSITIONS[ConnectionTestState.SUCCESS]) == 0
        assert len(VALID_STATE_TRANSITIONS[ConnectionTestState.FAILED]) == 0

    def test_invalid_transition_pending_to_success(self, session):
        """Test that PENDING -> SUCCESS is rejected."""
        request = ConnectionTestRequest.create_request(
            encrypted_connection_uri=TEST_ENCRYPTED_URI,
            conn_type=TEST_CONN_TYPE,
            session=session,
        )
        session.commit()

        with pytest.raises(ValueError, match="Invalid state transition"):
            request.mark_success("Should fail")

    def test_invalid_transition_success_to_running(self, session):
        """Test that SUCCESS -> RUNNING is rejected."""
        request = ConnectionTestRequest.create_request(
            encrypted_connection_uri=TEST_ENCRYPTED_URI,
            conn_type=TEST_CONN_TYPE,
            session=session,
        )
        session.commit()
        request.mark_running("worker-1")
        request.mark_success("OK")

        with pytest.raises(ValueError, match="Invalid state transition"):
            request.mark_running("worker-2")

    def test_invalid_transition_failed_to_success(self, session):
        """Test that FAILED -> SUCCESS is rejected."""
        request = ConnectionTestRequest.create_request(
            encrypted_connection_uri=TEST_ENCRYPTED_URI,
            conn_type=TEST_CONN_TYPE,
            session=session,
        )
        session.commit()
        request.mark_running("worker-1")
        request.mark_failed("Error")

        with pytest.raises(ValueError, match="Invalid state transition"):
            request.mark_success("Should fail")

    def test_valid_transition_pending_to_failed(self, session):
        """Test that PENDING -> FAILED is allowed (e.g., no executor available)."""
        request = ConnectionTestRequest.create_request(
            encrypted_connection_uri=TEST_ENCRYPTED_URI,
            conn_type=TEST_CONN_TYPE,
            session=session,
        )
        session.commit()

        # Should not raise
        request.mark_failed("No executor available")

        assert request.state == ConnectionTestState.FAILED
        assert request.result_status is False

    def test_invalid_transition_running_to_running(self, session):
        """Test that RUNNING -> RUNNING is rejected."""
        request = ConnectionTestRequest.create_request(
            encrypted_connection_uri=TEST_ENCRYPTED_URI,
            conn_type=TEST_CONN_TYPE,
            session=session,
        )
        session.commit()
        request.mark_running("worker-1")

        with pytest.raises(ValueError, match="Invalid state transition"):
            request.mark_running("worker-2")
