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
"""Connection test request model for async connection testing on workers."""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import TYPE_CHECKING

import uuid6
from sqlalchemy import Boolean, Index, Integer, String, Text, select
from sqlalchemy.orm import Mapped

from airflow._shared.timezones import timezone
from airflow.models.base import Base
from airflow.utils.sqlalchemy import UtcDateTime, mapped_column

if TYPE_CHECKING:
    from sqlalchemy.orm import Session


class ConnectionTestState(str, Enum):
    """All possible states of a connection test request."""

    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"

    def __str__(self) -> str:
        return self.value


TERMINAL_STATES = frozenset((ConnectionTestState.SUCCESS, ConnectionTestState.FAILED))

# Valid state transitions
VALID_STATE_TRANSITIONS: dict[ConnectionTestState, frozenset[ConnectionTestState]] = {
    ConnectionTestState.PENDING: frozenset((ConnectionTestState.RUNNING, ConnectionTestState.FAILED)),
    ConnectionTestState.RUNNING: frozenset((ConnectionTestState.SUCCESS, ConnectionTestState.FAILED)),
    ConnectionTestState.SUCCESS: frozenset(),  # Terminal state, no transitions allowed
    ConnectionTestState.FAILED: frozenset(),  # Terminal state, no transitions allowed
}


class ConnectionTestRequest(Base):
    """
    Stores connection test requests for asynchronous execution on workers.

    This model supports moving the test_connection functionality from the API server
    to workers for improved security and network isolation.
    """

    __tablename__ = "connection_test_request"

    id: Mapped[str] = mapped_column(String(36), primary_key=True)

    # State: PENDING -> RUNNING -> SUCCESS/FAILED
    state: Mapped[str] = mapped_column(String(10), nullable=False, default=ConnectionTestState.PENDING.value)

    # Encrypted connection URI (using Fernet encryption)
    encrypted_connection_uri: Mapped[str] = mapped_column(Text, nullable=False)

    # Connection type (needed to instantiate the hook on the worker)
    conn_type: Mapped[str] = mapped_column(String(500), nullable=False)

    # Result fields - populated when test completes
    result_status: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    result_message: Mapped[str | None] = mapped_column(Text, nullable=True)

    # Timestamps
    created_at: Mapped[datetime] = mapped_column(UtcDateTime, default=timezone.utcnow, nullable=False)
    started_at: Mapped[datetime | None] = mapped_column(UtcDateTime, nullable=True)
    completed_at: Mapped[datetime | None] = mapped_column(UtcDateTime, nullable=True)

    # Timeout in seconds (default 60s)
    timeout: Mapped[int] = mapped_column(Integer, default=60, nullable=False)

    # Which worker picked up this request
    worker_hostname: Mapped[str | None] = mapped_column(String(500), nullable=True)

    __table_args__ = (Index("idx_connection_test_state_created", "state", "created_at"),)

    def __repr__(self) -> str:
        return f"<ConnectionTestRequest id={self.id} state={self.state} conn_type={self.conn_type}>"

    def _validate_state_transition(self, new_state: ConnectionTestState) -> None:
        """
        Validate that a state transition is allowed.

        :param new_state: The target state
        :raises ValueError: If the transition is not allowed
        """
        current_state = ConnectionTestState(self.state)
        allowed_transitions = VALID_STATE_TRANSITIONS.get(current_state, frozenset())
        if new_state not in allowed_transitions:
            raise ValueError(
                f"Invalid state transition from {current_state.value!r} to {new_state.value!r}. "
                f"Allowed transitions: {[s.value for s in allowed_transitions]}"
            )

    @classmethod
    def create_request(
        cls,
        encrypted_connection_uri: str,
        conn_type: str,
        timeout: int = 60,
        session: Session | None = None,
    ) -> ConnectionTestRequest:
        """
        Create a new connection test request.

        :param encrypted_connection_uri: The Fernet-encrypted connection URI
        :param conn_type: The connection type (e.g., 'postgres', 'mysql')
        :param timeout: Timeout in seconds for the test
        :param session: Optional SQLAlchemy session to add the request to
        :return: The created ConnectionTestRequest
        """
        request = cls(
            id=str(uuid6.uuid7()),
            encrypted_connection_uri=encrypted_connection_uri,
            conn_type=conn_type,
            timeout=timeout,
            state=ConnectionTestState.PENDING.value,
        )
        if session:
            session.add(request)
        return request

    def mark_running(self, worker_hostname: str) -> None:
        """Mark the request as running on a specific worker."""
        self._validate_state_transition(ConnectionTestState.RUNNING)
        self.state = ConnectionTestState.RUNNING.value
        self.worker_hostname = worker_hostname
        self.started_at = timezone.utcnow()

    def mark_success(self, message: str) -> None:
        """Mark the request as successfully completed."""
        self._validate_state_transition(ConnectionTestState.SUCCESS)
        self.state = ConnectionTestState.SUCCESS.value
        self.result_status = True
        self.result_message = message
        self.completed_at = timezone.utcnow()

    def mark_failed(self, message: str) -> None:
        """Mark the request as failed."""
        self._validate_state_transition(ConnectionTestState.FAILED)
        self.state = ConnectionTestState.FAILED.value
        self.result_status = False
        self.result_message = message
        self.completed_at = timezone.utcnow()

    @property
    def is_terminal(self) -> bool:
        """Check if the request is in a terminal state."""
        return ConnectionTestState(self.state) in TERMINAL_STATES

    @classmethod
    def get_pending_requests(cls, session: Session, limit: int = 10) -> list[ConnectionTestRequest]:
        """
        Get pending connection test requests for worker processing.

        :param session: SQLAlchemy session
        :param limit: Maximum number of requests to return
        :return: List of pending ConnectionTestRequest objects
        """
        return list(
            session.scalars(
                select(cls)
                .where(cls.state == ConnectionTestState.PENDING.value)
                .order_by(cls.created_at)
                .limit(limit)
                .with_for_update(skip_locked=True)
            )
        )
