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

import secrets
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import TYPE_CHECKING
from uuid import UUID

import structlog
import uuid6
from sqlalchemy import (
    Boolean,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
    Uuid,
    select,
)
from sqlalchemy.orm import Mapped, mapped_column, validates

from airflow._shared.timezones import timezone
from airflow.models.base import Base
from airflow.models.connection import Connection
from airflow.models.crypto import FernetFieldsMixin, get_fernet
from airflow.utils.sqlalchemy import UtcDateTime

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

log = structlog.get_logger(__name__)


class ConnectionTestState(str, Enum):
    """All possible states of a connection test."""

    PENDING = "pending"
    QUEUED = "queued"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"

    def __str__(self) -> str:
        return self.value


ACTIVE_STATES = frozenset(
    (ConnectionTestState.PENDING, ConnectionTestState.QUEUED, ConnectionTestState.RUNNING)
)
DISPATCHED_STATES = frozenset((ConnectionTestState.QUEUED, ConnectionTestState.RUNNING))
TERMINAL_STATES = frozenset((ConnectionTestState.SUCCESS, ConnectionTestState.FAILED))


@dataclass(frozen=True, slots=True)
class ConnectionTestKey:
    """Typed key for connection-test workloads (wraps str(UUID))."""

    id: str

    def __str__(self) -> str:
        return self.id


class ConnectionTestRequest(Base, FernetFieldsMixin):
    """
    Tracks an async connection test request dispatched to a worker.

    Stores the full connection details so the worker reads from this table
    instead of the real ``connection`` table. The real ``connection`` table
    is only modified if the test succeeds and ``commit_on_success`` is True.
    """

    __tablename__ = "connection_test_request"

    id: Mapped[UUID] = mapped_column(Uuid(), primary_key=True, default=uuid6.uuid7)
    token: Mapped[str] = mapped_column(String(64), nullable=False, unique=True)
    connection_id: Mapped[str] = mapped_column(String(250), nullable=False)
    state: Mapped[str] = mapped_column(String(20), nullable=False, default=ConnectionTestState.PENDING)
    result_message: Mapped[str | None] = mapped_column(String(2000), nullable=True)
    created_at: Mapped[datetime] = mapped_column(UtcDateTime, default=timezone.utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        UtcDateTime, default=timezone.utcnow, onupdate=timezone.utcnow, nullable=False
    )
    executor: Mapped[str | None] = mapped_column(String(256), nullable=True)
    queue: Mapped[str | None] = mapped_column(String(256), nullable=True)

    conn_type: Mapped[str] = mapped_column(String(500), nullable=False)
    host: Mapped[str | None] = mapped_column(String(500), nullable=True)
    login: Mapped[str | None] = mapped_column(Text, nullable=True)
    schema: Mapped[str | None] = mapped_column("schema", String(500), nullable=True)
    port: Mapped[int | None] = mapped_column(Integer, nullable=True)
    commit_on_success: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=False, server_default="0"
    )
    is_encrypted: Mapped[bool] = mapped_column(
        Boolean, unique=False, default=False, nullable=False, server_default="0"
    )
    is_extra_encrypted: Mapped[bool] = mapped_column(
        Boolean, unique=False, default=False, nullable=False, server_default="0"
    )

    active_connection_id: Mapped[str | None] = mapped_column(String(250), nullable=True)
    team_name: Mapped[str | None] = mapped_column(String(50), nullable=True)

    __table_args__ = (
        Index("idx_connection_test_request_state_created_at", state, created_at),
        UniqueConstraint(
            "active_connection_id",
            name="uq_connection_test_request_active_conn",
        ),
    )

    def __init__(
        self,
        *,
        connection_id: str,
        conn_type: str,
        host: str | None = None,
        login: str | None = None,
        password: str | None = None,
        schema: str | None = None,
        port: int | None = None,
        extra: str | None = None,
        commit_on_success: bool = False,
        executor: str | None = None,
        queue: str | None = None,
        team_name: str | None = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.connection_id = connection_id
        self.conn_type = conn_type
        self.host = host
        self.login = login
        self.password = password
        self.schema = schema
        self.port = port
        self.extra = extra
        self.commit_on_success = commit_on_success
        self.executor = executor
        self.queue = queue
        self.team_name = team_name
        self.token = secrets.token_urlsafe(32)
        self.state = ConnectionTestState.PENDING

    @validates("state")
    def _sync_active_connection_id(
        self, _key: str, value: str | ConnectionTestState
    ) -> str | ConnectionTestState:
        self.active_connection_id = self.connection_id if value in ACTIVE_STATES else None
        return value

    def __repr__(self) -> str:
        return (
            f"<ConnectionTestRequest id={self.id!r} connection_id={self.connection_id!r} state={self.state}>"
        )

    def rotate_fernet_key(self):
        """Encrypts data with a new key. See: :ref:`security/fernet`."""
        fernet = get_fernet()
        if self._password and self.is_encrypted:
            self._password = fernet.rotate(self._password.encode("utf-8")).decode()
        if self._extra and self.is_extra_encrypted:
            self._extra = fernet.rotate(self._extra.encode("utf-8")).decode()

    def get_executor_name(self) -> str | None:
        """Return the executor name for scheduler routing."""
        return self.executor

    def get_dag_id(self) -> None:
        """Return None — connection tests are not associated with any DAG."""
        return None

    def to_connection(self) -> Connection:
        """Build a transient Connection object from the stored fields for testing."""
        return Connection(
            conn_id=self.connection_id,
            conn_type=self.conn_type,
            host=self.host,
            login=self.login,
            password=self.password,
            schema=self.schema,
            port=self.port,
            extra=self.extra,
        )

    def commit_to_connection_table(self, *, session: Session) -> None:
        """Upsert the tested connection into the real ``connection`` table."""
        conn = session.scalar(select(Connection).filter_by(conn_id=self.connection_id))
        if conn is None:
            conn = Connection(
                conn_id=self.connection_id,
                conn_type=self.conn_type,
                host=self.host,
                login=self.login,
                password=self.password,
                schema=self.schema,
                port=self.port,
                extra=self.extra,
                team_name=self.team_name,
            )
            session.add(conn)
            log.info("Created new connection from successful test", connection_id=self.connection_id)
        else:
            conn.conn_type = self.conn_type
            conn.host = self.host
            conn.login = self.login
            conn.password = self.password
            conn.schema = self.schema
            conn.port = self.port
            conn.extra = self.extra
            log.info("Updated existing connection from successful test", connection_id=self.connection_id)
