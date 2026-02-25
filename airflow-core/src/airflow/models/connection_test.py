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
from datetime import datetime
from enum import Enum
from typing import TYPE_CHECKING
from uuid import UUID

import structlog
import uuid6
from sqlalchemy import ForeignKey, Index, String, Text, Uuid
from sqlalchemy.orm import Mapped, mapped_column, relationship

from airflow._shared.timezones import timezone
from airflow.models.base import Base
from airflow.utils.sqlalchemy import UtcDateTime

if TYPE_CHECKING:
    from airflow.models.callback import Callback, ExecutorCallback

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


TERMINAL_STATES = frozenset((ConnectionTestState.SUCCESS, ConnectionTestState.FAILED))

# Path used by ExecutorCallback to locate the worker function.
RUN_CONNECTION_TEST_PATH = "airflow.models.connection_test.run_connection_test"


class _ImportPathCallbackDef:
    """
    Minimal implementation of ImportPathExecutorCallbackDefProtocol.

    ExecutorCallback.__init__ expects an object satisfying this protocol, but no concrete
    implementation exists in airflow-core — the only one (SyncCallback) lives in the task-sdk.
    Once #61153 lands and ExecuteCallback.make() is decoupled from DagRun, this adapter can
    be replaced with the proper factory method.
    """

    def __init__(self, path: str, kwargs: dict, executor: str | None = None):
        self.path = path
        self.kwargs = kwargs
        self.executor = executor

    def serialize(self) -> dict:
        return {"path": self.path, "kwargs": self.kwargs, "executor": self.executor}


class ConnectionTest(Base):
    """Tracks an async connection test dispatched to a worker via ExecutorCallback."""

    __tablename__ = "connection_test"

    id: Mapped[UUID] = mapped_column(Uuid(), primary_key=True, default=uuid6.uuid7)
    token: Mapped[str] = mapped_column(String(64), nullable=False, unique=True)
    connection_id: Mapped[str] = mapped_column(String(250), nullable=False)
    state: Mapped[str] = mapped_column(String(10), nullable=False, default=ConnectionTestState.PENDING)
    result_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(UtcDateTime, default=timezone.utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        UtcDateTime, default=timezone.utcnow, onupdate=timezone.utcnow, nullable=False
    )

    callback_id: Mapped[UUID | None] = mapped_column(
        Uuid(), ForeignKey("callback.id", ondelete="SET NULL"), nullable=True
    )
    callback: Mapped[Callback | None] = relationship("Callback", uselist=False)

    __table_args__ = (Index("idx_connection_test_state_created_at", state, created_at),)

    def __init__(self, *, connection_id: str, **kwargs):
        super().__init__(**kwargs)
        self.connection_id = connection_id
        self.token = secrets.token_urlsafe(32)
        self.state = ConnectionTestState.PENDING

    def __repr__(self) -> str:
        return (
            f"<ConnectionTest id={self.id!r} token={self.token!r}"
            f" connection_id={self.connection_id!r} state={self.state}>"
        )

    def create_callback(self) -> ExecutorCallback:
        """Create an ExecutorCallback that will run the connection test on a worker."""
        from airflow.models.callback import CallbackFetchMethod, ExecutorCallback

        callback_def = _ImportPathCallbackDef(
            path=RUN_CONNECTION_TEST_PATH,
            kwargs={
                "connection_id": self.connection_id,
                "connection_test_id": str(self.id),
            },
        )
        return ExecutorCallback(callback_def, fetch_method=CallbackFetchMethod.IMPORT_PATH)


def run_connection_test(*, connection_id: str, connection_test_id: str) -> None:
    """
    Worker-side function to execute a connection test.

    This is the function referenced by the ExecutorCallback's import path.
    It fetches the connection, runs test_connection(), and reports results
    back by updating the ConnectionTest row directly.
    """
    from airflow.models.connection import Connection
    from airflow.utils.session import create_session

    connection_test_uuid = UUID(connection_test_id)

    with create_session() as session:
        ct = session.get(ConnectionTest, connection_test_uuid)
        if ct:
            ct.state = ConnectionTestState.RUNNING

    try:
        conn = Connection.get_connection_from_secrets(connection_id)
        test_status, test_message = conn.test_connection()
    except Exception as e:
        test_status = False
        test_message = str(e)
        log.exception("Connection test failed", connection_id=connection_id)

    with create_session() as session:
        ct = session.get(ConnectionTest, connection_test_uuid)
        if ct:
            ct.result_message = test_message
            ct.state = ConnectionTestState.SUCCESS if test_status else ConnectionTestState.FAILED
