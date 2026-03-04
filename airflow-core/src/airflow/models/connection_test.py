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
from sqlalchemy import JSON, Boolean, Index, String, Text, Uuid, select
from sqlalchemy.orm import Mapped, mapped_column

from airflow._shared.timezones import timezone
from airflow.models.base import Base
from airflow.models.connection import Connection
from airflow.models.crypto import get_fernet
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


ACTIVE_STATES = frozenset((ConnectionTestState.QUEUED, ConnectionTestState.RUNNING))
TERMINAL_STATES = frozenset((ConnectionTestState.SUCCESS, ConnectionTestState.FAILED))


class ConnectionTest(Base):
    """Tracks an async connection test dispatched to a worker via a TestConnection workload."""

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
    executor: Mapped[str | None] = mapped_column(String(256), nullable=True)
    queue: Mapped[str | None] = mapped_column(String(256), nullable=True)
    connection_snapshot: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    reverted: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, server_default="0")

    __table_args__ = (Index("idx_connection_test_state_created_at", state, created_at),)

    def __init__(
        self, *, connection_id: str, executor: str | None = None, queue: str | None = None, **kwargs
    ):
        super().__init__(**kwargs)
        self.connection_id = connection_id
        self.executor = executor
        self.queue = queue
        self.token = secrets.token_urlsafe(32)
        self.state = ConnectionTestState.PENDING

    def __repr__(self) -> str:
        return f"<ConnectionTest id={self.id!r} connection_id={self.connection_id!r} state={self.state}>"


def run_connection_test(*, conn: Connection) -> tuple[bool, str]:
    """
    Worker-side function to execute a connection test.

    Returns a (success, message) tuple. The caller is responsible for
    reporting the result back via the Execution API.
    """
    try:
        return conn.test_connection()
    except Exception as e:
        log.exception("Connection test failed", connection_id=conn.conn_id)
        return False, str(e)


_SNAPSHOT_FIELDS = (
    "conn_type",
    "description",
    "host",
    "login",
    "_password",
    "schema",
    "port",
    "_extra",
    "is_encrypted",
    "is_extra_encrypted",
    "team_name",
)


def snapshot_connection(conn: Connection) -> dict:
    """
    Capture raw DB column values from a Connection for later restore.

    Encrypted fields (``_password``, ``_extra``) are stored as ciphertext
    so they can be written directly back without re-encryption.
    """
    return {field: getattr(conn, field) for field in _SNAPSHOT_FIELDS}


def _revert_connection(conn: Connection, snapshot: dict) -> None:
    """
    Restore a Connection's columns from a snapshot dict.

    Writes directly to ``_password`` and ``_extra`` (bypassing the
    encrypting property setters) so the stored ciphertext is preserved.
    """
    for field, value in snapshot.items():
        setattr(conn, field, value)


def _decrypt_snapshot_field(snapshot: dict, field: str) -> str | None:
    """Decrypt a single encrypted field from a snapshot dict using Fernet."""
    raw = snapshot.get(field)
    if raw is None:
        return None
    encrypted_flag = "is_encrypted" if field == "_password" else "is_extra_encrypted"
    if not snapshot.get(encrypted_flag, False):
        return raw
    fernet = get_fernet()
    return fernet.decrypt(bytes(raw, "utf-8")).decode()


def _can_safely_revert(conn: Connection, post_snapshot: dict) -> bool:
    """
    Check whether the connection's current state matches the post-edit snapshot.

    Compares **decrypted** values for encrypted fields and direct values for
    non-encrypted fields.  Returns ``False`` if any field differs, indicating
    a concurrent edit has occurred and the revert should be skipped.
    """
    for field in _SNAPSHOT_FIELDS:
        if field in ("is_encrypted", "is_extra_encrypted"):
            continue

        if field == "_password":
            current_val = conn.password
            snapshot_val = _decrypt_snapshot_field(post_snapshot, "_password")
        elif field == "_extra":
            current_val = conn.extra
            snapshot_val = _decrypt_snapshot_field(post_snapshot, "_extra")
        else:
            current_val = getattr(conn, field)
            snapshot_val = post_snapshot.get(field)

        if current_val != snapshot_val:
            return False
    return True


def attempt_revert(ct: ConnectionTest, *, session: Session) -> None:
    """Revert a connection to its pre-edit values if no concurrent edit has occurred."""
    if not ct.connection_snapshot:
        log.warning("attempt_revert called without snapshot", connection_test_id=ct.id)
        return

    pre_snapshot = ct.connection_snapshot["pre"]
    post_snapshot = ct.connection_snapshot["post"]

    conn = session.scalar(select(Connection).filter_by(conn_id=ct.connection_id))
    if conn is None:
        ct.result_message = (ct.result_message or "") + " | Revert skipped: connection no longer exists."
        log.warning("Revert skipped: connection no longer exists", connection_id=ct.connection_id)
        return

    if not _can_safely_revert(conn, post_snapshot):
        ct.result_message = (
            ct.result_message or ""
        ) + " | Revert skipped: connection was modified by another user."
        log.warning("Revert skipped: concurrent edit detected", connection_id=ct.connection_id)
        return

    _revert_connection(conn, pre_snapshot)
    ct.reverted = True
    log.info("Reverted connection to pre-edit state", connection_id=ct.connection_id)
