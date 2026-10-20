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

from datetime import datetime
from enum import Enum
from typing import Any
from uuid import UUID

import uuid6
from sqlalchemy import JSON, Boolean, ForeignKeyConstraint, Index, String, UniqueConstraint, Uuid
from sqlalchemy.orm import Mapped, mapped_column

from airflow._shared.timezones import timezone
from airflow.models.base import Base, StringID
from airflow.utils.sqlalchemy import UtcDateTime


class WaitStatus(str, Enum):
    """Where a webhook wait is, as ``InboundWait.status`` stores it."""

    #: The task registered the wait and no matching delivery has arrived.
    REGISTERED = "registered"
    #: A matching delivery arrived while the task was still running; the task resumes when it parks.
    RECEIVED = "received"
    #: A matching delivery resumed the task.
    RESUMED = "resumed"
    #: The wait no longer applies: the task registered another wait, or its attempt is no longer live.
    ENDED = "ended"
    #: No matching delivery arrived before the deadline, and the task resumed with the timeout event.
    TIMED_OUT = "timed_out"

    def __str__(self) -> str:
        return self.value


class InboundWait(Base):
    """
    A task instance waiting for a delivery to one inbound webhook.

    .. note:: |experimental|
    """

    __tablename__ = "inbound_wait"

    id: Mapped[UUID] = mapped_column(Uuid(), primary_key=True, default=uuid6.uuid7)
    ti_id: Mapped[UUID] = mapped_column(Uuid(), nullable=False)
    webhook: Mapped[str] = mapped_column(StringID(length=64), nullable=False)
    # none_as_null: plain JSON writes an explicit None as the JSON text 'null', which IS NULL misses.
    events: Mapped[list[str] | None] = mapped_column(JSON(none_as_null=True), nullable=True)
    match: Mapped[dict[str, Any]] = mapped_column(JSON(), nullable=False)
    key_digest: Mapped[str] = mapped_column(String(64), nullable=False)
    match_digest: Mapped[str] = mapped_column(String(64), nullable=False)
    # True while the wait can still be resumed, NULL afterwards; never False. Databases treat NULLs as
    # distinct, so the unique constraint below holds among open waits only.
    open: Mapped[bool | None] = mapped_column(Boolean, nullable=True, default=True)
    status: Mapped[str] = mapped_column(String(20), nullable=False, default=WaitStatus.REGISTERED.value)
    # The inbound_delivery row that resumed the wait. Not a foreign key: airflow db clean removes
    # deliveries on their own retention, which can end before the wait's.
    delivery_id: Mapped[UUID | None] = mapped_column(Uuid(), nullable=True)
    deadline: Mapped[datetime] = mapped_column(UtcDateTime, nullable=False)
    created_at: Mapped[datetime] = mapped_column(UtcDateTime, nullable=False, default=timezone.utcnow)
    ended_at: Mapped[datetime | None] = mapped_column(UtcDateTime, nullable=True)

    __table_args__ = (
        ForeignKeyConstraint(
            (ti_id,),
            ["task_instance.id"],
            name="inbound_wait_ti_fkey",
            ondelete="CASCADE",
            onupdate="CASCADE",
        ),
        # One key, one task: no two open waits on a webhook share their events and match.
        UniqueConstraint(webhook, match_digest, open, name="inbound_wait_webhook_match_digest_open_uq"),
        # Postgres does not index the columns of a foreign key.
        Index("idx_inbound_wait_ti_id", ti_id),
        Index("idx_inbound_wait_status_ti_id", status, ti_id),
        Index("idx_inbound_wait_deadline", deadline),
        # Every delivery looks up the open waits of its webhook by the key sets its attributes cover.
        Index("idx_inbound_wait_webhook_open_key_digest", webhook, open, key_digest),
    )
