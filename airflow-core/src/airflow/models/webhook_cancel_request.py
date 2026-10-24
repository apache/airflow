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
from sqlalchemy import JSON, ForeignKeyConstraint, Index, String, UniqueConstraint, Uuid
from sqlalchemy.orm import Mapped, mapped_column

from airflow.models.base import Base, StringID
from airflow.models.inbound_delivery import MAX_CORRELATION_KEY_LENGTH, MAX_DELIVERY_ID_LENGTH
from airflow.utils.sqlalchemy import UtcDateTime


class CancelRequestStatus(str, Enum):
    """Where a cancel request is, as ``WebhookCancelRequest.status`` stores it."""

    #: Recorded by a delivery; the scheduler has not applied it yet.
    PENDING = "pending"
    #: The scheduler applied it to the runs about its correlation key, if any; ``result`` says what it
    #: changed.
    APPLIED = "applied"
    #: Applying it failed, and the scheduler logged the error; ``result`` names the error.
    FAILED = "failed"

    def __str__(self) -> str:
        return self.value


class WebhookCancelRequest(Base):
    """
    A request, recorded by a webhook delivery, that the scheduler cancel a Dag's runs or tasks about a key.

    ``targets`` is ``{"run": {"correlation_key_confs": [...]} | None, "tasks": [{"task_id", "cancel_state",
    "correlation_key_confs"}]}``, with both keys always present: the whole run if a Dag-level condition
    matched, and each task whose condition matched. Each lists the conf keys its conditions name, by which
    a run triggered with a conf is about the key.

    .. note:: |experimental|
    """

    __tablename__ = "webhook_cancel_request"

    id: Mapped[UUID] = mapped_column(Uuid(), primary_key=True, default=uuid6.uuid7)
    # The inbound_delivery row. Not a foreign key, as for inbound_wait.delivery_id: airflow db clean ages
    # each table on its own column.
    inbound_delivery_id: Mapped[UUID] = mapped_column(Uuid(), nullable=False)
    # The sender's id for the delivery, as inbound_delivery.delivery_id stores it.
    delivery_id: Mapped[str | None] = mapped_column(StringID(length=MAX_DELIVERY_ID_LENGTH), nullable=True)
    webhook: Mapped[str] = mapped_column(StringID(length=64), nullable=False)
    correlation_key: Mapped[str] = mapped_column(String(MAX_CORRELATION_KEY_LENGTH), nullable=False)
    # When the delivery was first received, also for a request that a redelivery recorded.
    received_at: Mapped[datetime] = mapped_column(UtcDateTime, nullable=False)
    dag_id: Mapped[str] = mapped_column(StringID(), nullable=False)
    targets: Mapped[dict[str, Any]] = mapped_column(JSON(), nullable=False)
    status: Mapped[str] = mapped_column(String(16), nullable=False, default=CancelRequestStatus.PENDING.value)
    applied_at: Mapped[datetime | None] = mapped_column(UtcDateTime, nullable=True)
    # none_as_null: plain JSON writes an explicit None as the JSON text 'null', which IS NULL misses.
    result: Mapped[dict[str, Any] | None] = mapped_column(JSON(none_as_null=True), nullable=True)
    # Kept until then whatever its status, so that a run created later from an older event finds it.
    retain_until: Mapped[datetime] = mapped_column(UtcDateTime, nullable=False)

    __table_args__ = (
        ForeignKeyConstraint(
            (dag_id,),
            ["dag.dag_id"],
            name="webhook_cancel_request_dag_id_fkey",
            ondelete="CASCADE",
        ),
        # One request per Dag and delivery, so that a delivery processed again never requests twice.
        UniqueConstraint(
            inbound_delivery_id, dag_id, name="webhook_cancel_request_inbound_delivery_id_dag_id_uq"
        ),
        # The scheduler applies pending requests in the order they were received.
        Index("idx_webhook_cancel_request_status_received_at", status, received_at),
        # A run created later looks up the requests about its key; it also indexes the foreign key on
        # MySQL.
        Index("idx_webhook_cancel_request_dag_id_correlation_key", dag_id, correlation_key),
        Index("idx_webhook_cancel_request_retain_until", retain_until),
    )
