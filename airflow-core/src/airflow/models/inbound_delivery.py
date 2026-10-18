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
from sqlalchemy import JSON, Index, String, UniqueConstraint, Uuid
from sqlalchemy.orm import Mapped, mapped_column

from airflow.models.base import Base, StringID
from airflow.utils.sqlalchemy import UtcDateTime

MAX_DELIVERY_ID_LENGTH = 255
MAX_CORRELATION_KEY_LENGTH = 255


class DeliveryOutcome(str, Enum):
    """What Airflow did with a recorded delivery, as ``InboundDelivery.outcome`` stores it."""

    #: No Dag subscribes to the delivery, so nothing acted on it.
    NO_SUBSCRIBERS = "no_subscribers"

    def __str__(self) -> str:
        return self.value


class InboundDelivery(Base):
    """A verified delivery received by an inbound webhook, and what Airflow did with it."""

    __tablename__ = "inbound_delivery"

    id: Mapped[UUID] = mapped_column(Uuid(), primary_key=True, default=uuid6.uuid7)
    # Lets a later ingress kind share the table; every row written by inbound webhooks is "webhook".
    source_kind: Mapped[str] = mapped_column(String(16), nullable=False, default="webhook")
    webhook: Mapped[str] = mapped_column(StringID(length=64), nullable=False)
    delivery_id: Mapped[str | None] = mapped_column(StringID(length=MAX_DELIVERY_ID_LENGTH), nullable=True)
    body_digest: Mapped[str] = mapped_column(String(64), nullable=False)
    received_at: Mapped[datetime] = mapped_column(UtcDateTime, nullable=False)
    # ``airflow db clean`` ages rows out on this rather than on received_at, so a clean with any cutoff
    # keeps each row for the retention that was configured when it was received.
    retain_until: Mapped[datetime] = mapped_column(UtcDateTime, nullable=False)
    source_type: Mapped[str] = mapped_column(String(64), nullable=False)
    # none_as_null: plain JSON writes an explicit None as the JSON text 'null', which IS NULL misses.
    attributes: Mapped[dict[str, Any] | None] = mapped_column(JSON(none_as_null=True), nullable=True)
    candidates: Mapped[list[str] | None] = mapped_column(JSON(none_as_null=True), nullable=True)
    payload: Mapped[dict[str, Any] | None] = mapped_column(JSON(none_as_null=True), nullable=True)
    correlation_key: Mapped[str | None] = mapped_column(String(MAX_CORRELATION_KEY_LENGTH), nullable=True)
    outcome: Mapped[str] = mapped_column(String(32), nullable=False)
    actions: Mapped[dict[str, Any] | None] = mapped_column(JSON(none_as_null=True), nullable=True)
    # Not a foreign key to team.name, as for Log.team_name: deleting a team must not rewrite or
    # delete the record of what its webhooks received.
    team_name: Mapped[str | None] = mapped_column(String(50), nullable=True)

    __table_args__ = (
        # Recognises a sender's retry. Databases treat NULLs as distinct, so deliveries without a
        # delivery_id never collide with each other.
        UniqueConstraint(
            webhook,
            delivery_id,
            body_digest,
            name="inbound_delivery_webhook_delivery_id_body_digest_uq",
        ),
        Index("idx_inbound_delivery_webhook_received_at", webhook, received_at),
        Index("idx_inbound_delivery_retain_until", retain_until),
        Index("idx_inbound_delivery_correlation_key", correlation_key),
    )
