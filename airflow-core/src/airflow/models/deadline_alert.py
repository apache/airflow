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
from typing import TYPE_CHECKING
from uuid import UUID

import uuid6
from sqlalchemy import JSON, Float, ForeignKey, String, Text, Uuid, select
from sqlalchemy.exc import NoResultFound
from sqlalchemy.orm import Mapped, mapped_column

from airflow._shared.timezones import timezone
from airflow.models import Base
from airflow.serialization.definitions.deadline import SerializedReferenceModels
from airflow.utils.session import NEW_SESSION, provide_session
from airflow.utils.sqlalchemy import UtcDateTime

if TYPE_CHECKING:
    from sqlalchemy.orm import Session


class DeadlineAlert(Base):
    """Table containing DeadlineAlert properties."""

    __tablename__ = "deadline_alert"

    id: Mapped[UUID] = mapped_column(Uuid(), primary_key=True, default=uuid6.uuid7)
    created_at: Mapped[datetime] = mapped_column(UtcDateTime, nullable=False, default=timezone.utcnow)

    serialized_dag_id: Mapped[UUID] = mapped_column(
        Uuid(), ForeignKey("serialized_dag.id", ondelete="CASCADE"), nullable=False
    )

    name: Mapped[str | None] = mapped_column(String(250), nullable=True)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    reference: Mapped[dict] = mapped_column(JSON, nullable=False)
    interval: Mapped[float] = mapped_column(Float, nullable=False)
    callback_def: Mapped[dict] = mapped_column(JSON, nullable=False)

    def __repr__(self):
        interval_seconds = int(self.interval)

        if interval_seconds >= 3600:
            interval_display = f"{interval_seconds // 3600}h"
        elif interval_seconds >= 60:
            interval_display = f"{interval_seconds // 60}m"
        else:
            interval_display = f"{interval_seconds}s"

        return (
            f"[DeadlineAlert] "
            f"id={str(self.id)[:8]}, "
            f"created_at={self.created_at}, "
            f"name={self.name or 'Unnamed'}, "
            f"reference={self.reference}, "
            f"interval={interval_display}, "
            f"callback={self.callback_def}"
        )

    def __eq__(self, other):
        if not isinstance(other, DeadlineAlert):
            return False
        return (
            self.reference == other.reference
            and self.interval == other.interval
            and self.callback_def == other.callback_def
        )

    def __hash__(self):
        return hash((str(self.reference), self.interval, str(self.callback_def)))

    @property
    def reference_class(self) -> type[SerializedReferenceModels.SerializedBaseDeadlineReference]:
        """Return the deserialized reference class."""
        return SerializedReferenceModels.get_reference_class(
            self.reference[SerializedReferenceModels.REFERENCE_TYPE_FIELD]
        )

    @classmethod
    @provide_session
    def get_by_id(cls, deadline_alert_id: str | UUID, session: Session = NEW_SESSION) -> DeadlineAlert:
        """
        Retrieve a DeadlineAlert record by its UUID.

        :param deadline_alert_id: The UUID of the DeadlineAlert to retrieve (as string or UUID object)
        :param session: Database session
        """
        # Convert string to UUID if needed
        if isinstance(deadline_alert_id, str):
            deadline_alert_id = UUID(deadline_alert_id)
        result = session.scalar(select(cls).where(cls.id == deadline_alert_id))
        if result is None:
            raise NoResultFound(f"No DeadlineAlert found with id {deadline_alert_id}")
        return result
