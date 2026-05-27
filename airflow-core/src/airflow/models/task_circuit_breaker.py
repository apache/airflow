#
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

from datetime import datetime, timedelta
from typing import TYPE_CHECKING

from sqlalchemy import Boolean, Index, Integer, String, or_, update
from sqlalchemy.orm import Mapped, mapped_column

from airflow._shared.timezones import timezone
from airflow.models.base import Base, StringID
from airflow.utils.session import NEW_SESSION, provide_session
from airflow.utils.sqlalchemy import UtcDateTime

if TYPE_CHECKING:
    from sqlalchemy.engine import CursorResult
    from sqlalchemy.orm import Session


class TaskCircuitBreaker(Base):
    """
    Track per-task circuit breaker state.

    One row per (dag_id, task_id).  The circuit opens when failure_count
    reaches max_failures within the configured window.  Subsequent scheduled
    task instances are skipped until the circuit is reset — either manually
    via the REST API or automatically when reset_after elapses.
    """

    __tablename__ = "task_circuit_breaker"

    dag_id: Mapped[str] = mapped_column(StringID(), primary_key=True)
    task_id: Mapped[str] = mapped_column(StringID(), primary_key=True)

    is_open: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    opened_at: Mapped[datetime | None] = mapped_column(UtcDateTime, nullable=True)
    opened_reason: Mapped[str | None] = mapped_column(String(500), nullable=True)

    failure_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    window_start: Mapped[datetime | None] = mapped_column(UtcDateTime, nullable=True)

    # Scheduled auto-reset time; NULL means manual-only reset.
    reset_after: Mapped[datetime | None] = mapped_column(UtcDateTime, nullable=True)

    # Config snapshot from the operator definition.
    max_failures: Mapped[int] = mapped_column(Integer, nullable=False)
    window_seconds: Mapped[int] = mapped_column(Integer, nullable=False)

    __table_args__ = (Index("idx_tcb_open", is_open),)

    @classmethod
    @provide_session
    def record_failure(
        cls,
        *,
        dag_id: str,
        task_id: str,
        max_failures: int,
        window: timedelta | None,
        reset_delay: timedelta | None,
        session: Session = NEW_SESSION,
    ) -> bool:
        """
        Record one failure for (dag_id, task_id).

        Returns True if the circuit was opened by this call.
        """
        now = timezone.utcnow()
        if isinstance(window, timedelta):
            window_seconds = int(window.total_seconds())
        elif isinstance(window, (int, float)):
            window_seconds = int(window)
        else:
            window_seconds = 3600

        cb = session.get(cls, (dag_id, task_id))
        if cb is None:
            cb = cls(
                dag_id=dag_id,
                task_id=task_id,
                is_open=False,
                failure_count=0,
                max_failures=max_failures,
                window_seconds=window_seconds,
            )
            session.add(cb)

        cb.max_failures = max_failures
        cb.window_seconds = window_seconds

        # Reset window if previous window has expired.
        if cb.window_start is None or (now - cb.window_start).total_seconds() > window_seconds:
            cb.window_start = now
            cb.failure_count = 1
        else:
            cb.failure_count += 1

        if not cb.is_open and cb.failure_count >= max_failures:
            cb.is_open = True
            cb.opened_at = now
            cb.opened_reason = f"Opened after {cb.failure_count} failures in {window_seconds}s window"
            if isinstance(reset_delay, (int, float)):
                reset_delay = timedelta(seconds=reset_delay)
            cb.reset_after = (now + reset_delay) if reset_delay else None
            return True

        return False

    def reset(self) -> None:
        """Close the circuit and clear failure state."""
        self.is_open = False
        self.opened_at = None
        self.opened_reason = None
        self.failure_count = 0
        self.window_start = None
        self.reset_after = None

    @classmethod
    @provide_session
    def reset_expired(cls, session: Session = NEW_SESSION) -> int:
        """Close all circuits whose reset_after has elapsed. Returns count closed."""
        now = timezone.utcnow()
        result: CursorResult = session.execute(  # type: ignore[assignment]
            update(cls)
            .where(cls.is_open == True)  # noqa: E712
            .where(cls.reset_after != None)  # noqa: E711
            .where(cls.reset_after <= now)
            .values(
                is_open=False,
                opened_at=None,
                opened_reason=None,
                failure_count=0,
                window_start=None,
                reset_after=None,
            )
        )
        return result.rowcount

    @classmethod
    def open_circuits_subquery(cls):
        """Return a subquery of (dag_id, task_id) pairs with open circuits."""
        from sqlalchemy import select

        now = timezone.utcnow()
        return (
            select(cls.dag_id, cls.task_id)
            .where(cls.is_open == True)  # noqa: E712
            .where(
                or_(
                    cls.reset_after == None,  # noqa: E711
                    cls.reset_after > now,
                )
            )
            .subquery()
        )
