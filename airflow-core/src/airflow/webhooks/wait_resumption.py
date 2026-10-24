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
"""
Resume a task instance parked on its webhook wait: with a delivery that matches the wait, or on timeout.

A task resumes when it parks with a delivery its wait received while it was still running, when a
delivery that matches its wait arrives while it is parked, or when the timeout sweep finds it past its
wait's deadline. The callers hold the task instance's row lock before they take its wait's, in the order
a registration and a delivery take them. Nothing here commits, and nothing raises when a row is missing,
since the park-time check fails the task on any exception.

.. note:: |experimental|
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import select, update

from airflow._shared.timezones import timezone
from airflow.models.inbound_delivery import InboundDelivery
from airflow.models.inbound_wait import InboundWait, WaitStatus
from airflow.models.trigger import handle_event_submit
from airflow.triggers.base import TriggerEvent
from airflow.utils.sqlalchemy import with_row_locks
from airflow.webhooks.wait_registration import EVENT_ATTRIBUTE
from airflow.webhooks.waits import build_resume_event, build_timeout_event

if TYPE_CHECKING:
    from collections.abc import Collection
    from uuid import UUID

    from sqlalchemy.orm import Session

    from airflow.models.taskinstance import TaskInstance


def lock_current_waits(ti_ids: Collection[UUID], *, session: Session) -> dict[UUID, InboundWait]:
    """
    Lock the open waits of task instances in one query, and return each one's current wait.

    A registration ends its task instance's earlier open waits, so a task instance has at most one open
    wait; should it have more, the latest is its current wait, so that the lookup never fails.

    :return: The current waits by task instance id; a task instance without an open wait is left out.
    """
    query = (
        select(InboundWait)
        .where(InboundWait.ti_id.in_(ti_ids), InboundWait.open.is_(True))
        .order_by(InboundWait.created_at, InboundWait.id)
        .execution_options(populate_existing=True)
    )
    return {
        wait.ti_id: wait for wait in session.scalars(with_row_locks(query, session=session, key_share=False))
    }


def lock_current_wait(ti_id: UUID, *, session: Session) -> InboundWait | None:
    """
    Lock a task instance's open wait, and return its current wait, or None if it has no open wait.

    See ``lock_current_waits``.
    """
    return lock_current_waits((ti_id,), session=session).get(ti_id)


def end_open_waits(ti_ids: Collection[UUID], *, session: Session) -> None:
    """
    Mark the open waits of task instances ``ended``, as their task instances no longer run or park.

    The caller holds the task instances' row locks, so no delivery or registration changes the waits
    meanwhile.
    """
    if not ti_ids:
        return
    session.execute(
        update(InboundWait)
        .where(InboundWait.ti_id.in_(ti_ids), InboundWait.open.is_(True))
        .values(open=None, status=WaitStatus.ENDED, ended_at=timezone.utcnow())
        .execution_options(synchronize_session=False)
    )


def find_received_delivery(wait: InboundWait, *, session: Session) -> InboundDelivery | None:
    """
    Return the delivery a ``received`` wait holds.

    :return: The delivery's record, or None if the wait has not received one, or if ``airflow db clean``
        has removed the record since.
    """
    if wait.status != WaitStatus.RECEIVED or wait.delivery_id is None:
        return None
    return session.get(InboundDelivery, wait.delivery_id)


def _close_wait(wait: InboundWait, status: WaitStatus) -> None:
    wait.open = None
    wait.status = status
    wait.ended_at = timezone.utcnow()


def submit_resume_event(
    ti: TaskInstance, *, webhook: str, delivery: InboundDelivery, session: Session
) -> None:
    """
    Schedule ``ti`` to run its ``next_method`` with the resume event of ``delivery`` to ``webhook``.

    ``handle_event_submit`` routes ``ti`` to fail instead if its stored ``next_kwargs`` cannot be read. The
    caller holds the lock of ``ti`` and closes its wait.
    """
    resume_event = build_resume_event(
        webhook=webhook,
        event=(delivery.attributes or {}).get(EVENT_ATTRIBUTE),
        delivery_id=delivery.delivery_id,
        received_at=delivery.received_at,
        attributes=delivery.attributes,
        payload=delivery.payload,
    )
    handle_event_submit(TriggerEvent(resume_event), task_instance=ti, session=session)


def resume_wait(ti: TaskInstance, wait: InboundWait, delivery: InboundDelivery, *, session: Session) -> None:
    """
    Resume ``ti`` with ``delivery``, the delivery that matched its wait, and mark the wait ``resumed``.

    The caller holds the locks of ``ti`` and then ``wait``.
    """
    _close_wait(wait, WaitStatus.RESUMED)
    submit_resume_event(ti, webhook=wait.webhook, delivery=delivery, session=session)


def time_out_wait(ti: TaskInstance, wait: InboundWait, *, session: Session) -> None:
    """
    Resume ``ti`` with the timeout event of its wait, and mark the wait ``timed_out``.

    The caller holds the locks of ``ti`` and then ``wait``.
    """
    _close_wait(wait, WaitStatus.TIMED_OUT)
    handle_event_submit(TriggerEvent(build_timeout_event(wait.deadline)), task_instance=ti, session=session)
