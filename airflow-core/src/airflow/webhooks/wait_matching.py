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
Hand a delivery to the open webhook waits it matches, and look after the deliveries kept early for waits.

A delivery locks the task instances of all the waits it matches, in id order and in one query, before it
claims or ends any of the waits, and takes a delivery record last. A registration and a park also lock a
task instance before its waits and a delivery record. A registration ends the open wait of an earlier
attempt on its key without that attempt's task instance lock, which cannot close a cycle with a delivery,
since a delivery never waits for a task instance while it holds a wait.

.. note:: |experimental|
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, cast

from sqlalchemy import func, select, true, update

from airflow._shared.timezones import timezone
from airflow.models.inbound_delivery import DeliveryOutcome, InboundDelivery
from airflow.models.inbound_wait import InboundWait, WaitStatus
from airflow.models.taskinstance import TaskInstance
from airflow.utils.sqlalchemy import with_row_locks
from airflow.utils.state import TaskInstanceState
from airflow.webhooks.wait_registration import (
    EVENT_ATTRIBUTE,
    LIVE_STATES,
    build_wait_action,
    build_wait_audit_log,
    claim_early_delivery,
)
from airflow.webhooks.wait_resumption import submit_resume_event
from airflow.webhooks.waits import compute_covered_key_digests, evaluate

if TYPE_CHECKING:
    from collections.abc import Collection, Mapping, Sequence
    from datetime import datetime
    from uuid import UUID

    from sqlalchemy import CursorResult, Row
    from sqlalchemy.orm import Session

    WaitCandidate = Row[tuple[UUID, UUID, list[str] | None, dict[str, Any]]]

#: A delivery with more attributes than this looks at every open wait of its webhook, rather than at
#: those whose match keys it has only: there are 2 ** n key digests for n attributes.
MAX_KEY_DIGEST_ATTRIBUTES = 8
#: The most early deliveries that one delivery marks expired, which bounds its write when
#: ``[webhooks] early_delivery_max_per_webhook`` was lowered.
MAX_EXPIRED_PER_DELIVERY = 100


class EarlyDeliveryClaimedMeanwhile(Exception):
    """An early delivery was claimed for a registering wait after the waits it matches took it."""


def find_matching_waits(
    webhook: str, attributes: Mapping[str, Any] | None, *, session: Session
) -> list[WaitCandidate]:
    """
    Return the registered open waits of ``webhook`` that a delivery with ``attributes`` matches.

    The waits are read without a lock, so each must be locked and claimed before it takes the delivery.
    A wait whose match names an attribute the delivery lacks cannot match it, so only the waits whose
    key digest is that of a set of the delivery's attribute names are evaluated.

    :return: The waits' ``id``, ``ti_id``, ``events`` and ``match``, in task instance order.
    """
    attributes = attributes or {}
    query = select(InboundWait.id, InboundWait.ti_id, InboundWait.events, InboundWait.match).where(
        InboundWait.webhook == webhook,
        # "open = true", not "open IS true", so that MySQL looks the waits up on all three columns of
        # idx_inbound_wait_webhook_open_key_digest rather than on the webhook alone.
        InboundWait.open == true(),
        InboundWait.status == WaitStatus.REGISTERED,
    )
    if len(attributes) <= MAX_KEY_DIGEST_ATTRIBUTES:
        query = query.where(InboundWait.key_digest.in_(compute_covered_key_digests(attributes)))
    event = attributes.get(EVENT_ATTRIBUTE)
    return [
        wait
        for wait in session.execute(query.order_by(InboundWait.ti_id, InboundWait.id))
        if evaluate(events=wait.events, match=wait.match, event=event, attributes=attributes).matched
    ]


def _claim_wait(wait_id: UUID, values: dict[str, Any], *, session: Session) -> bool:
    """
    Set ``values`` on a wait if it is still registered and open, and return whether it was.

    This compare-and-set, rather than a row lock, keeps a wait from taking two deliveries, or a delivery
    after its registration claimed an early one, since SQLite ignores row locks.
    """
    claimed = session.execute(
        update(InboundWait)
        .where(
            InboundWait.id == wait_id,
            InboundWait.open.is_(True),
            InboundWait.status == WaitStatus.REGISTERED,
        )
        .values(**values)
        .execution_options(synchronize_session=False)
    )
    return cast("CursorResult", claimed).rowcount == 1


def _lock_task_instances(ti_ids: Collection[UUID], *, session: Session) -> dict[UUID, TaskInstance]:
    """Lock task instances in id order, archived attempts too, and return them by id."""
    query = (
        select(TaskInstance)
        .where(TaskInstance.id.in_(ti_ids))
        .order_by(TaskInstance.id)
        .with_for_update(of=TaskInstance)
        .execution_options(include_all_attempts=True, populate_existing=True)
    )
    return {ti.id: ti for ti in session.scalars(query)}


def _hand_over(
    candidate: WaitCandidate, ti: TaskInstance, record: InboundDelivery, *, session: Session
) -> dict[str, Any] | None:
    """
    Hand a delivery to a candidate wait if the wait can still take it, or end the wait if its task cannot.

    The caller holds the lock of the wait's task instance, ``ti``, and the wait is claimed now. A parked
    task resumes at once; a running task's wait is marked ``received``, and the park-time check resumes
    the task once it parks. A wait whose task instance is no longer in the working set and running or
    parked is ended: its attempt was archived or has finished, and never resumes again.

    :return: What the delivery's ``actions["waits"]`` records of the wait, or None if it did not take it.
    """
    now = timezone.utcnow()
    if ti.working_set is not True or ti.state not in LIVE_STATES:
        _claim_wait(
            candidate.id, {"open": None, "status": WaitStatus.ENDED, "ended_at": now}, session=session
        )
        return None
    parked = ti.state == TaskInstanceState.AWAITING_INPUT
    values: dict[str, Any] = (
        {"open": None, "status": WaitStatus.RESUMED, "ended_at": now, "delivery_id": record.id}
        if parked
        else {"status": WaitStatus.RECEIVED, "delivery_id": record.id}
    )
    if not _claim_wait(candidate.id, values, session=session):
        return None
    if parked:
        submit_resume_event(ti, webhook=record.webhook, delivery=record, session=session)
    session.add(build_wait_audit_log(record.webhook, candidate.id, ti, delivery_id=record.delivery_id))
    return build_wait_action(candidate.id, ti)


def hand_to_waits(
    candidates: Sequence[WaitCandidate], record: InboundDelivery, *, session: Session
) -> list[dict[str, Any]]:
    """
    Hand a delivery to each of ``candidates`` that can still take it, and end those whose task cannot.

    The task instances of all the candidates are locked before any wait is claimed or ended, so that the
    delivery never waits for a task instance's lock while it holds a wait's row.

    :param candidates: The waits the delivery matches, from ``find_matching_waits``.
    :param record: The delivery's record, with the id, webhook, received time, attributes and payload it
        is, or is about to be, stored with. It does not have to be in the session.
    :return: What the delivery's ``actions["waits"]`` records of the waits that took it, possibly none.
    """
    if not candidates:
        return []
    tis = _lock_task_instances({candidate.ti_id for candidate in candidates}, session=session)
    return [
        action
        for candidate in candidates
        if (ti := tis.get(candidate.ti_id)) is not None
        and (action := _hand_over(candidate, ti, record, session=session)) is not None
    ]


def expire_oldest_early_deliveries(
    webhook: str, *, cutoff: datetime, max_per_webhook: int, session: Session
) -> None:
    """
    Make room for one more early delivery to ``webhook`` if it keeps ``max_per_webhook`` of them.

    Its oldest early deliveries received after ``cutoff`` are marked ``expired`` and their payloads
    dropped, so that no wait can claim them: as many as leave one fewer than ``max_per_webhook``, which
    also expires the excess that concurrent deliveries leave when each counts before the others commit.
    Rows that another transaction holds are skipped, so that concurrent deliveries expire different
    rows, and the update is a compare-and-set on the outcome, as a claim is, so that a delivery a wait
    claims meanwhile stays claimed.
    """
    live_early = (
        InboundDelivery.webhook == webhook,
        InboundDelivery.outcome == DeliveryOutcome.EARLY,
        InboundDelivery.received_at > cutoff,
    )
    count = session.execute(select(func.count()).select_from(InboundDelivery).where(*live_early)).scalar_one()
    if (excess := count - max_per_webhook + 1) <= 0:
        return
    oldest = list(
        session.scalars(
            with_row_locks(
                select(InboundDelivery.id)
                .where(*live_early)
                .order_by(InboundDelivery.received_at, InboundDelivery.id)
                .limit(min(excess, MAX_EXPIRED_PER_DELIVERY)),
                session=session,
                skip_locked=True,
                key_share=False,
            )
        )
    )
    if not oldest:
        return
    session.execute(
        update(InboundDelivery)
        .where(InboundDelivery.id.in_(oldest), InboundDelivery.outcome == DeliveryOutcome.EARLY)
        .values(outcome=DeliveryOutcome.EXPIRED, payload=None)
        .execution_options(synchronize_session=False)
    )


def hand_to_waits_after_commit(record_id: UUID, *, cutoff: datetime, session: Session) -> bool:
    """
    Look again, after an early delivery committed, for open waits that it matches.

    A wait registered while the delivery was recorded was not seen by the delivery, and its registration
    did not see the delivery. Each side looks again after its own commit, and claims are compare-and-set,
    so the two are paired once, whichever looks last. The waits take the delivery before its record is
    claimed, so that the locks are taken in the order a registration takes them: task instances, waits,
    delivery.

    :param cutoff: Early deliveries received before this can no longer be claimed.
    :return: Whether waits took the delivery; False if none could, or if the delivery is no longer early
        and received after ``cutoff``, as when the wait's registration claimed it first.
    :raises EarlyDeliveryClaimedMeanwhile: if a registering wait claimed the delivery after the waits took
        it; rolling back undoes what they recorded.
    """
    record = session.get(InboundDelivery, record_id)
    if record is None or record.outcome != DeliveryOutcome.EARLY or record.received_at <= cutoff:
        return False
    candidates = find_matching_waits(record.webhook, record.attributes, session=session)
    if not (actions := hand_to_waits(candidates, record, session=session)):
        return False
    if not claim_early_delivery(record_id, actions, cutoff=cutoff, session=session):
        raise EarlyDeliveryClaimedMeanwhile
    return True
