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
Register a task instance's webhook wait, and let it claim a delivery that arrived before it.

A registration holds its task instance's row lock while it writes, and takes waits' rows before the
delivery it claims, in the order a delivery that resumes waits takes its locks: task instances, waits,
delivery. It also ends the open wait of an earlier attempt on its key without that attempt's task
instance lock; a delivery locks the task instances of all its waits before it takes any wait's row, so
the two never wait for each other in a cycle.

.. note:: |experimental|
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any, cast

from sqlalchemy import select, update
from sqlalchemy.exc import IntegrityError

from airflow._shared.timezones import timezone
from airflow.models.inbound_delivery import DeliveryOutcome, InboundDelivery
from airflow.models.inbound_wait import InboundWait, WaitStatus
from airflow.models.log import Log
from airflow.models.taskinstance import TaskInstance
from airflow.utils.state import TaskInstanceState
from airflow.webhooks.waits import compute_key_digest, compute_match_digest, evaluate

if TYPE_CHECKING:
    from collections.abc import Sequence
    from datetime import datetime
    from uuid import UUID

    from sqlalchemy import CursorResult, Row
    from sqlalchemy.orm import Session

#: The states in which a task instance can still be resumed by its wait: running, or parked.
LIVE_STATES = frozenset({TaskInstanceState.RUNNING, TaskInstanceState.AWAITING_INPUT})
#: The attribute in which source types give a delivery's event, which a wait's ``events`` filters on.
EVENT_ATTRIBUTE = "event"


class WaitRegistrationRefused(Exception):
    """The task instance cannot register a webhook wait."""


class TaskInstanceNotFound(WaitRegistrationRefused):
    """No task instance has the id."""


class TaskInstanceArchived(WaitRegistrationRefused):
    """The task instance is an archived attempt: its task was cleared or retried since."""


class TaskInstanceNotRunning(WaitRegistrationRefused):
    """
    The task instance is not running, so it is not about to park.

    :param state: The task instance's state.
    """

    def __init__(self, state: str | None) -> None:
        super().__init__(f"The task instance is {state}, not running")
        self.state = state


class WaitKeyTaken(WaitRegistrationRefused):
    """A running or parked task instance has an open wait on the webhook with the same events and match."""


def build_wait_action(wait_id: UUID, ti: TaskInstance) -> dict[str, Any]:
    """Return what a delivery's ``actions["waits"]`` records of a wait that the delivery went to."""
    return {
        "wait_id": str(wait_id),
        "ti_id": str(ti.id),
        "dag_id": ti.dag_id,
        "run_id": ti.run_id,
        "task_id": ti.task_id,
        "map_index": ti.map_index,
    }


def build_wait_audit_log(webhook: str, wait_id: UUID, ti: TaskInstance, *, delivery_id: str | None) -> Log:
    """
    Return the audit log entry of a delivery that went to a wait, which resumes its task now or once it parks.

    :param delivery_id: The sender's id for the delivery, or None.
    """
    return Log(
        event="webhook_delivery",
        task_instance=ti,
        owner=f"webhook:{webhook}",
        owner_display_name=f"Webhook {webhook}",
        extra=json.dumps(
            {"delivery_id": delivery_id, "outcome": DeliveryOutcome.RESUMED_WAIT, "wait_id": str(wait_id)}
        ),
        team_name=None,
    )


def claim_early_delivery(
    record_id: UUID, wait_actions: Sequence[dict[str, Any]], *, cutoff: datetime, session: Session
) -> bool:
    """
    Claim an early delivery received after ``cutoff`` for the waits that ``wait_actions`` record.

    The claim is a compare-and-set on the delivery's outcome, so each early delivery is claimed once,
    however many registrations and deliveries look at it. The waits are added to the delivery's actions
    once the claim holds its row, so that the skipped asset routes they list are kept.

    :return: Whether the delivery was claimed.
    """
    claimed = session.execute(
        update(InboundDelivery)
        .where(
            InboundDelivery.id == record_id,
            InboundDelivery.outcome == DeliveryOutcome.EARLY,
            InboundDelivery.received_at > cutoff,
        )
        .values(outcome=DeliveryOutcome.RESUMED_WAIT)
        .execution_options(synchronize_session=False)
    )
    if cast("CursorResult", claimed).rowcount != 1:
        return False
    actions = session.scalar(select(InboundDelivery.actions).where(InboundDelivery.id == record_id)) or {}
    session.execute(
        update(InboundDelivery)
        .where(InboundDelivery.id == record_id)
        .values(actions={**actions, "waits": list(wait_actions)})
        .execution_options(synchronize_session=False)
    )
    return True


def lock_registering_task_instance(ti_id: UUID, *, session: Session) -> TaskInstance:
    """
    Lock the task instance that registers a wait, and return it.

    :raises TaskInstanceNotFound: if there is no such task instance.
    :raises TaskInstanceArchived: if it is an archived attempt.
    :raises TaskInstanceNotRunning: if it is not running.
    """
    ti = session.scalar(
        select(TaskInstance)
        .where(TaskInstance.id == ti_id)
        .with_for_update(of=TaskInstance)
        .execution_options(include_all_attempts=True, populate_existing=True)
    )
    if ti is None:
        raise TaskInstanceNotFound("Task Instance not found")
    if ti.working_set is not True:
        raise TaskInstanceArchived(
            "Task Instance not found in the working set; its attempt has been archived"
        )
    if ti.state != TaskInstanceState.RUNNING:
        raise TaskInstanceNotRunning(ti.state)
    return ti


def _find_open_wait_on_key(
    webhook: str, match_digest: str, *, session: Session
) -> Row[tuple[UUID, str, UUID | None, bool | None, str | None]] | None:
    """Return the open wait on a key, if any, with its task instance's working set and state."""
    return session.execute(
        select(
            InboundWait.id,
            InboundWait.status,
            InboundWait.delivery_id,
            TaskInstance.working_set,
            TaskInstance.state,
        )
        .join(TaskInstance, TaskInstance.id == InboundWait.ti_id)
        .where(
            InboundWait.webhook == webhook,
            InboundWait.match_digest == match_digest,
            InboundWait.open.is_(True),
        )
        .execution_options(include_all_attempts=True)
    ).one_or_none()


def _end_wait_of_attempt_no_longer_live(
    webhook: str, match_digest: str, *, now: datetime, session: Session
) -> None:
    """
    End the open wait on a new wait's key, unless its task instance can still be resumed by it.

    The holder is read without a lock, so refusing a live holder's key takes none: an attempt that is no
    longer running or parked never becomes so again, since a new try is a new attempt.

    The delivery that the ended wait had received never resumed its task, so it is taken back from the
    wait. Only the transaction that ends the wait takes it back, so a delivery claimed again since is
    never taken back twice.

    :raises WaitKeyTaken: if a running or parked task instance holds the key.
    """
    holder = _find_open_wait_on_key(webhook, match_digest, session=session)
    if holder is None:
        return
    if holder.working_set is True and holder.state in LIVE_STATES:
        raise WaitKeyTaken(
            f"Another task instance has an open wait on webhook {webhook!r} with the same events and match"
        )
    ended = session.execute(
        update(InboundWait)
        .where(InboundWait.id == holder.id, InboundWait.open.is_(True))
        .values(open=None, status=WaitStatus.ENDED, ended_at=now)
        .execution_options(synchronize_session=False)
    )
    if cast("CursorResult", ended).rowcount != 1:
        return
    if holder.status == WaitStatus.RECEIVED and holder.delivery_id is not None:
        _take_back_delivery(holder.delivery_id, holder.id, session=session)


def _take_back_delivery(record_id: UUID, wait_id: UUID, *, session: Session) -> None:
    """
    Take an ended wait out of the actions of a delivery that it had received but never resumed with.

    A delivery that other waits hold stays theirs. One that no other wait holds gets the outcome it would
    have had if the wait had never existed: ``accepted`` if it recorded events, since such a delivery is
    never kept early, or else ``early`` again if it has a correlation key, so that a new wait can claim
    it. One that has neither keeps the wait, as its record says nothing of whether it would have been
    ``no_subscribers`` or ``dag_paused``.
    """
    record = session.execute(
        select(InboundDelivery.actions, InboundDelivery.correlation_key)
        .where(
            InboundDelivery.id == record_id,
            InboundDelivery.outcome == DeliveryOutcome.RESUMED_WAIT,
        )
        .with_for_update()
    ).one_or_none()
    if record is None:
        return
    actions = dict(record.actions or {})
    waits = [entry for entry in actions.pop("waits", []) if entry.get("wait_id") != str(wait_id)]
    values: dict[str, Any]
    if waits:
        values = {"actions": {**actions, "waits": waits}}
    elif actions.get("asset_events") or actions.get("asset_routes"):
        values = {"outcome": DeliveryOutcome.ACCEPTED, "actions": actions}
    elif record.correlation_key is not None:
        values = {"outcome": DeliveryOutcome.EARLY, "actions": actions or None}
    else:
        return
    session.execute(
        update(InboundDelivery)
        .where(InboundDelivery.id == record_id)
        .values(**values)
        .execution_options(synchronize_session=False)
    )


def _find_matching_early_deliveries(
    wait: InboundWait, *, cutoff: datetime, session: Session
) -> list[Row[tuple[UUID, str | None, dict[str, Any] | None]]]:
    """Return the wait's webhook's early deliveries received after ``cutoff`` that match it, oldest first."""
    candidates = session.execute(
        select(InboundDelivery.id, InboundDelivery.delivery_id, InboundDelivery.attributes)
        .where(
            InboundDelivery.webhook == wait.webhook,
            InboundDelivery.outcome == DeliveryOutcome.EARLY,
            InboundDelivery.received_at > cutoff,
        )
        .order_by(InboundDelivery.received_at, InboundDelivery.id)
    ).all()
    return [
        candidate
        for candidate in candidates
        if evaluate(
            events=wait.events,
            match=wait.match,
            event=(candidate.attributes or {}).get(EVENT_ATTRIBUTE),
            attributes=candidate.attributes,
        ).matched
    ]


def _claim_first(
    candidates: Sequence[Row[tuple[UUID, str | None, dict[str, Any] | None]]],
    wait: InboundWait,
    ti: TaskInstance,
    *,
    cutoff: datetime,
    session: Session,
) -> UUID | None:
    """Claim the first of ``candidates`` that is still early for ``wait``, and return its record's id."""
    for candidate in candidates:
        if claim_early_delivery(
            candidate.id, [build_wait_action(wait.id, ti)], cutoff=cutoff, session=session
        ):
            session.add(build_wait_audit_log(wait.webhook, wait.id, ti, delivery_id=candidate.delivery_id))
            return candidate.id
    return None


def register_wait(
    ti: TaskInstance,
    *,
    webhook: str,
    events: list[Any] | None,
    match: dict[str, Any] | None,
    deadline: datetime,
    early_delivery_cutoff: datetime,
    session: Session,
) -> InboundWait:
    """
    Record a wait for ``ti``, and let it claim the oldest early delivery that matches it.

    The caller holds ``ti``'s lock, from ``lock_registering_task_instance``.

    The new wait ends ``ti``'s open waits, so a second park in the same try never resumes with a delivery
    that matched the first. Within a webhook, an open wait's events and match belong to one task
    instance: another's open wait on the same key refuses the registration while it is running or
    parked, and is ended otherwise, as when an earlier try of the same task registered it.

    :param ti: The task instance registering the wait.
    :param webhook: The webhook's name.
    :param events: The events the wait accepts, or None for any; they follow the match grammar.
    :param match: The attributes the wait requires, or None; they follow the match grammar.
    :param deadline: When the wait times out.
    :param early_delivery_cutoff: Early deliveries received before this can no longer be claimed.
    :return: The wait, ``received`` with the claimed delivery's record id if it claimed one.
    :raises WaitKeyTaken: if a running or parked task instance holds the wait's key.
    """
    now = timezone.utcnow()
    match_digest = compute_match_digest(events, match)
    session.execute(
        update(InboundWait)
        .where(InboundWait.ti_id == ti.id, InboundWait.open.is_(True))
        .values(open=None, status=WaitStatus.ENDED, ended_at=now)
        .execution_options(synchronize_session=False)
    )
    _end_wait_of_attempt_no_longer_live(webhook, match_digest, now=now, session=session)
    wait = InboundWait(
        ti_id=ti.id,
        webhook=webhook,
        events=events,
        match=match or {},
        key_digest=compute_key_digest(match),
        match_digest=match_digest,
        deadline=deadline,
        created_at=now,
    )
    session.add(wait)
    try:
        session.flush()
    except IntegrityError as e:
        # A concurrent registration took the key after it was checked.
        raise WaitKeyTaken(
            f"Another task instance has an open wait on webhook {webhook!r} with the same events and match"
        ) from e
    candidates = _find_matching_early_deliveries(wait, cutoff=early_delivery_cutoff, session=session)
    if (
        record_id := _claim_first(candidates, wait, ti, cutoff=early_delivery_cutoff, session=session)
    ) is not None:
        wait.status = WaitStatus.RECEIVED
        wait.delivery_id = record_id
    return wait


def claim_early_delivery_after_commit(wait_id: UUID, *, cutoff: datetime, session: Session) -> UUID | None:
    """
    Look again, after a wait's registration committed, for an early delivery that it can claim.

    A delivery committed while the wait was registered saw no open wait and was kept early, and the
    registration did not see the delivery. Each side looks again after its own commit, and claims are
    compare-and-set, so the two are paired once, whichever looks last.

    The wait is first read without a lock, so that a wait no early delivery matches takes none.

    :return: The id of the claimed delivery's record, or None if the wait is no longer registered and
        open, its task instance no longer running, or no early delivery matches.
    """
    wait = session.get(InboundWait, wait_id)
    if wait is None or wait.open is not True or wait.status != WaitStatus.REGISTERED:
        return None
    candidates = _find_matching_early_deliveries(wait, cutoff=cutoff, session=session)
    if not candidates:
        return None
    try:
        ti = lock_registering_task_instance(wait.ti_id, session=session)
    except WaitRegistrationRefused:
        return None
    wait = session.scalar(
        select(InboundWait)
        .where(
            InboundWait.id == wait_id,
            InboundWait.open.is_(True),
            InboundWait.status == WaitStatus.REGISTERED,
        )
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    if wait is None:
        return None
    if (record_id := _claim_first(candidates, wait, ti, cutoff=cutoff, session=session)) is None:
        return None
    wait.status = WaitStatus.RECEIVED
    wait.delivery_id = record_id
    return record_id
