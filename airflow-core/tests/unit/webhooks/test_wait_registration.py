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

from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any
from unittest import mock

import pytest
from sqlalchemy import func, select, update

from airflow.models.inbound_delivery import DeliveryOutcome, InboundDelivery
from airflow.models.inbound_wait import InboundWait, WaitStatus
from airflow.models.log import Log
from airflow.models.taskinstance import TaskInstance
from airflow.utils.state import TaskInstanceState
from airflow.webhooks import wait_registration
from airflow.webhooks.wait_registration import WaitKeyTaken, claim_early_delivery_after_commit, register_wait
from airflow.webhooks.waits import compute_key_digest, compute_match_digest

from tests_common.test_utils.db import clear_db_inbound_deliveries, clear_db_logs, clear_db_runs

if TYPE_CHECKING:
    from uuid import UUID

    from sqlalchemy.orm import Session

pytestmark = pytest.mark.db_test

NOW = datetime(2026, 10, 20, 12, 0, tzinfo=UTC)
CUTOFF = NOW - timedelta(minutes=10)
EVENTS = ["pull_request"]
MATCH = {"number": 42}
LOCK_REGISTERING_TASK_INSTANCE = wait_registration.lock_registering_task_instance
FIND_MATCHING_EARLY_DELIVERIES = wait_registration._find_matching_early_deliveries
FIND_OPEN_WAIT_ON_KEY = wait_registration._find_open_wait_on_key
#: The actions of a delivery that another wait claimed.
OTHER_WAIT_ACTIONS = {"waits": [{"wait_id": "another-wait"}]}


@pytest.fixture(autouse=True)
def clean_db():
    clear_db_runs()
    clear_db_inbound_deliveries()
    clear_db_logs()
    yield
    clear_db_runs()
    clear_db_inbound_deliveries()
    clear_db_logs()


def add_wait(session: Session, ti_id: UUID, **overrides: Any) -> InboundWait:
    wait = InboundWait(
        ti_id=ti_id,
        webhook="fake",
        events=EVENTS,
        match=MATCH,
        key_digest=compute_key_digest(MATCH),
        match_digest=compute_match_digest(EVENTS, MATCH),
        deadline=NOW + timedelta(hours=1),
        **overrides,
    )
    session.add(wait)
    session.commit()
    return wait


def add_early_delivery(session: Session) -> UUID:
    record = InboundDelivery(
        webhook="fake",
        delivery_id="early-1",
        body_digest="0" * 64,
        received_at=NOW - timedelta(minutes=1),
        retain_until=NOW + timedelta(days=7),
        source_type="fake",
        attributes={"event": "pull_request", "number": 42},
        correlation_key="my-org/configs#42",
        outcome=DeliveryOutcome.EARLY,
    )
    session.add(record)
    session.commit()
    return record.id


@pytest.fixture
def running_ti(create_task_instance, session: Session) -> TaskInstance:
    ti = create_task_instance(state=TaskInstanceState.RUNNING)
    session.commit()
    return ti


def find_then_claim_elsewhere(wait: InboundWait, *, cutoff: datetime, session: Session):
    """Find the early deliveries that match ``wait``, then have another wait claim them first."""
    candidates = FIND_MATCHING_EARLY_DELIVERIES(wait, cutoff=cutoff, session=session)
    session.execute(
        update(InboundDelivery)
        .where(InboundDelivery.id.in_([candidate.id for candidate in candidates]))
        .values(outcome=DeliveryOutcome.RESUMED_WAIT, actions=OTHER_WAIT_ACTIONS)
    )
    return candidates


def assert_claimed_by_another_wait(session: Session, record_id: UUID) -> None:
    session.expire_all()
    record = session.get_one(InboundDelivery, record_id)
    assert (record.outcome, record.actions) == (DeliveryOutcome.RESUMED_WAIT, OTHER_WAIT_ACTIONS)
    assert session.scalar(select(func.count()).select_from(Log).where(Log.event == "webhook_delivery")) == 0


@mock.patch.object(wait_registration, "_end_wait_of_attempt_no_longer_live", autospec=True)
def test_register_wait_refuses_a_key_taken_after_it_was_checked(_, session, create_task_instance, running_ti):
    holder = create_task_instance(dag_id="holder", state=TaskInstanceState.RUNNING)
    add_wait(session, holder.id)
    ti = session.get_one(TaskInstance, running_ti.id)

    with pytest.raises(WaitKeyTaken):
        register_wait(
            ti,
            webhook="fake",
            events=EVENTS,
            match=MATCH,
            deadline=NOW + timedelta(hours=1),
            early_delivery_cutoff=CUTOFF,
            session=session,
        )


@mock.patch.object(
    wait_registration, "_find_matching_early_deliveries", autospec=True, side_effect=find_then_claim_elsewhere
)
def test_register_wait_leaves_a_delivery_claimed_meanwhile(_, session, running_ti):
    record_id = add_early_delivery(session)
    ti = session.get_one(TaskInstance, running_ti.id)

    wait = register_wait(
        ti,
        webhook="fake",
        events=EVENTS,
        match=MATCH,
        deadline=NOW + timedelta(hours=1),
        early_delivery_cutoff=CUTOFF,
        session=session,
    )

    assert (wait.status, wait.delivery_id) == (WaitStatus.REGISTERED, None)
    assert_claimed_by_another_wait(session, record_id)


@mock.patch.object(wait_registration, "_find_open_wait_on_key", autospec=True)
def test_register_wait_leaves_the_delivery_of_a_wait_ended_meanwhile(
    find_holder, session, create_task_instance, running_ti
):
    """Another registration ends the earlier attempt's wait after this one read it, and claims its delivery."""
    earlier = create_task_instance(dag_id="earlier", state=TaskInstanceState.FAILED)
    record_id = add_early_delivery(session)
    session.execute(
        update(InboundDelivery)
        .where(InboundDelivery.id == record_id)
        .values(outcome=DeliveryOutcome.RESUMED_WAIT)
    )
    earlier_wait = add_wait(session, earlier.id, status=WaitStatus.RECEIVED, delivery_id=record_id)

    def find_then_end_elsewhere(webhook, match_digest, *, session):
        holder = FIND_OPEN_WAIT_ON_KEY(webhook, match_digest, session=session)
        session.execute(
            update(InboundWait)
            .where(InboundWait.id == earlier_wait.id)
            .values(open=None, status=WaitStatus.ENDED)
        )
        session.execute(
            update(InboundDelivery).where(InboundDelivery.id == record_id).values(actions=OTHER_WAIT_ACTIONS)
        )
        return holder

    find_holder.side_effect = find_then_end_elsewhere
    ti = session.get_one(TaskInstance, running_ti.id)

    wait = register_wait(
        ti,
        webhook="fake",
        events=EVENTS,
        match=MATCH,
        deadline=NOW + timedelta(hours=1),
        early_delivery_cutoff=CUTOFF,
        session=session,
    )

    assert (wait.status, wait.delivery_id) == (WaitStatus.REGISTERED, None)
    assert_claimed_by_another_wait(session, record_id)


class TestClaimEarlyDeliveryAfterCommit:
    @pytest.mark.parametrize(
        "wait_fields",
        [
            pytest.param({"status": WaitStatus.RECEIVED}, id="received"),
            pytest.param({"status": WaitStatus.ENDED, "open": None}, id="ended"),
        ],
    )
    def test_leaves_a_wait_that_no_longer_waits(self, session, running_ti, wait_fields):
        wait = add_wait(session, running_ti.id, **wait_fields)
        record_id = add_early_delivery(session)

        assert claim_early_delivery_after_commit(wait.id, cutoff=CUTOFF, session=session) is None
        assert session.get_one(InboundDelivery, record_id).outcome == DeliveryOutcome.EARLY

    @pytest.mark.parametrize("state", [TaskInstanceState.AWAITING_INPUT, TaskInstanceState.FAILED])
    def test_leaves_a_wait_whose_task_instance_no_longer_runs(self, session, running_ti, state):
        wait = add_wait(session, running_ti.id)
        record_id = add_early_delivery(session)
        session.execute(update(TaskInstance).where(TaskInstance.id == running_ti.id).values(state=state))

        assert claim_early_delivery_after_commit(wait.id, cutoff=CUTOFF, session=session) is None
        assert session.get_one(InboundDelivery, record_id).outcome == DeliveryOutcome.EARLY

    @mock.patch.object(wait_registration, "lock_registering_task_instance", autospec=True)
    def test_leaves_a_wait_that_stopped_waiting_before_it_was_locked(self, lock_ti, session, running_ti):
        wait = add_wait(session, running_ti.id)
        record_id = add_early_delivery(session)

        def end_wait_then_lock(ti_id, *, session):
            session.execute(
                update(InboundWait)
                .where(InboundWait.id == wait.id)
                .values(status=WaitStatus.ENDED, open=None)
            )
            return LOCK_REGISTERING_TASK_INSTANCE(ti_id, session=session)

        lock_ti.side_effect = end_wait_then_lock

        assert claim_early_delivery_after_commit(wait.id, cutoff=CUTOFF, session=session) is None
        session.expire_all()
        assert session.get_one(InboundDelivery, record_id).outcome == DeliveryOutcome.EARLY
        assert (
            session.scalar(select(func.count()).select_from(Log).where(Log.event == "webhook_delivery")) == 0
        )

    @mock.patch.object(
        wait_registration,
        "_find_matching_early_deliveries",
        autospec=True,
        side_effect=find_then_claim_elsewhere,
    )
    def test_leaves_a_delivery_claimed_meanwhile(self, _, session, running_ti):
        wait = add_wait(session, running_ti.id)
        record_id = add_early_delivery(session)

        assert claim_early_delivery_after_commit(wait.id, cutoff=CUTOFF, session=session) is None
        assert_claimed_by_another_wait(session, record_id)
        wait = session.get_one(InboundWait, wait.id)
        assert (wait.status, wait.delivery_id) == (WaitStatus.REGISTERED, None)
