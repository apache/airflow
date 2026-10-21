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
Webhook waits and the deliveries they match, racing each other on real connections to a real database.

A wait's registration and the early delivery it targets each commit before they can see the other's rows,
so neither pairs them in its own transaction. Each side then looks again for the other after its commit,
in an order that each test sets.
"""

from __future__ import annotations

import itertools
import json
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from typing import TYPE_CHECKING
from unittest import mock

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import func, select, text, update

from airflow.api_fastapi.execution_api.datamodels.inbound_wait import InboundWaitRequest
from airflow.api_fastapi.execution_api.routes import inbound_waits
from airflow.api_fastapi.webhooks.app import create_webhooks_app
from airflow.models.inbound_delivery import DeliveryOutcome, InboundDelivery
from airflow.models.inbound_wait import InboundWait, WaitStatus
from airflow.models.log import Log
from airflow.models.taskinstance import TaskInstance
from airflow.providers_manager import ProvidersManager
from airflow.utils.session import create_session
from airflow.utils.state import TaskInstanceState
from airflow.webhooks import pipeline, wait_matching, wait_registration
from airflow.webhooks.registry import WebhookRegistry

from tests_common.test_utils.config import conf_vars
from tests_common.test_utils.db import clear_db_inbound_deliveries, clear_db_logs, clear_db_runs
from tests_common.test_utils.webhooks import FakeWaitSource, build_conn_env, post_signed_delivery

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

pytestmark = pytest.mark.db_test

REGISTRY = WebhookRegistry(
    names=frozenset({"fake"}),
    source_type_owners={},
    connection_cache_ttl=60,
    max_body_bytes=1024,
    delivery_retention=timedelta(days=7),
    rate_limit_per_minute=0,
    store_payload="full",
    max_event_payload_bytes=65536,
    early_delivery_ttl=timedelta(minutes=10),
    early_delivery_max_per_webhook=1000,
)
WAIT = InboundWaitRequest(
    webhook="fake",
    events=["pull_request"],
    match={"repository": "my-org/configs", "number": 42},
    timeout=timedelta(hours=1),
)
#: A wait on another key that the deliveries of BODY match as well.
OTHER_KEY_WAIT = WAIT.model_copy(update={"match": {"repository": "my-org/configs"}})
BODY = json.dumps({"path": "my-org/configs", "repository": "my-org/configs", "number": 42}).encode()
FIND_MATCHING_EARLY_DELIVERIES = wait_registration._find_matching_early_deliveries
FIND_MATCHING_WAITS = pipeline.find_matching_waits
CLAIM_EARLY_DELIVERY_AFTER_COMMIT = inbound_waits.claim_early_delivery_after_commit
HAND_TO_WAITS_AFTER_COMMIT = pipeline.hand_to_waits_after_commit
LOCK_REGISTERING_TASK_INSTANCE = inbound_waits.lock_registering_task_instance
#: How long either side waits for the other at a barrier before the test fails.
TIMEOUT = 30
#: The registration looks again before the delivery commits, so only the delivery finds the other.
REGISTRATION_LOOKS_AGAIN_FIRST = "registration-looks-again-first"
#: The registration looks again once the delivery has committed and before the delivery looks again.
DELIVERY_COMMITS_FIRST = "delivery-commits-first"
#: Both look again once both have committed, so that each finds the other, and they then claim at once.
BOTH_LOOK_AGAIN_AT_ONCE = "both-look-again-at-once"
#: Queries that count the transactions waiting for a row lock, by dialect.
COUNT_LOCK_WAITS = {
    "postgresql": "SELECT count(*) FROM pg_locks WHERE NOT granted",
    "mysql": "SELECT count(*) FROM information_schema.innodb_trx WHERE trx_state = 'LOCK WAIT'",
}


@pytest.fixture(autouse=True)
def fake_webhook():
    with (
        conf_vars({("webhooks", "names"): "fake"}),
        mock.patch.dict(os.environ, build_conn_env("fake")),
        mock.patch.object(
            ProvidersManager, "get_webhook_source_class", autospec=True, return_value=FakeWaitSource
        ),
    ):
        yield


@pytest.fixture(autouse=True)
def clean_db():
    clear_db_runs()
    clear_db_inbound_deliveries()
    clear_db_logs()
    yield
    clear_db_runs()
    clear_db_inbound_deliveries()
    clear_db_logs()


def wait_for(event: threading.Event) -> None:
    if not event.wait(timeout=TIMEOUT):
        raise TimeoutError("The other side did not get there in time")


def wait_until_a_transaction_waits_for_a_lock() -> None:
    deadline = time.monotonic() + TIMEOUT
    while True:
        with create_session(scoped=False) as session:
            query = COUNT_LOCK_WAITS[session.get_bind().dialect.name]
            if session.execute(text(query)).scalar_one():
                return
        if time.monotonic() > deadline:
            raise TimeoutError("No transaction waited for a lock in time")
        # MySQL refreshes INNODB_TRX only once it has not been read for 0.1 seconds.
        time.sleep(0.2)


def race_registration_and_delivery(
    ti_id, order: str, *, look_again: bool = True, found_again: dict[str, bool] | None = None
) -> tuple[WaitStatus, str]:
    """
    Register a wait for ``ti_id`` while a delivery it matches is recorded, so that neither sees the other.

    The registration inserts its wait, and the delivery finds no open wait; then the registration finds
    no early delivery and commits, and the delivery is kept early and commits. Each side then looks again
    for the other in ``order``.

    :param order: ``REGISTRATION_LOOKS_AGAIN_FIRST``, ``DELIVERY_COMMITS_FIRST`` or
        ``BOTH_LOOK_AGAIN_AT_ONCE``.
    :param look_again: False to have neither side look again after its commit, as in the naive design.
    :param found_again: Filled, with ``BOTH_LOOK_AGAIN_AT_ONCE``, with whether the ``registration`` and
        the ``delivery`` each found the other when it looked again.
    :return: The status the registration answers with, and the outcome the delivery is answered with.
    """
    both_looked = threading.Barrier(2)
    both_looked_again = threading.Barrier(2)
    registration_scanned = threading.Event()
    registration_committed = threading.Event()
    registration_looked_again = threading.Event()
    delivery_committed = threading.Event()
    scans = itertools.count()
    found_again = {} if found_again is None else found_again

    def wait_for_the_delivery_then_scan(wait, *, cutoff, session):
        # Only the registration's own scan: looking again after its commit scans again.
        if next(scans) > 0:
            found = FIND_MATCHING_EARLY_DELIVERIES(wait, cutoff=cutoff, session=session)
            if order == BOTH_LOOK_AGAIN_AT_ONCE:
                found_again["registration"] = bool(found)
                both_looked_again.wait(timeout=TIMEOUT)
            return found
        # The wait is inserted, and early deliveries are not scanned yet.
        both_looked.wait(timeout=TIMEOUT)
        try:
            return FIND_MATCHING_EARLY_DELIVERIES(wait, cutoff=cutoff, session=session)
        finally:
            registration_scanned.set()

    def find_waits_then_wait_for_the_registration(webhook, attributes, *, session):
        found = FIND_MATCHING_WAITS(webhook, attributes, session=session)
        # The waits are scanned, and the early delivery is not inserted yet.
        both_looked.wait(timeout=TIMEOUT)
        wait_for(registration_scanned)
        if order == REGISTRATION_LOOKS_AGAIN_FIRST:
            wait_for(registration_looked_again)
        return found

    def find_waits_again_then_wait_for_the_registration(webhook, attributes, *, session):
        found = FIND_MATCHING_WAITS(webhook, attributes, session=session)
        if order == BOTH_LOOK_AGAIN_AT_ONCE:
            found_again["delivery"] = bool(found)
            both_looked_again.wait(timeout=TIMEOUT)
        return found

    def registration_looks_again(wait_id, *, cutoff, session):
        registration_committed.set()
        if order in (DELIVERY_COMMITS_FIRST, BOTH_LOOK_AGAIN_AT_ONCE):
            wait_for(delivery_committed)
        try:
            if not look_again:
                return None
            return CLAIM_EARLY_DELIVERY_AFTER_COMMIT(wait_id, cutoff=cutoff, session=session)
        finally:
            registration_looked_again.set()

    def delivery_looks_again(record_id, *, cutoff, session):
        delivery_committed.set()
        if order == DELIVERY_COMMITS_FIRST:
            wait_for(registration_looked_again)
        elif order == BOTH_LOOK_AGAIN_AT_ONCE:
            wait_for(registration_committed)
        if not look_again:
            return False
        return HAND_TO_WAITS_AFTER_COMMIT(record_id, cutoff=cutoff, session=session)

    def register():
        return inbound_waits.register_inbound_wait(ti_id, WAIT).status

    with (
        mock.patch.object(
            wait_registration,
            "_find_matching_early_deliveries",
            autospec=True,
            side_effect=wait_for_the_delivery_then_scan,
        ),
        mock.patch.object(
            pipeline,
            "find_matching_waits",
            autospec=True,
            side_effect=find_waits_then_wait_for_the_registration,
        ),
        mock.patch.object(
            wait_matching,
            "find_matching_waits",
            autospec=True,
            side_effect=find_waits_again_then_wait_for_the_registration,
        ),
        mock.patch.object(
            inbound_waits,
            "claim_early_delivery_after_commit",
            autospec=True,
            side_effect=registration_looks_again,
        ),
        mock.patch.object(
            pipeline, "hand_to_waits_after_commit", autospec=True, side_effect=delivery_looks_again
        ),
        ThreadPoolExecutor(2) as executor,
    ):
        registration = executor.submit(register)
        delivery = executor.submit(deliver)
        return registration.result(timeout=2 * TIMEOUT), delivery.result(timeout=2 * TIMEOUT)


def deliver(delivery_id: str = "delivery-1") -> str:
    """Post a delivery of BODY, and return the outcome it is answered with."""
    client = TestClient(create_webhooks_app(REGISTRY))
    response = post_signed_delivery(client, body=BODY, event="pull_request", delivery_id=delivery_id)
    assert response.status_code == 202, response.text
    return response.json()["outcome"]


def read_wait_and_delivery(session: Session, ti_id) -> tuple[InboundWait, InboundDelivery]:
    session.expire_all()
    (wait,) = session.scalars(select(InboundWait).where(InboundWait.ti_id == ti_id))
    (record,) = session.scalars(select(InboundDelivery))
    return wait, record


def count_audit_logs(session: Session) -> int:
    return session.execute(
        select(func.count()).select_from(Log).where(Log.event == "webhook_delivery")
    ).scalar_one()


def assert_paired_once(session: Session, wait: InboundWait, record: InboundDelivery) -> None:
    """Assert that the running task's wait received the delivery, and that nothing else did."""
    assert (wait.open, wait.status, wait.delivery_id) == (True, WaitStatus.RECEIVED, record.id)
    assert record.outcome == DeliveryOutcome.RESUMED_WAIT
    assert record.actions is not None
    assert [entry["wait_id"] for entry in record.actions["waits"]] == [str(wait.id)]
    assert count_audit_logs(session) == 1


#: What a registration and a delivery are answered with when one of them pairs them: the other answers
#: as if it had not looked again.
PAIRED_ONCE_ANSWERS = [
    (WaitStatus.REGISTERED, DeliveryOutcome.RESUMED_WAIT),
    (WaitStatus.RECEIVED, DeliveryOutcome.EARLY),
]


@pytest.fixture
def running_ti(create_task_instance, session: Session):
    ti = create_task_instance(state=TaskInstanceState.RUNNING)
    session.commit()
    return ti


@pytest.mark.parametrize(
    ("order", "expected_answers"),
    [
        pytest.param(
            REGISTRATION_LOOKS_AGAIN_FIRST,
            (WaitStatus.REGISTERED, DeliveryOutcome.RESUMED_WAIT),
            id="the-delivery-pairs-them",
        ),
        pytest.param(
            DELIVERY_COMMITS_FIRST,
            (WaitStatus.RECEIVED, DeliveryOutcome.EARLY),
            id="the-registration-pairs-them",
        ),
    ],
)
def test_registration_racing_its_early_delivery_pairs_with_it_once(
    session, running_ti, order, expected_answers
):
    """The side that looks again and finds the other pairs them, and the other answers as if it had not."""
    assert race_registration_and_delivery(running_ti.id, order) == expected_answers

    assert_paired_once(session, *read_wait_and_delivery(session, running_ti.id))


def test_registration_and_early_delivery_that_both_find_the_other_pair_once(session, running_ti):
    """
    Both look again at once and find the other, and the first to claim pairs them.

    Each side locks the task instance before it claims, and the claims are compare-and-set, so the other
    finds the wait or the delivery claimed and answers as if it had found nothing.
    """
    found_again: dict[str, bool] = {}

    answers = race_registration_and_delivery(running_ti.id, BOTH_LOOK_AGAIN_AT_ONCE, found_again=found_again)

    assert found_again == {"registration": True, "delivery": True}
    assert answers in PAIRED_ONCE_ANSWERS
    assert_paired_once(session, *read_wait_and_delivery(session, running_ti.id))


@pytest.mark.parametrize("order", [REGISTRATION_LOOKS_AGAIN_FIRST, DELIVERY_COMMITS_FIRST])
def test_without_looking_again_after_commit_the_early_delivery_is_lost(session, running_ti, order):
    """The design that D-23 replaces: each side looks for the other only before it commits."""
    assert race_registration_and_delivery(running_ti.id, order, look_again=False) == (
        WaitStatus.REGISTERED,
        DeliveryOutcome.EARLY,
    )

    wait, record = read_wait_and_delivery(session, running_ti.id)
    assert (wait.status, wait.delivery_id) == (WaitStatus.REGISTERED, None)
    assert record.outcome == DeliveryOutcome.EARLY


def test_deliveries_racing_for_a_parked_wait_resume_its_task_once(session, running_ti):
    """Both find the wait registered, and the first to claim it resumes the task; the other is kept early."""
    assert inbound_waits.register_inbound_wait(running_ti.id, WAIT).status == WaitStatus.REGISTERED
    session.execute(
        update(TaskInstance)
        .where(TaskInstance.id == running_ti.id)
        .values(state=TaskInstanceState.AWAITING_INPUT, next_method="execute_complete", next_kwargs={})
    )
    session.commit()
    both_found = threading.Barrier(2)
    found: list[int] = []

    def find_waits_then_wait_for_the_other_delivery(webhook, attributes, *, session):
        waits = FIND_MATCHING_WAITS(webhook, attributes, session=session)
        found.append(len(waits))
        both_found.wait(timeout=TIMEOUT)
        return waits

    delivery_ids = ["delivery-1", "delivery-2"]
    with (
        mock.patch.object(
            pipeline,
            "find_matching_waits",
            autospec=True,
            side_effect=find_waits_then_wait_for_the_other_delivery,
        ),
        ThreadPoolExecutor(2) as executor,
    ):
        outcomes = dict(zip(delivery_ids, executor.map(deliver, delivery_ids)))

    assert found == [1, 1]
    assert sorted(outcomes.values()) == [DeliveryOutcome.EARLY, DeliveryOutcome.RESUMED_WAIT]
    (winner,) = [key for key, outcome in outcomes.items() if outcome == DeliveryOutcome.RESUMED_WAIT]
    session.expire_all()
    (wait,) = session.scalars(select(InboundWait).where(InboundWait.ti_id == running_ti.id))
    record = session.get_one(InboundDelivery, wait.delivery_id)
    assert (wait.status, record.delivery_id, record.outcome) == (
        WaitStatus.RESUMED,
        winner,
        DeliveryOutcome.RESUMED_WAIT,
    )
    ti = session.get_one(TaskInstance, running_ti.id)
    assert (ti.state, ti.next_kwargs["event"]["delivery_id"]) == (TaskInstanceState.SCHEDULED, winner)
    assert count_audit_logs(session) == 1


@pytest.mark.backend("postgres", "mysql")
def test_a_delivery_does_not_deadlock_with_a_retry_ending_its_earlier_tries_wait(session, running_ti):
    """
    A retry's registration ends its earlier try's wait without that try's lock while a delivery ends it.

    The delivery matches the earlier try's wait and the retry's own open wait. It holds no wait while it
    waits for the retry's lock, which the registration holds, so the registration can end the earlier
    try's wait and commit, and the delivery then finds both waits ended.
    """
    assert inbound_waits.register_inbound_wait(running_ti.id, WAIT).status == WaitStatus.REGISTERED
    retry = running_ti.prepare_db_for_next_try(session)
    retry.state = TaskInstanceState.RUNNING
    session.commit()
    # The delivery takes the earlier try's wait first.
    assert running_ti.id < retry.id
    assert inbound_waits.register_inbound_wait(retry.id, OTHER_KEY_WAIT).status == WaitStatus.REGISTERED
    delivery_found = threading.Event()
    registration_locked = threading.Event()
    found: list[int] = []

    def find_waits_then_wait_for_the_registration(webhook, attributes, *, session):
        waits = FIND_MATCHING_WAITS(webhook, attributes, session=session)
        found.append(len(waits))
        delivery_found.set()
        wait_for(registration_locked)
        return waits

    def lock_then_wait_for_the_delivery_to_wait(ti_id, *, session):
        ti = LOCK_REGISTERING_TASK_INSTANCE(ti_id, session=session)
        registration_locked.set()
        wait_until_a_transaction_waits_for_a_lock()
        return ti

    def register():
        wait_for(delivery_found)
        return inbound_waits.register_inbound_wait(retry.id, WAIT).status

    with (
        mock.patch.object(
            pipeline,
            "find_matching_waits",
            autospec=True,
            side_effect=find_waits_then_wait_for_the_registration,
        ),
        mock.patch.object(
            inbound_waits,
            "lock_registering_task_instance",
            autospec=True,
            side_effect=lock_then_wait_for_the_delivery_to_wait,
        ),
        ThreadPoolExecutor(2) as executor,
    ):
        registration = executor.submit(register)
        delivery = executor.submit(deliver)
        answers = (registration.result(timeout=2 * TIMEOUT), delivery.result(timeout=2 * TIMEOUT))

    assert found == [2]
    assert answers in PAIRED_ONCE_ANSWERS
    session.expire_all()
    (earlier_wait,) = session.scalars(select(InboundWait).where(InboundWait.ti_id == running_ti.id))
    first_wait, wait = session.scalars(
        select(InboundWait).where(InboundWait.ti_id == retry.id).order_by(InboundWait.created_at)
    )
    assert [(w.status, w.delivery_id) for w in (earlier_wait, first_wait)] == [(WaitStatus.ENDED, None)] * 2
    (record,) = session.scalars(select(InboundDelivery))
    assert_paired_once(session, wait, record)
