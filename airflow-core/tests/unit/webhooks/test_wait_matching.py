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
"""How deliveries go to the webhook waits they match, and are kept early for waits registered later."""

from __future__ import annotations

import hashlib
import json
import os
import uuid
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any
from unittest import mock

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import select, update
from sqlalchemy.exc import OperationalError

from airflow._shared.timezones import timezone
from airflow._shared.webhook_naming.matching import compute_key_digest, compute_match_digest
from airflow.api_fastapi.webhooks.app import create_webhooks_app
from airflow.models.asset import AssetDagRunQueue, AssetEvent
from airflow.models.inbound_delivery import DeliveryOutcome, InboundDelivery
from airflow.models.inbound_wait import InboundWait, WaitStatus
from airflow.models.log import Log
from airflow.models.taskinstance import TaskInstance
from airflow.providers_manager import ProvidersManager
from airflow.sdk import Asset
from airflow.utils.session import create_session
from airflow.utils.state import TaskInstanceState
from airflow.webhooks import pipeline, wait_matching
from airflow.webhooks.naming import build_topic_name
from airflow.webhooks.registry import WebhookRegistry
from airflow.webhooks.wait_matching import (
    expire_oldest_early_deliveries,
    find_matching_waits,
    hand_to_waits_after_commit,
)
from airflow.webhooks.wait_registration import lock_registering_task_instance, register_wait

from tests_common.test_utils.db import (
    clear_db_assets,
    clear_db_inbound_deliveries,
    clear_db_logs,
    clear_db_runs,
)
from tests_common.test_utils.webhooks import FakeWaitSource, build_conn_env, post_signed_delivery

if TYPE_CHECKING:
    from collections.abc import Iterator

    from httpx2 import Response
    from sqlalchemy.orm import Session

    from airflow.webhooks.wait_matching import WaitCandidate

pytestmark = pytest.mark.db_test

NOW = datetime(2026, 10, 20, 12, 0, tzinfo=UTC)
EARLY_DELIVERY_TTL = timedelta(minutes=10)
REGISTRY = WebhookRegistry(
    names=frozenset({"fake"}),
    source_type_owners={},
    connection_cache_ttl=60,
    max_body_bytes=1024,
    delivery_retention=timedelta(days=7),
    rate_limit_per_minute=0,
    store_payload="full",
    max_event_payload_bytes=65536,
    early_delivery_ttl=EARLY_DELIVERY_TTL,
    early_delivery_max_per_webhook=1000,
)
EVENTS = ["pull_request"]
MATCH = {"repository": "my-org/configs", "number": 42}
PAYLOAD = {"path": "my-org/configs", "repository": "my-org/configs", "number": 42, "merged": True}
#: The attributes FakeWaitSource parses from a pull_request delivery of PAYLOAD.
ATTRIBUTES = {"event": "pull_request", "repository": "my-org/configs", "number": 42}
RESUME_EVENT = {
    "webhook": "fake",
    "event": "pull_request",
    "delivery_id": "delivery-1",
    "received_at": "2026-10-20T12:00:00+00:00",
    "attributes": ATTRIBUTES,
    "payload": PAYLOAD,
}
#: The actions of a delivery that another wait claimed.
OTHER_WAIT_ACTIONS = {"waits": [{"wait_id": "another-wait"}]}
#: The record id of a delivery that another delivery's wait holds.
OTHER_RECORD_ID = uuid.UUID(int=1)
#: The topic that the pull_request deliveries of PAYLOAD offer.
TOPIC = build_topic_name("fake", "my-org/configs", "pull_request")
#: The asset that the asset route of ROUTED_CONN_ENV records an event on for each delivery offering TOPIC.
ROUTED_ASSET = Asset(name="configs_pull_requests", uri="s3://warehouse/configs/pull_requests")
#: The connection of webhook fake, with an asset route from TOPIC to ROUTED_ASSET.
ROUTED_CONN_ENV = build_conn_env("fake", asset_routes=[{"topic": TOPIC, "asset": ROUTED_ASSET.uri}])
FIND_MATCHING_WAITS = wait_matching.find_matching_waits


@pytest.fixture(autouse=True)
def frozen_time(time_machine):
    time_machine.move_to(NOW, tick=False)


@pytest.fixture(autouse=True)
def fake_webhook():
    with (
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
    clear_db_assets()
    yield
    clear_db_runs()
    clear_db_inbound_deliveries()
    clear_db_logs()
    clear_db_assets()


@pytest.fixture
def client() -> TestClient:
    return TestClient(create_webhooks_app(REGISTRY))


@pytest.fixture
def events_client(events: str) -> Iterator[TestClient]:
    """A client of webhook fake, with the asset route of ROUTED_CONN_ENV if ``events`` is ``route``."""
    with mock.patch.dict(os.environ, ROUTED_CONN_ENV if events == "route" else {}):
        yield TestClient(create_webhooks_app(REGISTRY))


@pytest.fixture
def make_ti(create_task_instance, session: Session):
    """Create a task instance, parked as a webhook wait parks it unless another ``state`` is given."""

    def make(state: TaskInstanceState = TaskInstanceState.AWAITING_INPUT, dag_id: str = "waiting"):
        ti = create_task_instance(dag_id=dag_id, state=state)
        if state == TaskInstanceState.AWAITING_INPUT:
            ti.next_method = "execute_complete"
            ti.next_kwargs = {}
        session.commit()
        return ti

    return make


def build_body(number: int | None) -> bytes:
    """Return PAYLOAD with ``number``, or without one, and so without correlation key, as a body."""
    payload = {key: value for key, value in PAYLOAD.items() if key != "number"}
    if number is not None:
        payload["number"] = number
    return json.dumps(payload).encode()


def post(client: TestClient, *, number: int | None = 42, delivery_id: str = "delivery-1") -> Response:
    """Post a pull_request delivery of ``build_body(number)``."""
    return post_signed_delivery(
        client, body=build_body(number), event="pull_request", delivery_id=delivery_id
    )


def add_wait(
    session: Session,
    ti_id,
    *,
    webhook: str = "fake",
    events: list[str] | None = EVENTS,
    match: dict[str, Any] = MATCH,
    is_open: bool = True,
    **overrides: Any,
) -> InboundWait:
    fields = {
        "key_digest": compute_key_digest(match),
        "match_digest": compute_match_digest(events, match),
        "deadline": NOW + timedelta(hours=1),
        "created_at": NOW - timedelta(minutes=1),
    }
    wait = InboundWait(ti_id=ti_id, webhook=webhook, events=events, match=match, **(fields | overrides))
    session.add(wait)
    session.flush()
    # Set after the insert: the ORM inserts a column's default in place of None.
    wait.open = True if is_open else None
    session.commit()
    return wait


def add_early_delivery(
    session: Session, *, received_at: datetime = NOW - timedelta(minutes=1), **overrides: Any
) -> InboundDelivery:
    fields = {
        "delivery_id": "early-1",
        "body_digest": "0" * 64,
        "attributes": ATTRIBUTES,
        "correlation_key": "my-org/configs#42",
        "payload": PAYLOAD,
        "outcome": DeliveryOutcome.EARLY,
    }
    record = InboundDelivery(
        webhook="fake",
        source_type="fake",
        received_at=received_at,
        retain_until=received_at + timedelta(days=7),
        **(fields | overrides),
    )
    session.add(record)
    session.commit()
    return record


def register(
    session: Session, ti_id, *, number: int = 42, match: dict[str, Any] | None = None
) -> InboundWait:
    """Register a wait for ``ti_id`` as the register-wait route does, and commit it."""
    wait = register_wait(
        lock_registering_task_instance(ti_id, session=session),
        webhook="fake",
        events=EVENTS,
        match={**MATCH, "number": number} if match is None else match,
        deadline=NOW + timedelta(days=1),
        early_delivery_cutoff=timezone.utcnow() - EARLY_DELIVERY_TTL,
        session=session,
    )
    session.commit()
    return wait


def subscribe(dag_maker, events: str = "topic", *, is_paused: bool = False) -> None:
    """
    Add a Dag that the deliveries of PAYLOAD record events for.

    :param events: ``topic`` for a Dag scheduled on TOPIC, which they record topic events for, or
        ``route`` for one scheduled on ROUTED_ASSET, which the route of ROUTED_CONN_ENV records events on.
    """
    with dag_maker(f"on_{events}", schedule=Asset(name=TOPIC) if events == "topic" else ROUTED_ASSET):
        pass
    if is_paused:
        dag_maker.dag_model.is_paused = True
        dag_maker.session.commit()


def count_asset_events(session: Session) -> int:
    return len(session.scalars(select(AssetEvent.id)).all())


def read(session: Session, model: type, row_id):
    session.expire_all()
    return session.get_one(model, row_id)


def get_deliveries(session: Session) -> list[InboundDelivery]:
    session.expire_all()
    return list(session.scalars(select(InboundDelivery).order_by(InboundDelivery.received_at)))


def get_audit_logs(session: Session) -> list[Log]:
    return list(session.scalars(select(Log).where(Log.event == "webhook_delivery").order_by(Log.id)))


def build_wait_action(wait: InboundWait, ti: TaskInstance) -> dict[str, Any]:
    return {
        "wait_id": str(wait.id),
        "ti_id": str(ti.id),
        "dag_id": ti.dag_id,
        "run_id": ti.run_id,
        "task_id": ti.task_id,
        "map_index": ti.map_index,
    }


class TestHandToWaits:
    @pytest.mark.parametrize(
        ("state", "expected_ti_state", "expected_next_kwargs", "expected_wait"),
        [
            pytest.param(
                TaskInstanceState.AWAITING_INPUT,
                TaskInstanceState.SCHEDULED,
                {"event": RESUME_EVENT},
                (None, WaitStatus.RESUMED, NOW),
                id="parked",
            ),
            pytest.param(
                TaskInstanceState.RUNNING,
                TaskInstanceState.RUNNING,
                None,
                (True, WaitStatus.RECEIVED, None),
                id="running",
            ),
        ],
    )
    def test_a_matching_wait_takes_the_delivery(
        self, client, session, make_ti, state, expected_ti_state, expected_next_kwargs, expected_wait
    ):
        ti = make_ti(state)
        wait = add_wait(session, ti.id)

        response = post(client)

        assert (response.status_code, response.json()) == (
            202,
            {"delivery_id": "delivery-1", "outcome": "resumed_wait"},
        )
        (record,) = get_deliveries(session)
        assert (record.outcome, record.payload, record.actions) == (
            "resumed_wait",
            PAYLOAD,
            {"waits": [build_wait_action(wait, ti)]},
        )
        wait = read(session, InboundWait, wait.id)
        assert (wait.open, wait.status, wait.ended_at) == expected_wait
        assert wait.delivery_id == record.id
        ti = read(session, TaskInstance, ti.id)
        assert (ti.state, ti.next_kwargs) == (expected_ti_state, expected_next_kwargs)
        (audit,) = get_audit_logs(session)
        assert (audit.task_instance_id, audit.dag_id, audit.run_id, audit.task_id) == (
            ti.id,
            ti.dag_id,
            ti.run_id,
            ti.task_id,
        )
        assert (audit.owner, audit.owner_display_name, audit.team_name) == (
            "webhook:fake",
            "Webhook fake",
            None,
        )
        assert json.loads(audit.extra) == {
            "delivery_id": "delivery-1",
            "outcome": "resumed_wait",
            "wait_id": str(wait.id),
        }

    @pytest.mark.parametrize(
        ("store_payload", "resumed_payload", "early_payload"),
        [
            pytest.param("full", PAYLOAD, {**PAYLOAD, "number": 43}, id="full"),
            pytest.param("summary", {"number": 42}, {"number": 43}, id="summary"),
            pytest.param("none", None, None, id="none"),
        ],
    )
    @mock.patch.object(
        FakeWaitSource,
        "summarize",
        autospec=True,
        side_effect=lambda source, payload: {"number": payload["number"]},
    )
    def test_the_payload_kept_and_resumed_with_is_what_store_payload_keeps(
        self, _, session, make_ti, store_payload, resumed_payload, early_payload
    ):
        ti = make_ti()
        add_wait(session, ti.id)

        with mock.patch.dict(os.environ, build_conn_env("fake", store_payload=store_payload)):
            client = TestClient(create_webhooks_app(REGISTRY))
            post(client)
            post(client, number=43, delivery_id="delivery-2")

        resumed, early = get_deliveries(session)
        assert (resumed.outcome, resumed.payload) == ("resumed_wait", resumed_payload)
        assert (early.outcome, early.payload) == ("early", early_payload)
        assert read(session, TaskInstance, ti.id).next_kwargs["event"]["payload"] == resumed_payload

    def test_a_delivery_without_correlation_key_resumes_a_wait_with_its_payload(
        self, client, session, make_ti
    ):
        ti = make_ti()
        add_wait(session, ti.id, match={"repository": "my-org/configs"})

        assert post(client, number=None).json()["outcome"] == "resumed_wait"

        payload = json.loads(build_body(None))
        (record,) = get_deliveries(session)
        assert (record.correlation_key, record.payload) == (None, payload)
        assert read(session, TaskInstance, ti.id).next_kwargs["event"]["payload"] == payload

    def test_each_wait_the_delivery_matches_takes_it_once(self, client, session, make_ti):
        on_key = make_ti(dag_id="on_key")
        on_event = make_ti(dag_id="on_event")
        waits = {
            on_key.id: add_wait(session, on_key.id),
            on_event.id: add_wait(session, on_event.id, match={}),
        }

        assert post(client).json()["outcome"] == "resumed_wait"

        (record,) = get_deliveries(session)
        tis = sorted([on_key, on_event], key=lambda ti: str(ti.id))
        assert record.actions == {"waits": [build_wait_action(waits[ti.id], ti) for ti in tis]}
        for ti in tis:
            assert read(session, TaskInstance, ti.id).next_kwargs == {"event": RESUME_EVENT}
            assert read(session, InboundWait, waits[ti.id].id).status == WaitStatus.RESUMED
        assert [audit.task_instance_id for audit in get_audit_logs(session)] == [ti.id for ti in tis]

    @pytest.mark.parametrize(
        "wait_fields",
        [
            pytest.param({"match": {**MATCH, "number": 43}}, id="other-value"),
            pytest.param({"events": ["push"]}, id="other-event"),
            pytest.param({"webhook": "other"}, id="other-webhook"),
            pytest.param({"match": {"label": "deploy"}}, id="attribute-the-delivery-lacks"),
            pytest.param({"status": WaitStatus.RECEIVED}, id="received-already"),
            pytest.param({"status": WaitStatus.ENDED, "is_open": False}, id="closed"),
        ],
    )
    def test_a_wait_the_delivery_does_not_match_keeps_waiting(self, client, session, make_ti, wait_fields):
        ti = make_ti()
        wait = add_wait(session, ti.id, **wait_fields)

        assert post(client).json()["outcome"] == "early"

        wait = read(session, InboundWait, wait.id)
        assert (wait.status, wait.delivery_id) == (wait_fields.get("status", WaitStatus.REGISTERED), None)
        assert read(session, TaskInstance, ti.id).state == TaskInstanceState.AWAITING_INPUT
        assert get_audit_logs(session) == []

    @pytest.mark.parametrize(
        "state",
        [TaskInstanceState.FAILED, TaskInstanceState.SUCCESS, TaskInstanceState.UP_FOR_RETRY, "archived"],
    )
    @pytest.mark.parametrize(
        ("number", "match", "expected_outcome", "expected_payload"),
        [
            pytest.param(42, MATCH, "early", PAYLOAD, id="with-correlation-key"),
            pytest.param(None, {}, "no_subscribers", None, id="without-correlation-key"),
        ],
    )
    def test_a_wait_whose_task_can_no_longer_resume_is_ended(
        self, client, session, make_ti, state, number, match, expected_outcome, expected_payload
    ):
        ti = make_ti(TaskInstanceState.RUNNING if state == "archived" else state)
        wait = add_wait(session, ti.id, match=match)
        if state == "archived":
            ti.prepare_db_for_next_try(session)
            session.commit()

        assert post(client, number=number).json()["outcome"] == expected_outcome

        wait = read(session, InboundWait, wait.id)
        assert (wait.open, wait.status, wait.ended_at, wait.delivery_id) == (
            None,
            WaitStatus.ENDED,
            NOW,
            None,
        )
        (record,) = get_deliveries(session)
        assert (record.payload, record.actions) == (expected_payload, None)
        assert get_audit_logs(session) == []

    def test_a_new_try_claims_the_delivery_its_earlier_try_missed(self, client, session, make_ti):
        earlier = make_ti(TaskInstanceState.RUNNING)
        add_wait(session, earlier.id)
        retry = earlier.prepare_db_for_next_try(session)
        retry.state = TaskInstanceState.RUNNING
        session.commit()

        assert post(client).json()["outcome"] == "early"
        wait = register(session, retry.id)

        (record,) = get_deliveries(session)
        assert (wait.status, wait.delivery_id) == (WaitStatus.RECEIVED, record.id)
        assert record.actions == {"waits": [build_wait_action(wait, retry)]}

    def test_a_new_try_takes_back_only_its_earlier_try_from_a_delivery_that_went_to_several_waits(
        self, client, session, make_ti
    ):
        earlier = make_ti(TaskInstanceState.RUNNING, dag_id="retried")
        other = make_ti(TaskInstanceState.RUNNING, dag_id="other")
        add_wait(session, earlier.id)
        other_wait = add_wait(session, other.id, match={})
        assert post(client).json()["outcome"] == "resumed_wait"
        retry = earlier.prepare_db_for_next_try(session)
        retry.state = TaskInstanceState.RUNNING
        session.commit()

        wait = register(session, retry.id)

        (record,) = get_deliveries(session)
        assert (wait.status, wait.delivery_id) == (WaitStatus.REGISTERED, None)
        assert (record.outcome, record.actions) == (
            "resumed_wait",
            {"waits": [build_wait_action(other_wait, other)]},
        )
        other_wait = read(session, InboundWait, other_wait.id)
        assert (other_wait.status, other_wait.delivery_id) == (WaitStatus.RECEIVED, record.id)

    def test_a_retry_of_a_delivery_that_went_to_a_wait_is_a_duplicate(self, client, session, make_ti):
        ti = make_ti()
        add_wait(session, ti.id)

        responses = [post(client), post(client)]

        assert [(response.status_code, response.json()["outcome"]) for response in responses] == [
            (202, "resumed_wait"),
            (200, "duplicate"),
        ]
        assert len(get_audit_logs(session)) == 1
        assert read(session, TaskInstance, ti.id).next_kwargs == {"event": RESUME_EVENT}

    def test_a_copy_that_finds_a_wait_the_copy_recorded_first_did_not_takes_the_delivery(
        self, client, session, make_ti
    ):
        assert post(client).json()["outcome"] == "early"
        ti = make_ti()
        wait = add_wait(session, ti.id)
        find_delivery = pipeline.find_delivery

        def miss_the_first_lookup(*args, **kwargs):
            # As when this copy looked before the first copy had committed.
            return None if mock_find_delivery.call_count == 1 else find_delivery(*args, **kwargs)

        with mock.patch.object(
            pipeline, "find_delivery", autospec=True, side_effect=miss_the_first_lookup
        ) as mock_find_delivery:
            response = post(client)

        assert (response.status_code, response.json()["outcome"]) == (202, "resumed_wait")
        (record,) = get_deliveries(session)
        assert record.actions == {"waits": [build_wait_action(wait, ti)]}
        assert read(session, TaskInstance, ti.id).next_kwargs == {"event": RESUME_EVENT}
        assert len(get_audit_logs(session)) == 1

    @pytest.mark.parametrize("events", ["topic", "route"])
    @pytest.mark.parametrize(
        ("waiting", "number", "expected_outcome", "expected_payload"),
        [
            pytest.param(True, 42, "resumed_wait", PAYLOAD, id="to-a-wait"),
            pytest.param(False, 42, "accepted", PAYLOAD, id="with-correlation-key"),
            pytest.param(False, None, "accepted", json.loads(build_body(None)), id="without-correlation-key"),
        ],
    )
    def test_a_delivery_that_records_events_is_accepted_unless_a_wait_takes_it(
        self,
        events_client,
        session,
        make_ti,
        dag_maker,
        events,
        waiting,
        number,
        expected_outcome,
        expected_payload,
    ):
        """It is never kept early, even with a correlation key, so its retry is a duplicate."""
        ti = make_ti()
        if waiting:
            wait = add_wait(session, ti.id)
        subscribe(dag_maker, events)

        responses = [post(events_client, number=number), post(events_client, number=number)]

        assert [(response.status_code, response.json()["outcome"]) for response in responses] == [
            (202, expected_outcome),
            (200, "duplicate"),
        ]
        (record,) = get_deliveries(session)
        events_key = "asset_events" if events == "topic" else "asset_routes"
        assert (record.outcome, record.payload, set(record.actions)) == (
            expected_outcome,
            expected_payload,
            {events_key, "waits"} if waiting else {events_key},
        )
        if waiting:
            assert record.actions["waits"] == [build_wait_action(wait, ti)]
        assert count_asset_events(session) == 1
        *wait_audits, events_audit = get_audit_logs(session)
        assert [json.loads(audit.extra)["wait_id"] for audit in wait_audits] == (
            [str(wait.id)] if waiting else []
        )
        expected_routes = [{"topic": TOPIC, "asset": ROUTED_ASSET.uri}]
        assert json.loads(events_audit.extra) == {
            "delivery_id": "delivery-1",
            "names": [TOPIC] if events == "topic" else [],
            "outcome": expected_outcome,
            **({"asset_routes": expected_routes} if events == "route" else {}),
        }

    @pytest.mark.parametrize(
        "changed",
        [
            pytest.param({"open": None, "status": WaitStatus.ENDED}, id="ended"),
            pytest.param(
                {"status": WaitStatus.RECEIVED, "delivery_id": OTHER_RECORD_ID},
                id="taken-by-another-delivery",
            ),
        ],
    )
    @mock.patch.object(pipeline, "find_matching_waits", autospec=True)
    def test_a_wait_that_stops_waiting_after_it_is_found_does_not_take_the_delivery(
        self, find_waits, client, session, make_ti, changed
    ):
        """The wait is claimed by a compare-and-set, which holds where row locks do not, as on SQLite."""
        ti = make_ti()
        wait = add_wait(session, ti.id)

        def find_then_change_elsewhere(webhook, attributes, *, session):
            found = FIND_MATCHING_WAITS(webhook, attributes, session=session)
            with create_session(scoped=False) as other_session:
                other_session.execute(update(InboundWait).where(InboundWait.id == wait.id).values(**changed))
            return found

        find_waits.side_effect = find_then_change_elsewhere

        assert post(client).json()["outcome"] == "early"
        wait = read(session, InboundWait, wait.id)
        assert (wait.open, wait.status, wait.delivery_id) == (
            changed.get("open", True),
            changed["status"],
            changed.get("delivery_id"),
        )
        assert read(session, TaskInstance, ti.id).state == TaskInstanceState.AWAITING_INPUT
        assert get_audit_logs(session) == []


class TestEarlyDeliveries:
    @pytest.mark.parametrize("paused_subscriber", [False, True], ids=["no-subscriber", "paused-subscriber"])
    def test_a_delivery_no_wait_takes_is_kept_early_until_a_wait_claims_it(
        self, client, session, make_ti, dag_maker, paused_subscriber
    ):
        if paused_subscriber:
            subscribe(dag_maker, is_paused=True)

        response = post(client)

        assert (response.status_code, response.json()) == (
            202,
            {"delivery_id": "delivery-1", "outcome": "early"},
        )
        (record,) = get_deliveries(session)
        assert (record.outcome, record.payload, record.actions) == ("early", PAYLOAD, None)
        assert get_audit_logs(session) == []

        ti = make_ti(TaskInstanceState.RUNNING)
        wait = register(session, ti.id)

        assert (wait.status, wait.delivery_id) == (WaitStatus.RECEIVED, record.id)
        assert read(session, InboundDelivery, record.id).outcome == DeliveryOutcome.RESUMED_WAIT

    @pytest.mark.parametrize("events", ["topic", "route"])
    @pytest.mark.parametrize(
        ("number", "match"),
        [
            pytest.param(42, MATCH, id="with-correlation-key"),
            pytest.param(None, {"repository": "my-org/configs"}, id="without-correlation-key"),
        ],
    )
    def test_a_new_try_cannot_claim_a_delivery_that_recorded_events_from_its_earlier_try(
        self, events_client, session, make_ti, dag_maker, events, number, match
    ):
        """Without the earlier try's wait, the delivery would have been accepted, not kept early."""
        earlier = make_ti(TaskInstanceState.RUNNING)
        add_wait(session, earlier.id, match=match)
        subscribe(dag_maker, events)
        assert post(events_client, number=number).json()["outcome"] == "resumed_wait"
        (record,) = get_deliveries(session)
        events_key = "asset_events" if events == "topic" else "asset_routes"
        recorded_events = record.actions[events_key]
        retry = earlier.prepare_db_for_next_try(session)
        retry.state = TaskInstanceState.RUNNING
        session.commit()

        wait = register(session, retry.id, match=match)

        assert (wait.status, wait.delivery_id) == (WaitStatus.REGISTERED, None)
        record = read(session, InboundDelivery, record.id)
        assert (record.outcome, record.actions) == ("accepted", {events_key: recorded_events})
        assert count_asset_events(session) == 1

    @pytest.mark.parametrize("events", ["route"])
    def test_a_wait_that_claims_an_early_delivery_keeps_the_asset_routes_it_skipped(
        self, events_client, session, make_ti
    ):
        assert post(events_client).json()["outcome"] == "early"
        (record,) = get_deliveries(session)
        first_actions = record.actions
        assert set(first_actions) == {"skipped_asset_routes"}
        ti = make_ti(TaskInstanceState.RUNNING)

        wait = register(session, ti.id)

        record = read(session, InboundDelivery, record.id)
        assert (record.outcome, record.actions) == (
            DeliveryOutcome.RESUMED_WAIT,
            {
                "skipped_asset_routes": first_actions["skipped_asset_routes"],
                "waits": [build_wait_action(wait, ti)],
            },
        )

    @pytest.mark.parametrize("events", ["topic", "route"])
    @pytest.mark.parametrize("waiting", [False, True], ids=["no-wait", "wait-registered-since"])
    @pytest.mark.parametrize("dag_since", ["subscribed", "unpaused"])
    def test_a_redelivery_of_an_early_delivery_is_processed_again_in_full(
        self, events_client, session, make_ti, dag_maker, events, waiting, dag_since
    ):
        """It recorded no events, so a Dag that subscribed or was unpaused since then receives it, once."""
        ti = make_ti()
        if dag_since == "unpaused":
            subscribe(dag_maker, events, is_paused=True)
        assert post(events_client).json()["outcome"] == "early"
        if dag_since == "unpaused":
            dag_maker.dag_model.is_paused = False
            dag_maker.session.commit()
        else:
            subscribe(dag_maker, events)
        if waiting:
            wait = add_wait(session, ti.id)

        responses = [post(events_client), post(events_client)]

        expected_outcome = "resumed_wait" if waiting else "accepted"
        assert [(response.status_code, response.json()["outcome"]) for response in responses] == [
            (202, expected_outcome),
            (200, "duplicate"),
        ]
        (record,) = get_deliveries(session)
        events_key = "asset_events" if events == "topic" else "asset_routes"
        # The asset routes the first copy skipped are no longer listed.
        assert (record.outcome, set(record.actions)) == (
            expected_outcome,
            {events_key, "waits"} if waiting else {events_key},
        )
        if waiting:
            assert record.actions["waits"] == [build_wait_action(wait, ti)]
        assert count_asset_events(session) == 1
        assert session.scalars(select(AssetDagRunQueue.target_dag_id)).all() == [f"on_{events}"]
        assert [json.loads(audit.extra)["outcome"] for audit in get_audit_logs(session)] == (
            ["resumed_wait", "resumed_wait"] if waiting else ["accepted"]
        )

    def test_at_the_cap_the_oldest_live_early_delivery_expires_and_cannot_be_claimed(
        self, session, make_ti, time_machine
    ):
        client = TestClient(create_webhooks_app(replace(REGISTRY, early_delivery_max_per_webhook=2)))
        past_ttl = add_early_delivery(session, received_at=NOW - EARLY_DELIVERY_TTL - timedelta(seconds=1))
        for number in (1, 2, 3):
            time_machine.move_to(NOW + timedelta(seconds=number), tick=False)
            assert post(client, number=number, delivery_id=f"delivery-{number}").json()["outcome"] == "early"

        assert [
            (record.delivery_id, record.outcome, record.payload is None) for record in get_deliveries(session)
        ] == [
            ("early-1", "early", False),
            ("delivery-1", "expired", True),
            ("delivery-2", "early", False),
            ("delivery-3", "early", False),
        ]
        assert read(session, InboundDelivery, past_ttl.id).outcome == DeliveryOutcome.EARLY
        assert (
            register(session, make_ti(TaskInstanceState.RUNNING).id, number=1).status == WaitStatus.REGISTERED
        )

    @pytest.mark.parametrize(
        ("max_expired", "expected_outcomes"),
        [
            pytest.param(100, ["expired", "expired", "early", "early"], id="the-excess"),
            pytest.param(1, ["expired", "early", "early", "early"], id="at-most-max-expired"),
        ],
    )
    def test_over_the_cap_the_excess_of_early_deliveries_expires(
        self, session, max_expired, expected_outcomes
    ):
        """Concurrent deliveries each count before the others commit, so they can leave one too many."""
        client = TestClient(create_webhooks_app(replace(REGISTRY, early_delivery_max_per_webhook=2)))
        for minutes in (3, 2, 1):
            add_early_delivery(
                session, received_at=NOW - timedelta(minutes=minutes), delivery_id=f"early-{minutes}"
            )

        with mock.patch.object(wait_matching, "MAX_EXPIRED_PER_DELIVERY", max_expired):
            assert post(client).json()["outcome"] == "early"

        assert [record.outcome for record in get_deliveries(session)] == expected_outcomes

    @pytest.mark.backend("postgres", "mysql")
    def test_at_the_cap_a_delivery_expires_the_oldest_early_delivery_that_no_other_holds(self, session):
        """Concurrent deliveries at the cap expire different early deliveries."""
        client = TestClient(create_webhooks_app(replace(REGISTRY, early_delivery_max_per_webhook=2)))
        oldest = add_early_delivery(session, received_at=NOW - timedelta(minutes=2), delivery_id="oldest")
        add_early_delivery(session, delivery_id="next-oldest")

        with create_session(scoped=False) as other_session:
            other_session.execute(
                select(InboundDelivery.id).where(InboundDelivery.id == oldest.id).with_for_update()
            )
            assert post(client).json()["outcome"] == "early"

        assert [(record.delivery_id, record.outcome) for record in get_deliveries(session)] == [
            ("oldest", "early"),
            ("next-oldest", "expired"),
            ("delivery-1", "early"),
        ]

    @pytest.mark.backend("sqlite")
    def test_an_early_delivery_that_a_wait_claims_while_it_is_expired_stays_claimed(self, session):
        """Without row locks, as on SQLite, the compare-and-set on its outcome keeps it claimed."""
        oldest = add_early_delivery(session, received_at=NOW - timedelta(minutes=2), delivery_id="oldest")
        add_early_delivery(session, delivery_id="newer")
        scalars = session.scalars

        def select_then_claim_elsewhere(*args, **kwargs):
            selected = list(scalars(*args, **kwargs))
            with create_session(scoped=False) as other_session:
                other_session.execute(
                    update(InboundDelivery)
                    .where(InboundDelivery.id == oldest.id)
                    .values(outcome=DeliveryOutcome.RESUMED_WAIT, actions=OTHER_WAIT_ACTIONS)
                )
            return selected

        with mock.patch.object(session, "scalars", autospec=True, side_effect=select_then_claim_elsewhere):
            expire_oldest_early_deliveries(
                "fake", cutoff=NOW - EARLY_DELIVERY_TTL, max_per_webhook=2, session=session
            )
        session.commit()

        record = read(session, InboundDelivery, oldest.id)
        assert (record.outcome, record.payload, record.actions) == (
            DeliveryOutcome.RESUMED_WAIT,
            PAYLOAD,
            OTHER_WAIT_ACTIONS,
        )

    @pytest.mark.parametrize("no_longer_claimable", ["past-ttl", "expired"])
    def test_a_redelivery_of_a_delivery_kept_early_goes_to_a_wait_registered_since(
        self, client, session, make_ti, time_machine, no_longer_claimable
    ):
        assert post(client).json()["outcome"] == "early"
        (record,) = get_deliveries(session)
        if no_longer_claimable == "past-ttl":
            time_machine.move_to(NOW + EARLY_DELIVERY_TTL + timedelta(seconds=1), tick=False)
        else:
            session.execute(
                update(InboundDelivery)
                .where(InboundDelivery.id == record.id)
                .values(outcome=DeliveryOutcome.EXPIRED, payload=None)
            )
            session.commit()
        ti = make_ti(TaskInstanceState.RUNNING)
        wait = register(session, ti.id)
        assert wait.status == WaitStatus.REGISTERED
        session.execute(
            update(TaskInstance)
            .where(TaskInstance.id == ti.id)
            .values(state=TaskInstanceState.AWAITING_INPUT, next_method="execute_complete", next_kwargs={})
        )
        session.commit()

        response = post(client)

        assert (response.status_code, response.json()["outcome"]) == (202, "resumed_wait")
        (record,) = get_deliveries(session)
        assert (record.outcome, record.payload) == ("resumed_wait", PAYLOAD)
        assert read(session, InboundWait, wait.id).delivery_id == record.id
        # The event tells when the delivery was first received, not redelivered.
        assert read(session, TaskInstance, ti.id).next_kwargs == {"event": RESUME_EVENT}

    @pytest.mark.parametrize(
        ("first_outcome", "first_received_at", "expected_other_outcome"),
        [
            pytest.param(DeliveryOutcome.EARLY, NOW - timedelta(minutes=1), "early", id="still-early"),
            pytest.param(DeliveryOutcome.EXPIRED, NOW - EARLY_DELIVERY_TTL, "early", id="expired-past-ttl"),
            pytest.param(
                DeliveryOutcome.EXPIRED, NOW - timedelta(minutes=1), "expired", id="expired-within-ttl"
            ),
        ],
    )
    def test_a_redelivery_counts_against_the_cap_only_if_it_becomes_a_live_early_delivery_again(
        self, session, first_outcome, first_received_at, expected_other_outcome
    ):
        """
        A delivery is early for its TTL from when it was first received, whenever it is redelivered.

        One that was early already was counted when it was first received.
        """
        client = TestClient(create_webhooks_app(replace(REGISTRY, early_delivery_max_per_webhook=1)))
        add_early_delivery(
            session,
            received_at=first_received_at,
            delivery_id="delivery-1",
            body_digest=hashlib.sha256(build_body(42)).hexdigest(),
            outcome=first_outcome,
        )
        add_early_delivery(session, received_at=NOW - timedelta(seconds=30), delivery_id="other")

        assert post(client).json()["outcome"] == "early"

        assert [(row.delivery_id, row.outcome) for row in get_deliveries(session)] == [
            ("delivery-1", "early"),
            ("other", expected_other_outcome),
        ]

    def test_a_copy_that_finds_a_dag_the_copy_kept_early_first_did_not_records_its_event_once(
        self, client, session, dag_maker
    ):
        """It rolls back what it recorded as a first delivery, and is processed again as a redelivery."""
        assert post(client).json()["outcome"] == "early"
        subscribe(dag_maker)
        find_delivery = pipeline.find_delivery

        def miss_the_first_lookup(*args, **kwargs):
            # As when this copy looked before the first copy had committed.
            return None if mock_find_delivery.call_count == 1 else find_delivery(*args, **kwargs)

        with mock.patch.object(
            pipeline, "find_delivery", autospec=True, side_effect=miss_the_first_lookup
        ) as mock_find_delivery:
            response = post(client)

        assert (response.status_code, response.json()["outcome"]) == (202, "accepted")
        (record,) = get_deliveries(session)
        (asset_event,) = session.scalars(select(AssetEvent))
        assert (record.outcome, record.actions) == (
            "accepted",
            {
                "asset_events": [
                    {"asset": TOPIC, "asset_id": asset_event.asset_id, "event_id": asset_event.id}
                ]
            },
        )
        assert [json.loads(audit.extra)["outcome"] for audit in get_audit_logs(session)] == ["accepted"]

    def test_an_early_delivery_whose_second_look_for_waits_fails_is_kept_for_a_retry(
        self, client, session, make_ti
    ):
        """Its record has committed, so it is answered 503 for its sender to offer it to the waits again."""
        with mock.patch.object(
            pipeline,
            "hand_to_waits_after_commit",
            autospec=True,
            side_effect=OperationalError("SELECT", {}, Exception("Lock wait timeout exceeded")),
        ):
            assert post(client).status_code == 503

        (record,) = get_deliveries(session)
        assert (record.outcome, record.payload) == ("early", PAYLOAD)
        ti = make_ti()
        wait = add_wait(session, ti.id)

        assert post(client).json()["outcome"] == "resumed_wait"
        assert read(session, InboundWait, wait.id).delivery_id == record.id

    @mock.patch.object(pipeline, "find_matching_waits", autospec=True, return_value=[])
    def test_an_early_delivery_goes_to_a_wait_registered_while_it_was_recorded(
        self, _, client, session, make_ti
    ):
        ti = make_ti(TaskInstanceState.RUNNING)
        wait = add_wait(session, ti.id)

        response = post(client)

        assert response.json()["outcome"] == "resumed_wait"
        (record,) = get_deliveries(session)
        assert (record.outcome, record.payload, record.actions) == (
            "resumed_wait",
            PAYLOAD,
            {"waits": [build_wait_action(wait, ti)]},
        )
        wait = read(session, InboundWait, wait.id)
        assert (wait.status, wait.delivery_id) == (WaitStatus.RECEIVED, record.id)
        assert len(get_audit_logs(session)) == 1


class TestHandToWaitsAfterCommit:
    def test_hands_the_delivery_to_each_wait_that_can_take_it(self, session, make_ti):
        record = add_early_delivery(session)
        parked = make_ti(dag_id="parked")
        running = make_ti(TaskInstanceState.RUNNING, dag_id="running")
        waits = {parked.id: add_wait(session, parked.id), running.id: add_wait(session, running.id, match={})}

        assert hand_to_waits_after_commit(record.id, cutoff=NOW - EARLY_DELIVERY_TTL, session=session) is True
        session.commit()

        tis = sorted([parked, running], key=lambda ti: str(ti.id))
        record = read(session, InboundDelivery, record.id)
        assert (record.outcome, record.actions) == (
            DeliveryOutcome.RESUMED_WAIT,
            {"waits": [build_wait_action(waits[ti.id], ti) for ti in tis]},
        )
        assert read(session, InboundWait, waits[parked.id].id).status == WaitStatus.RESUMED
        assert read(session, InboundWait, waits[running.id].id).status == WaitStatus.RECEIVED

    @pytest.mark.parametrize(
        "record_state",
        [
            pytest.param({"outcome": DeliveryOutcome.RESUMED_WAIT}, id="claimed"),
            pytest.param({"received_at": NOW - EARLY_DELIVERY_TTL}, id="past-ttl"),
            pytest.param(None, id="removed"),
        ],
    )
    def test_leaves_a_delivery_that_is_no_longer_early(self, session, make_ti, record_state):
        record = add_early_delivery(session)
        record_id = record.id
        if record_state is None:
            clear_db_inbound_deliveries()
        else:
            session.execute(
                update(InboundDelivery).where(InboundDelivery.id == record_id).values(**record_state)
            )
            session.commit()
        wait = add_wait(session, make_ti().id)

        assert (
            hand_to_waits_after_commit(record_id, cutoff=NOW - EARLY_DELIVERY_TTL, session=session) is False
        )
        assert read(session, InboundWait, wait.id).status == WaitStatus.REGISTERED

    def test_ends_a_wait_whose_task_can_no_longer_resume(self, session, make_ti):
        record = add_early_delivery(session)
        wait = add_wait(session, make_ti(TaskInstanceState.FAILED).id)

        assert (
            hand_to_waits_after_commit(record.id, cutoff=NOW - EARLY_DELIVERY_TTL, session=session) is False
        )
        session.commit()

        assert read(session, InboundWait, wait.id).status == WaitStatus.ENDED
        assert read(session, InboundDelivery, record.id).outcome == DeliveryOutcome.EARLY

    @mock.patch.object(pipeline, "find_matching_waits", autospec=True, return_value=[])
    @mock.patch.object(wait_matching, "find_matching_waits", autospec=True)
    def test_a_delivery_claimed_after_its_waits_took_it_is_left_to_the_claim(
        self, find_waits, _, client, session, make_ti
    ):
        """The waits take the delivery before its record is claimed, so losing that claim undoes theirs."""
        ti = make_ti()
        wait = add_wait(session, ti.id)

        def find_then_claim_elsewhere(webhook, attributes, *, session) -> list[WaitCandidate]:
            found = FIND_MATCHING_WAITS(webhook, attributes, session=session)
            with create_session(scoped=False) as other_session:
                other_session.execute(
                    update(InboundDelivery).values(
                        outcome=DeliveryOutcome.RESUMED_WAIT, actions=OTHER_WAIT_ACTIONS
                    )
                )
            return found

        find_waits.side_effect = find_then_claim_elsewhere

        assert post(client).json()["outcome"] == "early"

        (record,) = get_deliveries(session)
        assert record.actions == OTHER_WAIT_ACTIONS
        assert read(session, InboundWait, wait.id).status == WaitStatus.REGISTERED
        assert read(session, TaskInstance, ti.id).state == TaskInstanceState.AWAITING_INPUT
        assert get_audit_logs(session) == []


class TestFindMatchingWaits:
    @pytest.mark.parametrize(
        ("extra_attributes", "expected"),
        [
            pytest.param(0, ["on_key_digest"], id="few-attributes"),
            pytest.param(
                wait_matching.MAX_KEY_DIGEST_ATTRIBUTES,
                ["on_key_digest", "off_key_digest"],
                id="many-attributes",
            ),
        ],
    )
    def test_looks_at_the_waits_whose_key_sets_the_delivery_has(
        self, session, make_ti, extra_attributes, expected
    ):
        on_key_digest = add_wait(session, make_ti(dag_id="on_key_digest").id)
        # A key digest that no key set has, which only evaluating every open wait of the webhook finds.
        off_key_digest = add_wait(
            session, make_ti(dag_id="off_key_digest").id, events=None, key_digest="0" * 64
        )
        names = {on_key_digest.id: "on_key_digest", off_key_digest.id: "off_key_digest"}
        attributes = {**ATTRIBUTES, **{f"extra_{index}": "x" for index in range(extra_attributes)}}

        found = find_matching_waits("fake", attributes, session=session)

        assert sorted(names[wait.id] for wait in found) == sorted(expected)
        assert [wait.ti_id for wait in found] == sorted(wait.ti_id for wait in found)
