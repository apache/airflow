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

import json
import os
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any
from unittest import mock
from uuid import UUID, uuid4

import pytest
from sqlalchemy import select, update

from airflow._shared.webhook_naming.matching import compute_key_digest, compute_match_digest
from airflow.api_fastapi.auth.tokens import JWTValidator
from airflow.api_fastapi.execution_api.app import lifespan
from airflow.api_fastapi.execution_api.routes import inbound_waits
from airflow.api_fastapi.execution_api.security import require_auth
from airflow.models.dag import DagModel
from airflow.models.dagbundle import DagBundleModel
from airflow.models.inbound_delivery import DeliveryOutcome, InboundDelivery
from airflow.models.inbound_wait import InboundWait, WaitStatus
from airflow.models.log import Log
from airflow.models.taskinstance import TaskInstance
from airflow.models.team import Team
from airflow.providers_manager import ProvidersManager
from airflow.utils.session import create_session
from airflow.utils.state import TaskInstanceState
from airflow.webhooks.connections import ResolutionFailure, UnresolvedWebhook

from tests_common.test_utils.config import conf_vars
from tests_common.test_utils.db import (
    clear_db_dag_bundles,
    clear_db_dags,
    clear_db_inbound_deliveries,
    clear_db_logs,
    clear_db_runs,
    clear_db_teams,
)
from tests_common.test_utils.webhooks import FakeHmacSource, build_conn_env

if TYPE_CHECKING:
    from fastapi import FastAPI
    from fastapi.testclient import TestClient
    from sqlalchemy.orm import Session

    from tests_common.pytest_plugin import CreateTaskInstance

pytestmark = pytest.mark.db_test

NOW = datetime(2026, 10, 20, 12, 0, tzinfo=UTC)
EVENTS = ["pull_request"]
MATCH = {"repository": "my-org/configs", "number": 42}
WAIT: dict[str, Any] = {"webhook": "fake", "events": EVENTS, "match": MATCH, "timeout": "PT1H"}
#: The attributes of a delivery that WAIT matches.
MATCHING_ATTRIBUTES = {
    "event": "pull_request",
    "action": "closed",
    "repository": "my-org/configs",
    "number": 42,
}
CLAIM_EARLY_DELIVERY_AFTER_COMMIT = inbound_waits.claim_early_delivery_after_commit


@pytest.fixture(autouse=True)
def frozen_time(time_machine):
    time_machine.move_to(NOW, tick=False)


@pytest.fixture(autouse=True)
def clean_db():
    clear_db_runs()
    clear_db_inbound_deliveries()
    clear_db_logs()
    yield
    clear_db_runs()
    clear_db_inbound_deliveries()
    clear_db_logs()


@pytest.fixture(autouse=True)
def fake_webhook():
    """Serve webhook ``fake``, whose connection uses the core tests' source type."""
    with (
        conf_vars({("webhooks", "names"): "fake,unconfigured,misconfigured"}),
        mock.patch.dict(
            os.environ, {**build_conn_env("fake"), **build_conn_env("misconfigured", conn_type="http")}
        ),
        mock.patch.object(
            ProvidersManager, "get_webhook_source_class", autospec=True, return_value=FakeHmacSource
        ),
    ):
        yield


@pytest.fixture
def move_dag_to_bundle(session: Session):
    """Move a Dag to a new bundle of ``team_name``, or of no team if it is None."""

    def move(dag_id: str, team_name: str | None) -> None:
        bundle = DagBundleModel(name="team-bundle")
        if team_name is not None:
            bundle.teams.append(Team(name=team_name))
        session.add(bundle)
        session.flush()
        session.execute(update(DagModel).where(DagModel.dag_id == dag_id).values(bundle_name=bundle.name))
        session.commit()

    yield move
    clear_db_runs()
    clear_db_dags()
    clear_db_dag_bundles()
    clear_db_teams()


@pytest.fixture
def running_ti(create_task_instance: CreateTaskInstance, session: Session) -> TaskInstance:
    ti = create_task_instance(state=TaskInstanceState.RUNNING)
    session.commit()
    return ti


def register(client: TestClient, ti_id: UUID, **overrides: Any):
    return client.post(f"/execution/inbound-waits/{ti_id}", json=WAIT | overrides)


def authenticate_as(exec_app: FastAPI, ti_id: UUID) -> None:
    """Authenticate requests with ``require_auth`` and a task token for ``ti_id``, not the test stand-in."""
    exec_app.dependency_overrides.pop(require_auth, None)
    validator = mock.AsyncMock(spec=JWTValidator)
    validator.avalidated_claims.return_value = {
        "sub": str(ti_id),
        "scope": "execution",
        "exp": 9999999999,
        "iat": 1000000000,
        "nbf": 1000000000,
    }
    lifespan.registry.register_value(JWTValidator, validator)


def read_waits(session: Session, ti_id: UUID) -> list[InboundWait]:
    session.expire_all()
    return list(
        session.scalars(
            select(InboundWait)
            .where(InboundWait.ti_id == ti_id)
            .order_by(InboundWait.created_at, InboundWait.id)
        )
    )


def add_early_delivery(
    session: Session,
    *,
    delivery_id: str = "early-1",
    webhook: str = "fake",
    received_at: datetime = NOW - timedelta(minutes=1),
    attributes: dict[str, Any] | None = None,
    outcome: DeliveryOutcome = DeliveryOutcome.EARLY,
    correlation_key: str | None = "my-org/configs#42",
    actions: dict[str, Any] | None = None,
) -> UUID:
    record = InboundDelivery(
        webhook=webhook,
        delivery_id=delivery_id,
        body_digest="0" * 64,
        received_at=received_at,
        retain_until=received_at + timedelta(days=7),
        source_type="fake",
        attributes=MATCHING_ATTRIBUTES if attributes is None else attributes,
        correlation_key=correlation_key,
        payload={"pull_request": {"merged": True}},
        outcome=outcome,
        actions=actions,
    )
    session.add(record)
    session.commit()
    return record.id


def read_delivery(session: Session, record_id: UUID) -> InboundDelivery:
    session.expire_all()
    return session.get_one(InboundDelivery, record_id)


class TestRegisterInboundWait:
    @pytest.mark.parametrize(
        ("overrides", "stored_events", "stored_match"),
        [
            pytest.param({}, EVENTS, MATCH, id="events-and-match"),
            pytest.param({"events": None}, None, MATCH, id="any-event"),
            pytest.param({"match": None}, EVENTS, {}, id="no-match"),
        ],
    )
    def test_registers_a_wait(self, client, session, running_ti, overrides, stored_events, stored_match):
        response = register(client, running_ti.id, **overrides)

        assert response.status_code == 201, response.json()
        (wait,) = read_waits(session, running_ti.id)
        assert response.json() == {
            "id": str(wait.id),
            "status": "registered",
            "deadline": "2026-10-20T13:00:00Z",
        }
        request = WAIT | overrides
        assert (wait.webhook, wait.events, wait.match) == ("fake", stored_events, stored_match)
        assert wait.key_digest == compute_key_digest(request["match"])
        assert wait.match_digest == compute_match_digest(request["events"], request["match"])
        assert (wait.open, wait.status, wait.delivery_id) == (True, WaitStatus.REGISTERED, None)
        assert wait.deadline == NOW + timedelta(hours=1)

    @conf_vars({("webhooks", "default_wait_timeout"): "600"})
    def test_a_wait_without_timeout_uses_the_default(self, client, session, running_ti):
        response = register(client, running_ti.id, timeout=None)

        assert response.status_code == 201, response.json()
        assert response.json()["deadline"] == "2026-10-20T12:10:00Z"

    def test_accepts_the_longest_timeout(self, client, session, running_ti):
        response = register(client, running_ti.id, timeout="P365D")

        assert response.status_code == 201, response.json()
        assert response.json()["deadline"] == "2027-10-20T12:00:00Z"
        (wait,) = read_waits(session, running_ti.id)
        assert wait.deadline == NOW + timedelta(days=365)

    def test_refuses_a_wait_without_timeout_when_there_is_no_default(self, client, session, running_ti):
        response = register(client, running_ti.id, timeout=None)

        assert response.status_code == 422
        assert response.json()["detail"]["reason"] == "timeout_required"
        assert read_waits(session, running_ti.id) == []

    @pytest.mark.parametrize(
        ("overrides", "message"),
        [
            pytest.param({"match": {"number": {"gt": 4}}}, "reserved for operators", id="dict-value"),
            pytest.param({"events": []}, "events must not be empty", id="empty-events"),
            pytest.param({"timeout": 0}, "timeout must be positive", id="zero-timeout"),
            pytest.param(
                {"timeout": "P366D"}, "timeout must be 365 days or less", id="timeout-above-maximum"
            ),
            pytest.param({"unknown": 1}, "Extra inputs are not permitted", id="extra-field"),
        ],
    )
    def test_refuses_an_invalid_wait(self, client, session, running_ti, overrides, message):
        response = register(client, running_ti.id, **overrides)

        assert response.status_code == 422
        assert message in response.json()["detail"][0]["msg"]
        assert read_waits(session, running_ti.id) == []

    @pytest.mark.parametrize(
        "webhook",
        [
            pytest.param("unlisted", id="not-listed"),
            pytest.param("unconfigured", id="no-connection"),
            pytest.param("misconfigured", id="connection-of-another-type"),
        ],
    )
    def test_refuses_a_webhook_that_is_not_served(self, client, session, running_ti, webhook):
        response = register(client, running_ti.id, webhook=webhook)

        assert response.status_code == 404
        assert response.json()["detail"]["reason"] == "webhook_not_found"
        assert read_waits(session, running_ti.id) == []

    @mock.patch.object(
        inbound_waits,
        "resolve_webhook",
        autospec=True,
        return_value=UnresolvedWebhook(failure=ResolutionFailure.UNAVAILABLE, reason="backend down"),
    )
    def test_answers_503_while_the_connection_cannot_be_read(self, _, client, session, running_ti):
        response = register(client, running_ti.id)

        assert response.status_code == 503
        assert response.json()["detail"]["reason"] == "webhook_unavailable"
        assert read_waits(session, running_ti.id) == []

    @pytest.mark.parametrize(
        "state",
        [
            TaskInstanceState.QUEUED,
            TaskInstanceState.AWAITING_INPUT,
            TaskInstanceState.DEFERRED,
            TaskInstanceState.SUCCESS,
        ],
    )
    def test_refuses_a_task_instance_that_is_not_running(self, client, session, create_task_instance, state):
        ti = create_task_instance(state=state)
        session.commit()

        response = register(client, ti.id)

        assert response.status_code == 409
        assert response.json()["detail"]["reason"] == "invalid_state"
        assert response.json()["detail"]["previous_state"] == state
        assert read_waits(session, ti.id) == []

    @pytest.mark.parametrize(
        "overrides",
        [
            pytest.param({"webhook": "unlisted"}, id="webhook-not-served"),
            pytest.param({"timeout": None}, id="no-timeout-and-no-default"),
        ],
    )
    def test_refuses_an_archived_attempt_before_reading_its_wait(
        self, client, exec_app, session, running_ti, overrides
    ):
        running_ti.prepare_db_for_next_try(session)
        session.commit()
        authenticate_as(exec_app, running_ti.id)

        response = register(client, running_ti.id, **overrides)

        assert response.status_code == 410
        assert response.json()["detail"]["reason"] == "not_found"
        assert read_waits(session, running_ti.id) == []

    def test_refuses_an_attempt_archived_after_it_was_admitted(self, client, session, running_ti):
        """The test client's stand-in for ``require_auth`` admits the attempt, as before a racing archival."""
        running_ti.prepare_db_for_next_try(session)
        session.commit()

        response = register(client, running_ti.id)

        assert response.status_code == 410
        assert response.json()["detail"]["reason"] == "not_found"
        assert read_waits(session, running_ti.id) == []

    def test_refuses_an_unknown_task_instance(self, client):
        response = register(client, uuid4())

        assert response.status_code == 404
        assert response.json()["detail"]["reason"] == "not_found"

    def test_refuses_a_token_for_another_task_instance(
        self, client, exec_app, session, create_task_instance, running_ti
    ):
        other = create_task_instance(dag_id="other", state=TaskInstanceState.RUNNING)
        session.commit()
        authenticate_as(exec_app, other.id)

        response = register(client, running_ti.id)

        assert response.status_code == 403
        assert read_waits(session, running_ti.id) == []

    @pytest.mark.parametrize(
        ("team_name", "expected_status"),
        [pytest.param("analytics", 403, id="dag-with-team"), pytest.param(None, 201, id="dag-without-team")],
    )
    @conf_vars({("core", "multi_team"): "True"})
    def test_in_multi_team_mode_serves_only_dags_without_a_team(
        self, client, session, move_dag_to_bundle, running_ti, team_name, expected_status
    ):
        move_dag_to_bundle(running_ti.dag_id, team_name)

        response = register(client, running_ti.id)

        assert response.status_code == expected_status, response.json()
        if team_name is not None:
            assert response.json()["detail"]["reason"] == "dag_has_team"
            assert "'analytics'" in response.json()["detail"]["message"]
        assert len(read_waits(session, running_ti.id)) == (expected_status == 201)


class TestOneKeyOneTask:
    @pytest.mark.parametrize("holder_state", [TaskInstanceState.RUNNING, TaskInstanceState.AWAITING_INPUT])
    def test_refuses_the_key_of_a_live_task_instance(
        self, client, session, create_task_instance, running_ti, holder_state
    ):
        holder = create_task_instance(dag_id="holder", state=TaskInstanceState.RUNNING)
        session.commit()
        assert register(client, holder.id).status_code == 201
        session.execute(update(TaskInstance).where(TaskInstance.id == holder.id).values(state=holder_state))
        session.commit()

        response = register(client, running_ti.id)

        assert response.status_code == 409
        assert response.json()["detail"]["reason"] == "wait_key_taken"
        assert read_waits(session, running_ti.id) == []
        (held,) = read_waits(session, holder.id)
        assert (held.open, held.status) == (True, WaitStatus.REGISTERED)

    def test_a_retry_takes_over_the_key_of_its_earlier_try(self, client, session, running_ti):
        assert register(client, running_ti.id).status_code == 201
        retry = running_ti.prepare_db_for_next_try(session)
        retry.state = TaskInstanceState.RUNNING
        session.commit()

        response = register(client, retry.id)

        assert response.status_code == 201, response.json()
        (earlier,) = read_waits(session, running_ti.id)
        assert (earlier.open, earlier.status, earlier.ended_at) == (None, WaitStatus.ENDED, NOW)
        (current,) = read_waits(session, retry.id)
        assert (current.open, current.status) == (True, WaitStatus.REGISTERED)

    def test_a_retry_claims_the_delivery_its_earlier_try_received(self, client, session, running_ti):
        """The earlier try never resumed with the delivery, so the delivery is early again for the retry."""
        record_id = add_early_delivery(session)
        assert register(client, running_ti.id).json()["status"] == "received"
        retry = running_ti.prepare_db_for_next_try(session)
        retry.state = TaskInstanceState.RUNNING
        session.commit()

        response = register(client, retry.id)

        assert response.json()["status"] == "received"
        (earlier,) = read_waits(session, running_ti.id)
        assert (earlier.open, earlier.status) == (None, WaitStatus.ENDED)
        (current,) = read_waits(session, retry.id)
        assert (str(current.id), current.status, current.delivery_id) == (
            response.json()["id"],
            WaitStatus.RECEIVED,
            record_id,
        )
        record = read_delivery(session, record_id)
        assert record.outcome == DeliveryOutcome.RESUMED_WAIT
        assert [wait["ti_id"] for wait in record.actions["waits"]] == [str(retry.id)]
        audits = session.scalars(select(Log).where(Log.event == "webhook_delivery").order_by(Log.id))
        assert [audit.task_instance_id for audit in audits] == [running_ti.id, retry.id]

    def test_a_retry_leaves_a_received_delivery_without_correlation_key(self, client, session, running_ti):
        """Without the wait it would have recorded nothing, so it keeps the earlier try's wait."""
        record_id = add_early_delivery(session, correlation_key=None)
        assert register(client, running_ti.id).json()["status"] == "received"
        retry = running_ti.prepare_db_for_next_try(session)
        retry.state = TaskInstanceState.RUNNING
        session.commit()

        response = register(client, retry.id)

        assert response.json()["status"] == "registered"
        (current,) = read_waits(session, retry.id)
        assert current.delivery_id is None
        record = read_delivery(session, record_id)
        assert record.outcome == DeliveryOutcome.RESUMED_WAIT
        assert [wait["ti_id"] for wait in record.actions["waits"]] == [str(running_ti.id)]

    def test_takes_over_the_key_of_a_finished_task_instance(
        self, client, session, create_task_instance, running_ti
    ):
        holder = create_task_instance(dag_id="holder", state=TaskInstanceState.RUNNING)
        session.commit()
        assert register(client, holder.id).status_code == 201
        session.execute(
            update(TaskInstance).where(TaskInstance.id == holder.id).values(state=TaskInstanceState.FAILED)
        )
        session.commit()

        assert register(client, running_ti.id).status_code == 201
        (ended,) = read_waits(session, holder.id)
        assert (ended.open, ended.status) == (None, WaitStatus.ENDED)

    def test_waits_with_different_keys_are_both_open(self, client, session, create_task_instance, running_ti):
        other = create_task_instance(dag_id="other", state=TaskInstanceState.RUNNING)
        session.commit()

        assert register(client, other.id).status_code == 201
        assert register(client, running_ti.id, match={**MATCH, "number": 43}).status_code == 201
        assert [wait.open for wait in read_waits(session, other.id) + read_waits(session, running_ti.id)] == [
            True,
            True,
        ]


class TestLatestWaitWins:
    @pytest.mark.parametrize(
        "second", [pytest.param({}, id="same-key"), pytest.param({"events": ["push"]}, id="other-key")]
    )
    def test_a_second_wait_ends_the_first(self, client, session, running_ti, second):
        assert register(client, running_ti.id).status_code == 201

        response = register(client, running_ti.id, **second)

        assert response.status_code == 201, response.json()
        first, latest = read_waits(session, running_ti.id)
        assert (first.open, first.status, first.ended_at) == (None, WaitStatus.ENDED, NOW)
        assert (str(latest.id), latest.open, latest.status) == (
            response.json()["id"],
            True,
            WaitStatus.REGISTERED,
        )

    def test_a_second_wait_does_not_replay_the_delivery_of_the_first(self, client, session, running_ti):
        record_id = add_early_delivery(session)
        assert register(client, running_ti.id).json()["status"] == "received"

        response = register(client, running_ti.id)

        assert response.json()["status"] == "registered"
        first, latest = read_waits(session, running_ti.id)
        assert (first.status, first.delivery_id) == (WaitStatus.ENDED, record_id)
        assert (latest.status, latest.delivery_id) == (WaitStatus.REGISTERED, None)


class TestClaimEarlyDeliveries:
    @pytest.mark.parametrize(
        ("outcome", "actions"),
        [
            pytest.param(DeliveryOutcome.EARLY, None, id="early"),
            pytest.param(
                DeliveryOutcome.CANCEL_REQUESTED,
                {"cancel_requests": [{"request_id": "a-request", "dag_id": "cancellable", "scope": "run"}]},
                id="cancel-requested",
            ),
        ],
    )
    def test_claims_a_matching_early_delivery(self, client, session, running_ti, outcome, actions):
        record_id = add_early_delivery(session, outcome=outcome, actions=actions)

        response = register(client, running_ti.id)

        assert response.status_code == 201, response.json()
        assert response.json()["status"] == "received"
        (wait,) = read_waits(session, running_ti.id)
        assert (wait.open, wait.status, wait.delivery_id) == (True, WaitStatus.RECEIVED, record_id)
        record = read_delivery(session, record_id)
        assert record.outcome == DeliveryOutcome.RESUMED_WAIT
        assert record.actions == {
            **(actions or {}),
            "waits": [
                {
                    "wait_id": str(wait.id),
                    "ti_id": str(running_ti.id),
                    "dag_id": running_ti.dag_id,
                    "run_id": running_ti.run_id,
                    "task_id": running_ti.task_id,
                    "map_index": running_ti.map_index,
                }
            ],
        }
        (audit,) = session.scalars(select(Log).where(Log.event == "webhook_delivery"))
        assert (audit.owner, audit.owner_display_name, audit.task_instance_id) == (
            "webhook:fake",
            "Webhook fake",
            running_ti.id,
        )
        assert (audit.dag_id, audit.run_id, audit.task_id) == (
            running_ti.dag_id,
            running_ti.run_id,
            running_ti.task_id,
        )
        assert json.loads(audit.extra) == {
            "delivery_id": "early-1",
            "outcome": "resumed_wait",
            "wait_id": str(wait.id),
        }

    @pytest.mark.parametrize(
        "delivery",
        [
            pytest.param({"received_at": NOW - timedelta(seconds=600)}, id="older-than-ttl"),
            pytest.param({"attributes": {**MATCHING_ATTRIBUTES, "number": 43}}, id="other-attribute"),
            pytest.param({"attributes": {**MATCHING_ATTRIBUTES, "event": "push"}}, id="other-event"),
            pytest.param({"webhook": "other"}, id="other-webhook"),
            pytest.param({"outcome": DeliveryOutcome.NO_SUBSCRIBERS}, id="not-early"),
        ],
    )
    def test_leaves_deliveries_it_cannot_claim(self, client, session, running_ti, delivery):
        record_id = add_early_delivery(session, **delivery)
        outcome = delivery.get("outcome", DeliveryOutcome.EARLY)

        response = register(client, running_ti.id)

        assert response.json()["status"] == "registered"
        assert read_delivery(session, record_id).outcome == outcome

    def test_a_wait_for_any_event_claims_a_delivery_of_any_event(self, client, session, running_ti):
        record_id = add_early_delivery(session, attributes={**MATCHING_ATTRIBUTES, "event": "push"})

        assert register(client, running_ti.id, events=None).json()["status"] == "received"
        assert read_delivery(session, record_id).outcome == DeliveryOutcome.RESUMED_WAIT

    @conf_vars({("webhooks", "early_delivery_ttl"): "3600"})
    def test_early_delivery_ttl_bounds_the_claim(self, client, session, running_ti):
        record_id = add_early_delivery(session, received_at=NOW - timedelta(minutes=59))

        assert register(client, running_ti.id).json()["status"] == "received"
        assert read_delivery(session, record_id).outcome == DeliveryOutcome.RESUMED_WAIT

    def test_claims_the_oldest_matching_delivery(self, client, session, running_ti):
        newer = add_early_delivery(session, delivery_id="newer", received_at=NOW - timedelta(minutes=1))
        older = add_early_delivery(session, delivery_id="older", received_at=NOW - timedelta(minutes=2))

        assert register(client, running_ti.id).json()["status"] == "received"

        (wait,) = read_waits(session, running_ti.id)
        assert wait.delivery_id == older
        assert read_delivery(session, newer).outcome == DeliveryOutcome.EARLY

    @mock.patch.object(inbound_waits, "claim_early_delivery_after_commit", autospec=True)
    def test_claims_a_delivery_kept_early_while_the_wait_was_registered(
        self, claim_after_commit, client, session, running_ti
    ):
        """A delivery that commits between the registration and its re-check is claimed by the re-check."""
        record_ids = []

        def deliver_then_claim(wait_id, *, cutoff, session):
            with create_session(scoped=False) as delivery_session:
                record_ids.append(add_early_delivery(delivery_session))
            return CLAIM_EARLY_DELIVERY_AFTER_COMMIT(wait_id, cutoff=cutoff, session=session)

        claim_after_commit.side_effect = deliver_then_claim

        response = register(client, running_ti.id)

        assert response.json()["status"] == "received"
        (wait,) = read_waits(session, running_ti.id)
        assert (wait.status, wait.delivery_id) == (WaitStatus.RECEIVED, record_ids[0])
        assert read_delivery(session, record_ids[0]).outcome == DeliveryOutcome.RESUMED_WAIT
