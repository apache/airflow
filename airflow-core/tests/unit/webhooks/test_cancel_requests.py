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
"""How deliveries record cancel requests for the Dags whose ``cancel_on`` they match."""

from __future__ import annotations

import json
import os
import uuid
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any, NamedTuple
from unittest import mock

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import select

from airflow._shared.timezones import timezone
from airflow._shared.webhook_naming.matching import evaluate
from airflow.api_fastapi.webhooks.app import create_webhooks_app
from airflow.models.asset import AssetEvent
from airflow.models.inbound_delivery import DeliveryOutcome, InboundDelivery
from airflow.models.inbound_wait import InboundWait, WaitStatus
from airflow.models.webhook_cancel_request import CancelRequestStatus, WebhookCancelRequest
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers_manager import ProvidersManager
from airflow.sdk import Asset, WebhookMatch
from airflow.utils.state import TaskInstanceState
from airflow.webhooks import cancel_requests, pipeline
from airflow.webhooks.base import WebhookDelivery
from airflow.webhooks.cancel_requests import (
    CancelScope,
    build_cancel_requests,
    build_targets,
    find_matching_cancel_conditions,
)
from airflow.webhooks.connections import ConfiguredWebhook
from airflow.webhooks.naming import build_topic_name
from airflow.webhooks.registry import WebhookRegistry
from airflow.webhooks.wait_registration import lock_registering_task_instance, register_wait

from tests_common.test_utils.asserts import count_queries
from tests_common.test_utils.config import conf_vars
from tests_common.test_utils.db import (
    clear_db_assets,
    clear_db_dags,
    clear_db_inbound_deliveries,
    clear_db_logs,
    clear_db_runs,
)
from tests_common.test_utils.webhooks import (
    FakeWaitSource,
    build_conn_env,
    get_audit_logs,
    get_deliveries,
    post_signed_delivery,
    read,
    record_tables,
    team_bundle as team_bundle,
)

if TYPE_CHECKING:
    from httpx2 import Response
    from sqlalchemy.orm import Session

NOW = datetime(2026, 10, 23, 12, 0, tzinfo=UTC)
RETENTION = timedelta(days=7)
EARLY_DELIVERY_TTL = timedelta(minutes=10)
REGISTRY = WebhookRegistry(
    names=frozenset({"fake"}),
    source_type_owners={},
    connection_cache_ttl=60,
    max_body_bytes=1024,
    delivery_retention=RETENTION,
    rate_limit_per_minute=0,
    store_payload="full",
    max_event_payload_bytes=65536,
    early_delivery_ttl=EARLY_DELIVERY_TTL,
    early_delivery_max_per_webhook=1000,
)
#: Webhook ``fake``, for calling ``pipeline.record_delivery`` directly.
WEBHOOK = ConfiguredWebhook(
    name="fake",
    source_type="fake",
    source=FakeWaitSource(),
    secret="s3cret",
    max_body_bytes=1024,
    store_payload="full",
)
REPOSITORY = "my-org/web"
PAYLOAD = {"path": REPOSITORY, "repository": REPOSITORY, "number": 42}
#: The attributes FakeWaitSource parses from a pull_request delivery of PAYLOAD.
ATTRIBUTES: dict[str, str | int] = {"event": "pull_request", "repository": REPOSITORY, "number": 42}
DELIVERY = WebhookDelivery(
    path=REPOSITORY,
    kinds=[("pull_request",)],
    attributes=ATTRIBUTES,
    delivery_id="delivery-1",
    payload=PAYLOAD,
    correlation_key=f"{REPOSITORY}#42",
)
RECORD_ID = uuid.UUID("01a14ee2-0e00-7366-b2e2-38ec29a5a228")
#: A condition that the pull_request deliveries of PAYLOAD match.
ON_PULL_REQUEST = WebhookMatch("fake", events=["pull_request"], match={"repository": REPOSITORY})
#: ON_PULL_REQUEST, which a run triggered with the key in its conf key ``pr`` is about too.
ON_PULL_REQUEST_BY_CONF = WebhookMatch(
    "fake", events=["pull_request"], match={"repository": REPOSITORY}, correlation_key_conf="pr"
)
#: A condition that the deliveries of PAYLOAD do not match.
ON_PUSH = WebhookMatch("fake", events=["push"])
#: The topic that the pull_request deliveries of PAYLOAD offer.
TOPIC = build_topic_name("fake", REPOSITORY, "pull_request")


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


@pytest.fixture
def clean_db():
    def clean() -> None:
        clear_db_runs()
        # The cancel conditions and requests go with their Dags.
        clear_db_dags()
        clear_db_inbound_deliveries()
        clear_db_logs()
        clear_db_assets()

    clean()
    yield
    clean()


@pytest.fixture
def client() -> TestClient:
    return TestClient(create_webhooks_app(REGISTRY))


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


def declare(
    dag_maker,
    dag_id: str = "cancellable",
    *,
    cancel_on: list[WebhookMatch] | None = None,
    tasks: dict[str, dict[str, Any]] | None = None,
    **dag_kwargs: Any,
) -> None:
    """Add Dag ``dag_id`` with ``cancel_on``, and a task for each of ``tasks``, given those arguments."""
    with dag_maker(dag_id, schedule=None, cancel_on=cancel_on, **dag_kwargs):
        for task_id, task_kwargs in (tasks or {"task": {}}).items():
            EmptyOperator(task_id=task_id, **task_kwargs)


def subscribe(dag_maker, *, is_paused: bool = False) -> None:
    """Add Dag subscriber, which the deliveries of PAYLOAD record a topic event for unless it is paused."""
    with dag_maker("subscriber", schedule=Asset(name=TOPIC)):
        pass
    if is_paused:
        dag_maker.dag_model.is_paused = True
        dag_maker.session.commit()


def register(session: Session, ti_id) -> InboundWait:
    """Register a wait that the deliveries of PAYLOAD match for ``ti_id``, and commit it."""
    wait = register_wait(
        lock_registering_task_instance(ti_id, session=session),
        webhook="fake",
        events=["pull_request"],
        match={"repository": REPOSITORY, "number": 42},
        deadline=NOW + timedelta(days=1),
        early_delivery_cutoff=timezone.utcnow() - EARLY_DELIVERY_TTL,
        session=session,
    )
    session.commit()
    return wait


def get_requests(session: Session) -> list[WebhookCancelRequest]:
    session.expire_all()
    return list(
        session.scalars(
            select(WebhookCancelRequest).order_by(WebhookCancelRequest.dag_id, WebhookCancelRequest.id)
        )
    )


def build_actions(requests: list[WebhookCancelRequest], scopes: list[str]) -> list[dict[str, str]]:
    return [
        {"request_id": str(request.id), "dag_id": request.dag_id, "scope": scope}
        for request, scope in zip(requests, scopes, strict=True)
    ]


class Condition(NamedTuple):
    """A row of the conditions that a delivery matched, as build_targets reads it."""

    task_id: str
    correlation_key_conf: str | None = None
    cancel_state: str | None = None


class TestCancelScope:
    @pytest.mark.parametrize("scope", list(CancelScope))
    def test_formats_as_its_value(self, scope):
        assert str(scope) == f"{scope}" == scope.value


class TestBuildTargets:
    @pytest.mark.parametrize(
        ("conditions", "expected"),
        [
            pytest.param(
                [Condition("")],
                {"run": {"correlation_key_confs": []}, "tasks": []},
                id="dag-only",
            ),
            pytest.param(
                [
                    Condition("run_tests", cancel_state="skipped"),
                    Condition("deploy", cancel_state="failed"),
                    Condition("run_tests", cancel_state="skipped"),
                ],
                {
                    "run": None,
                    "tasks": [
                        {"task_id": "deploy", "cancel_state": "failed", "correlation_key_confs": []},
                        {"task_id": "run_tests", "cancel_state": "skipped", "correlation_key_confs": []},
                    ],
                },
                id="tasks-sorted-and-deduplicated",
            ),
            pytest.param(
                [Condition(""), Condition("deploy", cancel_state="failed")],
                {
                    "run": {"correlation_key_confs": []},
                    "tasks": [{"task_id": "deploy", "cancel_state": "failed", "correlation_key_confs": []}],
                },
                id="dag-and-task",
            ),
            pytest.param(
                [
                    Condition("", "pr"),
                    Condition(""),
                    Condition("", "change"),
                    Condition("", "pr"),
                    Condition("deploy", "pr", "failed"),
                    Condition("deploy", None, "failed"),
                ],
                {
                    "run": {"correlation_key_confs": ["change", "pr"]},
                    "tasks": [
                        {"task_id": "deploy", "cancel_state": "failed", "correlation_key_confs": ["pr"]}
                    ],
                },
                id="union-of-conf-keys",
            ),
        ],
    )
    def test_builds_the_run_and_each_task_with_their_conf_keys(self, conditions, expected):
        assert build_targets(conditions) == expected


@pytest.mark.db_test
@pytest.mark.usefixtures("clean_db")
class TestFindMatchingCancelConditions:
    @pytest.mark.parametrize(
        ("condition", "matched"),
        [
            pytest.param(ON_PULL_REQUEST, True, id="matching"),
            pytest.param(WebhookMatch("other", events=["pull_request"]), False, id="other-webhook"),
            pytest.param(ON_PUSH, False, id="other-event"),
            pytest.param(WebhookMatch("fake", match={"repository": "my-org/api"}), False, id="other-value"),
            pytest.param(WebhookMatch("fake", match={"label": "deploy"}), False, id="missing-attribute"),
            pytest.param(WebhookMatch("fake", match={"number": "42"}), True, id="string-against-integer"),
            pytest.param(WebhookMatch("fake", match={"repository": REPOSITORY}), True, id="any-event"),
        ],
    )
    def test_returns_the_conditions_of_its_webhook_that_the_delivery_matches(
        self, dag_maker, session, condition, matched
    ):
        declare(dag_maker, cancel_on=[condition], tasks={"deploy": {"cancel_on": [condition]}})

        found = find_matching_cancel_conditions("fake", ATTRIBUTES, multi_team=False, session=session)

        expected = [("cancellable", "", None), ("cancellable", "deploy", "failed")] if matched else []
        assert [(row.dag_id, row.task_id, row.cancel_state) for row in found] == expected

    @mock.patch.object(cancel_requests, "evaluate", autospec=True, side_effect=evaluate)
    def test_evaluates_each_distinct_condition_once(self, mock_evaluate, dag_maker, session):
        """A condition in default_args is stored for each task, and each task does not cost an evaluation."""
        declare(
            dag_maker,
            default_args={"cancel_on": [ON_PULL_REQUEST]},
            tasks={"deploy": {}, "run_tests": {}},
        )

        found = find_matching_cancel_conditions("fake", ATTRIBUTES, multi_team=False, session=session)

        assert [row.task_id for row in found] == ["deploy", "run_tests"]
        mock_evaluate.assert_called_once()


@pytest.mark.db_test
@pytest.mark.usefixtures("clean_db")
class TestBuildCancelRequests:
    @pytest.mark.parametrize(
        ("multi_team", "expected_dag_ids"),
        [
            pytest.param("true", ["teamless"], id="multi-team"),
            pytest.param("false", ["of_team_a", "teamless"], id="single-team"),
        ],
    )
    def test_under_multi_team_leaves_out_dags_of_a_team(
        self, team_bundle, dag_maker, session, multi_team, expected_dag_ids
    ):
        declare(dag_maker, "teamless", cancel_on=[ON_PULL_REQUEST])
        declare(dag_maker, "of_team_a", cancel_on=[ON_PULL_REQUEST], bundle_name=team_bundle)

        with conf_vars({("core", "multi_team"): multi_team}):
            requests = build_cancel_requests(
                "fake",
                DELIVERY,
                inbound_delivery_id=RECORD_ID,
                received_at=NOW,
                retain_until=NOW + RETENTION,
                reprocessed=False,
                session=session,
            )

        assert [request.dag_id for request in requests.new] == expected_dag_ids


@pytest.mark.db_test
@pytest.mark.usefixtures("clean_db")
class TestRecordCancelRequests:
    def test_a_keyed_delivery_records_one_request_per_matching_dag(self, client, session, dag_maker):
        declare(
            dag_maker,
            "deploy_preview",
            cancel_on=[ON_PULL_REQUEST],
            tasks={"run_tests": {"cancel_on": [ON_PULL_REQUEST_BY_CONF], "cancel_state": "skipped"}},
        )
        declare(dag_maker, "run_checks", tasks={"check": {"cancel_on": [ON_PULL_REQUEST]}})
        declare(dag_maker, "on_push", cancel_on=[ON_PUSH])

        response = post(client)

        assert (response.status_code, response.json()) == (
            202,
            {"delivery_id": "delivery-1", "outcome": "cancel_requested"},
        )
        (record,) = get_deliveries(session)
        requests = get_requests(session)
        assert [
            (
                request.dag_id,
                request.inbound_delivery_id,
                request.delivery_id,
                request.webhook,
                request.correlation_key,
                request.received_at,
                request.retain_until,
                request.status,
                request.applied_at,
                request.result,
            )
            for request in requests
        ] == [
            (
                dag_id,
                record.id,
                "delivery-1",
                "fake",
                "my-org/web#42",
                NOW,
                NOW + RETENTION,
                CancelRequestStatus.PENDING,
                None,
                None,
            )
            for dag_id in ("deploy_preview", "run_checks")
        ]
        assert [request.targets for request in requests] == [
            {
                "run": {"correlation_key_confs": []},
                "tasks": [
                    {"task_id": "run_tests", "cancel_state": "skipped", "correlation_key_confs": ["pr"]}
                ],
            },
            {
                "run": None,
                "tasks": [{"task_id": "check", "cancel_state": "failed", "correlation_key_confs": []}],
            },
        ]
        recorded_requests = build_actions(requests, ["run", "task"])
        assert (record.outcome, record.payload, record.actions) == (
            "cancel_requested",
            PAYLOAD,
            {"cancel_requests": recorded_requests},
        )
        (audit,) = get_audit_logs(session)
        assert (audit.owner, audit.dag_id, audit.run_id, audit.team_name) == (
            "webhook:fake",
            None,
            None,
            None,
        )
        assert json.loads(audit.extra) == {
            "delivery_id": "delivery-1",
            "names": [],
            "outcome": "cancel_requested",
            "cancel_requests": recorded_requests,
        }

    @mock.patch.object(cancel_requests, "find_matching_cancel_conditions", autospec=True)
    def test_a_delivery_without_correlation_key_reads_no_cancel_condition(
        self, mock_find, client, session, dag_maker
    ):
        declare(dag_maker, cancel_on=[ON_PULL_REQUEST])

        assert post(client, number=None).json()["outcome"] == "no_subscribers"

        mock_find.assert_not_called()
        assert get_requests(session) == []

    @pytest.mark.parametrize(
        ("other", "expected_outcome", "expected_actions"),
        [
            pytest.param("open-wait", "resumed_wait", {"cancel_requests", "waits"}, id="open-wait"),
            pytest.param(
                "live-subscriber", "accepted", {"asset_events", "cancel_requests"}, id="live-subscriber"
            ),
            pytest.param(
                "paused-subscriber", "cancel_requested", {"cancel_requests"}, id="paused-subscriber"
            ),
            pytest.param(None, "cancel_requested", {"cancel_requests"}, id="nothing-else"),
            pytest.param("no-matching-condition", "early", set(), id="no-matching-condition"),
        ],
    )
    def test_outcome_precedence(
        self, client, session, dag_maker, create_task_instance, other, expected_outcome, expected_actions
    ):
        declare(dag_maker, cancel_on=[ON_PUSH if other == "no-matching-condition" else ON_PULL_REQUEST])
        if other == "open-wait":
            ti = create_task_instance(dag_id="waiting", state=TaskInstanceState.RUNNING)
            session.commit()
            register(session, ti.id)
        elif other in ("live-subscriber", "paused-subscriber"):
            subscribe(dag_maker, is_paused=other == "paused-subscriber")

        assert post(client).json()["outcome"] == expected_outcome

        (record,) = get_deliveries(session)
        assert set(record.actions or {}) == expected_actions
        assert len(get_requests(session)) == (1 if "cancel_requests" in expected_actions else 0)

    def test_a_wait_registered_later_claims_a_delivery_that_requested_a_cancel(
        self, client, session, dag_maker, create_task_instance
    ):
        declare(dag_maker, cancel_on=[ON_PULL_REQUEST])
        assert post(client).json()["outcome"] == "cancel_requested"
        (record,) = get_deliveries(session)
        recorded_requests = record.actions["cancel_requests"]
        ti = create_task_instance(dag_id="waiting", state=TaskInstanceState.RUNNING)
        session.commit()

        wait = register(session, ti.id)

        assert (wait.status, wait.delivery_id) == (WaitStatus.RECEIVED, record.id)
        record = read(session, InboundDelivery, record.id)
        assert record.outcome == DeliveryOutcome.RESUMED_WAIT
        assert record.actions["cancel_requests"] == recorded_requests
        assert [entry["wait_id"] for entry in record.actions["waits"]] == [str(wait.id)]

    def test_a_redelivery_records_no_second_request_and_requests_for_a_dag_that_declared_cancel_on_since(
        self, client, session, dag_maker, time_machine
    ):
        declare(dag_maker, "first", cancel_on=[ON_PULL_REQUEST])
        assert post(client).json()["outcome"] == "cancel_requested"
        time_machine.move_to(NOW + timedelta(minutes=1), tick=False)
        declare(dag_maker, "since", cancel_on=[ON_PULL_REQUEST])
        # The retention that applies is the one the delivery was first received under.
        longer_retention = TestClient(
            create_webhooks_app(replace(REGISTRY, delivery_retention=RETENTION * 2))
        )

        response = post(longer_retention)

        assert (response.status_code, response.json()["outcome"]) == (202, "cancel_requested")
        (record,) = get_deliveries(session)
        requests = get_requests(session)
        assert [(request.dag_id, request.received_at, request.retain_until) for request in requests] == [
            ("first", NOW, NOW + RETENTION),
            ("since", NOW, NOW + RETENTION),
        ]
        assert record.actions == {"cancel_requests": build_actions(requests, ["run", "run"])}
        assert [json.loads(audit.extra)["cancel_requests"] for audit in get_audit_logs(session)] == [
            build_actions(requests[:1], ["run"]),
            build_actions(requests[1:], ["run"]),
        ]

    def test_a_redelivery_that_finds_nothing_new_is_processed_again_and_keeps_its_request(
        self, client, session, dag_maker
    ):
        declare(dag_maker, cancel_on=[ON_PULL_REQUEST])

        responses = [post(client), post(client)]

        assert [(response.status_code, response.json()["outcome"]) for response in responses] == [
            (202, "cancel_requested"),
            (202, "cancel_requested"),
        ]
        (record,) = get_deliveries(session)
        (request,) = get_requests(session)
        assert (record.outcome, record.payload, record.actions) == (
            "cancel_requested",
            PAYLOAD,
            {"cancel_requests": build_actions([request], ["run"])},
        )
        assert len(get_audit_logs(session)) == 1

    def test_a_redelivery_after_unpausing_a_subscriber_records_its_event_and_keeps_its_request(
        self, client, session, dag_maker
    ):
        declare(dag_maker, cancel_on=[ON_PULL_REQUEST])
        subscribe(dag_maker, is_paused=True)
        assert post(client).json()["outcome"] == "cancel_requested"
        dag_maker.dag_model.is_paused = False
        dag_maker.session.commit()

        response = post(client)

        assert (response.status_code, response.json()["outcome"]) == (202, "accepted")
        (record,) = get_deliveries(session)
        (request,) = get_requests(session)
        (asset_event,) = session.scalars(select(AssetEvent))
        assert record.actions == {
            "asset_events": [{"asset": TOPIC, "asset_id": asset_event.asset_id, "event_id": asset_event.id}],
            "cancel_requests": build_actions([request], ["run"]),
        }
        # This copy recorded the event and no request.
        assert [json.loads(audit.extra) for audit in get_audit_logs(session)][1] == {
            "delivery_id": "delivery-1",
            "names": [TOPIC],
            "outcome": "accepted",
        }

    def test_a_redelivery_of_an_accepted_delivery_that_requested_a_cancel_is_a_duplicate(
        self, client, session, dag_maker
    ):
        declare(dag_maker, cancel_on=[ON_PULL_REQUEST])
        subscribe(dag_maker)

        responses = [post(client), post(client)]

        assert [(response.status_code, response.json()["outcome"]) for response in responses] == [
            (202, "accepted"),
            (200, "duplicate"),
        ]
        assert len(get_requests(session)) == 1

    def test_a_copy_that_finds_a_cancel_condition_the_copy_recorded_first_did_not_records_its_request_once(
        self, client, session, dag_maker
    ):
        """It rolls back what it recorded as a first delivery, and is processed again as a redelivery."""
        assert post(client).json()["outcome"] == "early"
        declare(dag_maker, cancel_on=[ON_PULL_REQUEST])
        find_delivery = pipeline.find_delivery

        def miss_the_first_lookup(*args, **kwargs):
            # As when this copy looked before the first copy had committed.
            return None if mock_find_delivery.call_count == 1 else find_delivery(*args, **kwargs)

        with mock.patch.object(
            pipeline, "find_delivery", autospec=True, side_effect=miss_the_first_lookup
        ) as mock_find_delivery:
            response = post(client)

        assert (response.status_code, response.json()["outcome"]) == (202, "cancel_requested")
        (record,) = get_deliveries(session)
        (request,) = get_requests(session)
        assert request.inbound_delivery_id == record.id
        assert record.actions == {"cancel_requests": build_actions([request], ["run"])}
        assert len(get_audit_logs(session)) == 1

    def test_at_the_cap_a_delivery_that_requested_a_cancel_expires_and_keeps_its_request(
        self, session, dag_maker, time_machine
    ):
        client = TestClient(create_webhooks_app(replace(REGISTRY, early_delivery_max_per_webhook=1)))
        declare(dag_maker, cancel_on=[ON_PULL_REQUEST])
        assert post(client, number=1).json()["outcome"] == "cancel_requested"
        time_machine.move_to(NOW + timedelta(seconds=1), tick=False)

        assert post(client, number=2, delivery_id="delivery-2").json()["outcome"] == "cancel_requested"

        expired, kept = get_deliveries(session)
        assert (expired.outcome, expired.payload, kept.outcome) == ("expired", None, "cancel_requested")
        requests = {request.inbound_delivery_id: request for request in get_requests(session)}
        assert requests[expired.id].status == CancelRequestStatus.PENDING
        assert expired.actions == {"cancel_requests": build_actions([requests[expired.id]], ["run"])}

    def test_a_keyed_delivery_reads_and_writes_no_run_task_instance_or_dag_version(
        self, client, session, dag_maker
    ):
        declare(dag_maker, cancel_on=[ON_PULL_REQUEST])

        with record_tables() as tables:
            assert post(client).json()["outcome"] == "cancel_requested"

        assert {"dag_cancel_condition", "webhook_cancel_request"} <= tables
        assert tables.isdisjoint({"dag_run", "task_instance", "serialized_dag", "dag_version"})

    @pytest.mark.parametrize(
        "cancel_on",
        [
            pytest.param(None, id="no-dag-has-cancel-on"),
            pytest.param([ON_PUSH], id="a-condition-it-does-not-match"),
        ],
    )
    def test_a_keyed_delivery_that_requests_no_cancel_reads_the_conditions_in_one_statement(
        self, dag_maker, cancel_on
    ):
        declare(dag_maker, cancel_on=cancel_on)

        with count_queries() as queries:
            outcome = pipeline.record_delivery(
                WEBHOOK,
                DELIVERY,
                body_digest="digest",
                retention=RETENTION,
                max_event_payload_bytes=REGISTRY.max_event_payload_bytes,
                early_delivery_ttl=EARLY_DELIVERY_TTL,
                early_delivery_max_per_webhook=REGISTRY.early_delivery_max_per_webhook,
            )

        assert outcome == DeliveryOutcome.EARLY
        # The read happens whether or not any Dag has cancel_on: it is the one statement that cancel_on
        # adds to every keyed delivery.
        assert sum(n for site, n in queries.items() if "find_matching_cancel_conditions" in site) == 1
        assert sum(queries.values()) == 8, queries
