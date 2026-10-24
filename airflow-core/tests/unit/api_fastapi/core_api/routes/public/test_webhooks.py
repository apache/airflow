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
from uuid import UUID

import pytest

from airflow._shared.webhook_naming.matching import compute_key_digest, compute_match_digest
from airflow.api_fastapi.auth.managers.models.resource_details import ConnectionDetails
from airflow.api_fastapi.auth.managers.simple.simple_auth_manager import SimpleAuthManager
from airflow.api_fastapi.auth.managers.simple.user import SimpleAuthManagerUser
from airflow.models.dag import DagModel
from airflow.models.inbound_delivery import InboundDelivery
from airflow.models.inbound_wait import InboundWait, WaitStatus
from airflow.models.webhook_cancel_request import CancelRequestStatus, WebhookCancelRequest
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers_manager import ProviderInfo, ProvidersManager
from airflow.utils.session import create_session
from airflow.webhooks.cancel_application import mark_cancel_request_failed

from tests_common.test_utils.asserts import capture_orm_selects
from tests_common.test_utils.config import conf_vars
from tests_common.test_utils.db import (
    clear_db_dags,
    clear_db_inbound_deliveries,
    clear_db_runs,
    clear_db_webhook_cancel_requests,
)
from tests_common.test_utils.format_datetime import from_datetime_to_zulu_without_ms
from tests_common.test_utils.webhooks import FakeHmacSource, FakeSecretsBackend, build_webhook_connection

if TYPE_CHECKING:
    from collections.abc import Generator

    from sqlalchemy.orm import Session

    from airflow.models.taskinstance import TaskInstance

pytestmark = pytest.mark.db_test

WEBHOOK = "github"
URL = f"/webhooks/{WEBHOOK}/deliveries"
ALL_WEBHOOKS_URL = "/webhooks/~/deliveries"
RECEIVED_AT = datetime(2026, 10, 18, 12, 0, tzinfo=UTC)
ACCEPTED_ID = UUID("01990000-0000-7000-8000-000000000002")
# Received at the same time as ACCEPTED_ID, so only the id orders the two.
SIMULTANEOUS_ID = UUID("01990000-0000-7000-8000-000000000001")
ALL_DELIVERY_IDS = ["d-4", "d-3", "d-2", "d-1"]


def _build_delivery(delivery_id: str, received_at: datetime, **overrides: Any) -> InboundDelivery:
    fields: dict[str, Any] = {
        "webhook": WEBHOOK,
        "delivery_id": delivery_id,
        "body_digest": "0" * 64,
        "received_at": received_at,
        "retain_until": received_at + timedelta(days=7),
        "source_type": "github",
        "outcome": "no_subscribers",
    }
    return InboundDelivery(**(fields | overrides))


@pytest.fixture(autouse=True)
def deliveries() -> Generator[None, None, None]:
    clear_db_webhook_cancel_requests()
    clear_db_inbound_deliveries()
    with create_session() as session:
        session.add_all(
            [
                _build_delivery("d-1", RECEIVED_AT, correlation_key="pr-1"),
                _build_delivery("d-2", RECEIVED_AT + timedelta(minutes=1), id=SIMULTANEOUS_ID),
                _build_delivery(
                    "d-3",
                    RECEIVED_AT + timedelta(minutes=1),
                    id=ACCEPTED_ID,
                    attributes={"event": "pull_request", "number": 7},
                    candidates=["webhook:github/acme/repo:pull_request"],
                    correlation_key="pr-7",
                    outcome="accepted",
                    actions={"topic_events": ["webhook:github/acme/repo:pull_request"]},
                    payload={"number": 7},
                ),
                _build_delivery("d-4", RECEIVED_AT + timedelta(minutes=2), correlation_key="pr-1"),
                _build_delivery(
                    "d-5", RECEIVED_AT + timedelta(seconds=30), webhook="gitlab", source_type="gitlab"
                ),
            ]
        )
    yield
    clear_db_webhook_cancel_requests()
    clear_db_inbound_deliveries()


def _get_auth_headers(test_client, role: str) -> dict[str, str]:
    auth_manager = test_client.app.state.auth_manager
    token = auth_manager._get_token_signer().generate(
        auth_manager.serialize_user(SimpleAuthManagerUser(username=role, role=role))
    )
    return {"Authorization": f"Bearer {token}"}


class TestGetWebhookDeliveries:
    def test_should_respond_401(self, unauthenticated_test_client):
        assert unauthenticated_test_client.get(URL).status_code == 401

    @pytest.mark.parametrize("webhook", [WEBHOOK, "unknown"])
    def test_should_respond_403_whether_or_not_the_webhook_exists(self, unauthorized_test_client, webhook):
        assert unauthorized_test_client.get(f"/webhooks/{webhook}/deliveries").status_code == 403

    @pytest.mark.parametrize(("role", "expected_status_code"), [("viewer", 403), ("op", 200)])
    def test_needs_read_access_to_the_connection(self, test_client, role, expected_status_code):
        response = test_client.get(URL, headers=_get_auth_headers(test_client, role))

        assert response.status_code == expected_status_code

    @mock.patch.object(SimpleAuthManager, "is_authorized_connection", autospec=True, return_value=True)
    def test_authorizes_on_the_webhook_connection(self, mock_is_authorized_connection, test_client):
        assert test_client.get(URL).status_code == 200

        mock_is_authorized_connection.assert_called_once_with(
            mock.ANY,
            method="GET",
            details=ConnectionDetails(conn_id="webhook_github", team_name=None),
            user=mock.ANY,
        )

    @pytest.mark.parametrize("names", ["github", ""], ids=["listed", "no-longer-listed"])
    def test_lists_deliveries_newest_first_without_payloads(self, test_client, names):
        with conf_vars({("webhooks", "names"): names}):
            response = test_client.get(URL)

        assert response.status_code == 200
        body = response.json()
        assert body["total_entries"] == 4
        assert [delivery["delivery_id"] for delivery in body["deliveries"]] == ALL_DELIVERY_IDS
        assert body["deliveries"][1] == {
            "id": str(ACCEPTED_ID),
            "webhook": WEBHOOK,
            "delivery_id": "d-3",
            "received_at": from_datetime_to_zulu_without_ms(RECEIVED_AT + timedelta(minutes=1)),
            "source_type": "github",
            "attributes": {"event": "pull_request", "number": 7},
            "correlation_key": "pr-7",
            "outcome": "accepted",
            "candidates": ["webhook:github/acme/repo:pull_request"],
            "actions": {"topic_events": ["webhook:github/acme/repo:pull_request"]},
            "team_name": None,
            "cancel_requests": [],
        }

    @pytest.mark.parametrize("url", [URL, ALL_WEBHOOKS_URL])
    def test_page_query_does_not_select_payload(self, test_client, url):
        with capture_orm_selects("inbound_delivery") as statements:
            assert test_client.get(url).status_code == 200

        # The count's subquery names every column; databases do not read the ones count(*) ignores.
        reads = [statement for statement in statements if not statement.startswith("SELECT count(*)")]
        assert reads
        assert not any("inbound_delivery.payload" in statement for statement in reads)

    @conf_vars({("webhooks", "names"): "github,jira"})
    def test_listed_webhook_without_deliveries_has_an_empty_list(self, test_client):
        response = test_client.get("/webhooks/jira/deliveries")

        assert response.status_code == 200
        assert response.json() == {"deliveries": [], "total_entries": 0}

    @conf_vars({("webhooks", "names"): "github"})
    def test_should_respond_404_for_an_unknown_webhook(self, test_client):
        response = test_client.get("/webhooks/jira/deliveries")

        assert response.status_code == 404
        assert response.json() == {"detail": "The webhook `jira` was not found"}

    @pytest.mark.parametrize("webhook", ["GitHub", "a" * 65], ids=["uppercase", "too-long"])
    def test_should_respond_404_without_a_query_for_a_name_outside_the_grammar(self, test_client, webhook):
        with capture_orm_selects("inbound_delivery") as statements:
            response = test_client.get(f"/webhooks/{webhook}/deliveries")

        assert response.status_code == 404
        assert statements == []

    @conf_vars({("webhooks", "names"): "team:x"})
    @pytest.mark.parametrize(("webhook", "expected_status_code"), [(WEBHOOK, 200), ("jira", 404)])
    def test_invalid_names_leave_recorded_deliveries_readable(
        self, test_client, webhook, expected_status_code
    ):
        assert test_client.get(f"/webhooks/{webhook}/deliveries").status_code == expected_status_code

    @pytest.mark.parametrize(
        ("query_params", "expected_delivery_ids"),
        [
            pytest.param({"limit": 2}, ["d-4", "d-3"], id="limit"),
            pytest.param({"limit": 2, "offset": 2}, ["d-2", "d-1"], id="offset"),
        ],
    )
    def test_paginates(self, test_client, query_params, expected_delivery_ids):
        response = test_client.get(URL, params=query_params)

        assert response.status_code == 200
        body = response.json()
        assert [delivery["delivery_id"] for delivery in body["deliveries"]] == expected_delivery_ids
        assert body["total_entries"] == 4

    def test_sorts_by_order_by(self, test_client):
        response = test_client.get(URL, params={"order_by": "received_at"})

        assert response.status_code == 200
        assert [delivery["delivery_id"] for delivery in response.json()["deliveries"]] == [
            "d-1",
            "d-2",
            "d-3",
            "d-4",
        ]

    @pytest.mark.parametrize(
        ("url", "query_params", "expected_delivery_ids"),
        [
            pytest.param(URL, {"outcome": "accepted"}, ["d-3"], id="outcome"),
            pytest.param(URL, {"delivery_id": "d-2"}, ["d-2"], id="delivery_id"),
            pytest.param(URL, {"id": str(ACCEPTED_ID)}, ["d-3"], id="id"),
            pytest.param(URL, {"correlation_key": "pr-1"}, ["d-4", "d-1"], id="correlation_key"),
            pytest.param(
                URL,
                {"received_at_gte": "2026-10-18T12:00:00Z", "received_at_lt": "2026-10-18T12:02:00Z"},
                ["d-3", "d-2", "d-1"],
                id="received_at",
            ),
            pytest.param(URL, {"delivery_id": "d-5"}, [], id="other-webhook"),
            pytest.param(URL, {"source_type": "gitlab"}, [], id="source_type"),
            pytest.param(URL, {"webhook_names": "gitlab"}, [], id="webhook_names"),
            pytest.param(ALL_WEBHOOKS_URL, {"source_type": "gitlab"}, ["d-5"], id="all-source_type"),
            pytest.param(ALL_WEBHOOKS_URL, {"webhook_names": "gitlab"}, ["d-5"], id="all-webhook_names"),
            pytest.param(
                ALL_WEBHOOKS_URL,
                {"webhook_names": ["github", "gitlab"]},
                ["d-4", "d-3", "d-2", "d-5", "d-1"],
                id="all-several-webhook_names",
            ),
        ],
    )
    def test_filters(self, test_client, url, query_params, expected_delivery_ids):
        response = test_client.get(url, params=query_params)

        assert response.status_code == 200
        body = response.json()
        assert [delivery["delivery_id"] for delivery in body["deliveries"]] == expected_delivery_ids
        assert body["total_entries"] == len(expected_delivery_ids)


CANCEL_KEY = "my-org/shop#301"
CANCEL_DELIVERY_RECORD_ID = UUID("01990000-0000-7000-8000-000000000003")
CANCEL_RECEIVED_AT = RECEIVED_AT + timedelta(minutes=3)
APPLIED_AT = CANCEL_RECEIVED_AT + timedelta(seconds=1)
PREVIEW_DAG_ID = "preview_environment"
SHIP_DAG_ID = "ship_when_labeled"
PREVIEW_REQUEST_ID = UUID("01990000-0000-7000-8000-0000000000e1")
SHIP_REQUEST_ID = UUID("01990000-0000-7000-8000-0000000000e2")
RUN_TARGETS = {"run": {"correlation_key_confs": ["pr"]}, "tasks": []}
TASK_TARGETS = {
    "run": None,
    "tasks": [{"task_id": "run_e2e_tests", "cancel_state": "skipped", "correlation_key_confs": []}],
}
EMPTY_RESULT: dict[str, Any] = {"dag_runs": [], "task_instances": [], "skipped_runs": [], "error": None}
REGION_ID = "0199f8a0-0000-7000-8000-0000000000d2"


def _add_cancel_request(
    session: Session, request_id: UUID, dag_id: str, targets: dict[str, Any], **overrides: Any
) -> WebhookCancelRequest:
    request = WebhookCancelRequest(
        **{
            "id": request_id,
            "inbound_delivery_id": CANCEL_DELIVERY_RECORD_ID,
            "delivery_id": "close-301",
            "webhook": WEBHOOK,
            "correlation_key": CANCEL_KEY,
            "received_at": CANCEL_RECEIVED_AT,
            "dag_id": dag_id,
            "targets": targets,
            "retain_until": CANCEL_RECEIVED_AT + timedelta(days=7),
            **overrides,
        }
    )
    session.add(request)
    session.commit()
    return request


class TestGetDeliveryCancelRequests:
    @pytest.fixture(autouse=True)
    def cancel_delivery(self, testing_dag_bundle, session) -> Generator[None, None, None]:
        clear_db_dags()
        session.add_all(
            [
                DagModel(dag_id=PREVIEW_DAG_ID, bundle_name="testing"),
                DagModel(dag_id=SHIP_DAG_ID, bundle_name="testing"),
                _build_delivery(
                    "close-301",
                    CANCEL_RECEIVED_AT,
                    id=CANCEL_DELIVERY_RECORD_ID,
                    correlation_key=CANCEL_KEY,
                    outcome="cancel_requested",
                    actions={
                        "cancel_requests": [
                            {"request_id": str(PREVIEW_REQUEST_ID), "dag_id": PREVIEW_DAG_ID, "scope": "run"},
                            {"request_id": str(SHIP_REQUEST_ID), "dag_id": SHIP_DAG_ID, "scope": "task"},
                        ]
                    },
                ),
            ]
        )
        session.commit()
        yield
        clear_db_webhook_cancel_requests()
        clear_db_dags()

    @staticmethod
    def get_cancel_requests(test_client) -> list[dict[str, Any]]:
        response = test_client.get(URL, params={"id": str(CANCEL_DELIVERY_RECORD_ID)})
        assert response.status_code == 200, response.text
        [delivery] = response.json()["deliveries"]
        return delivery["cancel_requests"]

    def test_lists_each_cancel_request_with_its_status_and_result(self, test_client, session):
        # Added in the other order, so the response's order comes from the query.
        _add_cancel_request(session, SHIP_REQUEST_ID, SHIP_DAG_ID, TASK_TARGETS)
        _add_cancel_request(
            session,
            PREVIEW_REQUEST_ID,
            PREVIEW_DAG_ID,
            RUN_TARGETS,
            status=CancelRequestStatus.APPLIED,
            applied_at=APPLIED_AT,
            result={**EMPTY_RESULT, "dag_runs": [{"run_id": "asset_triggered__1", "state": "failed"}]},
        )

        assert self.get_cancel_requests(test_client) == [
            {
                "id": str(PREVIEW_REQUEST_ID),
                "dag_id": PREVIEW_DAG_ID,
                "targets": RUN_TARGETS,
                "status": "applied",
                "applied_at": from_datetime_to_zulu_without_ms(APPLIED_AT),
                "result": {**EMPTY_RESULT, "dag_runs": [{"run_id": "asset_triggered__1", "state": "failed"}]},
            },
            {
                "id": str(SHIP_REQUEST_ID),
                "dag_id": SHIP_DAG_ID,
                "targets": TASK_TARGETS,
                "status": "pending",
                "applied_at": None,
                "result": None,
            },
        ]

    def test_lists_the_task_instances_a_request_cancelled_and_the_runs_it_left(self, test_client, session):
        ti_id = "0199f8a0-0000-7000-8000-0000000000b1"
        _add_cancel_request(
            session,
            SHIP_REQUEST_ID,
            SHIP_DAG_ID,
            TASK_TARGETS,
            status=CancelRequestStatus.APPLIED,
            applied_at=APPLIED_AT,
            result={
                **EMPTY_RESULT,
                "task_instances": [
                    {
                        "ti_id": ti_id,
                        "dag_id": SHIP_DAG_ID,
                        "run_id": "manual_1",
                        "task_id": "run_e2e_tests",
                        "map_index": -1,
                        "state": "skipped",
                    },
                    {
                        "ti_id": ti_id,
                        "dag_id": SHIP_DAG_ID,
                        "run_id": "manual_1",
                        "task_id": "run_e2e_tests",
                        "map_index": 2,
                        "region_id": REGION_ID,
                        "region_index": 2,
                        "state": "skipped",
                    },
                ],
                "skipped_runs": [{"run_id": "manual_2", "reason": "dag_not_found"}],
            },
        )

        [request] = self.get_cancel_requests(test_client)

        assert request["result"] == {
            **EMPTY_RESULT,
            "task_instances": [
                {
                    "ti_id": ti_id,
                    "run_id": "manual_1",
                    "task_id": "run_e2e_tests",
                    "map_index": -1,
                    "state": "skipped",
                },
                {
                    "ti_id": ti_id,
                    "run_id": "manual_1",
                    "task_id": "run_e2e_tests",
                    "map_index": 2,
                    "region_id": REGION_ID,
                    "region_index": 2,
                    "state": "skipped",
                },
            ],
            "skipped_runs": [{"run_id": "manual_2", "reason": "dag_not_found"}],
        }

    @pytest.mark.parametrize("by_scheduler", [True, False], ids=["marked-by-the-scheduler", "error-only"])
    def test_lists_a_request_that_failed_to_apply(self, test_client, session, by_scheduler):
        request = _add_cancel_request(session, PREVIEW_REQUEST_ID, PREVIEW_DAG_ID, RUN_TARGETS)
        if by_scheduler:
            mark_cancel_request_failed(request, error="RuntimeError")
        else:
            request.status = CancelRequestStatus.FAILED
            request.applied_at = APPLIED_AT
            request.result = {"error": "RuntimeError"}
        session.commit()

        [listed] = self.get_cancel_requests(test_client)

        assert (listed["status"], listed["applied_at"] is not None, listed["result"]) == (
            "failed",
            True,
            {**EMPTY_RESULT, "error": "RuntimeError"},
        )

    def test_leaves_out_a_cancel_request_whose_record_is_gone(self, test_client, session):
        _add_cancel_request(session, SHIP_REQUEST_ID, SHIP_DAG_ID, TASK_TARGETS)

        response = test_client.get(URL, params={"id": str(CANCEL_DELIVERY_RECORD_ID)})

        assert response.status_code == 200, response.text
        [delivery] = response.json()["deliveries"]
        assert [request["id"] for request in delivery["cancel_requests"]] == [str(SHIP_REQUEST_ID)]
        assert len(delivery["actions"]["cancel_requests"]) == 2

    @pytest.mark.parametrize(
        ("query_params", "expected_statement_count"),
        [
            pytest.param({}, 1, id="keyed-deliveries"),
            # d-2 has no correlation key, so it can have recorded no request.
            pytest.param({"delivery_id": "d-2"}, 0, id="no-keyed-delivery"),
        ],
    )
    def test_cancel_requests_of_a_page_cost_one_query_and_none_without_any(
        self, test_client, session, query_params, expected_statement_count
    ):
        _add_cancel_request(session, PREVIEW_REQUEST_ID, PREVIEW_DAG_ID, RUN_TARGETS)
        _add_cancel_request(session, SHIP_REQUEST_ID, SHIP_DAG_ID, TASK_TARGETS)

        with capture_orm_selects("webhook_cancel_request") as statements:
            response = test_client.get(URL, params=query_params)

        assert response.status_code == 200, response.text
        assert len(statements) == expected_statement_count
        assert {
            delivery["delivery_id"]: [request["dag_id"] for request in delivery["cancel_requests"]]
            for delivery in response.json()["deliveries"]
        } == (
            {"close-301": [PREVIEW_DAG_ID, SHIP_DAG_ID], "d-4": [], "d-3": [], "d-2": [], "d-1": []}
            if expected_statement_count
            else {"d-2": []}
        )


def _allow_reading(*readable_conn_ids: str):
    def filter_authorized_connections(self, *, conn_ids: set[str], **kwargs: Any) -> set[str]:
        return conn_ids & set(readable_conn_ids)

    return filter_authorized_connections


class TestGetDeliveriesOfAllWebhooks:
    @conf_vars({("webhooks", "names"): "github,jira"})
    @mock.patch.object(SimpleAuthManager, "filter_authorized_connections", autospec=True)
    def test_filters_listed_and_recorded_webhooks_in_one_call(self, mock_filter, test_client):
        mock_filter.side_effect = _allow_reading("webhook_github")

        assert test_client.get(ALL_WEBHOOKS_URL).status_code == 200

        mock_filter.assert_called_once_with(
            mock.ANY,
            conn_ids={"webhook_github", "webhook_gitlab", "webhook_jira"},
            user=mock.ANY,
            method="GET",
        )

    @pytest.mark.parametrize(
        ("readable_conn_ids", "expected_delivery_ids"),
        [
            pytest.param(("webhook_github", "webhook_gitlab"), ["d-4", "d-3", "d-2", "d-5", "d-1"], id="all"),
            pytest.param(("webhook_github",), ALL_DELIVERY_IDS, id="listed"),
            # gitlab is not in [webhooks] names any more, but its deliveries are still recorded.
            pytest.param(("webhook_gitlab",), ["d-5"], id="no-longer-listed"),
            pytest.param((), [], id="none"),
        ],
    )
    @conf_vars({("webhooks", "names"): "github"})
    @mock.patch.object(SimpleAuthManager, "filter_authorized_connections", autospec=True)
    def test_lists_readable_webhooks_newest_first(
        self, mock_filter, test_client, readable_conn_ids, expected_delivery_ids
    ):
        mock_filter.side_effect = _allow_reading(*readable_conn_ids)

        response = test_client.get(ALL_WEBHOOKS_URL)

        assert response.status_code == 200
        body = response.json()
        assert [delivery["delivery_id"] for delivery in body["deliveries"]] == expected_delivery_ids
        assert body["total_entries"] == len(expected_delivery_ids)

    @pytest.mark.parametrize(("role", "expected_total_entries"), [("viewer", 0), ("op", 5)])
    def test_lists_nothing_rather_than_403_without_read_access(
        self, test_client, role, expected_total_entries
    ):
        response = test_client.get(ALL_WEBHOOKS_URL, headers=_get_auth_headers(test_client, role))

        assert response.status_code == 200
        assert response.json()["total_entries"] == expected_total_entries

    @mock.patch.object(SimpleAuthManager, "filter_authorized_connections", autospec=True)
    def test_webhook_names_cannot_add_an_unreadable_webhook(self, mock_filter, test_client):
        mock_filter.side_effect = _allow_reading("webhook_github")

        response = test_client.get(ALL_WEBHOOKS_URL, params={"webhook_names": "gitlab"})

        assert response.status_code == 200
        assert response.json() == {"deliveries": [], "total_entries": 0}

    @pytest.mark.parametrize(
        ("order_by", "expected_webhooks"),
        [
            ("webhook", ["github", "github", "github", "github", "gitlab"]),
            ("-webhook", ["gitlab", "github", "github", "github", "github"]),
        ],
    )
    def test_sorts_by_webhook(self, test_client, order_by, expected_webhooks):
        response = test_client.get(ALL_WEBHOOKS_URL, params={"order_by": order_by})

        assert response.status_code == 200
        assert [delivery["webhook"] for delivery in response.json()["deliveries"]] == expected_webhooks


OVERVIEW_URL = "/webhooks"


def _build_overview_row(
    name: str,
    *,
    listed: bool = True,
    connection_status: str = "not_found",
    source_type: str | None = None,
    source_type_status: str | None = None,
    last_received_at: datetime | None = None,
    last_outcome: str | None = None,
) -> dict[str, Any]:
    return {
        "name": name,
        "listed": listed,
        "connection_status": connection_status,
        "source_type": source_type,
        "source_type_status": source_type_status,
        "last_received_at": None
        if last_received_at is None
        else from_datetime_to_zulu_without_ms(last_received_at),
        "last_outcome": last_outcome,
    }


class TestGetWebhooks:
    @pytest.fixture(autouse=True)
    def providers(self, cleanup_providers_manager):
        fake_source = {"source-type": "fake", "classpath": f"{FakeHmacSource.__module__}.FakeHmacSource"}
        ProvidersManager()._provider_dict = {
            "fake-webhooks": ProviderInfo(version="0.0.1", data={"webhook-sources": [fake_source]}),
        }

    @pytest.fixture(autouse=True)
    def secrets_backend(self) -> Generator[FakeSecretsBackend, None, None]:
        backend = FakeSecretsBackend()
        with mock.patch(
            "airflow.webhooks.connections.ensure_secrets_loaded", autospec=True, return_value=[backend]
        ):
            yield backend

    def test_should_respond_401(self, unauthenticated_test_client):
        assert unauthenticated_test_client.get(OVERVIEW_URL).status_code == 401

    @conf_vars({("webhooks", "names"): "github,jira"})
    def test_lists_each_readable_webhook_with_its_last_delivery(self, test_client):
        response = test_client.get(OVERVIEW_URL)

        assert response.status_code == 200
        assert response.json() == {
            "webhooks": [
                _build_overview_row(
                    "github",
                    last_received_at=RECEIVED_AT + timedelta(minutes=2),
                    last_outcome="no_subscribers",
                ),
                # No longer listed, but its deliveries are still recorded.
                _build_overview_row(
                    "gitlab",
                    listed=False,
                    last_received_at=RECEIVED_AT + timedelta(seconds=30),
                    last_outcome="no_subscribers",
                ),
                _build_overview_row("jira"),
            ],
            "total_entries": 3,
        }

    @conf_vars({("webhooks", "names"): "jira"})
    @mock.patch.object(SimpleAuthManager, "filter_authorized_connections", autospec=True)
    def test_last_delivery_of_two_received_at_once_is_the_one_with_the_larger_id(
        self, mock_filter, test_client, session
    ):
        mock_filter.side_effect = _allow_reading("webhook_jira")
        received_at = RECEIVED_AT + timedelta(hours=1)
        session.add_all(
            [
                _build_delivery(
                    "j-2",
                    received_at,
                    webhook="jira",
                    id=UUID("01990000-0000-7000-8000-000000000012"),
                    outcome="accepted",
                ),
                _build_delivery(
                    "j-1", received_at, webhook="jira", id=UUID("01990000-0000-7000-8000-000000000011")
                ),
            ]
        )
        session.commit()

        response = test_client.get(OVERVIEW_URL)

        assert response.status_code == 200
        assert response.json()["webhooks"] == [
            _build_overview_row("jira", last_received_at=received_at, last_outcome="accepted")
        ]

    # The statuses themselves are tested in tests/unit/webhooks/test_status.py.
    @pytest.mark.parametrize(
        ("connection", "expected_statuses"),
        [
            pytest.param(
                build_webhook_connection("jira", source_type="fake", api_token="t0ken"),
                {"connection_status": "ok", "source_type": "fake", "source_type_status": "ok"},
                id="ok",
            ),
            # The secrets backend's error names a host and a token.
            pytest.param("failing", {"connection_status": "unavailable"}, id="unavailable"),
        ],
    )
    @conf_vars({("webhooks", "names"): "jira"})
    @mock.patch.object(SimpleAuthManager, "filter_authorized_connections", autospec=True)
    def test_reports_the_status_without_what_the_connection_or_its_backend_holds(
        self, mock_filter, test_client, secrets_backend, connection, expected_statuses
    ):
        mock_filter.side_effect = _allow_reading("webhook_jira")
        if connection == "failing":
            secrets_backend.failing.add("webhook_jira")
        else:
            secrets_backend.connections["webhook_jira"] = connection

        response = test_client.get(OVERVIEW_URL)

        assert response.status_code == 200
        assert response.json() == {
            "webhooks": [_build_overview_row("jira", **expected_statuses)],
            "total_entries": 1,
        }

    @pytest.mark.parametrize(
        ("readable_conn_ids", "expected_names"),
        [
            pytest.param(
                ("webhook_github", "webhook_gitlab", "webhook_jira"), ["github", "gitlab", "jira"], id="all"
            ),
            pytest.param(("webhook_gitlab",), ["gitlab"], id="no-longer-listed"),
            pytest.param((), [], id="none"),
        ],
    )
    @conf_vars({("webhooks", "names"): "github,jira"})
    @mock.patch.object(SimpleAuthManager, "filter_authorized_connections", autospec=True)
    def test_lists_only_readable_webhooks(self, mock_filter, test_client, readable_conn_ids, expected_names):
        mock_filter.side_effect = _allow_reading(*readable_conn_ids)

        response = test_client.get(OVERVIEW_URL)

        assert response.status_code == 200
        body = response.json()
        assert [webhook["name"] for webhook in body["webhooks"]] == expected_names
        assert body["total_entries"] == len(expected_names)
        mock_filter.assert_called_once_with(
            mock.ANY,
            conn_ids={"webhook_github", "webhook_gitlab", "webhook_jira"},
            user=mock.ANY,
            method="GET",
        )

    @pytest.mark.parametrize(("role", "expected_names"), [("viewer", []), ("op", ["github", "gitlab"])])
    def test_lists_nothing_rather_than_403_without_read_access(self, test_client, role, expected_names):
        response = test_client.get(OVERVIEW_URL, headers=_get_auth_headers(test_client, role))

        assert response.status_code == 200
        assert [webhook["name"] for webhook in response.json()["webhooks"]] == expected_names

    @conf_vars({("webhooks", "names"): "github,jira"})
    def test_reads_the_connections_of_the_page_only(self, test_client, secrets_backend):
        response = test_client.get(OVERVIEW_URL, params={"limit": 1, "offset": 1})

        assert response.status_code == 200
        body = response.json()
        assert [webhook["name"] for webhook in body["webhooks"]] == ["gitlab"]
        assert body["total_entries"] == 3
        assert secrets_backend.lookups == 1

    @conf_vars({("webhooks", "names"): "*"})
    def test_should_respond_500_for_an_invalid_webhooks_configuration(self, test_client, secrets_backend):
        response = test_client.get(OVERVIEW_URL)

        assert response.status_code == 500
        assert response.json() == {
            "detail": "The [webhooks] configuration is invalid, so no webhook can be served: run airflow "
            "webhooks check to see why"
        }
        assert secrets_backend.lookups == 0


WAITS_DAG_ID = "merge_gate"
WAITS_RUN_ID = "manual_run"
WAITS_TASK_ID = "wait_for_merge"
WAITS_URL = f"/dags/{WAITS_DAG_ID}/dagRuns/{WAITS_RUN_ID}/taskInstances/{WAITS_TASK_ID}/webhookWaits"
RESUMING_DELIVERY_ID = UUID("01990000-0000-7000-8000-0000000000aa")


def _add_wait(
    session: Session,
    ti_id: UUID,
    *,
    number: int,
    created_at: datetime,
    status: WaitStatus = WaitStatus.REGISTERED,
    **overrides: Any,
) -> InboundWait:
    match = {"repository": "octo/repo", "number": number}
    wait = InboundWait(
        ti_id=ti_id,
        webhook=WEBHOOK,
        events=["pull_request"],
        match=match,
        key_digest=compute_key_digest(match),
        match_digest=compute_match_digest(["pull_request"], match),
        status=status,
        deadline=created_at + timedelta(days=3),
        created_at=created_at,
        **overrides,
    )
    session.add(wait)
    session.flush()
    if status not in (WaitStatus.REGISTERED, WaitStatus.RECEIVED):
        # Set after the insert: the ORM inserts the column's default in place of None.
        wait.open = None
    session.commit()
    return wait


class TestGetWebhookWaits:
    @pytest.fixture(autouse=True)
    def clean_db(self) -> Generator[None, None, None]:
        clear_db_runs()
        clear_db_dags()
        yield
        clear_db_runs()
        clear_db_dags()

    @pytest.fixture
    def task_instance(self, dag_maker, session) -> TaskInstance:
        with dag_maker(WAITS_DAG_ID, serialized=True, session=session):
            EmptyOperator(task_id=WAITS_TASK_ID)
            EmptyOperator(task_id="apply")
        dag_run = dag_maker.create_dagrun(run_id=WAITS_RUN_ID)
        session.commit()
        return dag_run.get_task_instance(WAITS_TASK_ID, session=session)

    def test_should_respond_401(self, unauthenticated_test_client):
        assert unauthenticated_test_client.get(WAITS_URL).status_code == 401

    def test_should_respond_403(self, unauthorized_test_client):
        assert unauthorized_test_client.get(WAITS_URL).status_code == 403

    def test_needs_read_access_to_the_task_instance_only(self, test_client, task_instance):
        # The viewer role may read task instances but no connection, so no webhook's deliveries.
        response = test_client.get(WAITS_URL, headers=_get_auth_headers(test_client, "viewer"))

        assert response.status_code == 200

    def test_lists_the_waits_of_the_current_try_newest_first(self, test_client, task_instance, session):
        resumed = _add_wait(
            session,
            task_instance.id,
            number=7,
            created_at=RECEIVED_AT,
            status=WaitStatus.RESUMED,
            delivery_id=RESUMING_DELIVERY_ID,
            ended_at=RECEIVED_AT + timedelta(hours=1),
        )
        registered = _add_wait(
            session, task_instance.id, number=8, created_at=RECEIVED_AT + timedelta(hours=2)
        )
        other_task = task_instance.dag_run.get_task_instance("apply", session=session)
        _add_wait(session, other_task.id, number=9, created_at=RECEIVED_AT + timedelta(hours=3))

        response = test_client.get(WAITS_URL)

        assert response.status_code == 200, response.text
        assert response.json() == {
            "webhook_waits": [
                {
                    "id": str(registered.id),
                    "webhook": WEBHOOK,
                    "events": ["pull_request"],
                    "match": {"repository": "octo/repo", "number": 8},
                    "status": "registered",
                    "deadline": from_datetime_to_zulu_without_ms(RECEIVED_AT + timedelta(days=3, hours=2)),
                    "created_at": from_datetime_to_zulu_without_ms(RECEIVED_AT + timedelta(hours=2)),
                    "ended_at": None,
                    "inbound_delivery_id": None,
                },
                {
                    "id": str(resumed.id),
                    "webhook": WEBHOOK,
                    "events": ["pull_request"],
                    "match": {"repository": "octo/repo", "number": 7},
                    "status": "resumed",
                    "deadline": from_datetime_to_zulu_without_ms(RECEIVED_AT + timedelta(days=3)),
                    "created_at": from_datetime_to_zulu_without_ms(RECEIVED_AT),
                    "ended_at": from_datetime_to_zulu_without_ms(RECEIVED_AT + timedelta(hours=1)),
                    "inbound_delivery_id": str(RESUMING_DELIVERY_ID),
                },
            ],
            "total_entries": 2,
        }

    @pytest.mark.parametrize(("try_number", "expected_number"), [(None, 2), (1, 1), (2, 2)])
    def test_lists_the_waits_of_the_selected_try(
        self, test_client, task_instance, session, try_number, expected_number
    ):
        _add_wait(session, task_instance.id, number=1, created_at=RECEIVED_AT, status=WaitStatus.TIMED_OUT)
        task_instance.try_number, task_instance.state = 1, "failed"
        second_try = task_instance.prepare_db_for_next_try(session)
        session.commit()
        _add_wait(session, second_try.id, number=2, created_at=RECEIVED_AT + timedelta(hours=1))

        response = test_client.get(WAITS_URL, params={} if try_number is None else {"try_number": try_number})

        assert response.status_code == 200, response.text
        assert [wait["match"]["number"] for wait in response.json()["webhook_waits"]] == [expected_number]

    def test_paginates(self, test_client, task_instance, session):
        for number in range(3):
            _add_wait(
                session,
                task_instance.id,
                number=number,
                created_at=RECEIVED_AT + timedelta(hours=number),
                status=WaitStatus.ENDED,
            )

        response = test_client.get(WAITS_URL, params={"limit": 1, "offset": 1})

        assert response.status_code == 200, response.text
        body = response.json()
        assert [wait["match"]["number"] for wait in body["webhook_waits"]] == [1]
        assert body["total_entries"] == 3

    def test_task_instance_without_waits_has_an_empty_list(self, test_client, task_instance):
        response = test_client.get(WAITS_URL)

        assert response.status_code == 200, response.text
        assert response.json() == {"webhook_waits": [], "total_entries": 0}

    @pytest.mark.parametrize(
        ("url", "params"),
        [
            pytest.param(WAITS_URL.replace(WAITS_TASK_ID, "unknown"), {}, id="unknown-task"),
            pytest.param(WAITS_URL, {"try_number": 2}, id="unknown-try"),
            pytest.param(WAITS_URL, {"map_index": 0}, id="unknown-map-index"),
        ],
    )
    def test_should_respond_404_for_an_unknown_task_instance(self, test_client, task_instance, url, params):
        response = test_client.get(url, params=params)

        assert response.status_code == 404
