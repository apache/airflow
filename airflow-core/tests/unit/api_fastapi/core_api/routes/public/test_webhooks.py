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

from airflow.api_fastapi.auth.managers.models.resource_details import ConnectionDetails
from airflow.api_fastapi.auth.managers.simple.simple_auth_manager import SimpleAuthManager
from airflow.api_fastapi.auth.managers.simple.user import SimpleAuthManagerUser
from airflow.models.inbound_delivery import InboundDelivery
from airflow.utils.session import create_session

from tests_common.test_utils.asserts import capture_orm_selects
from tests_common.test_utils.config import conf_vars
from tests_common.test_utils.db import clear_db_inbound_deliveries
from tests_common.test_utils.format_datetime import from_datetime_to_zulu_without_ms

if TYPE_CHECKING:
    from collections.abc import Generator

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
