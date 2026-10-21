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

from dataclasses import replace
from datetime import timedelta
from typing import TYPE_CHECKING
from unittest import mock

import pytest
from sqlalchemy import delete
from sqlalchemy.orm import Session

from airflow.models.connection import Connection
from airflow.models.team import Team
from airflow.providers_manager import ProviderInfo, ProvidersManager
from airflow.webhooks.connections import ConfiguredWebhook, WebhookSourceTypeStatus
from airflow.webhooks.registry import WebhookRegistry
from airflow.webhooks.status import WebhookConnectionStatus, WebhookStatus, get_webhook_status

from tests_common.test_utils.webhooks import (
    FAKE_SECRET,
    FakeHmacSource,
    FakeSecretsBackend,
    build_webhook_connection,
)

if TYPE_CHECKING:
    from collections.abc import Generator

REGISTRY = WebhookRegistry(
    names=frozenset({"jira"}),
    source_type_owners={},
    connection_cache_ttl=60,
    max_body_bytes=1048576,
    delivery_retention=timedelta(days=7),
    rate_limit_per_minute=0,
    store_payload="full",
    max_event_payload_bytes=65536,
)


@pytest.fixture(autouse=True)
def providers(cleanup_providers_manager):
    fake_source = {"source-type": "fake", "classpath": f"{FakeHmacSource.__module__}.FakeHmacSource"}
    dup_source = {**fake_source, "source-type": "dup"}
    ProvidersManager()._provider_dict = {
        "fake-webhooks": ProviderInfo(version="0.0.1", data={"webhook-sources": [fake_source]}),
        "fake-webhooks-first": ProviderInfo(version="0.0.1", data={"webhook-sources": [dup_source]}),
        "fake-webhooks-second": ProviderInfo(version="0.0.1", data={"webhook-sources": [dup_source]}),
    }


@pytest.fixture(autouse=True)
def secrets_backend() -> Generator[FakeSecretsBackend, None, None]:
    backend = FakeSecretsBackend()
    with mock.patch(
        "airflow.webhooks.connections.ensure_secrets_loaded", autospec=True, return_value=[backend]
    ):
        yield backend


@pytest.fixture
def unused_session() -> Session:
    """A session for a status that is told without looking a connection's team up."""
    return mock.create_autospec(Session, instance=True)


@pytest.fixture
def team_connection(session: Session) -> Generator[None, None, None]:
    session.add(Team(name="analytics"))
    session.flush()
    session.add(
        Connection(
            conn_id="webhook_jira", conn_type="inbound_webhook", password=FAKE_SECRET, team_name="analytics"
        )
    )
    session.commit()
    yield
    session.execute(delete(Connection).where(Connection.conn_id == "webhook_jira"))
    session.execute(delete(Team).where(Team.name == "analytics"))
    session.commit()


def _build_unserved_status(
    connection_status: WebhookConnectionStatus,
    problem: str,
    *,
    source_type: str | None = None,
    source_type_status: WebhookSourceTypeStatus | None = None,
) -> WebhookStatus:
    return WebhookStatus(
        name="jira",
        listed=True,
        connection_status=connection_status,
        source_type=source_type,
        source_type_status=source_type_status,
        webhook=None,
        problems=(problem,),
    )


class TestGetWebhookStatus:
    @pytest.mark.parametrize(
        ("registry", "expected_listed", "expected_problems"),
        [
            pytest.param(REGISTRY, True, (), id="listed"),
            pytest.param(
                replace(REGISTRY, names=frozenset()),
                False,
                ("[webhooks] names does not list it, so its deliveries are answered with 404",),
                id="not-listed",
            ),
        ],
    )
    def test_a_webhook_its_connection_configures(
        self, secrets_backend, unused_session, registry, expected_listed, expected_problems
    ):
        secrets_backend.connections["webhook_jira"] = build_webhook_connection("jira", source_type="fake")

        status = get_webhook_status("jira", registry=registry, session=unused_session)

        assert status == WebhookStatus(
            name="jira",
            listed=expected_listed,
            connection_status=WebhookConnectionStatus.OK,
            source_type="fake",
            source_type_status=WebhookSourceTypeStatus.OK,
            webhook=mock.ANY,
            problems=expected_problems,
        )
        assert isinstance(status.webhook, ConfiguredWebhook)

    @pytest.mark.parametrize(
        ("connection", "expected_status"),
        [
            pytest.param(
                build_webhook_connection("jira", conn_type="generic", source_type="fake"),
                _build_unserved_status(
                    WebhookConnectionStatus.WRONG_TYPE,
                    "connection webhook_jira is misconfigured: its type is 'generic', not 'inbound_webhook'",
                ),
                id="wrong-type",
            ),
            pytest.param(
                build_webhook_connection("jira", password=None, source_type="fake"),
                _build_unserved_status(
                    WebhookConnectionStatus.MISCONFIGURED,
                    "connection webhook_jira is misconfigured: its password, which holds the shared secret, "
                    "is empty",
                ),
                id="misconfigured-before-the-source-type",
            ),
            pytest.param(
                build_webhook_connection("jira", source_type="fake", max_body_bytes=0),
                _build_unserved_status(
                    WebhookConnectionStatus.MISCONFIGURED,
                    "connection webhook_jira is misconfigured: max_body_bytes must be a positive integer",
                    source_type="fake",
                    source_type_status=WebhookSourceTypeStatus.OK,
                ),
                id="misconfigured-after-the-source-type",
            ),
            pytest.param(
                build_webhook_connection("jira", source_type="bitbucket"),
                _build_unserved_status(
                    WebhookConnectionStatus.MISCONFIGURED,
                    "connection webhook_jira is misconfigured: no installed provider serves source type "
                    "'bitbucket'",
                    source_type="bitbucket",
                    source_type_status=WebhookSourceTypeStatus.UNKNOWN,
                ),
                id="unknown-source-type",
            ),
            pytest.param(
                build_webhook_connection("jira", source_type="dup"),
                _build_unserved_status(
                    WebhookConnectionStatus.MISCONFIGURED,
                    "connection webhook_jira is misconfigured: source type 'dup' is declared by "
                    "fake-webhooks-first, fake-webhooks-second; set [webhooks] source_type_owners to the "
                    "package that serves it",
                    source_type="dup",
                    source_type_status=WebhookSourceTypeStatus.CONFLICT,
                ),
                id="conflicted-source-type",
            ),
            pytest.param(
                "failing",
                _build_unserved_status(
                    WebhookConnectionStatus.UNAVAILABLE,
                    f"secrets backend FakeSecretsBackend failed: could not reach https://vault.example.com "
                    f"with token {FAKE_SECRET}",
                ),
                id="unavailable",
            ),
        ],
    )
    def test_a_webhook_its_connection_does_not_configure(
        self, secrets_backend, unused_session, connection, expected_status
    ):
        if connection == "failing":
            secrets_backend.failing.add("webhook_jira")
        else:
            secrets_backend.connections["webhook_jira"] = connection

        assert get_webhook_status("jira", registry=REGISTRY, session=unused_session) == expected_status

    @pytest.mark.db_test
    def test_a_webhook_without_a_connection(self, session):
        assert get_webhook_status("jira", registry=REGISTRY, session=session) == _build_unserved_status(
            WebhookConnectionStatus.NOT_FOUND, "there is no connection webhook_jira"
        )

    @pytest.mark.db_test
    def test_a_connection_of_a_team_is_misconfigured(self, session, team_connection):
        # The fake secrets backend does not have it: only the metadata database says it has a team.
        assert get_webhook_status("jira", registry=REGISTRY, session=session) == _build_unserved_status(
            WebhookConnectionStatus.MISCONFIGURED,
            "connection webhook_jira belongs to team analytics, and this version of Airflow serves only "
            "webhooks without a team",
        )
