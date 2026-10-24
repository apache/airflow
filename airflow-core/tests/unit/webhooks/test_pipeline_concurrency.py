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
"""How the delivery pipeline behaves on a real database under concurrency and failures."""

from __future__ import annotations

import itertools
import json
import os
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from http import HTTPStatus
from typing import TYPE_CHECKING
from unittest import mock

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, event, func, select
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import sessionmaker

from airflow import settings
from airflow.api_fastapi.webhooks.app import create_webhooks_app
from airflow.assets.manager import AssetManager
from airflow.models.asset import AssetDagRunQueue, AssetEvent
from airflow.models.inbound_delivery import InboundDelivery
from airflow.models.log import Log
from airflow.models.webhook_cancel_request import WebhookCancelRequest
from airflow.providers_manager import ProvidersManager
from airflow.sdk import Asset, WebhookMatch
from airflow.webhooks import pipeline
from airflow.webhooks.naming import build_topic_name
from airflow.webhooks.registry import WebhookRegistry

from tests_common.test_utils.db import clear_db_assets, clear_db_inbound_deliveries, clear_db_logs
from tests_common.test_utils.webhooks import (
    DELIVERY_ID_HEADER,
    EVENT_HEADER,
    SIGNATURE_HEADER,
    FakeHmacSource,
    FakeWaitSource,
    compute_fake_signature,
)

if TYPE_CHECKING:
    from httpx2 import Response
    from sqlalchemy.orm import Session

pytestmark = pytest.mark.db_test

SECRET = "s3cret"
BODY = b'{"path": "my-org/web"}'
REGISTRY = WebhookRegistry(
    names=frozenset({"fake", "routed"}),
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
#: The data asset that webhook routed's asset route records an event on.
ORDERS = Asset(name="orders", uri="s3://warehouse/orders")
CONNECTION = {
    "AIRFLOW_CONN_WEBHOOK_FAKE": json.dumps({"conn_type": "inbound_webhook", "password": SECRET}),
    "AIRFLOW_CONN_WEBHOOK_ROUTED": json.dumps(
        {
            "conn_type": "inbound_webhook",
            "password": SECRET,
            "extra": {
                "source_type": "fake",
                "asset_routes": [
                    {"topic": build_topic_name("routed", "my-org/web", "push"), "asset": ORDERS.uri}
                ],
            },
        }
    ),
}
#: The topic that push deliveries of BODY to webhook fake offer.
TOPIC = Asset(name=build_topic_name("fake", "my-org/web", "push"))
#: The webhook whose deliveries a Dag subscribes to through TOPIC, or through the webhook's asset route.
SUBSCRIBED_WEBHOOKS = {"topic": "fake", "asset_route": "routed"}
SUBSCRIPTIONS = pytest.mark.parametrize("through", list(SUBSCRIBED_WEBHOOKS))
#: A push delivery about my-org/web#7, which FakeWaitSource gives that correlation key.
KEYED_BODY = b'{"path": "my-org/web", "repository": "my-org/web", "number": 7}'


@pytest.fixture(autouse=True)
def source_class():
    with mock.patch.object(
        ProvidersManager, "get_webhook_source_class", autospec=True, return_value=FakeHmacSource
    ) as mock_source_class:
        yield mock_source_class


@pytest.fixture(autouse=True)
def clean_db():
    clear_db_inbound_deliveries()
    clear_db_assets()
    clear_db_logs()
    yield
    clear_db_inbound_deliveries()
    clear_db_assets()
    clear_db_logs()


@pytest.fixture
def keyed_source(source_class):
    """Parse deliveries with FakeWaitSource, which gives those of KEYED_BODY a correlation key."""
    source_class.return_value = FakeWaitSource


def declare_cancel_on(dag_maker) -> None:
    """Add Dag cancellable, whose ``cancel_on`` the push deliveries to webhook fake match."""
    with dag_maker("cancellable", schedule=None, cancel_on=[WebhookMatch("fake", events=["push"])]):
        pass


def subscribe(dag_maker, through: str) -> str:
    """Have Dag subscriber subscribe ``through`` a topic or an asset route, and return the webhook's name."""
    with dag_maker("subscriber", schedule=TOPIC if through == "topic" else ORDERS):
        pass
    return SUBSCRIBED_WEBHOOKS[through]


@pytest.fixture
def client() -> TestClient:
    return TestClient(create_webhooks_app(REGISTRY), raise_server_exceptions=False)


def post(client: TestClient, name: str = "fake", body: bytes = BODY) -> Response:
    headers = {
        "content-type": "application/json",
        EVENT_HEADER: "push",
        DELIVERY_ID_HEADER: "delivery-1",
        SIGNATURE_HEADER: compute_fake_signature(body, SECRET),
    }
    return client.post(f"/{name}", content=body, headers=headers)


def post_copies(client: TestClient, copies: int, name: str = "fake", body: bytes = BODY) -> list[int]:
    """Post copies of one delivery at once, each looking it up before any of them has recorded it."""
    barrier = threading.Barrier(copies)
    lookups = itertools.count()
    find_delivery = pipeline.find_delivery

    def find_then_wait_for_the_other_copies(*args, **kwargs):
        found = find_delivery(*args, **kwargs)
        # Only each copy's first lookup: a copy that loses the race looks the delivery up again.
        if next(lookups) < copies:
            barrier.wait(timeout=30)
        return found

    with (
        mock.patch.object(
            pipeline, "find_delivery", autospec=True, side_effect=find_then_wait_for_the_other_copies
        ),
        ThreadPoolExecutor(copies) as executor,
    ):
        responses = list(executor.map(lambda _: post(client, name, body), range(copies)))
    return [response.status_code for response in responses]


def count_deliveries(session: Session) -> int:
    return session.execute(select(func.count()).select_from(InboundDelivery)).scalar_one()


def count_requests(session: Session) -> int:
    return session.execute(select(func.count()).select_from(WebhookCancelRequest)).scalar_one()


def count_actions(session: Session) -> tuple[int, ...]:
    """Count the asset events, the audit log entries and the queued Dag runs that deliveries made."""
    return tuple(
        session.execute(select(func.count()).select_from(table)).scalar_one()
        for table in (AssetEvent, Log, AssetDagRunQueue)
    )


@pytest.mark.backend("postgres", "mysql")
@pytest.mark.parametrize("copies", [2, 3])
@mock.patch.dict(os.environ, CONNECTION)
def test_concurrent_copies_of_a_delivery_are_recorded_once(client, session, copies):
    statuses = post_copies(client, copies)

    assert sorted(statuses) == [HTTPStatus.OK] * (copies - 1) + [HTTPStatus.ACCEPTED]
    assert count_deliveries(session) == 1


@pytest.mark.backend("postgres", "mysql")
@SUBSCRIPTIONS
@mock.patch.dict(os.environ, CONNECTION)
def test_concurrent_copies_of_a_subscribed_delivery_act_once(client, session, dag_maker, through):
    statuses = post_copies(client, 2, subscribe(dag_maker, through))

    assert sorted(statuses) == [HTTPStatus.OK, HTTPStatus.ACCEPTED]
    assert count_deliveries(session) == 1
    assert count_actions(session) == (1, 1, 1)


@pytest.mark.backend("postgres", "mysql")
@SUBSCRIPTIONS
@mock.patch.dict(os.environ, CONNECTION)
def test_concurrent_redeliveries_after_a_dag_subscribes_act_once(client, session, dag_maker, through):
    name = SUBSCRIBED_WEBHOOKS[through]
    assert post(client, name).json()["outcome"] == "no_subscribers"
    subscribe(dag_maker, through)

    statuses = post_copies(client, 2, name)

    assert sorted(statuses) == [HTTPStatus.OK, HTTPStatus.ACCEPTED]
    assert count_deliveries(session) == 1
    assert count_actions(session) == (1, 1, 1)


@pytest.mark.backend("postgres", "mysql")
@pytest.mark.usefixtures("keyed_source")
@mock.patch.dict(os.environ, CONNECTION)
def test_concurrent_copies_of_a_delivery_that_requests_a_cancel_record_one_request(
    client, session, dag_maker
):
    """The copy that loses has acted, so it is processed again, and keeps the request the other recorded."""
    declare_cancel_on(dag_maker)

    statuses = post_copies(client, 2, body=KEYED_BODY)

    assert statuses == [HTTPStatus.ACCEPTED, HTTPStatus.ACCEPTED]
    assert (count_deliveries(session), count_requests(session)) == (1, 1)
    assert count_actions(session) == (0, 1, 0)
    assert session.scalars(select(InboundDelivery.outcome)).one() == "cancel_requested"


@pytest.mark.backend("postgres", "mysql")
@pytest.mark.usefixtures("keyed_source")
@mock.patch.dict(os.environ, CONNECTION)
def test_concurrent_redeliveries_after_a_dag_declares_cancel_on_record_one_request(
    client, session, dag_maker
):
    """Both insert a request for the Dag; the unique key refuses the second, which is processed again."""
    assert post(client, body=KEYED_BODY).json()["outcome"] == "early"
    declare_cancel_on(dag_maker)

    statuses = post_copies(client, 2, body=KEYED_BODY)

    assert statuses == [HTTPStatus.ACCEPTED, HTTPStatus.ACCEPTED]
    assert (count_deliveries(session), count_requests(session)) == (1, 1)
    assert count_actions(session) == (0, 1, 0)
    assert session.scalars(select(InboundDelivery.outcome)).one() == "cancel_requested"


@pytest.mark.backend("sqlite")
@pytest.mark.usefixtures("keyed_source")
@mock.patch.dict(os.environ, CONNECTION)
def test_concurrent_copies_of_a_delivery_that_requests_a_cancel_record_at_most_one_request_on_sqlite(
    client, session, dag_maker
):
    declare_cancel_on(dag_maker)

    statuses = post_copies(client, 2, body=KEYED_BODY)

    # The copy that loses is processed again, unless it times out waiting for the database's lock.
    assert set(statuses) <= {HTTPStatus.ACCEPTED, HTTPStatus.SERVICE_UNAVAILABLE}
    recorded = count_deliveries(session)
    assert recorded == (1 if HTTPStatus.ACCEPTED in statuses else 0)
    assert count_requests(session) == recorded


@pytest.mark.backend("sqlite")
@mock.patch.dict(os.environ, CONNECTION)
def test_concurrent_copies_of_a_delivery_are_not_answered_as_errors_on_sqlite(client, session):
    # SQLite locks the whole database to write, so a copy may also time out waiting for the lock.
    statuses = post_copies(client, 2)

    assert set(statuses) <= {HTTPStatus.ACCEPTED, HTTPStatus.OK, HTTPStatus.SERVICE_UNAVAILABLE}
    recorded = count_deliveries(session)
    assert recorded == statuses.count(HTTPStatus.ACCEPTED)
    assert recorded <= 1


@pytest.mark.backend("sqlite")
@SUBSCRIPTIONS
@mock.patch.dict(os.environ, CONNECTION)
def test_concurrent_copies_of_a_subscribed_delivery_act_at_most_once_on_sqlite(
    client, session, dag_maker, through
):
    statuses = post_copies(client, 2, subscribe(dag_maker, through))

    assert set(statuses) <= {HTTPStatus.ACCEPTED, HTTPStatus.OK, HTTPStatus.SERVICE_UNAVAILABLE}
    recorded = statuses.count(HTTPStatus.ACCEPTED)
    assert recorded <= 1
    assert count_deliveries(session) == recorded
    assert count_actions(session) == (recorded, recorded, recorded)


@SUBSCRIPTIONS
@mock.patch.dict(os.environ, CONNECTION)
@mock.patch.object(AssetManager, "nofity_asset_event_emitted", autospec=True)
def test_delivery_whose_commit_fails_records_nothing_and_notifies_no_listener(
    mock_notify_event_emitted, client, session, dag_maker, through
):
    name = subscribe(dag_maker, through)

    def fail_to_commit_a_delivery(session: Session) -> None:
        if any(isinstance(instance, InboundDelivery) for instance in session.new):
            raise OperationalError("COMMIT", {}, Exception("database is locked"))

    event.listen(settings.NonScopedSession, "before_commit", fail_to_commit_a_delivery)
    try:
        response = post(client, name)
    finally:
        event.remove(settings.NonScopedSession, "before_commit", fail_to_commit_a_delivery)

    assert (response.status_code, response.headers.get("Retry-After")) == (503, "30")
    assert count_deliveries(session) == 0
    assert count_actions(session) == (0, 0, 0)
    mock_notify_event_emitted.assert_not_called()


@mock.patch.dict(os.environ, CONNECTION)
def test_delivery_is_answered_as_temporarily_unavailable_while_the_pool_is_exhausted(client, session):
    engine = create_engine(settings.get_engine().url, pool_size=1, max_overflow=0, pool_timeout=1)
    try:
        with engine.connect(), mock.patch.object(settings, "NonScopedSession", sessionmaker(engine)):
            response = post(client)
    finally:
        engine.dispose()

    assert (response.status_code, response.headers.get("Retry-After")) == (503, "30")
    assert count_deliveries(session) == 0
