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
from airflow.models.inbound_delivery import InboundDelivery
from airflow.providers_manager import ProvidersManager
from airflow.webhooks import pipeline
from airflow.webhooks.registry import WebhookRegistry

from tests_common.test_utils.db import clear_db_inbound_deliveries
from tests_common.test_utils.webhooks import (
    DELIVERY_ID_HEADER,
    EVENT_HEADER,
    SIGNATURE_HEADER,
    FakeHmacSource,
    compute_fake_signature,
)

if TYPE_CHECKING:
    from httpx2 import Response
    from sqlalchemy.orm import Session

pytestmark = pytest.mark.db_test

SECRET = "s3cret"
BODY = b'{"path": "my-org/web"}'
REGISTRY = WebhookRegistry(
    names=frozenset({"fake"}),
    source_type_owners={},
    connection_cache_ttl=60,
    max_body_bytes=1024,
    delivery_retention=timedelta(days=7),
)
CONNECTION = {
    "AIRFLOW_CONN_WEBHOOK_FAKE": json.dumps({"conn_type": "inbound_webhook", "password": SECRET}),
}


@pytest.fixture(autouse=True)
def source_class():
    with mock.patch.object(
        ProvidersManager, "get_webhook_source_class", autospec=True, return_value=FakeHmacSource
    ):
        yield


@pytest.fixture(autouse=True)
def clean_db():
    clear_db_inbound_deliveries()
    yield
    clear_db_inbound_deliveries()


@pytest.fixture
def client() -> TestClient:
    return TestClient(create_webhooks_app(REGISTRY), raise_server_exceptions=False)


def post(client: TestClient) -> Response:
    headers = {
        "content-type": "application/json",
        EVENT_HEADER: "push",
        DELIVERY_ID_HEADER: "delivery-1",
        SIGNATURE_HEADER: compute_fake_signature(BODY, SECRET),
    }
    return client.post("/fake", content=BODY, headers=headers)


def post_copies(client: TestClient, copies: int) -> list[int]:
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
        responses = list(executor.map(lambda _: post(client), range(copies)))
    return [response.status_code for response in responses]


def count_deliveries(session: Session) -> int:
    return session.execute(select(func.count()).select_from(InboundDelivery)).scalar_one()


@pytest.mark.backend("postgres", "mysql")
@pytest.mark.parametrize("copies", [2, 3])
@mock.patch.dict(os.environ, CONNECTION)
def test_concurrent_copies_of_a_delivery_are_recorded_once(client, session, copies):
    statuses = post_copies(client, copies)

    assert sorted(statuses) == [HTTPStatus.OK] * (copies - 1) + [HTTPStatus.ACCEPTED]
    assert count_deliveries(session) == 1


@pytest.mark.backend("sqlite")
@mock.patch.dict(os.environ, CONNECTION)
def test_concurrent_copies_of_a_delivery_are_not_answered_as_errors_on_sqlite(client, session):
    # SQLite locks the whole database to write, so a copy may also time out waiting for the lock.
    statuses = post_copies(client, 2)

    assert set(statuses) <= {HTTPStatus.ACCEPTED, HTTPStatus.OK, HTTPStatus.SERVICE_UNAVAILABLE}
    recorded = count_deliveries(session)
    assert recorded == statuses.count(HTTPStatus.ACCEPTED)
    assert recorded <= 1


@mock.patch.dict(os.environ, CONNECTION)
def test_delivery_whose_commit_fails_is_not_recorded(client, session):
    def fail_to_commit_a_delivery(session: Session) -> None:
        if any(isinstance(instance, InboundDelivery) for instance in session.new):
            raise OperationalError("COMMIT", {}, Exception("database is locked"))

    event.listen(settings.NonScopedSession, "before_commit", fail_to_commit_a_delivery)
    try:
        response = post(client)
    finally:
        event.remove(settings.NonScopedSession, "before_commit", fail_to_commit_a_delivery)

    assert (response.status_code, response.headers.get("Retry-After")) == (503, "30")
    assert count_deliveries(session) == 0


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
