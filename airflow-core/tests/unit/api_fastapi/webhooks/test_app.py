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

import asyncio
import hashlib
import json
import os
import re
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any
from unittest import mock

import pytest
import time_machine
from fastapi import FastAPI
from fastapi.routing import APIRoute
from fastapi.testclient import TestClient
from sqlalchemy import event, inspect, select
from sqlalchemy.exc import (
    InterfaceError,
    OperationalError,
    ProgrammingError,
    TimeoutError as PoolTimeoutError,
)

from airflow import settings
from airflow._shared.webhook_naming import MAX_PATH_SEGMENTS
from airflow.api_fastapi.webhooks.app import create_webhooks_app
from airflow.assets.manager import AssetManager
from airflow.models.asset import AssetEvent, AssetModel, AssetPartitionDagRun
from airflow.models.base import Base
from airflow.models.connection import Connection
from airflow.models.inbound_delivery import InboundDelivery
from airflow.models.log import Log
from airflow.providers.standard.webhooks.generic import HmacJsonSource
from airflow.providers_manager import ProvidersManager
from airflow.sdk import Asset, PartitionedAssetTimetable
from airflow.secrets.base_secrets import BaseSecretsBackend
from airflow.webhooks import pipeline
from airflow.webhooks.base import WebhookDelivery, WebhookRequest
from airflow.webhooks.naming import build_topic_name
from airflow.webhooks.pipeline import DeliveryPipeline
from airflow.webhooks.registry import WebhookRegistry

from tests_common.test_utils.db import (
    clear_db_apdr,
    clear_db_assets,
    clear_db_connections,
    clear_db_inbound_deliveries,
    clear_db_logs,
    clear_db_pakl,
)
from tests_common.test_utils.webhooks import (
    DELIVERY_ID_HEADER,
    EVENT_HEADER,
    FAKE_BODY as BODY,
    FAKE_SECRET as SECRET,
    SIGNATURE_HEADER,
    FakeHmacSource,
    build_conn_env,
    compute_fake_signature,
    count_deliveries,
    post_signed_delivery as post,
)

if TYPE_CHECKING:
    from sqlalchemy.orm import Session
    from starlette.types import Message, Scope

MAX_BODY_BYTES = 128
REGISTRY = WebhookRegistry(
    names=frozenset(
        {
            "fake",
            "copy",
            "unsigned",
            "unflat",
            "overlong",
            "broken",
            "stored",
            "nowhere",
            "elsewhere",
            "misconfigured",
            "etl",
        }
    ),
    source_type_owners={},
    connection_cache_ttl=60,
    max_body_bytes=MAX_BODY_BYTES,
    delivery_retention=timedelta(days=7),
    rate_limit_per_minute=0,
    store_payload="full",
    max_event_payload_bytes=65536,
    early_delivery_ttl=timedelta(minutes=10),
    early_delivery_max_per_webhook=1000,
)
UNAVAILABLE = (503, "30", {"detail": "Temporarily unavailable"})
#: The topic that push deliveries of BODY to webhook fake offer.
TOPIC = Asset(name=build_topic_name("fake", "my-org/web", "push"))
#: What a service that loads tables sends webhook etl, which uses hmac_json, when it has loaded one.
ETL_PAYLOAD = {"event": "load.completed", "table": "orders", "data": {"date": "2026-10-19"}}
ETL_BODY = json.dumps(ETL_PAYLOAD).encode()
ETL_TOPIC = Asset(name=build_topic_name("etl", "orders", "load.completed"))
#: The data asset that webhook etl's asset route records an event on, for each day loaded.
ORDERS = Asset(name="orders", uri="s3://warehouse/orders")


class UnsignedHandshakeSource(FakeHmacSource):
    unsigned_handshake = True


class UnflatAttributesSource(FakeHmacSource):
    def parse(self, request: WebhookRequest) -> WebhookDelivery:
        attributes: dict[str, Any] = {"labels": ["deploy-preview"]}
        return WebhookDelivery(path=None, kinds=[], attributes=attributes, delivery_id=None, payload={})


class OverlongCorrelationKeySource(FakeHmacSource):
    def parse(self, request: WebhookRequest) -> WebhookDelivery:
        return replace(super().parse(request), correlation_key="x" * 256)


class BrokenHandshakeCheckSource(UnsignedHandshakeSource):
    def is_handshake(self, request: WebhookRequest) -> bool:
        raise KeyError(EVENT_HEADER)


SOURCE_CLASSES = {
    "fake": FakeHmacSource,
    "hmac_json": HmacJsonSource,
    "unsigned": UnsignedHandshakeSource,
    "unflat": UnflatAttributesSource,
    "overlong": OverlongCorrelationKeySource,
    "broken": BrokenHandshakeCheckSource,
}


CONNECTIONS = {
    **build_conn_env("fake"),
    **build_conn_env("copy", source_type="fake"),
    **build_conn_env("unsigned"),
    **build_conn_env("unflat"),
    **build_conn_env("overlong"),
    **build_conn_env("broken"),
    **build_conn_env("misconfigured", conn_type="generic"),
    **build_conn_env(
        "etl",
        source_type="hmac_json",
        signature_header=SIGNATURE_HEADER,
        signature_prefix="sha256=",
        event_field="event",
        path_field="table",
        delivery_id_header=DELIVERY_ID_HEADER,
        asset_routes=[{"topic": ETL_TOPIC.name, "asset": ORDERS.uri, "partition_key_field": "data.date"}],
    ),
}


def send_incomplete_body(app: FastAPI, name: str) -> list[Message]:
    """
    Send a request whose client disconnects before sending all of its body, and return what is sent back.

    As raw ASGI messages, since a test client always sends the whole body.
    """
    received: list[Message] = [{"type": "http.request", "body": BODY[:10], "more_body": True}]
    sent: list[Message] = []

    async def receive() -> Message:
        return received.pop(0) if received else {"type": "http.disconnect"}

    async def send(message: Message) -> None:
        sent.append(message)

    scope: Scope = {
        "type": "http",
        "asgi": {"version": "3.0"},
        "http_version": "1.1",
        "method": "POST",
        "scheme": "http",
        "path": f"/{name}",
        "raw_path": f"/{name}".encode(),
        "root_path": "",
        "query_string": b"",
        "headers": [(b"content-type", b"application/json"), (b"content-length", str(len(BODY)).encode())],
        "client": ("127.0.0.1", 50000),
        "server": ("testserver", 80),
    }
    asyncio.run(app(scope, receive, send))
    return sent


def get_deliveries(session: Session) -> list[InboundDelivery]:
    return list(session.scalars(select(InboundDelivery).order_by(InboundDelivery.received_at)))


def get_audit_logs(session: Session) -> list[Log]:
    return list(session.scalars(select(Log).where(Log.event == "webhook_delivery")))


def build_topic_actions(asset_event: AssetEvent) -> dict[str, Any]:
    """Return the actions of a delivery that recorded ``asset_event`` on TOPIC."""
    return {
        "asset_events": [{"asset": TOPIC.name, "asset_id": asset_event.asset_id, "event_id": asset_event.id}]
    }


def build_route_event(asset_event: AssetEvent) -> dict[str, Any]:
    """Return how the actions of a delivery to webhook etl list ``asset_event``, recorded on ORDERS."""
    return {
        "topic": ETL_TOPIC.name,
        "asset": ORDERS.uri,
        "asset_id": asset_event.asset_id,
        "event_id": asset_event.id,
        "partition_key": "2026-10-19",
    }


def build_skipped_route(reason: str) -> dict[str, Any]:
    """Return how the actions of a delivery to webhook etl list its route, skipped for ``reason``."""
    return {
        "topic": ETL_TOPIC.name,
        "asset": ORDERS.uri,
        "partition_key_field": "data.date",
        "reason": reason,
    }


def schedule_by_day(dag_maker, session: Session) -> None:
    """Have Dag orders_by_day schedule on the days of ORDERS, so that webhook etl's route records events."""
    with dag_maker("orders_by_day", schedule=PartitionedAssetTimetable(assets=ORDERS), session=session):
        pass


def subscribe(dag_maker, session: Session, **dag_state: bool) -> None:
    """Have Dag subscriber schedule on TOPIC, in ``dag_state``, such as ``is_paused=True``."""
    with dag_maker("subscriber", schedule=TOPIC, session=session):
        pass
    for column, value in dag_state.items():
        setattr(dag_maker.dag_model, column, value)
    session.commit()


@contextmanager
def count_checkouts() -> Iterator[list[object]]:
    """Collect each connection the requests check out of the pool, which every database access needs."""
    checkouts: list[object] = []

    def on_checkout(dbapi_connection, connection_record, connection_proxy) -> None:
        checkouts.append(dbapi_connection)

    event.listen(settings.get_engine(), "checkout", on_checkout)
    try:
        yield checkouts
    finally:
        event.remove(settings.get_engine(), "checkout", on_checkout)


@contextmanager
def record_tables() -> Iterator[set[str]]:
    """Collect the tables that the statements the requests execute read or write."""
    tables: set[str] = set()

    def on_execute(connection, cursor, statement, parameters, context, executemany) -> None:
        tables.update(
            table
            for table in Base.metadata.tables
            # Not just the name, which a column can have too, such as dag.deadline.
            if re.search(rf"\b(?:FROM|JOIN|INTO|UPDATE)\s+\(?{table}\b", statement)
        )

    event.listen(settings.get_engine(), "before_cursor_execute", on_execute)
    try:
        yield tables
    finally:
        event.remove(settings.get_engine(), "before_cursor_execute", on_execute)


class TestCreateWebhooksApp:
    def test_serves_only_the_allowlisted_routes(self):
        # Anyone who can reach the app can call it, so a route added here must be added on purpose.
        app = create_webhooks_app(REGISTRY)

        assert all(isinstance(route, APIRoute) for route in app.routes)
        assert {
            (method, route.path)
            for route in app.routes
            if isinstance(route, APIRoute)
            for method in route.methods
        } == {("GET", "/_health"), ("POST", "/{name}"), ("POST", "/{name}/")}
        assert (app.docs_url, app.redoc_url, app.openapi_url) == (None, None, None)


@pytest.mark.db_test
@mock.patch.dict(os.environ, CONNECTIONS)
class TestReceiveDelivery:
    @pytest.fixture(autouse=True)
    def source_classes(self):
        with mock.patch.object(
            ProvidersManager,
            "get_webhook_source_class",
            autospec=True,
            side_effect=lambda providers_manager, source_type, package_name=None: SOURCE_CLASSES.get(
                source_type
            ),
        ):
            yield

    @pytest.fixture(autouse=True)
    def clean_db(self):
        clear_db_inbound_deliveries()
        clear_db_assets()
        clear_db_logs()
        yield
        clear_db_inbound_deliveries()
        clear_db_assets()
        clear_db_logs()

    @pytest.fixture
    def client(self) -> TestClient:
        # Not following redirects, which senders such as GitHub do not follow either.
        return TestClient(create_webhooks_app(REGISTRY), follow_redirects=False)

    @pytest.fixture
    def stored_connection(self, session):
        """Connection webhook_stored, in the metadata database rather than in an environment variable."""
        session.add(
            Connection(
                conn_id="webhook_stored",
                conn_type="inbound_webhook",
                password=SECRET,
                extra='{"source_type": "fake"}',
            )
        )
        session.commit()
        yield
        clear_db_connections(add_default_connections_back=False)

    @time_machine.travel(datetime(2026, 10, 18, 12, tzinfo=UTC), tick=False)
    def test_delivery_is_recorded_with_what_arrived(self, client, session):
        response = post(client)

        assert (response.status_code, response.json()) == (
            202,
            {"delivery_id": "delivery-1", "outcome": "no_subscribers"},
        )
        (delivery,) = get_deliveries(session)
        assert {column: getattr(delivery, column) for column in inspect(InboundDelivery).columns.keys()} == {
            "id": mock.ANY,
            "source_kind": "webhook",
            "webhook": "fake",
            "delivery_id": "delivery-1",
            "body_digest": hashlib.sha256(BODY).hexdigest(),
            "received_at": datetime(2026, 10, 18, 12, tzinfo=UTC),
            "retain_until": datetime(2026, 10, 25, 12, tzinfo=UTC),
            "source_type": "fake",
            "attributes": {"event": "push"},
            "candidates": ["fake/my-org/web/push", "fake/my-org/*/push", "fake/*/push"],
            # A delivery that recorded nothing keeps no payload.
            "payload": None,
            "correlation_key": None,
            "outcome": "no_subscribers",
            "actions": None,
            "team_name": None,
        }

    def test_url_with_a_trailing_slash_is_served(self, client, session):
        response = client.post(
            "/fake/",
            content=BODY,
            headers={
                "content-type": "application/json",
                EVENT_HEADER: "push",
                SIGNATURE_HEADER: compute_fake_signature(BODY, SECRET),
            },
        )

        assert response.status_code == 202
        assert count_deliveries(session) == 1

    def test_redelivery_is_processed_again_without_a_second_row(self, client, session):
        responses = [post(client), post(client)]

        assert [(response.status_code, response.json()) for response in responses] == [
            (202, {"delivery_id": "delivery-1", "outcome": "no_subscribers"})
        ] * 2
        assert count_deliveries(session) == 1

    def test_copy_recorded_meanwhile_is_answered_as_a_duplicate(self, client, session):
        assert post(client).status_code == 202
        find_delivery = pipeline.find_delivery

        def miss_the_first_lookup(*args, **kwargs):
            # As when both copies looked before either had committed.
            return None if mock_find_delivery.call_count == 1 else find_delivery(*args, **kwargs)

        with mock.patch.object(
            pipeline, "find_delivery", autospec=True, side_effect=miss_the_first_lookup
        ) as mock_find_delivery:
            response = post(client)

        assert (response.status_code, response.json()) == (
            200,
            {"delivery_id": "delivery-1", "outcome": "duplicate"},
        )
        assert count_deliveries(session) == 1

    def test_delivery_to_a_subscribed_topic_records_an_event_and_an_audit_entry(
        self, client, session, dag_maker
    ):
        subscribe(dag_maker, session)

        response = post(client)

        assert (response.status_code, response.json()) == (
            202,
            {"delivery_id": "delivery-1", "outcome": "accepted"},
        )
        (delivery,) = get_deliveries(session)
        (asset_event,) = session.scalars(select(AssetEvent))
        assert (delivery.outcome, delivery.payload, delivery.actions) == (
            "accepted",
            {"path": "my-org/web"},
            build_topic_actions(asset_event),
        )
        assert asset_event.extra["inbound_delivery_id"] == str(delivery.id)
        (audit_log,) = get_audit_logs(session)
        assert (
            audit_log.owner,
            audit_log.owner_display_name,
            audit_log.dag_id,
            audit_log.team_name,
            json.loads(audit_log.extra),
        ) == (
            "webhook:fake",
            "Webhook fake",
            None,
            None,
            {"delivery_id": "delivery-1", "names": [TOPIC.name], "outcome": "accepted"},
        )

    @pytest.fixture
    def partition_records(self):
        """Remove the records that events with a partition key leave for the Dags they queue."""
        yield
        clear_db_apdr()
        clear_db_pakl()

    @pytest.mark.usefixtures("partition_records")
    def test_delivery_through_an_asset_route_records_an_event_on_its_asset_and_an_audit_entry(
        self, client, session, dag_maker
    ):
        schedule_by_day(dag_maker, session)

        response = post(client, name="etl", body=ETL_BODY)

        assert (response.status_code, response.json()) == (
            202,
            {"delivery_id": "delivery-1", "outcome": "accepted"},
        )
        (delivery,) = get_deliveries(session)
        (asset_event,) = session.scalars(select(AssetEvent))
        assert asset_event.asset_id == session.scalar(
            select(AssetModel.id).where(AssetModel.uri == ORDERS.uri)
        )
        assert (asset_event.partition_key, asset_event.extra["inbound_delivery_id"]) == (
            "2026-10-19",
            str(delivery.id),
        )
        assert (delivery.outcome, delivery.payload, delivery.actions) == (
            "accepted",
            ETL_PAYLOAD,
            {"asset_routes": [build_route_event(asset_event)]},
        )
        assert session.execute(
            select(AssetPartitionDagRun.target_dag_id, AssetPartitionDagRun.partition_key)
        ).all() == [("orders_by_day", "2026-10-19")]
        (audit_log,) = get_audit_logs(session)
        assert (audit_log.owner, json.loads(audit_log.extra)) == (
            "webhook:etl",
            {
                "delivery_id": "delivery-1",
                "names": [],
                "outcome": "accepted",
                "asset_routes": [{"topic": ETL_TOPIC.name, "asset": ORDERS.uri}],
            },
        )

    @pytest.mark.usefixtures("partition_records")
    def test_delivery_that_records_topic_and_route_events_lists_both(self, client, session, dag_maker):
        with dag_maker("on_load", schedule=ETL_TOPIC, session=session):
            pass
        schedule_by_day(dag_maker, session)

        assert post(client, name="etl", body=ETL_BODY).json()["outcome"] == "accepted"

        (delivery,) = get_deliveries(session)
        asset_events = {
            asset_event.partition_key: asset_event for asset_event in session.scalars(select(AssetEvent))
        }
        assert delivery.actions == {
            "asset_events": [
                {
                    "asset": ETL_TOPIC.name,
                    "asset_id": asset_events[None].asset_id,
                    "event_id": asset_events[None].id,
                }
            ],
            "asset_routes": [build_route_event(asset_events["2026-10-19"])],
        }
        (audit_log,) = get_audit_logs(session)
        assert json.loads(audit_log.extra) == {
            "delivery_id": "delivery-1",
            "names": [ETL_TOPIC.name],
            "outcome": "accepted",
            "asset_routes": [{"topic": ETL_TOPIC.name, "asset": ORDERS.uri}],
        }

    @pytest.mark.usefixtures("partition_records")
    def test_delivery_that_acts_only_through_a_route_acts_once_redelivered_after_a_dag_schedules_on_its_asset(
        self, client, session, dag_maker
    ):
        responses = [post(client, name="etl", body=ETL_BODY)]
        (delivery,) = get_deliveries(session)
        actions_before = delivery.actions
        schedule_by_day(dag_maker, session)
        responses.extend(post(client, name="etl", body=ETL_BODY) for _ in range(2))

        assert [(response.status_code, response.json()["outcome"]) for response in responses] == [
            (202, "no_subscribers"),
            (202, "accepted"),
            (200, "duplicate"),
        ]
        assert actions_before == {"skipped_asset_routes": [build_skipped_route("asset_not_active")]}
        session.expire_all()
        (delivery,) = get_deliveries(session)
        (asset_event,) = session.scalars(select(AssetEvent))
        assert (delivery.outcome, delivery.payload, delivery.actions) == (
            "accepted",
            ETL_PAYLOAD,
            {"asset_routes": [build_route_event(asset_event)]},
        )
        assert len(get_audit_logs(session)) == 1

    def test_redelivery_that_still_records_nothing_through_a_route_records_why_it_now_skips_it(
        self, client, session, dag_maker
    ):
        responses = [post(client, name="etl", body=ETL_BODY)]
        # The route reads a partition key, so the asset is active now, but its event cannot start this Dag.
        with dag_maker("whole_table", schedule=ORDERS, session=session):
            pass
        responses.append(post(client, name="etl", body=ETL_BODY))

        assert [(response.status_code, response.json()["outcome"]) for response in responses] == [
            (202, "no_subscribers"),
            (202, "no_subscribers"),
        ]
        session.expire_all()
        (delivery,) = get_deliveries(session)
        assert (delivery.outcome, delivery.payload, delivery.actions) == (
            "no_subscribers",
            None,
            {"skipped_asset_routes": [build_skipped_route("no_subscribers")]},
        )

    @pytest.mark.backend("sqlite")
    @pytest.mark.usefixtures("partition_records")
    def test_delivery_whose_transaction_sqlite_lock_contention_rolls_back_records_nothing(
        self, client, session, dag_maker
    ):
        with dag_maker("on_load", schedule=ETL_TOPIC, session=session):
            pass
        schedule_by_day(dag_maker, session)
        locked = []

        def fail_once_while_locked(connection, cursor, statement, parameters, context, executemany) -> None:
            # The asset manager rolls back the transaction, which holds the topic event, takes the lock
            # in a new one and records the route's event there.
            if statement.startswith("UPDATE asset SET id=asset.id") and not locked:
                locked.append(statement)
                raise OperationalError(statement, parameters, Exception("database is locked"))

        event.listen(settings.get_engine(), "before_cursor_execute", fail_once_while_locked)
        try:
            response = post(client, name="etl", body=ETL_BODY)
        finally:
            event.remove(settings.get_engine(), "before_cursor_execute", fail_once_while_locked)

        assert locked
        assert (response.status_code, response.headers.get("Retry-After"), response.json()) == UNAVAILABLE
        assert count_deliveries(session) == 0
        assert session.scalars(select(AssetEvent)).all() == []
        assert session.scalars(select(AssetPartitionDagRun)).all() == []
        assert get_audit_logs(session) == []

    @pytest.mark.parametrize(
        ("connection_store_payload", "default_store_payload", "expected_payload"),
        [
            pytest.param("full", "none", {"path": "my-org/web", "sender": "octocat"}, id="full"),
            pytest.param("summary", "none", {"sender": "octocat"}, id="summary"),
            pytest.param("none", "full", None, id="none"),
            pytest.param(None, "summary", {"sender": "octocat"}, id="option"),
        ],
    )
    @mock.patch.object(
        FakeHmacSource,
        "summarize",
        autospec=True,
        side_effect=lambda source, payload: {"sender": payload["sender"]},
    )
    def test_store_payload_applies_to_the_delivery_and_its_events(
        self,
        mock_summarize,
        session,
        dag_maker,
        connection_store_payload,
        default_store_payload,
        expected_payload,
    ):
        client = TestClient(create_webhooks_app(replace(REGISTRY, store_payload=default_store_payload)))
        subscribe(dag_maker, session)
        extra = {} if connection_store_payload is None else {"store_payload": connection_store_payload}

        with mock.patch.dict(os.environ, build_conn_env("fake", **extra)):
            post(client, body=b'{"path": "my-org/web", "sender": "octocat"}')

        (delivery,) = get_deliveries(session)
        (asset_event,) = session.scalars(select(AssetEvent))
        assert (delivery.payload, asset_event.extra["payload"]) == (expected_payload, expected_payload)

    def test_payload_over_max_event_payload_bytes_is_left_out_of_the_events_only(self, session, dag_maker):
        client = TestClient(create_webhooks_app(replace(REGISTRY, max_event_payload_bytes=1)))
        subscribe(dag_maker, session)

        post(client)

        (delivery,) = get_deliveries(session)
        (asset_event,) = session.scalars(select(AssetEvent))
        assert delivery.payload == {"path": "my-org/web"}
        assert (asset_event.extra["payload"], asset_event.extra["payload_truncated"]) == (None, True)

    def test_delivery_only_paused_dags_subscribe_to_acts_once_redelivered_after_unpausing(
        self, client, session, dag_maker
    ):
        subscribe(dag_maker, session, is_paused=True)
        responses = [post(client)]
        dag_maker.dag_model.is_paused = False
        session.commit()
        responses.append(post(client))
        with record_tables() as tables:
            responses.append(post(client))

        assert [(response.status_code, response.json()["outcome"]) for response in responses] == [
            (202, "dag_paused"),
            (202, "accepted"),
            (200, "duplicate"),
        ]
        (delivery,) = get_deliveries(session)
        (asset_event,) = session.scalars(select(AssetEvent))
        assert (delivery.outcome, delivery.actions) == (
            "accepted",
            build_topic_actions(asset_event),
        )
        assert asset_event.extra["inbound_delivery_id"] == str(delivery.id)
        assert len(get_audit_logs(session)) == 1
        # A retry of a delivery that acted is only looked up.
        assert tables == {"inbound_delivery"}

    def test_dag_paused_delivery_keeps_no_payload(self, client, session, dag_maker):
        subscribe(dag_maker, session, is_paused=True)

        post(client)

        (delivery,) = get_deliveries(session)
        assert (delivery.outcome, delivery.payload, delivery.actions) == ("dag_paused", None, None)
        assert session.scalars(select(AssetEvent)).all() == []
        assert get_audit_logs(session) == []

    def test_redelivery_that_still_records_nothing_records_its_new_outcome(self, client, session, dag_maker):
        responses = [post(client)]
        subscribe(dag_maker, session, is_paused=True)
        responses.append(post(client))

        assert [(response.status_code, response.json()["outcome"]) for response in responses] == [
            (202, "no_subscribers"),
            (202, "dag_paused"),
        ]
        (delivery,) = get_deliveries(session)
        assert (delivery.outcome, delivery.payload, delivery.actions) == ("dag_paused", None, None)

    @pytest.mark.parametrize(
        "paused_meanwhile",
        [
            pytest.param(False, id="acting"),
            # This copy records nothing, but would still have recorded its own outcome over the other's.
            pytest.param(True, id="recording-nothing"),
        ],
    )
    def test_redelivery_that_another_copy_acted_on_meanwhile_is_answered_as_a_duplicate(
        self, client, session, dag_maker, paused_meanwhile
    ):
        assert post(client).json()["outcome"] == "no_subscribers"
        subscribe(dag_maker, session)
        # The delivery as both copies of its redelivery find it, before either has acted.
        found_before_acting = pipeline.find_delivery(
            "fake", "delivery-1", hashlib.sha256(BODY).hexdigest(), session=session
        )
        assert post(client).json()["outcome"] == "accepted"
        dag_maker.dag_model.is_paused = paused_meanwhile
        session.commit()

        find_delivery = pipeline.find_delivery

        def find_as_before_acting(*args, **kwargs):
            return (
                found_before_acting if mock_find_delivery.call_count == 1 else find_delivery(*args, **kwargs)
            )

        with mock.patch.object(
            pipeline, "find_delivery", autospec=True, side_effect=find_as_before_acting
        ) as mock_find_delivery:
            response = post(client)

        assert (response.status_code, response.json()) == (
            200,
            {"delivery_id": "delivery-1", "outcome": "duplicate"},
        )
        assert [delivery.outcome for delivery in get_deliveries(session)] == ["accepted"]
        assert len(session.scalars(select(AssetEvent)).all()) == 1
        assert len(get_audit_logs(session)) == 1

    @pytest.mark.parametrize(
        "record_removed",
        [
            # As when the copy that recorded nothing had not committed yet when this copy looked.
            pytest.param(False, id="first-copy-recorded-nothing"),
            # As when airflow db clean removed the record after this copy found it.
            pytest.param(True, id="record-removed"),
        ],
    )
    def test_copy_that_found_a_dag_to_act_for_though_no_copy_acted_is_processed_again(
        self, client, session, dag_maker, record_removed
    ):
        assert post(client).json()["outcome"] == "no_subscribers"
        subscribe(dag_maker, session)
        find_delivery = pipeline.find_delivery
        stale_lookup = None
        if record_removed:
            stale_lookup = find_delivery(
                "fake", "delivery-1", hashlib.sha256(BODY).hexdigest(), session=session
            )
            clear_db_inbound_deliveries()

        def find_as_before(*args, **kwargs):
            return stale_lookup if mock_find_delivery.call_count == 1 else find_delivery(*args, **kwargs)

        with mock.patch.object(
            pipeline, "find_delivery", autospec=True, side_effect=find_as_before
        ) as mock_find_delivery:
            response = post(client)

        assert (response.status_code, response.json()) == (
            202,
            {"delivery_id": "delivery-1", "outcome": "accepted"},
        )
        (delivery,) = get_deliveries(session)
        (asset_event,) = session.scalars(select(AssetEvent))
        assert (delivery.outcome, delivery.actions) == (
            "accepted",
            build_topic_actions(asset_event),
        )
        assert asset_event.extra["inbound_delivery_id"] == str(delivery.id)
        assert len(get_audit_logs(session)) == 1

    def test_copy_that_loses_twice_to_copies_that_recorded_nothing_is_answered_unavailable(
        self, client, session, dag_maker
    ):
        assert post(client).json()["outcome"] == "no_subscribers"
        subscribe(dag_maker, session)
        find_delivery = pipeline.find_delivery

        def miss_the_lookups_before_acting(*args, **kwargs):
            # The lookups after rolling back find the record.
            return None if mock_find_delivery.call_count % 2 else find_delivery(*args, **kwargs)

        with mock.patch.object(
            pipeline, "find_delivery", autospec=True, side_effect=miss_the_lookups_before_acting
        ) as mock_find_delivery:
            response = post(client)

        assert (response.status_code, response.headers.get("Retry-After"), response.json()) == UNAVAILABLE
        assert [delivery.outcome for delivery in get_deliveries(session)] == ["no_subscribers"]
        assert session.scalars(select(AssetEvent)).all() == []
        assert get_audit_logs(session) == []

    def test_delivery_with_a_deep_path_is_offered_only_under_paths_a_topic_can_have(
        self, client, session, dag_maker
    ):
        deepest_wildcard_path = f"{'a/' * (MAX_PATH_SEGMENTS - 1)}*"
        with dag_maker(
            "subscriber",
            schedule=Asset(name=build_topic_name("fake", deepest_wildcard_path, "push")),
            session=session,
        ):
            pass

        response = post(client, body=json.dumps({"path": "/".join(["a"] * (MAX_PATH_SEGMENTS + 8))}).encode())

        assert response.json()["outcome"] == "accepted"
        (delivery,) = get_deliveries(session)
        assert delivery.candidates == [
            f"fake/{'a/' * depth}*/push" for depth in range(MAX_PATH_SEGMENTS - 1, -1, -1)
        ]

    @mock.patch.object(AssetManager, "nofity_asset_event_emitted", autospec=True)
    def test_listeners_hear_of_topic_events_once_they_are_committed(
        self, mock_notify_event_emitted, client, session, dag_maker
    ):
        subscribe(dag_maker, session)
        calls_at_commit = []

        def count_calls_at_commit(session: Session) -> None:
            calls_at_commit.append(mock_notify_event_emitted.call_count)

        event.listen(settings.NonScopedSession, "before_commit", count_calls_at_commit)
        try:
            assert post(client).status_code == 202
        finally:
            event.remove(settings.NonScopedSession, "before_commit", count_calls_at_commit)

        assert calls_at_commit == [0]
        mock_notify_event_emitted.assert_called_once()

    @mock.patch.object(pipeline, "find_delivery", autospec=True, return_value=None)
    def test_integrity_error_that_no_recorded_delivery_explains_is_a_bug(self, mock_find_delivery, session):
        client = TestClient(create_webhooks_app(REGISTRY))
        assert post(client).status_code == 202

        response = post(client)

        assert (response.status_code, response.json()) == (500, {"detail": "Internal Server Error"})
        assert count_deliveries(session) == 1

    def test_another_body_under_a_recorded_delivery_id_is_a_new_delivery(self, client, session):
        # The delivery id is a header no signature covers, so it cannot claim a body it did not come with.
        post(client)
        post(client, body=b'{"path": "my-org/api"}')

        assert count_deliveries(session) == 2

    def test_delivery_ids_are_unique_per_webhook(self, client, session):
        post(client)
        post(client, name="copy")

        assert sorted(delivery.webhook for delivery in get_deliveries(session)) == ["copy", "fake"]

    @pytest.mark.parametrize("delivery_id", [pytest.param(None, id="missing"), pytest.param("", id="empty")])
    def test_deliveries_without_an_id_are_never_taken_for_retries(self, client, session, delivery_id):
        responses = [post(client, delivery_id=delivery_id) for _ in range(2)]

        assert [response.json() for response in responses] == [
            {"delivery_id": None, "outcome": "no_subscribers"}
        ] * 2
        assert [delivery.delivery_id for delivery in get_deliveries(session)] == [None, None]

    def test_longest_delivery_id_that_can_be_stored_is_recorded(self, client, session):
        assert post(client, delivery_id="x" * 255).status_code == 202

        assert [delivery.delivery_id for delivery in get_deliveries(session)] == ["x" * 255]

    @pytest.mark.parametrize(
        "delivery_id",
        [
            pytest.param("x" * 256, id="too-long"),
            pytest.param("délivery", id="not-ascii"),
            # PostgreSQL refuses to store NUL in text.
            pytest.param("a\x00b", id="control-character"),
        ],
    )
    def test_delivery_id_that_cannot_be_stored_is_refused(self, client, session, delivery_id):
        # From parse rather than a header: a source type may take the id from the body, which can carry
        # what a header cannot.
        delivery = WebhookDelivery(
            path=None, kinds=[("push",)], attributes={}, delivery_id=delivery_id, payload={}
        )
        with mock.patch.object(FakeHmacSource, "parse", autospec=True, return_value=delivery):
            response = post(client)

        assert (response.status_code, response.json()) == (
            400,
            {"detail": "Delivery ID must be at most 255 printable ASCII characters"},
        )
        assert count_deliveries(session) == 0

    @pytest.mark.parametrize(
        ("request_args", "expected_status", "expected_detail"),
        [
            pytest.param({"name": "gitlab"}, 404, "Unknown webhook", id="not-listed"),
            pytest.param({"name": "nowhere"}, 404, "Unknown webhook", id="no-connection"),
            pytest.param({"body": b"x" * 129}, 413, "Body exceeds 128 bytes", id="too-large"),
            pytest.param({"secret": "wrong"}, 401, "Invalid webhook credentials", id="bad-signature"),
            pytest.param({"secret": None}, 401, "Invalid webhook credentials", id="no-signature"),
            pytest.param({"event": ""}, 400, "Missing X-Fake-Event header", id="no-event"),
        ],
    )
    def test_refused_delivery(self, client, session, request_args, expected_status, expected_detail):
        response = post(client, **request_args)

        assert (response.status_code, response.json()) == (expected_status, {"detail": expected_detail})
        assert count_deliveries(session) == 0

    @pytest.mark.parametrize(
        ("chunks", "headers", "expected_status"),
        [
            pytest.param([BODY], {"content-length": "129"}, 413, id="declared-over"),
            # More digits than int() converts.
            pytest.param([BODY], {"content-length": "9" * 5000}, 413, id="declared-far-over"),
            # Without a Content-Length to refuse it by.
            pytest.param([b"x" * 100, b"x" * 100], {}, 413, id="streamed-over"),
            # A body within the limit is read, and then fails verification, since it is unsigned.
            pytest.param([b"x" * 128], {"content-length": "128"}, 401, id="declared-at-the-limit"),
            pytest.param(
                [b"x" * 128], {"content-length": "0" * 5000 + "128"}, 401, id="declared-with-leading-zeros"
            ),
            pytest.param([b"x" * 64, b"x" * 64], {}, 401, id="streamed-at-the-limit"),
        ],
    )
    def test_body_limit(self, client, chunks, headers, expected_status):
        response = client.post("/fake", content=iter(chunks), headers=headers)

        assert response.status_code == expected_status

    def test_client_that_disconnects_before_sending_the_body_is_refused_without_logging(
        self, session, caplog
    ):
        start, body = send_incomplete_body(create_webhooks_app(REGISTRY), "fake")

        assert (start["status"], json.loads(body["body"])) == (400, {"detail": "Incomplete body"})
        assert count_deliveries(session) == 0
        assert list(caplog) == []

    @mock.patch.object(FakeHmacSource, "verify", autospec=True)
    def test_body_limit_is_checked_before_verification(self, mock_verify, client):
        assert post(client, body=b"x" * 129).status_code == 413

        mock_verify.assert_not_called()

    @pytest.mark.parametrize(
        ("name", "secret", "expected_status", "expected_body"),
        [
            pytest.param("fake", SECRET, 200, {"challenge": "c-1"}, id="signed"),
            pytest.param(
                "fake", None, 401, {"detail": "Invalid webhook credentials"}, id="signed-unverified"
            ),
            pytest.param("unsigned", None, 200, {"challenge": "c-1"}, id="declared-unsigned"),
        ],
    )
    def test_handshake_is_answered_without_recording_anything(
        self, client, session, name, secret, expected_status, expected_body
    ):
        with count_checkouts() as checkouts:
            response = post(client, name=name, body=b'{"challenge": "c-1"}', event="ping", secret=secret)

        assert (response.status_code, response.json()) == (expected_status, expected_body)
        assert checkouts == []
        assert count_deliveries(session) == 0

    def test_unsigned_handshake_source_still_verifies_deliveries(self, client):
        assert post(client, name="unsigned", secret=None).status_code == 401

    @mock.patch.object(UnsignedHandshakeSource, "is_handshake", autospec=True, return_value=False)
    def test_unsigned_handshake_source_is_asked_once_whether_a_request_is_a_handshake(
        self, mock_is_handshake, client
    ):
        assert post(client, name="unsigned").status_code == 202

        mock_is_handshake.assert_called_once()

    def test_malformed_handshake_is_refused(self, client):
        response = post(client, body=b"{}", event="ping")

        assert (response.status_code, response.json()) == (400, {"detail": "Missing challenge"})

    def test_secret_changed_in_the_connection_applies_to_the_first_delivery_signed_with_it(self, client):
        assert post(client, delivery_id="before").status_code == 202

        with mock.patch.dict(os.environ, build_conn_env("fake", password="rotated")):
            response = post(client, delivery_id="after", secret="rotated")

        assert response.status_code == 202

    @mock.patch.object(FakeHmacSource, "verify", autospec=True, side_effect=KeyError("signature"))
    def test_verifier_that_raises_is_answered_as_a_failed_verification(
        self, mock_verify, client, session, caplog
    ):
        response = post(client)

        assert (response.status_code, response.json()) == (401, {"detail": "Invalid webhook credentials"})
        assert count_deliveries(session) == 0
        assert {"event": "Refused a webhook delivery: verifying it raised", "log_level": "error"} in caplog

    @pytest.mark.parametrize(
        ("name", "event"),
        [
            pytest.param("unflat", "push", id="attributes-not-flat"),
            pytest.param("overlong", "push", id="correlation-key-too-long"),
            pytest.param("broken", "ping", id="handshake-check-raises"),
        ],
    )
    def test_bug_in_a_source_type_is_answered_without_detail(self, session, caplog, name, event):
        client = TestClient(create_webhooks_app(REGISTRY))

        response = post(client, name=name, event=event)

        assert (response.status_code, response.json()) == (500, {"detail": "Internal Server Error"})
        assert count_deliveries(session) == 0
        assert {"event": "Unhandled error in the webhooks app", "log_level": "error"} in caplog

    @mock.patch.dict(os.environ, build_conn_env("fake", conn_type="generic"))
    def test_misconfigured_connection_is_answered_as_temporarily_unavailable(self, client):
        response = post(client)

        assert (response.status_code, response.headers.get("Retry-After"), response.json()) == UNAVAILABLE

    @mock.patch("airflow.webhooks.connections.ensure_secrets_loaded", autospec=True)
    def test_failing_secrets_backend_is_answered_as_temporarily_unavailable(
        self, mock_ensure_secrets_loaded, client, caplog
    ):
        backend = mock.create_autospec(BaseSecretsBackend, instance=True)
        backend.get_connection.side_effect = OperationalError("SELECT", {}, Exception("could not connect"))
        mock_ensure_secrets_loaded.return_value = [backend]

        response = post(client)

        assert (response.status_code, response.headers.get("Retry-After"), response.json()) == UNAVAILABLE
        assert {
            "event": "Refused a webhook delivery: its connection could not be read",
            "webhook": "fake",
        } in caplog

    @pytest.mark.parametrize(
        "error",
        [
            pytest.param(OperationalError("SELECT", {}, Exception("database is locked")), id="locked"),
            pytest.param(InterfaceError("SELECT", {}, Exception("connection closed")), id="disconnected"),
            pytest.param(PoolTimeoutError("QueuePool limit reached"), id="pool-exhausted"),
        ],
    )
    def test_unusable_database_is_answered_as_temporarily_unavailable(self, client, caplog, error):
        with mock.patch.object(pipeline, "find_delivery", autospec=True, side_effect=error):
            responses = [post(client) for _ in range(2)]

        assert [(r.status_code, r.headers.get("Retry-After"), r.json()) for r in responses] == [
            UNAVAILABLE
        ] * 2
        # Once, though both failed: while the database is down, every delivery fails the same way.
        assert [
            entry["webhook"]
            for entry in caplog
            if entry["event"].startswith("Could not record a webhook delivery")
        ] == ["fake"]

    @pytest.mark.parametrize(
        "has_delivery_table",
        [
            pytest.param({"return_value": False}, id="missing"),
            pytest.param(
                {"side_effect": OperationalError("SELECT", {}, Exception("server closed the connection"))},
                id="database-lost-while-checking",
            ),
        ],
    )
    @mock.patch.object(
        pipeline,
        "find_delivery",
        autospec=True,
        side_effect=ProgrammingError("SELECT", {}, Exception("relation does not exist")),
    )
    def test_missing_table_is_answered_as_temporarily_unavailable(
        self, mock_find_delivery, client, has_delivery_table
    ):
        # find_delivery fails as PostgreSQL and MySQL report a missing table.
        with mock.patch.object(pipeline, "_has_delivery_table", autospec=True, **has_delivery_table):
            response = post(client)

        assert (response.status_code, response.headers.get("Retry-After"), response.json()) == UNAVAILABLE

    @mock.patch.object(
        pipeline,
        "find_delivery",
        autospec=True,
        side_effect=ProgrammingError("SELECT", {}, Exception("syntax error")),
    )
    def test_other_programming_errors_are_bugs(self, mock_find_delivery):
        client = TestClient(create_webhooks_app(REGISTRY))

        response = post(client)

        assert (response.status_code, response.json()) == (500, {"detail": "Internal Server Error"})

    @pytest.mark.parametrize(
        ("request_args", "names"),
        [
            pytest.param({}, ("nowhere", "elsewhere"), id="no-connection"),
            pytest.param({"body": b"x" * 129}, ("fake", "copy"), id="too-large"),
            pytest.param({"secret": "wrong"}, ("fake", "copy"), id="bad-signature"),
            pytest.param({"event": ""}, ("fake", "copy"), id="malformed"),
        ],
    )
    @mock.patch("airflow.webhooks.pipeline.monotonic", autospec=True)
    def test_refused_deliveries_are_logged_at_most_once_a_minute_per_webhook(
        self, mock_monotonic, client, caplog, request_args, names
    ):
        # Anyone can send requests that are refused, so logging each one would let them flood the log.
        name, other_name = names
        for now, webhook in [(1000.0, name), (1059.0, name), (1059.0, other_name), (1060.0, name)]:
            mock_monotonic.return_value = now
            post(client, name=webhook, **request_args)

        assert [entry["webhook"] for entry in caplog if entry["event"].startswith("Refused")] == [
            name,
            other_name,
            name,
        ]

    @mock.patch.object(DeliveryPipeline, "get_webhook", autospec=True, side_effect=RuntimeError("bug"))
    @mock.patch("airflow.webhooks.pipeline.monotonic", autospec=True)
    def test_errors_are_logged_at_most_once_a_minute_per_webhook(
        self, mock_monotonic, mock_get_webhook, caplog
    ):
        # A request that nothing has verified can reach a bug, such as one in an unsigned is_handshake.
        client = TestClient(create_webhooks_app(REGISTRY))
        for now, name in [
            (1000.0, "fake"),
            (1059.0, "fake"),
            (1059.0, "copy"),
            (1059.0, "gitlab"),
            (1059.0, "bitbucket"),
            (1060.0, "fake"),
        ]:
            mock_monotonic.return_value = now
            assert post(client, name=name).status_code == 500

        # Names that are not listed are chosen by whoever sends them, so they share one slot.
        assert [
            entry["webhook"] for entry in caplog if entry["event"] == "Unhandled error in the webhooks app"
        ] == ["fake", "copy", None, "fake"]

    @pytest.mark.usefixtures("stored_connection")
    def test_delivery_reads_and_writes_only_what_recording_its_topic_events_and_finding_its_waits_needs(
        self, client, session, dag_maker
    ):
        with dag_maker(
            "subscriber",
            schedule=Asset(name=build_topic_name("stored", "my-org/web", "push")),
            session=session,
        ):
            pass

        with record_tables() as tables:
            assert post(client, name="stored").status_code == 202

        assert tables == {
            "connection",
            "inbound_delivery",
            "asset",
            "asset_active",
            "asset_alias",
            "asset_alias_asset",
            "dag",
            "dag_schedule_asset_reference",
            "dag_schedule_asset_name_reference",
            "dag_schedule_asset_uri_reference",
            "asset_event",
            "asset_dag_run_queue",
            "inbound_wait",
            "log",
        }

    @pytest.mark.usefixtures("partition_records")
    def test_delivery_reads_and_writes_only_what_recording_keyed_route_events_and_finding_its_waits_needs(
        self, client, session, dag_maker
    ):
        schedule_by_day(dag_maker, session)

        with record_tables() as tables:
            assert post(client, name="etl", body=ETL_BODY).status_code == 202

        assert tables == {
            "inbound_delivery",
            "asset",
            "asset_active",
            "asset_alias",
            "asset_alias_asset",
            "dag",
            "dag_schedule_asset_reference",
            "dag_schedule_asset_name_reference",
            "dag_schedule_asset_uri_reference",
            "asset_event",
            "serialized_dag",
            "dag_version",
            "asset_partition_dag_run",
            "partitioned_asset_key_log",
            "inbound_wait",
            "log",
        }

    @pytest.mark.usefixtures("stored_connection")
    def test_unsigned_and_unlisted_requests_do_not_reach_the_database(self, client, caplog):
        # The first unsigned request has the connection read again, in case its secret has changed.
        assert [post(client, name="stored", secret=secret).status_code for secret in (SECRET, None)] == [
            202,
            401,
        ]

        with count_checkouts() as checkouts:
            statuses = {post(client, name="stored", secret=None).status_code for _ in range(100)}
            statuses |= {post(client, name="gitlab").status_code for _ in range(100)}

        assert statuses == {401, 404}
        assert checkouts == []
        # Unlisted names are chosen by whoever sends them, so they are never logged.
        assert [entry["webhook"] for entry in caplog if entry["event"].startswith("Refused")] == ["stored"]

    def test_health_does_not_reach_the_database(self, client):
        with count_checkouts() as checkouts:
            response = client.get("/_health")

        assert (response.status_code, response.json()) == (200, {"status": "healthy"})
        assert checkouts == []

    @mock.patch("airflow.webhooks.pipeline.monotonic", autospec=True)
    def test_deliveries_over_the_rate_limit_are_refused_until_it_refills(self, mock_monotonic, session):
        client = TestClient(create_webhooks_app(replace(REGISTRY, rate_limit_per_minute=2)))
        mock_monotonic.return_value = 1000.0
        responses = [post(client, delivery_id=f"delivery-{n}") for n in range(3)]
        # Two a minute is one more delivery every 30 seconds.
        mock_monotonic.return_value = 1030.0
        responses.append(post(client, delivery_id="delivery-3"))

        assert [response.status_code for response in responses] == [202, 202, 429, 202]
        assert (responses[2].headers.get("Retry-After"), responses[2].json()) == (
            "30",
            {"detail": "Too many deliveries"},
        )
        assert sorted(delivery.delivery_id for delivery in get_deliveries(session)) == [
            "delivery-0",
            "delivery-1",
            "delivery-3",
        ]

    @pytest.mark.parametrize(
        ("request_args", "expected_status"),
        [
            pytest.param({"secret": "wrong"}, 401, id="failed-verification"),
            pytest.param({"body": b'{"challenge": "c-1"}', "event": "ping"}, 200, id="handshake"),
        ],
    )
    def test_requests_that_are_not_verified_deliveries_do_not_use_up_the_rate_limit(
        self, request_args, expected_status
    ):
        client = TestClient(create_webhooks_app(replace(REGISTRY, rate_limit_per_minute=1)))

        statuses = [post(client, **request_args).status_code for _ in range(3)]
        statuses += [post(client, delivery_id=f"delivery-{n}").status_code for n in range(2)]

        assert statuses == [expected_status] * 3 + [202, 429]

    def test_each_webhook_has_its_own_rate_limit(self):
        client = TestClient(create_webhooks_app(replace(REGISTRY, rate_limit_per_minute=1)))

        assert [post(client, name=name).status_code for name in ("fake", "fake", "copy")] == [202, 429, 202]

    @mock.patch.object(FakeHmacSource, "parse", autospec=True, side_effect=FakeHmacSource.parse)
    def test_deliveries_over_the_rate_limit_are_not_parsed(self, mock_parse):
        client = TestClient(create_webhooks_app(replace(REGISTRY, rate_limit_per_minute=1)))

        assert [post(client, delivery_id=f"delivery-{n}").status_code for n in range(2)] == [202, 429]
        mock_parse.assert_called_once()

    @mock.patch("airflow.webhooks.pipeline.monotonic", autospec=True)
    def test_deliveries_over_the_rate_limit_are_logged_at_most_once_a_minute_per_webhook(
        self, mock_monotonic, caplog
    ):
        client = TestClient(create_webhooks_app(replace(REGISTRY, rate_limit_per_minute=1)))
        statuses = []
        for now, name in [
            (1000.0, "fake"),
            (1000.0, "fake"),
            (1059.0, "fake"),
            (1059.0, "copy"),
            (1059.0, "copy"),
            (1061.0, "fake"),
            (1061.0, "fake"),
        ]:
            mock_monotonic.return_value = now
            statuses.append(post(client, name=name).status_code)

        assert statuses == [202, 429, 429, 202, 429, 202, 429]
        assert [entry["webhook"] for entry in caplog if entry["event"].startswith("Refused")] == [
            "fake",
            "copy",
            "fake",
        ]

    @pytest.mark.parametrize(
        ("dag_state", "expected_outcome"),
        [
            pytest.param(None, "no_subscribers", id="no-subscribers"),
            pytest.param({"is_paused": True}, "dag_paused", id="dag-paused"),
            pytest.param({}, "accepted", id="accepted"),
        ],
    )
    @mock.patch("airflow.webhooks.pipeline.stats", autospec=True)
    def test_recorded_delivery_is_counted_by_outcome_and_timed(
        self, mock_stats, client, session, dag_maker, dag_state, expected_outcome
    ):
        if dag_state is not None:
            subscribe(dag_maker, session, **dag_state)

        assert post(client).status_code == 202

        mock_stats.timer.assert_called_once_with("webhooks.delivery_duration", tags={"webhook": "fake"})
        mock_stats.timer.return_value.stop.assert_called_once_with()
        mock_stats.incr.assert_called_once_with(
            "webhooks.deliveries", tags={"webhook": "fake", "outcome": expected_outcome}
        )

    @mock.patch.object(
        pipeline, "find_delivery", autospec=True, side_effect=PoolTimeoutError("QueuePool limit reached")
    )
    @mock.patch("airflow.webhooks.pipeline.stats", autospec=True)
    def test_recording_that_fails_is_not_timed(self, mock_stats, mock_find_delivery, client):
        assert post(client).status_code == 503

        mock_stats.timer.return_value.stop.assert_called_once_with(send=False)

    @pytest.mark.parametrize(
        ("request_args", "expected_tags"),
        [
            pytest.param({"name": "gitlab"}, {"webhook": "other", "status": "404"}, id="not-listed"),
            pytest.param({"name": "nowhere"}, {"webhook": "nowhere", "status": "404"}, id="no-connection"),
            pytest.param({"body": b"x" * 129}, {"webhook": "fake", "status": "413"}, id="too-large"),
            pytest.param({"secret": "wrong"}, {"webhook": "fake", "status": "401"}, id="bad-signature"),
            pytest.param({"event": ""}, {"webhook": "fake", "status": "400"}, id="malformed"),
            # As is a webhook whose source type two installed providers declare.
            pytest.param(
                {"name": "misconfigured"}, {"webhook": "misconfigured", "status": "503"}, id="misconfigured"
            ),
        ],
    )
    @mock.patch("airflow.webhooks.pipeline.stats", autospec=True)
    def test_refused_delivery_is_counted_by_webhook_and_status(
        self, mock_stats, client, request_args, expected_tags
    ):
        post(client, **request_args)

        mock_stats.incr.assert_called_once_with("webhooks.rejections", tags=expected_tags)

    @pytest.mark.parametrize(
        ("method", "path", "expected_answer", "expected_tags"),
        [
            pytest.param(
                "POST",
                "/fake/deliveries",
                (404, None, {"detail": "Not Found"}),
                {"webhook": "other", "status": "404"},
                id="sub-path",
            ),
            pytest.param(
                "GET",
                "/fake",
                (405, "POST", {"detail": "Method Not Allowed"}),
                {"webhook": "fake", "status": "405"},
                id="method",
            ),
        ],
    )
    @mock.patch("airflow.webhooks.pipeline.stats", autospec=True)
    def test_request_that_no_route_serves_is_counted_as_refused(
        self, mock_stats, client, method, path, expected_answer, expected_tags
    ):
        response = client.request(method, path)

        assert (response.status_code, response.headers.get("Allow"), response.json()) == expected_answer
        mock_stats.incr.assert_called_once_with("webhooks.rejections", tags=expected_tags)
