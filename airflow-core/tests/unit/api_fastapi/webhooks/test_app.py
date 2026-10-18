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
from airflow.api_fastapi.webhooks.app import create_webhooks_app
from airflow.models.base import Base
from airflow.models.connection import Connection
from airflow.models.inbound_delivery import InboundDelivery
from airflow.providers_manager import ProvidersManager
from airflow.secrets.base_secrets import BaseSecretsBackend
from airflow.webhooks import pipeline
from airflow.webhooks.base import WebhookDelivery, WebhookRequest
from airflow.webhooks.pipeline import DeliveryPipeline
from airflow.webhooks.registry import WebhookRegistry

from tests_common.test_utils.db import clear_db_connections, clear_db_inbound_deliveries
from tests_common.test_utils.webhooks import (
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
        {"fake", "copy", "unsigned", "unflat", "overlong", "broken", "stored", "nowhere", "elsewhere"}
    ),
    source_type_owners={},
    connection_cache_ttl=60,
    max_body_bytes=MAX_BODY_BYTES,
    delivery_retention=timedelta(days=7),
)
UNAVAILABLE = (503, "30", {"detail": "Temporarily unavailable"})


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
    """Collect the tables that the statements the requests execute name."""
    tables: set[str] = set()

    def on_execute(connection, cursor, statement, parameters, context, executemany) -> None:
        tables.update(table for table in Base.metadata.tables if re.search(rf"\b{table}\b", statement))

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
        yield
        clear_db_inbound_deliveries()

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
            "candidates": None,
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
    def test_delivery_reads_only_its_connection_and_the_delivery_table(self, client):
        with record_tables() as tables:
            assert post(client, name="stored").status_code == 202

        assert tables == {"connection", "inbound_delivery"}

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
