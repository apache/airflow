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

import hashlib
import hmac
import json
from typing import TYPE_CHECKING, Any

from sqlalchemy import func, select
from starlette.datastructures import Headers, QueryParams

from airflow.models.inbound_delivery import InboundDelivery
from airflow.webhooks.base import (
    WebhookDelivery,
    WebhookRequest,
    WebhookSource,
    get_text,
    load_json_object,
    verify_hmac_sha256,
)
from airflow.webhooks.naming import build_candidate_names

if TYPE_CHECKING:
    from fastapi.testclient import TestClient
    from httpx2 import Response
    from sqlalchemy.orm import Session

SIGNATURE_HEADER = "X-Fake-Signature"
EVENT_HEADER = "X-Fake-Event"
DELIVERY_ID_HEADER = "X-Fake-Delivery"
FAKE_SECRET = "s3cret"
FAKE_BODY = b'{"path": "my-org/web"}'

#: Values of the wrong type, or empty, for any field a source type reads.
ODD_VALUES = [None, 7, 1.5, True, "", "text", [], ["text"], [None], {}, {"text": 1}]


def compute_fake_signature(body: bytes, secret: str) -> str:
    """Return the ``X-Fake-Signature`` value that ``FakeHmacSource`` accepts for ``body``."""
    return f"sha256={hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()}"


def build_conn_env(
    name: str, *, conn_type: str = "inbound_webhook", password: str = FAKE_SECRET, **extra: Any
) -> dict[str, str]:
    """Return the environment variable that defines the ``webhook_<name>`` connection."""
    conn = {"conn_type": conn_type, "password": password, "extra": extra}
    return {f"AIRFLOW_CONN_WEBHOOK_{name.upper()}": json.dumps(conn)}


def post_signed_delivery(
    client: TestClient,
    *,
    name: str = "fake",
    body: bytes = FAKE_BODY,
    event: str = "push",
    delivery_id: str | None = "delivery-1",
    secret: str | None = FAKE_SECRET,
    content_type: str = "application/json",
) -> Response:
    """
    Post a delivery as a sender sends it to ``FakeHmacSource``: signed, with no Airflow credentials.

    :param secret: The secret to sign the body with, or None to send no signature.
    """
    headers = {"content-type": content_type, EVENT_HEADER: event}
    if delivery_id is not None:
        headers[DELIVERY_ID_HEADER] = delivery_id
    if secret is not None:
        headers[SIGNATURE_HEADER] = compute_fake_signature(body, secret)
    # As bytes, so that a header can carry what a client may send but httpx would not encode.
    return client.post(
        f"/{name}", content=body, headers={key.encode(): value.encode() for key, value in headers.items()}
    )


def count_deliveries(session: Session) -> int:
    return session.execute(select(func.count()).select_from(InboundDelivery)).scalar_one()


def build_webhook_request(body: bytes = b"{}", headers: dict[str, str] | None = None) -> WebhookRequest:
    """Return a ``POST`` request as the webhooks app would hand it to a source type."""
    return WebhookRequest(method="POST", headers=Headers(headers or {}), query=QueryParams(""), body=body)


def _replace_field(payload: dict[str, Any], dotted_path: str, value: Any) -> dict[str, Any]:
    payload = json.loads(json.dumps(payload))
    *parents, last = dotted_path.split(".")
    target: Any = payload
    for key in parents:
        target = target[int(key)] if isinstance(target, list) else target[key]
    if isinstance(target, list):
        target[int(last)] = value
    else:
        target[last] = value
    return payload


def assert_odd_values_are_parsed_or_refused(
    source: WebhookSource, payload: dict[str, Any], fields: list[str], headers: dict[str, str] | None = None
) -> None:
    """
    Assert that ``source`` parses or refuses ``payload`` with any of ``fields`` set to any of ``ODD_VALUES``.

    A signature proves who sent a delivery, not its shape. Anything but a ``ValueError`` (a 400) or a
    delivery of strings that core builds candidate names from would be a 500, which senders retry and
    some answer by disabling the webhook.

    :param fields: Dotted paths into ``payload`` of every field the source type reads, and of their
        parents; a list item is named by its index.
    """
    for field in fields:
        for value in ODD_VALUES:
            body = json.dumps(_replace_field(payload, field, value)).encode()
            request = build_webhook_request(body, {"content-type": "application/json", **(headers or {})})
            try:
                delivery = source.parse(request)
            except ValueError:
                continue
            assert delivery.path is None or isinstance(delivery.path, str), (field, value)
            assert all(isinstance(segment, str) for kind in delivery.kinds for segment in kind), (
                field,
                value,
            )
            assert delivery.delivery_id is None or isinstance(delivery.delivery_id, str), (field, value)
            build_candidate_names("hooks", delivery.path, delivery.kinds)


class FakeHmacSource(WebhookSource):
    """
    Webhook source type for core tests that must not depend on a provider.

    The body is signed with HMAC-SHA256 in ``X-Fake-Signature``, the event is named in ``X-Fake-Event``
    and the JSON body's ``path`` is the path. The event ``ping`` is a signed handshake, answered with the
    body's ``challenge``.
    """

    def verify(self, request: WebhookRequest, secret: str) -> None:
        verify_hmac_sha256(request.headers.get(SIGNATURE_HEADER), secret, request.body, prefix="sha256=")

    def parse(self, request: WebhookRequest) -> WebhookDelivery:
        payload = load_json_object(request)
        if not (event := request.headers.get(EVENT_HEADER)):
            raise ValueError(f"Missing {EVENT_HEADER} header")
        return WebhookDelivery(
            path=get_text(payload, "path"),
            kinds=[(event,)],
            attributes={"event": event},
            delivery_id=request.headers.get(DELIVERY_ID_HEADER),
            payload=payload,
        )

    def is_handshake(self, request: WebhookRequest) -> bool:
        return request.headers.get(EVENT_HEADER) == "ping"

    def handshake(self, request: WebhookRequest) -> dict[str, Any]:
        if not (challenge := get_text(load_json_object(request), "challenge")):
            raise ValueError("Missing challenge")
        return {"challenge": challenge}
