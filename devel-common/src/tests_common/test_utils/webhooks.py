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
from typing import Any

from starlette.datastructures import Headers, QueryParams

from airflow.webhooks.base import (
    WebhookDelivery,
    WebhookRequest,
    WebhookSource,
    get_text,
    load_json_object,
    verify_hmac_sha256,
)

SIGNATURE_HEADER = "X-Fake-Signature"
EVENT_HEADER = "X-Fake-Event"
DELIVERY_ID_HEADER = "X-Fake-Delivery"


def compute_fake_signature(body: bytes, secret: str) -> str:
    """Return the ``X-Fake-Signature`` value that ``FakeHmacSource`` accepts for ``body``."""
    return f"sha256={hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()}"


def build_webhook_request(body: bytes = b"{}", headers: dict[str, str] | None = None) -> WebhookRequest:
    """Return a ``POST`` request as the webhooks app would hand it to a source type."""
    return WebhookRequest(method="POST", headers=Headers(headers or {}), query=QueryParams(""), body=body)


class FakeHmacSource(WebhookSource):
    """
    Webhook source type for tests that need one before a provider ships any.

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
