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

import base64
import hashlib
import hmac
import json

import pytest

pytest.importorskip("airflow.webhooks.base")

from airflow.providers.standard.webhooks.generic import HmacJsonSource, TokenHeaderSource
from airflow.webhooks.base import WebhookDelivery, WebhookSource, WebhookVerificationError

from tests_common.test_utils.webhooks import assert_odd_values_are_parsed_or_refused, build_webhook_request

SECRET = "s3cret"
BODY = b'{"ref": "refs/heads/main"}'
JSON = {"content-type": "application/json"}
DELIVERY_OPTIONS = {"event_field": "data.type", "path_field": "service", "delivery_id_field": "meta.id"}
#: The conn-fields of the inbound_webhook connection type in provider.yaml.
CONN_FIELDS = (
    "source_type",
    "signature_header",
    "signature_prefix",
    "signature_encoding",
    "token_header",
    "token_prefix",
    "event_field",
    "event_header",
    "path_field",
    "delivery_id_field",
    "delivery_id_header",
    "store_payload",
    "max_body_bytes",
)


def compute_hex_signature(body: bytes = BODY, secret: str = SECRET) -> str:
    return hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()


def parse_json(source: WebhookSource, payload: dict, **headers: str) -> WebhookDelivery:
    return source.parse(build_webhook_request(json.dumps(payload).encode(), {**JSON, **headers}))


class TestHmacJsonSource:
    EXTRA = {"signature_header": "X-Signature", "event_field": "data.type", "path_field": "service"}

    def test_from_extra_verify_and_parse(self):
        source = HmacJsonSource.from_extra(self.EXTRA)
        payload = {"data": {"type": "deployed"}, "service": "billing"}
        body = json.dumps(payload).encode()
        request = build_webhook_request(body, {**JSON, "X-Signature": compute_hex_signature(body)})

        source.verify(request, SECRET)

        assert source.parse(request) == WebhookDelivery(
            path="billing",
            kinds=[("deployed",)],
            attributes={"event": "deployed", "path": "billing"},
            delivery_id=None,
            payload=payload,
        )

    @pytest.mark.parametrize(
        ("options", "signature"),
        [
            pytest.param({"signature_prefix": "v1="}, f"v1={compute_hex_signature()}", id="prefixed-hex"),
            pytest.param(
                {"signature_encoding": "base64"},
                base64.b64encode(hmac.new(SECRET.encode(), BODY, hashlib.sha256).digest()).decode(),
                id="base64",
            ),
        ],
    )
    def test_verify_signature_formats(self, options, signature):
        source = HmacJsonSource.from_extra({**self.EXTRA, **options})

        source.verify(build_webhook_request(BODY, {"X-Signature": signature}), SECRET)

    @pytest.mark.parametrize(
        ("options", "headers"),
        [
            pytest.param({}, {}, id="missing"),
            pytest.param(
                {"signature_prefix": "v1="}, {"X-Signature": compute_hex_signature()}, id="no-prefix"
            ),
            pytest.param({}, {"X-Signature": compute_hex_signature(secret="wrong")}, id="wrong-secret"),
            pytest.param(
                {"signature_encoding": "base64"},
                {"X-Signature": compute_hex_signature()},
                id="hex-for-base64",
            ),
        ],
    )
    def test_verify_rejects(self, options, headers):
        source = HmacJsonSource.from_extra({**self.EXTRA, **options})

        with pytest.raises(WebhookVerificationError):
            source.verify(build_webhook_request(BODY, headers), SECRET)

    def test_event_from_header(self):
        source = HmacJsonSource.from_extra({"signature_header": "X-Signature", "event_header": "X-Event"})

        parsed = parse_json(source, {}, **{"X-Event": "deployed"})

        assert (parsed.path, parsed.kinds, parsed.attributes) == (
            None,
            [("deployed",)],
            {"event": "deployed"},
        )

    def test_parse_treats_an_empty_path_as_none(self):
        source = HmacJsonSource.from_extra(self.EXTRA)

        parsed = parse_json(source, {"data": {"type": "deployed"}, "service": ""})

        assert (parsed.path, parsed.attributes) == (None, {"event": "deployed"})

    @pytest.mark.parametrize(
        "extra",
        [
            pytest.param({"event_field": "type"}, id="no-signature-header"),
            pytest.param({"signature_header": None, "event_field": "type"}, id="null-signature-header"),
            pytest.param({"signature_header": "", "event_field": "type"}, id="empty-signature-header"),
            pytest.param({"signature_header": "X-Signature"}, id="no-event-source"),
            pytest.param(
                {"signature_header": "X-Signature", "event_field": "type", "event_header": "X-Event"},
                id="both-event-sources",
            ),
            pytest.param(
                {
                    "signature_header": "X-Signature",
                    "event_field": "type",
                    "signature_encoding": "hexadecimal",
                },
                id="unknown-encoding",
            ),
            pytest.param(
                {
                    "signature_header": "X-Signature",
                    "event_field": "type",
                    "delivery_id_field": "id",
                    "delivery_id_header": "X-Delivery",
                },
                id="both-delivery-id-sources",
            ),
        ],
    )
    def test_from_extra_rejects(self, extra):
        with pytest.raises(ValueError, match="hmac_json"):
            HmacJsonSource.from_extra(extra)

    @pytest.mark.parametrize(
        "payload",
        [
            {"data": {}},
            {"data": {"type": "deployed"}, "service": 1},
            {"data": {"type": "deployed"}, "id": {"value": "d-1"}},
            {"data": {"type": "deployed"}, "id": True},
        ],
        ids=["no-event", "non-string-path", "object-delivery-id", "boolean-delivery-id"],
    )
    def test_parse_rejects(self, payload):
        source = HmacJsonSource.from_extra({**self.EXTRA, "delivery_id_field": "id"})

        with pytest.raises(ValueError, match="event name|not a string"):
            parse_json(source, payload)

    @pytest.mark.parametrize(
        ("options", "payload", "headers", "expected"),
        [
            pytest.param({}, {"id": "d-1"}, {"X-Delivery": "h-1"}, None, id="not-configured"),
            pytest.param({"delivery_id_header": "X-Delivery"}, {}, {"X-Delivery": "h-1"}, "h-1", id="header"),
            pytest.param({"delivery_id_field": "meta.id"}, {"meta": {"id": "d-1"}}, {}, "d-1", id="field"),
            pytest.param({"delivery_id_field": "meta.id"}, {"meta": {"id": 42}}, {}, "42", id="numbered"),
            pytest.param({"delivery_id_field": "meta.id"}, {}, {}, None, id="missing"),
        ],
    )
    def test_delivery_id(self, options, payload, headers, expected):
        source = HmacJsonSource.from_extra({**self.EXTRA, **options})

        parsed = parse_json(source, {"data": {"type": "deployed"}, **payload}, **headers)

        assert parsed.delivery_id == expected


class TestTokenHeaderSource:
    EXTRA = {"token_header": "Authorization", "token_prefix": "Bearer ", "event_field": "event_type"}

    def test_from_extra_verify_and_parse(self):
        source = TokenHeaderSource.from_extra(self.EXTRA)
        payload = {"event_type": "monitor_alert"}
        body = json.dumps(payload).encode()
        request = build_webhook_request(body, {**JSON, "Authorization": f"Bearer {SECRET}"})

        source.verify(request, SECRET)

        assert source.parse(request) == WebhookDelivery(
            path=None,
            kinds=[("monitor_alert",)],
            attributes={"event": "monitor_alert"},
            delivery_id=None,
            payload=payload,
        )

    @pytest.mark.parametrize(
        "headers",
        [
            pytest.param({}, id="missing"),
            pytest.param({"Authorization": SECRET}, id="missing-prefix"),
            pytest.param({"Authorization": "Bearer wrong"}, id="wrong"),
        ],
    )
    def test_verify_rejects(self, headers):
        source = TokenHeaderSource.from_extra(self.EXTRA)

        with pytest.raises(WebhookVerificationError):
            source.verify(build_webhook_request(BODY, headers), SECRET)

    @pytest.mark.parametrize(
        "extra",
        [
            pytest.param({"event_field": "event_type"}, id="no-token-header"),
            pytest.param({"token_header": "", "event_field": "event_type"}, id="empty-token-header"),
            pytest.param({"token_header": "X-Webhook-Secret"}, id="no-event-source"),
        ],
    )
    def test_from_extra_rejects(self, extra):
        with pytest.raises(ValueError, match="token_header"):
            TokenHeaderSource.from_extra(extra)


@pytest.mark.parametrize(
    ("source_class", "extra"),
    [
        pytest.param(HmacJsonSource, {"signature_header": ["X-Signature"], "event_field": "type"}, id="own"),
        pytest.param(HmacJsonSource, {"signature_header": "X-Signature", "event_field": 1}, id="inherited"),
        pytest.param(TokenHeaderSource, {"token_header": "X-Token", "event_header": {}}, id="token_header"),
        pytest.param(
            TokenHeaderSource,
            {"token_header": "X-Token", "event_header": "X-Event", "token_prefix": []},
            id="prefix",
        ),
    ],
)
def test_from_extra_rejects_options_that_are_not_strings(source_class, extra):
    with pytest.raises(ValueError, match="must be a string"):
        source_class.from_extra(extra)


@pytest.mark.parametrize("unset", [None, ""], ids=["null", "empty"])
@pytest.mark.parametrize(
    ("source_class", "extra", "expected"),
    [
        pytest.param(
            HmacJsonSource,
            {"source_type": "hmac_json", "signature_header": "X-Signature", "event_field": "event"},
            HmacJsonSource(signature_header="X-Signature", event_field="event"),
            id="hmac_json",
        ),
        pytest.param(
            TokenHeaderSource,
            {"source_type": "token_header", "token_header": "X-Token", "event_header": "X-Event"},
            TokenHeaderSource(token_header="X-Token", event_header="X-Event"),
            id="token_header",
        ),
    ],
)
def test_from_extra_treats_fields_left_empty_in_the_form_as_unset(source_class, extra, expected, unset):
    form_extra = {**dict.fromkeys(CONN_FIELDS, unset), **extra}

    assert source_class.from_extra(form_extra) == expected


@pytest.mark.parametrize(
    "source",
    [
        pytest.param(
            HmacJsonSource.from_extra({"signature_header": "X-Signature", **DELIVERY_OPTIONS}), id="hmac_json"
        ),
        pytest.param(
            TokenHeaderSource.from_extra({"token_header": "X-Token", **DELIVERY_OPTIONS}), id="token_header"
        ),
    ],
)
def test_signed_delivery_of_any_shape_is_parsed_or_refused(source):
    assert_odd_values_are_parsed_or_refused(
        source,
        {"data": {"type": "deployed"}, "service": "billing", "meta": {"id": "d-1"}},
        ["data", "data.type", "service", "meta", "meta.id"],
    )
