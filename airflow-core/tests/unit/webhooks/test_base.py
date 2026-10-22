#
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

from airflow.webhooks.base import (
    WebhookDelivery,
    WebhookRequest,
    WebhookSource,
    WebhookVerificationError,
    get_field,
    get_text,
    load_json_object,
    verify_hmac_sha256,
    verify_token,
)

from tests_common.test_utils.webhooks import build_webhook_request

SECRET = "s3cret"
BODY = b'{"event": "deployed"}'
JSON = {"content-type": "application/json"}
HEX_DIGEST = hmac.new(SECRET.encode(), BODY, hashlib.sha256).hexdigest()
BASE64_DIGEST = base64.b64encode(hmac.new(SECRET.encode(), BODY, hashlib.sha256).digest()).decode()


class MinimalSource(WebhookSource):
    def verify(self, request: WebhookRequest, secret: str) -> None:
        pass

    def parse(self, request: WebhookRequest) -> WebhookDelivery:
        raise NotImplementedError


class TestWebhookSource:
    def test_optional_members_default_to_no_handshake_and_no_payload_choice(self):
        source = MinimalSource.from_extra({"source_type": "minimal"})
        request = build_webhook_request()
        payload = {"event": "deployed"}

        assert isinstance(source, MinimalSource)
        assert source.is_handshake(request) is False
        assert source.handshake(request) == {}
        assert source.summarize(payload) is payload
        assert (MinimalSource.default_store_payload, MinimalSource.unsigned_handshake) == (None, False)


class TestWebhookDelivery:
    def test_accepts_string_and_integer_attributes(self):
        delivery = WebhookDelivery(
            path=None, kinds=[], attributes={"action": "opened", "number": 7}, delivery_id=None, payload={}
        )

        assert delivery.attributes == {"action": "opened", "number": 7}

    @pytest.mark.parametrize(
        ("value", "type_name"),
        [
            pytest.param({"name": "main"}, "dict", id="object"),
            pytest.param(["main"], "list", id="array"),
            pytest.param(True, "bool", id="bool"),
            pytest.param(1.5, "float", id="float"),
            pytest.param(None, "NoneType", id="null"),
        ],
    )
    def test_rejects_other_attribute_values(self, value, type_name):
        with pytest.raises(
            TypeError, match=f"Attribute 'ref' must be a string or an integer, not {type_name}"
        ):
            WebhookDelivery(path=None, kinds=[], attributes={"ref": value}, delivery_id=None, payload={})


class TestVerifyHmacSha256:
    @pytest.mark.parametrize(
        ("received", "options"),
        [
            pytest.param(HEX_DIGEST, {}, id="hex"),
            pytest.param(HEX_DIGEST.upper(), {}, id="uppercase-hex"),
            pytest.param(f"sha256={HEX_DIGEST}", {"prefix": "sha256="}, id="prefixed-hex"),
            pytest.param(BASE64_DIGEST, {"encoding": "base64"}, id="base64"),
            pytest.param(
                f"v1={BASE64_DIGEST}", {"prefix": "v1=", "encoding": "base64"}, id="prefixed-base64"
            ),
        ],
    )
    def test_accepts(self, received, options):
        verify_hmac_sha256(received, SECRET, BODY, **options)

    @pytest.mark.parametrize(
        ("received", "options"),
        [
            pytest.param(None, {}, id="missing"),
            pytest.param(HEX_DIGEST, {"prefix": "sha256="}, id="missing-prefix"),
            pytest.param(hmac.new(b"wrong", BODY, hashlib.sha256).hexdigest(), {}, id="wrong-secret"),
            pytest.param("é", {}, id="non-ascii"),
            pytest.param(BASE64_DIGEST.swapcase(), {"encoding": "base64"}, id="base64-is-case-sensitive"),
            pytest.param(HEX_DIGEST, {"encoding": "base64"}, id="hex-for-base64"),
        ],
    )
    def test_rejects(self, received, options):
        with pytest.raises(WebhookVerificationError):
            verify_hmac_sha256(received, SECRET, BODY, **options)

    def test_rejects_an_empty_secret(self):
        with pytest.raises(WebhookVerificationError, match="No shared secret is configured"):
            verify_hmac_sha256(hmac.new(b"", BODY, hashlib.sha256).hexdigest(), "", BODY)


class TestVerifyToken:
    def test_accepts(self):
        verify_token(f"Bearer {SECRET}", SECRET, prefix="Bearer ")

    @pytest.mark.parametrize(
        "received",
        [
            pytest.param(None, id="missing"),
            pytest.param(SECRET, id="missing-prefix"),
            pytest.param("Bearer wrong", id="wrong"),
            pytest.param("Bearer é", id="non-ascii"),
        ],
    )
    def test_rejects(self, received):
        with pytest.raises(WebhookVerificationError):
            verify_token(received, SECRET, prefix="Bearer ")

    def test_rejects_an_empty_secret(self):
        with pytest.raises(WebhookVerificationError, match="No shared secret is configured"):
            verify_token("Bearer ", "", prefix="Bearer ")


def build_nested_object(depth: int) -> bytes:
    """Return a JSON object whose innermost object is ``depth`` levels deep, the outermost being 1."""
    return b'{"a": ' * (depth - 1) + b"{}" + b"}" * (depth - 1)


class TestLoadJsonObject:
    @pytest.mark.parametrize(
        ("content_type", "body", "expected"),
        [
            pytest.param("application/json; charset=utf-8", BODY, {"event": "deployed"}, id="charset"),
            pytest.param("Application/JSON; charset=utf-8", BODY, {"event": "deployed"}, id="mixed-case"),
            pytest.param(
                "application/json",
                b'{"emoji": "\\ud83d\\ude00"}',
                {"emoji": "\U0001f600"},
                id="surrogate-pair",
            ),
            pytest.param(
                "application/json",
                build_nested_object(64),
                json.loads(build_nested_object(64)),
                id="deepest-nesting",
            ),
        ],
    )
    def test_returns_the_object(self, content_type, body, expected):
        request = build_webhook_request(body, {"content-type": content_type})

        assert load_json_object(request) == expected

    @pytest.mark.parametrize(
        ("headers", "body", "match"),
        [
            pytest.param({}, BODY, "got none", id="no-content-type"),
            pytest.param(
                {"content-type": "application/x-www-form-urlencoded"},
                b"payload=%7B%7D",
                "got application/x-www-form-urlencoded",
                id="form",
            ),
            pytest.param(JSON, b"[]", "Expected a JSON object", id="not-an-object"),
            pytest.param(JSON, b'{"value": NaN}', "NaN, which JSON does not allow", id="nan"),
            pytest.param(JSON, b'{"value": Infinity}', "Infinity, which JSON does not allow", id="infinity"),
            pytest.param(
                JSON, b'{"value": -Infinity}', "-Infinity, which JSON does not allow", id="minus-infinity"
            ),
            # Deeper than the recursion limit, in 400 KB: small enough that a body size limit does not stop it.
            pytest.param(JSON, b"[" * 200_000 + b"]" * 200_000, "nested too deeply", id="nested-too-deeply"),
            # MySQL refuses JSON nested more than 100 levels, which Python's json reads on every version.
            pytest.param(JSON, build_nested_object(65), "more than 64 levels", id="objects-over-the-limit"),
            pytest.param(JSON, b'{"a": ' + b"[" * 64 + b"]" * 64 + b"}", "more than 64 levels", id="arrays"),
            pytest.param(JSON, b'{"a": 1e400}', "number too large to store", id="float-overflow"),
            pytest.param(JSON, b'{"a": [-1e999]}', "number too large to store", id="negative-float-overflow"),
            pytest.param(JSON, b'{"a": {"b": "x\\u0000"}}', "U\\+0000, which a database", id="nul"),
            pytest.param(JSON, b'{"\\u0000": 1}', "U\\+0000, which a database", id="nul-in-key"),
            pytest.param(JSON, b'{"a": ["\\ud800"]}', "U\\+D800, which a database", id="lone-surrogate"),
        ],
    )
    def test_rejects(self, headers, body, match):
        with pytest.raises(ValueError, match=match):
            load_json_object(build_webhook_request(body, headers))


class TestGetField:
    @pytest.mark.parametrize(
        ("dotted_path", "expected"),
        [
            pytest.param("issue.number", 7, id="nested"),
            pytest.param("issue.title", None, id="missing"),
            pytest.param("issue.number.value", None, id="below-a-non-object"),
        ],
    )
    def test_get_field(self, dotted_path, expected):
        assert get_field({"issue": {"number": 7}}, dotted_path) == expected


class TestGetText:
    @pytest.mark.parametrize(
        ("value", "expected"),
        [
            pytest.param("main", "main", id="text"),
            pytest.param("", None, id="empty"),
            pytest.param(7, None, id="number"),
            pytest.param({"name": "main"}, None, id="object"),
            pytest.param(None, None, id="null"),
        ],
    )
    def test_get_text(self, value, expected):
        assert get_text({"ref": {"name": value}}, "ref.name") == expected
