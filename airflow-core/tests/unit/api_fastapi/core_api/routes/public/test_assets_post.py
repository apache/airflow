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

from uuid import uuid4

import pytest

pytestmark = pytest.mark.db_test

# Route matches existing tests in this folder:
ROUTE = "/assets"

@pytest.mark.parametrize(
    "payload",
    [
        {"name": "s3_my_dataset", "uri": "s3://bucket/path", "group": "raw", "extra": {"owner": "data-eng"}},
        {"name": "only_required", "uri": "s3://b/k"},
    ],
    ids=["with_optional_fields", "only_required_fields"],
)
def test_assets_create_happy_path(test_client, payload):
    resp = test_client.post(ROUTE, json=payload)
    assert resp.status_code in (200, 201)
    body = resp.json()
    assert isinstance(body, dict)
    assert body["name"] == payload["name"]
    assert body["uri"] == payload["uri"]

def test_assets_create_conflict_returns_409(test_client):
    uniq = uuid4().hex[:8]
    payload = {"name": f"dup_asset_{uniq}", "uri": f"s3://bucket/key-{uniq}"}
    r1 = test_client.post(ROUTE, json=payload)
    assert r1.status_code in (200, 201)
    r2 = test_client.post(ROUTE, json=payload)
    assert r2.status_code == 409
    # Optional: minimal error-shape check if API returns JSON on conflict
    if r2.headers.get("content-type", "").startswith("application/json"):
        assert isinstance(r2.json(), dict)

@pytest.mark.parametrize(
    "payload",
    [
        {"name": "", "uri": ""},
        {"name": 123, "uri": ["not-a-string"]},
    ],
    ids=["empty_strings", "invalid_types"],
)
def test_assets_create_validation_error_returns_4xx(test_client, payload):
    resp = test_client.post(ROUTE, json=payload)
    assert resp.status_code in (400, 422)
    if resp.headers.get("content-type", "").startswith("application/json"):
        assert isinstance(resp.json(), dict)

def test_assets_create_requires_auth_401_or_403_when_unauth(unauthenticated_test_client):
    payload = {"name": "no_auth", "uri": "s3://bucket/no-auth"}
    resp = unauthenticated_test_client.post(ROUTE, json=payload)
    assert resp.status_code in (401, 403)

def test_assets_create_forbidden_when_unauthorized(unauthorized_test_client):
    payload = {"name": "forbidden", "uri": "s3://bucket/forbidden"}
    resp = unauthorized_test_client.post(ROUTE, json=payload)
    assert resp.status_code == 403
