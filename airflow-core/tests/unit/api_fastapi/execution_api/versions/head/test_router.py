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

from unittest.mock import AsyncMock

import pytest
from fastapi import FastAPI

from airflow.api_fastapi.auth.tokens import JWTValidator
from airflow.api_fastapi.execution_api.app import lifespan


@pytest.fixture
def exec_app(client):
    last_route = client.app.routes[-1]
    assert isinstance(last_route.app, FastAPI)
    return last_route.app


@pytest.mark.parametrize(
    ("age", "validity", "expect_refreshed_token"),
    (
        pytest.param(25, 60, False, id="25s-of-60s"),
        pytest.param(31, 60, True, id="30s-of-60s"),
        # Test the "20% left" behaviour for larger validities
        pytest.param(400, 600, False, id="400s-of-600s"),
        pytest.param(480, 600, True, id="480s-of-600s"),
    ),
)
@pytest.mark.db_test
def test_expiring_token_is_reissued(
    client, exec_app: FastAPI, time_machine, age, validity, expect_refreshed_token
):
    moment = 1743451846  # A "random" unix epoch timestamp.
    auth = AsyncMock(spec=JWTValidator)
    auth.avalidated_claims.return_value = {
        "sub": "edb09971-4e0e-4221-ad3f-800852d38085",
        "iat": moment,
        "exp": moment + validity,
    }

    time_machine.move_to(moment + age, tick=False)

    # Inject our fake JWTValidator object. Can be over-ridden by tests if they want
    lifespan.registry.register_value(JWTValidator, auth)
    # In order to test this we need any endpoint to hit. The easiest one to use is variable get

    response = client.get("/execution/variables/key1", headers={"Authorization": "Bearer dummy"})

    if expect_refreshed_token:
        assert "Refreshed-API-Token" in response.headers
    else:
        assert "Refreshed-API-Token" not in response.headers
    auth.avalidated_claims.assert_awaited_once_with("dummy", {})


@pytest.mark.db_test
def test_just_expired_token_is_reissued_within_grace_period(client, exec_app: FastAPI, time_machine):
    """Token that expires mid-request is still reissued by the middleware.

    Regression test for the TOCTOU race in JWTReissueMiddleware: a heartbeat arrives with a
    token that has ~0s left, JWTBearer validates it (still technically valid at that moment),
    the request completes, and the middleware runs. In the old code the middleware would call
    avalidated_claims a second time and get ExpiredSignatureError — no Refreshed-API-Token
    header would be set, and the task would die on the next heartbeat.

    With the fix the middleware reads claims from request.scope (set by JWTBearer) instead of
    re-validating, so it still issues a fresh token even when the original has since expired.
    """
    moment = 1743451846
    auth = AsyncMock(spec=JWTValidator)
    auth.avalidated_claims.return_value = {
        "sub": "edb09971-4e0e-4221-ad3f-800852d38085",
        "iat": moment,
        "exp": moment + 600,
    }

    # Move time to 1 second past the token's expiry — simulates the middleware running after
    # the token boundary. JWTBearer already accepted the token (mocked); the middleware must
    # still issue a refresh rather than silently dropping it.
    time_machine.move_to(moment + 601, tick=False)

    lifespan.registry.register_value(JWTValidator, auth)

    response = client.get("/execution/variables/key1", headers={"Authorization": "Bearer dummy"})

    assert "Refreshed-API-Token" in response.headers
    # avalidated_claims must be called exactly once — by JWTBearer only.
    auth.avalidated_claims.assert_awaited_once_with("dummy", {})
