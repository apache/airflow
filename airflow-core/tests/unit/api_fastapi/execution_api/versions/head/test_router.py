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

import jwt
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


@pytest.mark.db_test
def test_just_expired_token_is_reissued_within_grace_period(client, exec_app: FastAPI, time_machine):
    """A token that expired just before JWTReissueMiddleware runs is still refreshed.

    Covers the race where a token expires between the security middleware's auth
    check and this middleware's own validation, causing tasks to die even though
    heartbeats were being sent on time.
    """
    moment = 1743451846  # A "random" unix epoch timestamp.
    validity = 600
    claims = {
        "sub": "edb09971-4e0e-4221-ad3f-800852d38085",
        "iat": moment,
        "exp": moment + validity,
    }

    auth = AsyncMock(spec=JWTValidator)
    # First call (strict validation, no extra_leeway) fails because the token just expired.
    # Second call (with REISSUE_GRACE_LEEWAY) succeeds, allowing a fresh token to be issued.
    auth.avalidated_claims.side_effect = [
        jwt.ExpiredSignatureError("Signature has expired"),
        claims,
    ]

    # Move time to 5 seconds past token expiry to simulate the race condition.
    time_machine.move_to(moment + validity + 5, tick=False)

    lifespan.registry.register_value(JWTValidator, auth)

    response = client.get("/execution/variables/key1", headers={"Authorization": "Bearer dummy"})

    # The Refreshed-API-Token header must be present so the client can update its token
    # and recover on the next retry, even though the original token had just expired.
    assert "Refreshed-API-Token" in response.headers
