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
import svcs
from fastapi import Depends, FastAPI
from fastapi.testclient import TestClient
from jwt import ExpiredSignatureError

from airflow.api_fastapi.auth.tokens import JWTGenerator, JWTValidator
from airflow.api_fastapi.execution_api.app import JWTReissueMiddleware, lifespan
from airflow.api_fastapi.execution_api.security import _jwt_bearer, require_auth


@pytest.fixture
def jwt_bearer_client(client, exec_app, monkeypatch):
    """Test client that exercises JWTBearer so request.scope is populated for the middleware."""
    monkeypatch.delitem(exec_app.dependency_overrides, require_auth)
    return client


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
def test_expiring_token_is_reissued(jwt_bearer_client, time_machine, age, validity, expect_refreshed_token):
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

    response = jwt_bearer_client.get("/execution/variables/key1", headers={"Authorization": "Bearer dummy"})

    if expect_refreshed_token:
        assert "Refreshed-API-Token" in response.headers
    else:
        assert "Refreshed-API-Token" not in response.headers
    # avalidated_claims must be called exactly once — by JWTBearer only, not by the middleware.
    auth.avalidated_claims.assert_awaited_once_with("dummy", {})


@pytest.mark.parametrize(
    ("request_age", "expected_status"),
    [
        pytest.param(599, 200, id="expires-during-request"),
        pytest.param(601, 403, id="expired-before-request"),
    ],
)
def test_token_expiring_mid_request_is_reissued_without_revalidation(
    time_machine, request_age, expected_status
):
    """Middleware reissues from cached JWTBearer claims without re-validating the token.

    Regression test for the TOCTOU race in JWTReissueMiddleware: a heartbeat arrives with a
    token that has 1s left, JWTBearer validates it (still valid at that moment), the handler
    advances time past expiry, and the middleware runs. In the old code the middleware would
    call avalidated_claims a second time and get ExpiredSignatureError — no Refreshed-API-Token
    header would be set, and the task would die on the next heartbeat.

    With the fix the middleware reads claims from request.scope (set by JWTBearer) instead of
    calling avalidated_claims again, so it still issues a fresh token even when the original
    has since expired. A token that is already expired when the request arrives must still be
    rejected without a refresh.
    """
    moment = 1743451846
    secret = "jwt-refresh-test-secret" * 4
    audience = "test-jwt-refresh"
    generator = JWTGenerator(secret_key=secret, audience=audience, valid_for=600)
    validator = JWTValidator(secret_key=secret, audience=audience, leeway=0)
    claims = {
        "sub": "edb09971-4e0e-4221-ad3f-800852d38085",
        "scope": "execution",
        "custom": {"key": "value"},
    }
    time_machine.move_to(moment, tick=False)
    token = generator.generate(claims)
    app = FastAPI()
    app.add_middleware(JWTReissueMiddleware)

    @app.get("/protected", dependencies=[Depends(_jwt_bearer)])
    async def protected():
        time_machine.move_to(moment + 601, tick=False)
        return {"ok": True}

    with svcs.Registry() as registry:
        registry.register_value(JWTGenerator, generator)
        registry.register_value(JWTValidator, validator)
        app.state.svcs_registry = registry
        time_machine.move_to(moment + request_age, tick=False)

        with TestClient(app) as client:
            response = client.get("/protected", headers={"Authorization": f"Bearer {token}"})

    assert response.status_code == expected_status
    with pytest.raises(ExpiredSignatureError):
        validator.validated_claims(token)

    if expected_status == 200:
        assert response.json() == {"ok": True}
        refreshed_claims = validator.validated_claims(response.headers["Refreshed-API-Token"])
        assert refreshed_claims.items() >= claims.items()
        assert refreshed_claims["exp"] == moment + 1201
    else:
        assert response.json() == {"detail": "Invalid auth token"}
        assert "Refreshed-API-Token" not in response.headers


@pytest.mark.db_test
def test_expiring_dag_processor_token_is_not_reissued(client, time_machine):
    moment = 1743451846
    auth = AsyncMock(spec=JWTValidator)
    auth.avalidated_claims.return_value = {
        "sub": "edb09971-4e0e-4221-ad3f-800852d38085",
        "scope": "dag_processor",
        "dag_bundles": ["dags-folder"],
        "iat": moment,
        "exp": moment + 60,
    }
    time_machine.move_to(moment + 55, tick=False)
    lifespan.registry.register_value(JWTValidator, auth)

    response = client.get("/execution/variables/key1", headers={"Authorization": "Bearer dummy"})

    assert "Refreshed-API-Token" not in response.headers
