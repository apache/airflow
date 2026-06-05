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
from fastapi import FastAPI, HTTPException, Request, status
from fastapi.security import HTTPBearer
from fastapi.testclient import TestClient

from airflow.api_fastapi.auth.tokens import JWTValidator
from airflow.api_fastapi.execution_api.app import lifespan
from airflow.api_fastapi.execution_api.datamodels.token import TIClaims, TIToken
from airflow.api_fastapi.execution_api.security import (
    _REQUEST_SCOPE_TOKEN_KEY,
    _jwt_bearer,
)


@pytest.fixture
def client():
    """Test client that exercises JWTBearer so request.scope is populated for the middleware."""
    from starlette.routing import Mount

    from airflow.api_fastapi.app import cached_app

    app = cached_app(apps="execution")

    exec_app: FastAPI | None = None
    for route in app.routes:
        if isinstance(route, Mount) and route.path == "/execution" and isinstance(route.app, FastAPI):
            exec_app = route.app
            break
    if exec_app is None:
        raise RuntimeError("Execution API sub-app not found")

    _http_bearer = HTTPBearer(auto_error=False)

    async def mock_jwt_bearer(request: Request):
        """Drop-in for _jwt_bearer that uses the registered JWTValidator mock and sets scope."""
        if cached := request.scope.get(_REQUEST_SCOPE_TOKEN_KEY):
            return cached

        creds = await _http_bearer(request)
        if not creds:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Missing auth token")

        async with svcs.Container(request.app.state.svcs_registry) as services:
            validator: JWTValidator = await services.aget(JWTValidator)
            try:
                claims = await validator.avalidated_claims(creds.credentials, {})
            except Exception:
                raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Invalid auth token")

        claims.setdefault("scope", "execution")
        token = TIToken(id=claims["sub"], claims=TIClaims(**claims))
        request.scope[_REQUEST_SCOPE_TOKEN_KEY] = token
        return token

    exec_app.dependency_overrides[_jwt_bearer] = mock_jwt_bearer

    with TestClient(app) as c:
        yield c

    exec_app.dependency_overrides.pop(_jwt_bearer, None)


@pytest.fixture
def exec_app(client):
    from starlette.routing import Mount

    for route in client.app.routes:
        if isinstance(route, Mount) and route.path == "/execution" and isinstance(route.app, FastAPI):
            return route.app
    raise RuntimeError("Execution API sub-app not found")


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
    # avalidated_claims must be called exactly once — by JWTBearer only, not by the middleware.
    auth.avalidated_claims.assert_awaited_once_with("dummy", {})


@pytest.mark.db_test
def test_token_expiring_mid_request_is_reissued_without_revalidation(client, exec_app: FastAPI, time_machine):
    """Middleware reissues from cached JWTBearer claims without re-validating the token.

    Regression test for the TOCTOU race in JWTReissueMiddleware: a heartbeat arrives with a
    token that has ~0s left, JWTBearer validates it (still technically valid at that moment),
    the request completes, and the middleware runs. In the old code the middleware would call
    avalidated_claims a second time and get ExpiredSignatureError — no Refreshed-API-Token
    header would be set, and the task would die on the next heartbeat.

    With the fix the middleware reads claims from request.scope (set by JWTBearer) instead of
    calling avalidated_claims again, so it still issues a fresh token even when the original
    has since expired.
    """
    moment = 1743451846
    auth = AsyncMock(spec=JWTValidator)
    auth.avalidated_claims.return_value = {
        "sub": "edb09971-4e0e-4221-ad3f-800852d38085",
        "iat": moment,
        "exp": moment + 600,
    }

    # Move time to 1 second past the token's expiry. JWTBearer already accepted the token
    # (mocked); the middleware must still issue a refresh using the cached claims rather than
    # silently dropping it.
    time_machine.move_to(moment + 601, tick=False)

    lifespan.registry.register_value(JWTValidator, auth)

    response = client.get("/execution/variables/key1", headers={"Authorization": "Bearer dummy"})

    assert "Refreshed-API-Token" in response.headers
    # avalidated_claims must be called exactly once — by JWTBearer only.
    auth.avalidated_claims.assert_awaited_once_with("dummy", {})
