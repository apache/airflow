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
from uuid import UUID

import pytest
from fastapi import FastAPI, Request

from airflow.api_fastapi.auth.tokens import JWTValidator
from airflow.api_fastapi.execution_api.app import lifespan
from airflow.api_fastapi.execution_api.datamodels.token import TIClaims, TIToken
from airflow.api_fastapi.execution_api.security import _REQUEST_SCOPE_TOKEN_KEY, require_auth


@pytest.fixture
def exec_app(client):
    last_route = client.app.routes[-1]
    assert isinstance(last_route.app, FastAPI)
    return last_route.app


def _install_scoped_token(
    exec_app: FastAPI,
    *,
    moment: int,
    validity: int,
    sub: str = "edb09971-4e0e-4221-ad3f-800852d38085",
) -> None:
    """Override require_auth to cache JWT claims on the ASGI scope like JWTBearer does."""

    async def mock_require_auth(request: Request) -> TIToken:
        claims = TIClaims.model_validate(
            {
                "sub": sub,
                "scope": "execution",
                "iat": moment,
                "exp": moment + validity,
            }
        )
        token = TIToken(id=UUID(sub), claims=claims)
        request.scope[_REQUEST_SCOPE_TOKEN_KEY] = token
        return token

    exec_app.dependency_overrides[require_auth] = mock_require_auth


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
    _install_scoped_token(exec_app, moment=moment, validity=validity)
    time_machine.move_to(moment + age, tick=False)

    # Any authenticated endpoint exercises JWTReissueMiddleware.
    response = client.get("/execution/variables/key1", headers={"Authorization": "Bearer dummy"})

    if expect_refreshed_token:
        assert "Refreshed-API-Token" in response.headers
    else:
        assert "Refreshed-API-Token" not in response.headers


@pytest.mark.db_test
def test_token_expiring_mid_request_is_reissued(client, exec_app: FastAPI, time_machine):
    """JWT reissue must survive tokens that expire during request handling.

    Regression for the TOCTOU race where JWTBearer accepted a near-expiry token,
    the handler ran, then JWTReissueMiddleware re-validated the bearer string and
    hit ExpiredSignatureError — swallowing the error and omitting Refreshed-API-Token
    so the next heartbeat failed with 403.
    """
    moment = 1743451846
    validity = 600
    sub = "edb09971-4e0e-4221-ad3f-800852d38085"

    # Start just before expiry so auth would still accept the token.
    time_machine.move_to(moment + validity - 1, tick=False)

    # Route that mimics JWTBearer caching claims, then advances the clock past exp.
    @exec_app.get("/__test_mid_request_expiry")
    async def _expire_during_request(request: Request):
        request.scope[_REQUEST_SCOPE_TOKEN_KEY] = TIToken(
            id=UUID(sub),
            claims=TIClaims.model_validate(
                {
                    "sub": sub,
                    "scope": "execution",
                    "iat": moment,
                    "exp": moment + validity,
                }
            ),
        )
        time_machine.move_to(moment + validity + 1, tick=False)
        return {"ok": True}

    # Ensure avalidated_claims is not needed by middleware (and would fail if called).
    auth = AsyncMock(spec=JWTValidator)
    auth.avalidated_claims.side_effect = Exception("middleware must not re-validate JWT")
    lifespan.registry.register_value(JWTValidator, auth)

    response = client.get(
        "/execution/__test_mid_request_expiry",
        headers={"Authorization": "Bearer dummy"},
    )

    assert response.status_code == 200
    assert "Refreshed-API-Token" in response.headers
    auth.avalidated_claims.assert_not_awaited()
