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

from unittest.mock import MagicMock

import pytest
from fastapi import FastAPI, Request
from fastapi.testclient import TestClient

from airflow.api_fastapi.app import cached_app
from airflow.api_fastapi.auth.tokens import JWTGenerator
from airflow.api_fastapi.execution_api.app import lifespan
from airflow.api_fastapi.execution_api.datamodels.token import TIToken
from airflow.api_fastapi.execution_api.security import _jwt_bearer


def _get_execution_api_app(root_app: FastAPI) -> FastAPI:
    """Find the mounted execution API sub-app."""
    for route in root_app.routes:
        if hasattr(route, "path") and route.path == "/execution":
            return route.app
    raise RuntimeError("Execution API sub-app not found")


@pytest.fixture
def exec_app(client):
    """Return the execution API sub-app."""
    return _get_execution_api_app(client.app)


@pytest.fixture
def client(request: pytest.FixtureRequest):
    app = cached_app(apps="execution")
    exec_app = _get_execution_api_app(app)

    async def mock_jwt_bearer(request: Request):
        from uuid import UUID

        ti_id = UUID(request.path_params.get("task_instance_id", "00000000-0000-0000-0000-000000000000"))
        return TIToken(id=ti_id, claims={"sub": str(ti_id), "scope": "execution"})

    exec_app.dependency_overrides[_jwt_bearer] = mock_jwt_bearer

    with TestClient(app, headers={"Authorization": "Bearer fake"}) as client:
        # Register mock JWTGenerator after lifespan starts so endpoints can issue tokens
        mock_generator = MagicMock(spec=JWTGenerator)
        mock_generator.generate.return_value = "mock-execution-token"
        mock_generator.generate_workload_token.return_value = "mock-workload-token"
        lifespan.registry.register_value(JWTGenerator, mock_generator)

        yield client

    exec_app.dependency_overrides.pop(_jwt_bearer, None)
