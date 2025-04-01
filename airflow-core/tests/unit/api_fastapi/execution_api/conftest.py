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
from fastapi.testclient import TestClient

from airflow.api_fastapi.app import cached_app
from airflow.api_fastapi.auth.tokens import JWTValidator
from airflow.api_fastapi.execution_api.app import lifespan


@pytest.fixture
def client(request: pytest.FixtureRequest):
    app = cached_app(apps="execution")

    with TestClient(app, headers={"Authorization": "Bearer fake"}) as client:
        auth = AsyncMock(spec=JWTValidator)
        auth.avalidated_claims.return_value = {"sub": "edb09971-4e0e-4221-ad3f-800852d38085"}

        # Inject our fake JWTValidator object. Can be over-ridden by tests if they want
        lifespan.registry.register_value(JWTValidator, auth)
        yield client
