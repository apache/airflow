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

        # Create a side_effect function that dynamically extracts the task instance ID from validators
        def smart_validated_claims(cred, validators=None):
            # Extract task instance ID from validators if present
            # This handles the JWTBearerTIPathDep case where the validator contains the task ID from the path
            if (
                validators
                and "sub" in validators
                and isinstance(validators["sub"], dict)
                and "value" in validators["sub"]
            ):
                return {
                    "sub": validators["sub"]["value"],
                    "exp": 9999999999,  # Far future expiration
                    "iat": 1000000000,  # Past issuance time
                    "aud": "test-audience",
                }

            # For other cases (like JWTBearerDep) where no specific validators are provided
            # Return a default UUID with all required claims
            return {
                "sub": "00000000-0000-0000-0000-000000000000",
                "exp": 9999999999,  # Far future expiration
                "iat": 1000000000,  # Past issuance time
                "aud": "test-audience",
            }

        # Set the side_effect for avalidated_claims
        auth.avalidated_claims.side_effect = smart_validated_claims
        lifespan.registry.register_value(JWTValidator, auth)

        yield client
