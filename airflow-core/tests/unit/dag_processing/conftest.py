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

import httpx2
import jwt
import pytest


@pytest.fixture
def make_job_token():
    def create(**claims):
        return jwt.encode(
            {"iat": 0, "exp": 300, "scope": "dag_processor", "job_id": 1, **claims},
            "processor-client-unit-test-signing-key",
        )

    return create


@pytest.fixture
def make_registration_response(make_job_token):
    def create(*, job_id=1, token=None, expires_in=300, **kwargs):
        return httpx2.Response(
            201,
            json={
                "job_id": job_id,
                "token": token or make_job_token(job_id=job_id),
                "expires_in": expires_in,
            },
            **kwargs,
        )

    return create


@pytest.fixture
def make_parse_token_response(make_job_token):
    def create(*, expires_in=60, **claims):
        token = make_job_token(
            **{
                "scope": "dag_parse",
                "sub": "00000000-0000-0000-0000-000000000001",
                "session_id": "00000000-0000-0000-0000-000000000002",
                "dag_bundles": ["bundle-a"],
                "relative_fileloc": "dag.py",
                "exp": 60,
                **claims,
            }
        )
        return httpx2.Response(200, json={"token": token, "expires_in": expires_in})

    return create
