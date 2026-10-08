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

from uuid import UUID

import pytest

from airflow.api_fastapi.execution_api.datamodels.token import DagProcessorClaims, DagProcessorToken
from airflow.api_fastapi.execution_api.security import require_auth

pytestmark = pytest.mark.db_test

MISSING_JOB_HEARTBEAT_URL = "/execution/jobs/0/heartbeat"


class TestDagProcessorJobEndpointsVersioning:
    """The jobs endpoints didn't exist before the 2027-02-28 API version."""

    @pytest.fixture(autouse=True)
    def processor_identity(self, exec_app):
        def authenticate():
            return DagProcessorToken(
                id=UUID(int=1),
                claims=DagProcessorClaims(job_id=1, dag_bundles=frozenset({"bundle"}), exp=1),
            )

        exec_app.dependency_overrides[require_auth] = authenticate

    @pytest.mark.parametrize(
        "path",
        [
            MISSING_JOB_HEARTBEAT_URL,
            "/execution/jobs/0/parse-token",
        ],
    )
    def test_old_version_returns_404(self, client, path):
        client.headers["Airflow-API-Version"] = "2026-10-30"

        response = client.post(path)

        assert response.status_code == 404
        assert response.json() == {"detail": "Not Found"}

    def test_head_version_routes_to_endpoint(self, client):
        response = client.post(MISSING_JOB_HEARTBEAT_URL)

        assert response.status_code == 404
        assert response.json()["detail"]["reason"] == "not_found"

    def test_head_version_routes_to_parse_token_exchange(self, client):
        response = client.post(
            "/execution/jobs/0/parse-token",
            json={
                "attempt_id": "00000000-0000-0000-0000-000000000001",
                "bundle_name": "bundle",
                "relative_fileloc": "dag.py",
            },
        )

        assert response.status_code == 404
        assert response.json()["detail"]["reason"] == "not_found"
