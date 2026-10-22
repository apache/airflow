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

import pytest

pytestmark = pytest.mark.db_test

MISSING_CALLBACK_URL = "/execution/callbacks/00000000-0000-0000-0000-000000000000"


@pytest.mark.parametrize(
    ("method", "path", "body"),
    [
        pytest.param("GET", "context", None, id="context"),
        pytest.param("PATCH", "state", {"state": "success"}, id="state"),
    ],
)
class TestCallbackContextAndStateEndpointVersioning:
    """The callback context and state endpoints didn't exist before the 2027-02-28 API version."""

    def test_old_version_returns_404(self, client, method, path, body):
        client.headers["Airflow-API-Version"] = "2026-10-30"

        response = client.request(method, f"{MISSING_CALLBACK_URL}/{path}", json=body)

        assert response.status_code == 404
        assert response.json() == {"detail": "Not Found"}

    def test_head_version_routes_to_endpoint(self, client, method, path, body):
        response = client.request(method, f"{MISSING_CALLBACK_URL}/{path}", json=body)

        assert response.status_code == 404
        assert response.json()["detail"]["reason"] == "not_found"
