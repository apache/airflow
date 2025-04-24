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
from fastapi.testclient import TestClient

from airflow.api_fastapi.app import create_app

from tests_common.test_utils.config import conf_vars


@pytest.fixture
def all_access_test_client():
    with conf_vars(
        {
            ("core", "simple_auth_manager_all_admins"): "true",
            ("webserver", "expose_config"): "true",
        }
    ):
        app = create_app()
        yield TestClient(app)


@pytest.mark.parametrize(
    "method, path",
    [
        (method, route.path)
        for route in sorted(create_app().routes, key=lambda r: r.path)  # type: ignore[attr-defined]
        if hasattr(route, "path") and hasattr(route, "methods")
        for method in route.methods
    ],
)
def test_all_endpoints_without_auth_header(all_access_test_client, method, path):
    response = all_access_test_client.request(method, path)
    assert response.status_code not in {401, 403}, (
        f"Unexpected status code {response.status_code} for {method} {path}"
    )
