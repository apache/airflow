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

from base64 import b64encode

import pytest

from tests_common.test_utils.compat import AIRFLOW_V_3_0_PLUS
from tests_common.test_utils.config import conf_vars
from tests_common.test_utils.db import clear_db_pools

pytestmark = [
    pytest.mark.db_test,
    pytest.mark.skip_if_database_isolation_mode,
    pytest.mark.skipif(not AIRFLOW_V_3_0_PLUS, reason="Test requires Airflow 3.0+"),
]


class BaseTestAuth:
    @pytest.fixture(autouse=True)
    def set_attrs(self, minimal_app_for_auth_api):
        self.app = minimal_app_for_auth_api

        sm = self.app.appbuilder.sm
        tester = sm.find_user(username="test")
        if not tester:
            role_admin = sm.find_role("Admin")
            sm.add_user(
                username="test",
                first_name="test",
                last_name="test",
                email="test@fab.org",
                role=role_admin,
                password="test",
            )


class TestEmptyCors(BaseTestAuth):
    @pytest.fixture(autouse=True, scope="class")
    def with_basic_auth_backend(self, minimal_app_for_auth_api):
        from airflow.www.extensions.init_security import init_api_auth

        old_auth = getattr(minimal_app_for_auth_api, "api_auth")

        try:
            with conf_vars(
                {
                    (
                        "api",
                        "auth_backends",
                    ): "airflow.providers.fab.auth_manager.api.auth.backend.basic_auth"
                }
            ):
                init_api_auth(minimal_app_for_auth_api)
                yield
        finally:
            setattr(minimal_app_for_auth_api, "api_auth", old_auth)

    def test_empty_cors_headers(self):
        token = "Basic " + b64encode(b"test:test").decode()
        clear_db_pools()

        with self.app.test_client() as test_client:
            response = test_client.get("/api/v1/pools", headers={"Authorization": token})
            assert response.status_code == 200
            assert "Access-Control-Allow-Headers" not in response.headers
            assert "Access-Control-Allow-Methods" not in response.headers
            assert "Access-Control-Allow-Origin" not in response.headers


class TestCorsOrigin(BaseTestAuth):
    @pytest.fixture(autouse=True, scope="class")
    def with_basic_auth_backend(self, minimal_app_for_auth_api):
        from airflow.www.extensions.init_security import init_api_auth

        old_auth = getattr(minimal_app_for_auth_api, "api_auth")

        try:
            with conf_vars(
                {
                    (
                        "api",
                        "auth_backends",
                    ): "airflow.providers.fab.auth_manager.api.auth.backend.basic_auth",
                    (
                        "api",
                        "access_control_allow_origins",
                    ): "http://apache.org http://example.com",
                }
            ):
                init_api_auth(minimal_app_for_auth_api)
                yield
        finally:
            setattr(minimal_app_for_auth_api, "api_auth", old_auth)

    def test_cors_origin_reflection(self):
        token = "Basic " + b64encode(b"test:test").decode()
        clear_db_pools()

        with self.app.test_client() as test_client:
            response = test_client.get("/api/v1/pools", headers={"Authorization": token})
            assert response.status_code == 200
            assert response.headers["Access-Control-Allow-Origin"] == "http://apache.org"

            response = test_client.get(
                "/api/v1/pools",
                headers={"Authorization": token, "Origin": "http://apache.org"},
            )
            assert response.status_code == 200
            assert response.headers["Access-Control-Allow-Origin"] == "http://apache.org"

            response = test_client.get(
                "/api/v1/pools",
                headers={"Authorization": token, "Origin": "http://example.com"},
            )
            assert response.status_code == 200
            assert response.headers["Access-Control-Allow-Origin"] == "http://example.com"


class TestCorsWildcard(BaseTestAuth):
    @pytest.fixture(autouse=True, scope="class")
    def with_basic_auth_backend(self, minimal_app_for_auth_api):
        from airflow.www.extensions.init_security import init_api_auth

        old_auth = getattr(minimal_app_for_auth_api, "api_auth")

        try:
            with conf_vars(
                {
                    (
                        "api",
                        "auth_backends",
                    ): "airflow.providers.fab.auth_manager.api.auth.backend.basic_auth",
                    ("api", "access_control_allow_origins"): "*",
                }
            ):
                init_api_auth(minimal_app_for_auth_api)
                yield
        finally:
            setattr(minimal_app_for_auth_api, "api_auth", old_auth)

    def test_cors_origin_reflection(self):
        token = "Basic " + b64encode(b"test:test").decode()
        clear_db_pools()

        with self.app.test_client() as test_client:
            response = test_client.get(
                "/api/v1/pools",
                headers={"Authorization": token, "Origin": "http://example.com"},
            )
            assert response.status_code == 200
            assert response.headers["Access-Control-Allow-Origin"] == "*"
