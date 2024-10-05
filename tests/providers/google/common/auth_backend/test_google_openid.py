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

from unittest import mock

import pytest
from google.auth.exceptions import GoogleAuthError

from airflow.www.app import create_app
from tests.test_utils.compat import AIRFLOW_V_2_9_PLUS
from tests.test_utils.config import conf_vars
from tests.test_utils.db import clear_db_pools
from tests.test_utils.decorators import dont_initialize_flask_app_submodules


@pytest.fixture(scope="module")
def google_openid_app():
    @dont_initialize_flask_app_submodules(
        skip_all_except=[
            "init_appbuilder",
            "init_api_auth",
            "init_api_connexion",
            "init_api_error_handlers",
            "init_airflow_session_interface",
            "init_appbuilder_views",
        ]
    )
    def factory():
        with conf_vars(
            {
                ("api", "auth_backends"): "airflow.providers.google.common.auth_backend.google_openid",
                (
                    "core",
                    "auth_manager",
                ): "airflow.providers.fab.auth_manager.fab_auth_manager.FabAuthManager",
            }
        ):
            _app = create_app(testing=True, config={"WTF_CSRF_ENABLED": False})  # type:ignore
            _app.config["AUTH_ROLE_PUBLIC"] = None
            return _app

    return factory()


@pytest.fixture(scope="module")
def admin_user(google_openid_app):
    appbuilder = google_openid_app.appbuilder
    role_admin = appbuilder.sm.find_role("Admin")
    tester = appbuilder.sm.find_user(username="test")
    if not tester:
        appbuilder.sm.add_user(
            username="test",
            first_name="test",
            last_name="test",
            email="test@fab.org",
            role=role_admin,
            password="test",
        )
    return role_admin


@pytest.mark.skipif(not AIRFLOW_V_2_9_PLUS, reason="The tests should be skipped for Airflow < 2.9")
@pytest.mark.skip_if_database_isolation_mode
@pytest.mark.db_test
class TestGoogleOpenID:
    @pytest.fixture(autouse=True)
    def _set_attrs(self, google_openid_app, admin_user) -> None:
        self.app = google_openid_app
        self.admin_user = admin_user

    @mock.patch("google.oauth2.id_token.verify_token")
    def test_success(self, mock_verify_token):
        clear_db_pools()
        mock_verify_token.return_value = {
            "iss": "accounts.google.com",
            "email_verified": True,
            "email": "test@fab.org",
        }

        with self.app.test_client() as test_client:
            response = test_client.get("/api/v1/pools", headers={"Authorization": "bearer JWT_TOKEN"})

            assert 200 == response.status_code
            assert "Default pool" in str(response.json)

    @pytest.mark.parametrize("auth_header", ["bearer", "JWT_TOKEN", "bearer "])
    @mock.patch("google.oauth2.id_token.verify_token")
    def test_malformed_headers(self, mock_verify_token, auth_header):
        mock_verify_token.return_value = {
            "iss": "accounts.google.com",
            "email_verified": True,
            "email": "test@fab.org",
        }

        with self.app.test_client() as test_client:
            response = test_client.get("/api/v1/pools", headers={"Authorization": auth_header})

        assert 401 == response.status_code

    @mock.patch("google.oauth2.id_token.verify_token")
    def test_invalid_iss_in_jwt_token(self, mock_verify_token):
        mock_verify_token.return_value = {
            "iss": "INVALID",
            "email_verified": True,
            "email": "test@fab.org",
        }

        with self.app.test_client() as test_client:
            response = test_client.get("/api/v1/pools", headers={"Authorization": "bearer JWT_TOKEN"})

        assert 401 == response.status_code

    @mock.patch("google.oauth2.id_token.verify_token")
    def test_user_not_exists(self, mock_verify_token):
        mock_verify_token.return_value = {
            "iss": "accounts.google.com",
            "email_verified": True,
            "email": "invalid@fab.org",
        }

        with self.app.test_client() as test_client:
            response = test_client.get("/api/v1/pools", headers={"Authorization": "bearer JWT_TOKEN"})

        assert 401 == response.status_code

    @conf_vars({("api", "auth_backends"): "airflow.providers.google.common.auth_backend.google_openid"})
    def test_missing_id_token(self):
        with self.app.test_client() as test_client:
            response = test_client.get("/api/v1/pools")

        assert 401 == response.status_code

    @conf_vars({("api", "auth_backends"): "airflow.providers.google.common.auth_backend.google_openid"})
    @mock.patch("google.oauth2.id_token.verify_token")
    def test_invalid_id_token(self, mock_verify_token):
        mock_verify_token.side_effect = GoogleAuthError("Invalid token")

        with self.app.test_client() as test_client:
            response = test_client.get("/api/v1/pools", headers={"Authorization": "bearer JWT_TOKEN"})

        assert 401 == response.status_code
