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
from flask_login import current_user
from google.auth.exceptions import GoogleAuthError

from airflow.www.app import create_app
from tests.test_utils.config import conf_vars
from tests.test_utils.db import clear_db_pools


@pytest.fixture(scope="module")
def google_openid_app():
    confs = {
        ("api", "auth_backends"): "airflow.providers.google.common.auth_backend.google_openid",
        ("api", "enable_experimental_api"): "true",
    }
    with conf_vars(confs):
        return create_app(testing=True)


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
            response = test_client.get(
                "/api/experimental/pools", headers={"Authorization": "bearer JWT_TOKEN"}
            )
            assert "test@fab.org" == current_user.email

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
            response = test_client.get("/api/experimental/pools", headers={"Authorization": auth_header})

        assert 403 == response.status_code
        assert "Forbidden" == response.data.decode()

    @mock.patch("google.oauth2.id_token.verify_token")
    def test_invalid_iss_in_jwt_token(self, mock_verify_token):
        mock_verify_token.return_value = {
            "iss": "INVALID",
            "email_verified": True,
            "email": "test@fab.org",
        }

        with self.app.test_client() as test_client:
            response = test_client.get(
                "/api/experimental/pools", headers={"Authorization": "bearer JWT_TOKEN"}
            )

        assert 403 == response.status_code
        assert "Forbidden" == response.data.decode()

    @mock.patch("google.oauth2.id_token.verify_token")
    def test_user_not_exists(self, mock_verify_token):
        mock_verify_token.return_value = {
            "iss": "accounts.google.com",
            "email_verified": True,
            "email": "invalid@fab.org",
        }

        with self.app.test_client() as test_client:
            response = test_client.get(
                "/api/experimental/pools", headers={"Authorization": "bearer JWT_TOKEN"}
            )

        assert 403 == response.status_code
        assert "Forbidden" == response.data.decode()

    @conf_vars({("api", "auth_backends"): "airflow.providers.google.common.auth_backend.google_openid"})
    def test_missing_id_token(self):
        with self.app.test_client() as test_client:
            response = test_client.get("/api/experimental/pools")

        assert 403 == response.status_code
        assert "Forbidden" == response.data.decode()

    @conf_vars({("api", "auth_backends"): "airflow.providers.google.common.auth_backend.google_openid"})
    @mock.patch("google.oauth2.id_token.verify_token")
    def test_invalid_id_token(self, mock_verify_token):
        mock_verify_token.side_effect = GoogleAuthError("Invalid token")

        with self.app.test_client() as test_client:
            response = test_client.get(
                "/api/experimental/pools", headers={"Authorization": "bearer JWT_TOKEN"}
            )

        assert 403 == response.status_code
        assert "Forbidden" == response.data.decode()
