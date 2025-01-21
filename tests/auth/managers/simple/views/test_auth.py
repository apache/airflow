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

import json
from unittest.mock import Mock, patch

import pytest
from flask import session, url_for

from airflow.auth.managers.simple.simple_auth_manager import SimpleAuthManager
from airflow.www import app as application

from tests_common.test_utils.config import conf_vars


@pytest.fixture
def simple_app():
    with conf_vars(
        {
            (
                "core",
                "auth_manager",
            ): "airflow.auth.managers.simple.simple_auth_manager.SimpleAuthManager",
            ("core", "simple_auth_manager_users"): "test:admin",
        }
    ):
        with open(SimpleAuthManager.get_generated_password_file(), "w") as file:
            user = {"test": "test"}
            file.write(json.dumps(user))

        return application.create_app(testing=True)


@pytest.mark.db_test
class TestSimpleAuthManagerAuthenticationViews:
    def test_logout_redirects_to_login_and_clear_user(self, simple_app):
        with simple_app.test_client() as client:
            response = client.get("/logout")
            assert response.status_code == 302
            assert response.location == "/login"
            assert session.get("user") is None

    @pytest.mark.parametrize(
        "username, password, is_successful, query_params, expected_redirect",
        [
            ("test", "test", True, {}, None),
            ("test", "test2", False, {}, None),
            ("", "", False, {}, None),
            ("test", "test", True, {"next": "next_url"}, "next_url?token=token"),
        ],
    )
    @patch("airflow.auth.managers.simple.views.auth.get_auth_manager")
    def test_login_submit(
        self,
        mock_get_auth_manager,
        simple_app,
        username,
        password,
        is_successful,
        query_params,
        expected_redirect,
    ):
        auth_manager = Mock()
        auth_manager.get_jwt_token.return_value = "token"
        mock_get_auth_manager.return_value = auth_manager
        with simple_app.test_client() as client:
            response = client.post(
                "/login_submit", query_string=query_params, data={"username": username, "password": password}
            )
            assert response.status_code == 302
            if is_successful:
                if not expected_redirect:
                    expected_redirect = url_for("Airflow.index", token="token")
                assert response.location == expected_redirect
            else:
                assert response.location == url_for("SimpleAuthManagerAuthenticationViews.login", error=["1"])
