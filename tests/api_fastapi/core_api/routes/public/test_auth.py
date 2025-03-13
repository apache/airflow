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

from unittest.mock import MagicMock

import pytest

from tests_common.test_utils.config import conf_vars

AUTH_MANAGER_LOGIN_URL = "http://some_login_url"

pytestmark = pytest.mark.db_test


class TestAuthEndpoint:
    @pytest.fixture(autouse=True)
    def setup(self, test_client) -> None:
        auth_manager_mock = MagicMock()
        auth_manager_mock.get_url_login.return_value = AUTH_MANAGER_LOGIN_URL
        test_client.app.state.auth_manager = auth_manager_mock


class TestGetLogin(TestAuthEndpoint):
    @pytest.mark.parametrize(
        "params",
        [
            {},
            {"next": None},
            {"next": "http://localhost:8080"},
            {"next": "http://localhost:8080", "other_param": "something_else"},
        ],
    )
    def test_should_respond_307(self, test_client, params):
        response = test_client.get("/public/auth/login", follow_redirects=False, params=params)

        assert response.status_code == 307
        assert (
            response.headers["location"] == f"{AUTH_MANAGER_LOGIN_URL}?next={params.get('next')}"
            if params.get("next")
            else AUTH_MANAGER_LOGIN_URL
        )

    @pytest.mark.parametrize(
        "params",
        [
            {"next": "http://fake_domain.com:8080"},
            {"next": "http://localhost:8080/../../up"},
        ],
    )
    @conf_vars({("api", "base_url"): "http://localhost:8080/prefix"})
    def test_should_respond_400(self, test_client, params):
        response = test_client.get("/public/auth/login", follow_redirects=False, params=params)

        assert response.status_code == 400


class TestLogout(TestAuthEndpoint):
    @pytest.mark.parametrize(
        "mock_logout_url, expected_redirection",
        [
            # logout_url is None, should redirect to the login page directly.
            (None, AUTH_MANAGER_LOGIN_URL),
            # logout_url is defined, should redirect to the logout_url.
            ("http://localhost/auth/some_logout_url", "http://localhost/auth/some_logout_url"),
        ],
    )
    def test_should_respond_307(
        self,
        test_client,
        mock_logout_url,
        expected_redirection,
    ):
        test_client.app.state.auth_manager.get_url_logout.return_value = mock_logout_url
        response = test_client.get("/public/auth/logout", follow_redirects=False)

        assert response.status_code == 307
        assert response.headers["location"] == expected_redirection
