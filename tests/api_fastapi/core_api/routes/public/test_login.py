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

AUTH_MANAGER_LOGIN_URL = "http://some_login_url"

pytestmark = pytest.mark.db_test


class TestLoginEndpoint:
    @pytest.fixture(autouse=True)
    def setup(self, test_client) -> None:
        auth_manager_mock = MagicMock()
        auth_manager_mock.get_url_login.return_value = AUTH_MANAGER_LOGIN_URL
        test_client.app.state.auth_manager = auth_manager_mock


class TestGetLogin(TestLoginEndpoint):
    @pytest.mark.parametrize(
        "params",
        [
            {},
            {"next": None},
            {"next": "http://localhost:29091"},
            {"next": "http://localhost:29091", "other_param": "something_else"},
        ],
    )
    def test_should_respond_308(self, test_client, params):
        response = test_client.get("/public/login", follow_redirects=False, params=params)

        assert response.status_code == 307
        assert (
            response.headers["location"] == f"{AUTH_MANAGER_LOGIN_URL}?next={params.get('next')}"
            if params.get("next")
            else AUTH_MANAGER_LOGIN_URL
        )
