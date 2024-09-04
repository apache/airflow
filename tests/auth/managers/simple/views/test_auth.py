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
from flask import session, url_for

from airflow.www import app as application
from tests.test_utils.config import conf_vars


@pytest.fixture
def simple_app():
    with conf_vars(
        {
            (
                "core",
                "auth_manager",
            ): "airflow.auth.managers.simple.simple_auth_manager.SimpleAuthManager",
        }
    ):
        return application.create_app(
            testing=True,
            config={
                "SIMPLE_AUTH_MANAGER_USERS": [
                    {
                        "username": "test",
                        "password": "test",
                        "role": "admin",
                    }
                ]
            },
        )


@pytest.mark.db_test
class TestSimpleAuthManagerAuthenticationViews:
    def test_logout_redirects_to_login_and_clear_user(self, simple_app):
        with simple_app.test_client() as client:
            response = client.get("/logout")
            assert response.status_code == 302
            assert response.location == "/login"
            assert session.get("user") is None

    @pytest.mark.parametrize(
        "username, password, is_successful",
        [("test", "test", True), ("test", "test2", False), ("", "", False)],
    )
    def test_login_submit(self, simple_app, username, password, is_successful):
        simple_app.config["SIMPLE_AUTH_MANAGER_USERS"] = [
            {
                "username": "test",
                "password": "test",
                "role": "admin",
            }
        ]

        with simple_app.test_client() as client:
            response = client.post("/login_submit", data={"username": username, "password": password})
            assert response.status_code == 302
            if is_successful:
                assert response.location == url_for("Airflow.index")
            else:
                assert response.location == url_for("SimpleAuthManagerAuthenticationViews.login", error=["1"])
