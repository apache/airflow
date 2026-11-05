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

from airflow.api_fastapi.auth.managers.base_auth_manager import COOKIE_NAME_JWT_TOKEN
from airflow.providers.fab.www import app as application
from airflow.providers.fab.www.security import permissions

from tests_common.test_utils.config import conf_vars
from unit.fab.auth_manager.test_utils import create_user, delete_user
from unit.fab.utils import client_with_login

pytestmark = pytest.mark.db_test

USERNAME = "test_login_next"


@pytest.fixture
def app():
    with conf_vars(
        {("core", "auth_manager"): "airflow.providers.fab.auth_manager.fab_auth_manager.FabAuthManager"}
    ):
        app = application.create_app(enable_plugins=False)
        app.config["WTF_CSRF_ENABLED"] = False
        try:
            yield app
        finally:
            with app.app_context():
                for fab_engine in app.extensions["sqlalchemy"].engines.values():
                    fab_engine.dispose()


@pytest.fixture
def client(app):
    with app.app_context():
        create_user(
            app,
            username=USERNAME,
            role_name="role_login_next",
            permissions=[(permissions.ACTION_CAN_READ, permissions.RESOURCE_WEBSITE)],
        )
        try:
            yield client_with_login(app, username=USERNAME, password=USERNAME)
        finally:
            delete_user(app, USERNAME)


def _sets_token_cookie(response) -> bool:
    return any(
        header.startswith(f"{COOKIE_NAME_JWT_TOKEN}=") for header in response.headers.getlist("Set-Cookie")
    )


class TestLoginWhenAuthenticated:
    def test_redirects_to_next(self, client):
        response = client.get("/login/?next=/dags/my_dag/tasks/my_task")

        assert response.status_code == 302
        assert response.location == "http://localhost/dags/my_dag/tasks/my_task"
        assert _sets_token_cookie(response)

    def test_redirects_to_index_without_next(self, client):
        response = client.get("/login/")

        assert response.status_code == 302
        assert response.location == "/"
        assert _sets_token_cookie(response)

    def test_rejects_external_next(self, client):
        response = client.get("/login/?next=https://evil.example.com/")

        assert response.status_code == 302
        assert "evil.example.com" not in response.location
