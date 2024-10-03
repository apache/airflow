#
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

from airflow.security import permissions
from airflow.www import app as application
from tests.providers.fab.auth_manager.api_endpoints.api_connexion_utils import create_user, delete_user
from tests.test_utils.compat import AIRFLOW_V_2_9_PLUS
from tests.test_utils.www import client_with_login

pytestmark = [
    pytest.mark.skipif(not AIRFLOW_V_2_9_PLUS, reason="Tests for Airflow 2.9.0+ only"),
]


@pytest.fixture(scope="module")
def fab_app():
    return application.create_app(testing=True)


@pytest.fixture(scope="module")
def user_user_reader(fab_app):
    yield create_user(
        fab_app,
        username="user_user",
        role_name="role_user",
        permissions=[
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_MY_PASSWORD),
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_WEBSITE),
        ],
    )

    delete_user(fab_app, "user_user")


@pytest.fixture
def client_user_reader(fab_app, user_user_reader):
    fab_app.config["WTF_CSRF_ENABLED"] = False
    return client_with_login(
        fab_app,
        username="user_user_reader",
        password="user_user_reader",
    )


@pytest.mark.db_test
class TestUserEditView:
    def test_reset_my_password_view(self, client_user_reader):
        resp = client_user_reader.get("/resetmypassword/form", follow_redirects=True)
        assert resp.status_code == 200
