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

from airflow.providers.fab.www import app as application
from airflow.providers.fab.www.security import permissions

from tests_common.test_utils.config import conf_vars
from unit.fab.auth_manager.api_endpoints.api_connexion_utils import create_user, delete_user
from unit.fab.auth_manager.views import _assert_dataset_deprecation_warning
from unit.fab.utils import client_with_login


@pytest.fixture(scope="module")
def fab_app():
    with conf_vars(
        {
            (
                "core",
                "auth_manager",
            ): "airflow.providers.fab.auth_manager.fab_auth_manager.FabAuthManager",
        }
    ):
        yield application.create_app(enable_plugins=False)


@pytest.fixture(scope="module")
def user_permissions_reader(fab_app):
    with fab_app.app_context():
        yield create_user(
            fab_app,
            username="user_permissions",
            role_name="role_permissions",
            permissions=[
                (permissions.ACTION_CAN_READ, permissions.RESOURCE_ACTION),
                (permissions.ACTION_CAN_READ, permissions.RESOURCE_PERMISSION),
                (permissions.ACTION_CAN_READ, permissions.RESOURCE_RESOURCE),
                (permissions.ACTION_CAN_READ, permissions.RESOURCE_WEBSITE),
            ],
        )

        delete_user(fab_app, "user_permissions")


@pytest.fixture
def client_permissions_reader(fab_app, user_permissions_reader):
    fab_app.config["WTF_CSRF_ENABLED"] = False
    return client_with_login(
        fab_app,
        username="user_permissions",
        password="user_permissions",
    )


@pytest.mark.db_test
class TestPermissionsView:
    def test_action_model_view(self, client_permissions_reader, recwarn):
        resp = client_permissions_reader.get("/actions/list/", follow_redirects=True)

        _assert_dataset_deprecation_warning(recwarn)
        assert resp.status_code == 200

    def test_permission_pair_model_view(self, client_permissions_reader, recwarn):
        resp = client_permissions_reader.get("/permissions/list/", follow_redirects=True)

        _assert_dataset_deprecation_warning(recwarn)
        assert resp.status_code == 200

    def test_resource_model_view(self, client_permissions_reader, recwarn):
        resp = client_permissions_reader.get("/resources/list/", follow_redirects=True)

        _assert_dataset_deprecation_warning(recwarn)
        assert resp.status_code == 200
