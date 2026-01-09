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
import os
from unittest import mock

import pytest

from airflow.api_fastapi.app import AUTH_MANAGER_FASTAPI_APP_PREFIX
from airflow.api_fastapi.auth.managers.models.resource_details import AccessView
from airflow.api_fastapi.auth.managers.simple.user import SimpleAuthManagerUser
from airflow.api_fastapi.common.types import MenuItem

from tests_common.test_utils.config import conf_vars


class TestSimpleAuthManager:
    def test_get_users(self, auth_manager):
        with conf_vars(
            {
                ("core", "simple_auth_manager_users"): "test1:viewer,test2:viewer",
            }
        ):
            users = auth_manager.get_users()
            assert users == [{"role": "viewer", "username": "test1"}, {"role": "viewer", "username": "test2"}]

    @pytest.mark.parametrize(
        ("file_content", "expected"),
        [
            ("{}", {}),
            ("", {}),
            ('{"test1": "test1"}', {"test1": "test1"}),
            ('{"test1": "test1", "test2": "test2"}', {"test1": "test1", "test2": "test2"}),
        ],
    )
    def test_get_passwords(self, auth_manager, file_content, expected):
        with conf_vars(
            {
                ("core", "simple_auth_manager_users"): "test1:viewer,test2:viewer",
            }
        ):
            with open(auth_manager.get_generated_password_file(), "w") as file:
                file.write(file_content)
            passwords = auth_manager.get_passwords()
            assert passwords == expected

    def test_init_with_default_user(self, auth_manager):
        auth_manager.init()
        with open(auth_manager.get_generated_password_file()) as file:
            passwords_str = file.read().strip()
            user_passwords_from_file = json.loads(passwords_str)

            assert len(user_passwords_from_file) == 1

    def test_init_with_users(self, auth_manager):
        with conf_vars(
            {
                ("core", "simple_auth_manager_users"): "test1:viewer,test2:viewer",
            }
        ):
            auth_manager.init()
            with open(auth_manager.get_generated_password_file()) as file:
                passwords_str = file.read().strip()
                user_passwords_from_file = json.loads(passwords_str)

                assert len(user_passwords_from_file) == 2

    @pytest.mark.parametrize(
        ("file_content", "expected"),
        [
            ({"test1": "test1"}, {"test1": "test1"}),
            ({"test2": "test2", "test3": "test3"}, {"test1": mock.ANY, "test2": "test2", "test3": "test3"}),
        ],
    )
    def test_init_with_users_with_password(self, auth_manager, file_content, expected):
        with conf_vars(
            {
                ("core", "simple_auth_manager_users"): "test1:viewer",
            }
        ):
            with open(auth_manager.get_generated_password_file(), "w") as file:
                file.write(json.dumps(file_content) + "\n")
            auth_manager.init()
            with open(auth_manager.get_generated_password_file()) as file:
                passwords_str = file.read().strip()
                user_passwords_from_file = json.loads(passwords_str)

                assert user_passwords_from_file == expected

    def test_init_with_all_admins(self, auth_manager):
        with conf_vars({("core", "simple_auth_manager_all_admins"): "true"}):
            auth_manager.init()
            assert not os.path.exists(auth_manager.get_generated_password_file())

    def test_get_url_login(self, auth_manager):
        result = auth_manager.get_url_login()
        assert result == AUTH_MANAGER_FASTAPI_APP_PREFIX + "/login"

    def test_get_url_login_with_all_admins(self, auth_manager):
        with conf_vars({("core", "simple_auth_manager_all_admins"): "true"}):
            result = auth_manager.get_url_login()
            assert result == AUTH_MANAGER_FASTAPI_APP_PREFIX + "/token/login"

    def test_deserialize_user(self, auth_manager):
        result = auth_manager.deserialize_user({"sub": "test", "role": "admin"})
        assert result.username == "test"
        assert result.role == "admin"

    def test_serialize_user(self, auth_manager):
        user = SimpleAuthManagerUser(username="test", role="admin")
        result = auth_manager.serialize_user(user)
        assert result == {"sub": "test", "role": "admin"}

    @pytest.mark.parametrize(
        "api",
        [
            "is_authorized_configuration",
            "is_authorized_connection",
            "is_authorized_dag",
            "is_authorized_asset",
            "is_authorized_asset_alias",
            "is_authorized_backfill",
            "is_authorized_pool",
            "is_authorized_variable",
        ],
    )
    @pytest.mark.parametrize(
        ("role", "method", "result"),
        [
            ("ADMIN", "GET", True),
            ("ADMIN", "DELETE", True),
            ("VIEWER", "POST", False),
            ("VIEWER", "PUT", False),
            ("VIEWER", "DELETE", False),
        ],
    )
    def test_is_authorized_methods(self, auth_manager, api, role, method, result):
        assert (
            getattr(auth_manager, api)(method=method, user=SimpleAuthManagerUser(username="test", role=role))
            is result
        )

    @pytest.mark.parametrize(
        ("api", "kwargs"),
        [
            ("is_authorized_view", {"access_view": AccessView.CLUSTER_ACTIVITY}),
            (
                "is_authorized_custom_view",
                {
                    "method": "GET",
                    "resource_name": "test",
                },
            ),
        ],
    )
    @pytest.mark.parametrize(
        ("role", "result"),
        [
            ("ADMIN", True),
            ("VIEWER", True),
            ("USER", True),
            ("OP", True),
        ],
    )
    def test_is_authorized_view_methods(self, auth_manager, api, kwargs, role, result):
        assert (
            getattr(auth_manager, api)(**kwargs, user=SimpleAuthManagerUser(username="test", role=role))
            is result
        )

    @pytest.mark.parametrize(
        "api",
        [
            "is_authorized_configuration",
            "is_authorized_connection",
            "is_authorized_asset",
            "is_authorized_asset_alias",
            "is_authorized_backfill",
            "is_authorized_pool",
            "is_authorized_variable",
        ],
    )
    @pytest.mark.parametrize(
        ("role", "method", "result"),
        [
            ("ADMIN", "GET", True),
            ("OP", "DELETE", True),
            ("USER", "DELETE", False),
            ("VIEWER", "PUT", False),
        ],
    )
    def test_is_authorized_methods_op_role_required(self, auth_manager, api, role, method, result):
        assert (
            getattr(auth_manager, api)(method=method, user=SimpleAuthManagerUser(username="test", role=role))
            is result
        )

    @pytest.mark.parametrize(
        "api",
        ["is_authorized_dag"],
    )
    @pytest.mark.parametrize(
        ("role", "method", "result"),
        [
            ("ADMIN", "GET", True),
            ("OP", "DELETE", True),
            ("USER", "GET", True),
            ("USER", "DELETE", True),
            ("VIEWER", "PUT", False),
        ],
    )
    def test_is_authorized_methods_user_role_required(self, auth_manager, api, role, method, result):
        assert (
            getattr(auth_manager, api)(method=method, user=SimpleAuthManagerUser(username="test", role=role))
            is result
        )

    @pytest.mark.parametrize(
        "api",
        [
            "is_authorized_dag",
            "is_authorized_asset",
            "is_authorized_asset_alias",
            "is_authorized_backfill",
            "is_authorized_pool",
        ],
    )
    @pytest.mark.parametrize(
        ("role", "method", "result"),
        [
            ("ADMIN", "GET", True),
            ("VIEWER", "GET", True),
            ("OP", "GET", True),
            ("USER", "GET", True),
            ("VIEWER", "POST", False),
        ],
    )
    def test_is_authorized_methods_viewer_role_required_for_get(
        self, auth_manager, api, role, method, result
    ):
        assert (
            getattr(auth_manager, api)(method=method, user=SimpleAuthManagerUser(username="test", role=role))
            is result
        )

    def test_is_authorized_team(self, auth_manager):
        result = auth_manager.is_authorized_team(
            method="GET", user=SimpleAuthManagerUser(username="test", role=None)
        )
        assert result is True

    def test_filter_authorized_menu_items(self, auth_manager):
        items = [MenuItem.ASSETS]
        results = auth_manager.filter_authorized_menu_items(
            items, user=SimpleAuthManagerUser(username="test", role=None)
        )
        assert results == items

    @pytest.mark.parametrize(
        ("all_admins", "user_id", "assigned_users", "expected"),
        [
            # When simple_auth_manager_all_admins=True, any user should be allowed
            (True, "user1", {"user2"}, True),
            (True, "user2", {"user2"}, True),
            (True, "admin", {"test_user"}, True),
            # When simple_auth_manager_all_admins=False, user must be in assigned_users
            (False, "user1", {"user1"}, True),
            (False, "user2", {"user1"}, False),
            (False, "admin", {"test_user"}, False),
            # When no assigned_users, allow access
            (False, "user1", set(), True),
        ],
    )
    def test_is_authorized_hitl_task(self, auth_manager, all_admins, user_id, assigned_users, expected):
        """Test is_authorized_hitl_task method with different configurations."""
        with conf_vars({("core", "simple_auth_manager_all_admins"): str(all_admins)}):
            user = SimpleAuthManagerUser(username=user_id, role="user")
            result = auth_manager.is_authorized_hitl_task(assigned_users=assigned_users, user=user)
            assert result == expected
