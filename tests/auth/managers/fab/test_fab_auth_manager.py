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

from itertools import chain
from unittest import mock
from unittest.mock import Mock

import pytest

from airflow.auth.managers.fab.fab_auth_manager import FabAuthManager
from airflow.auth.managers.fab.models import User
from airflow.auth.managers.fab.security_manager.override import FabAirflowSecurityManagerOverride
from airflow.auth.managers.models.resource_details import DagAccessEntity, DagDetails
from airflow.exceptions import AirflowException
from airflow.security.permissions import (
    ACTION_CAN_ACCESS_MENU,
    ACTION_CAN_CREATE,
    ACTION_CAN_DELETE,
    ACTION_CAN_EDIT,
    ACTION_CAN_READ,
    RESOURCE_CLUSTER_ACTIVITY,
    RESOURCE_CONFIG,
    RESOURCE_CONNECTION,
    RESOURCE_DAG,
    RESOURCE_DAG_RUN,
    RESOURCE_DATASET,
    RESOURCE_TASK_INSTANCE,
    RESOURCE_VARIABLE,
    RESOURCE_WEBSITE,
)
from airflow.www.security_appless import ApplessAirflowSecurityManager

IS_AUTHORIZED_METHODS_SIMPLE = {
    "is_authorized_configuration": RESOURCE_CONFIG,
    "is_authorized_cluster_activity": RESOURCE_CLUSTER_ACTIVITY,
    "is_authorized_connection": RESOURCE_CONNECTION,
    "is_authorized_dataset": RESOURCE_DATASET,
    "is_authorized_variable": RESOURCE_VARIABLE,
}


@pytest.fixture
def auth_manager():
    app_mock = Mock(name="flask_app")
    app_mock.config.get.return_value = None  # this is called to get the security manager override (if any)
    auth_manager = FabAuthManager(app_mock)
    auth_manager.security_manager = ApplessAirflowSecurityManager()
    return auth_manager


class TestFabAuthManager:
    @pytest.mark.parametrize(
        "id,first_name,last_name,username,email,expected",
        [
            (1, "First", "Last", None, None, "1"),
            (1, None, None, None, None, "1"),
            (1, "First", "Last", "user", None, "user"),
            (1, "First", "Last", "user", "email", "user"),
            (1, None, None, None, "email", "email"),
            (1, "First", "Last", None, "email", "email"),
        ],
    )
    @mock.patch.object(FabAuthManager, "get_user")
    def test_get_user_name(
        self, mock_get_user, id, first_name, last_name, username, email, expected, auth_manager
    ):
        user = User()
        user.id = id
        user.first_name = first_name
        user.last_name = last_name
        user.username = username
        user.email = email
        mock_get_user.return_value = user

        assert auth_manager.get_user_name() == expected

    @pytest.mark.parametrize(
        "id,first_name,last_name,username,email,expected",
        [
            (1, "First", "Last", None, None, "First Last"),
            (1, "First", None, "user", None, "First"),
            (1, None, "Last", "user", "email", "Last"),
            (1, None, None, None, "email", ""),
            (1, None, None, None, "email", ""),
        ],
    )
    @mock.patch.object(FabAuthManager, "get_user")
    def test_get_user_display_name(
        self, mock_get_user, id, first_name, last_name, username, email, expected, auth_manager
    ):
        user = User()
        user.id = id
        user.first_name = first_name
        user.last_name = last_name
        user.username = username
        user.email = email
        mock_get_user.return_value = user

        assert auth_manager.get_user_display_name() == expected

    @mock.patch("flask_login.utils._get_user")
    def test_get_user(self, mock_current_user, auth_manager):
        user = Mock()
        user.is_anonymous.return_value = True
        mock_current_user.return_value = user

        assert auth_manager.get_user() == user

    @mock.patch.object(FabAuthManager, "get_user")
    def test_get_user_id(self, mock_get_user, auth_manager):
        user_id = "test"
        user = Mock()
        user.get_id.return_value = user_id
        mock_get_user.return_value = user

        assert auth_manager.get_user_id() == user_id

    @mock.patch.object(FabAuthManager, "get_user")
    def test_is_logged_in(self, mock_get_user, auth_manager):
        user = Mock()
        user.is_anonymous.return_value = True
        mock_get_user.return_value = user

        assert auth_manager.is_logged_in() is False

    @pytest.mark.parametrize(
        "api_name, method, user_permissions, expected_result",
        chain(
            *[
                (
                    # With permission
                    (
                        api_name,
                        "POST",
                        [(ACTION_CAN_CREATE, resource_type)],
                        True,
                    ),
                    # With permission
                    (
                        api_name,
                        "GET",
                        [(ACTION_CAN_READ, resource_type)],
                        True,
                    ),
                    # With permission (with several user permissions)
                    (
                        api_name,
                        "DELETE",
                        [(ACTION_CAN_DELETE, resource_type), (ACTION_CAN_CREATE, "resource_test")],
                        True,
                    ),
                    # With permission (testing that ACTION_CAN_ACCESS_MENU gives GET permissions)
                    (
                        api_name,
                        "GET",
                        [(ACTION_CAN_ACCESS_MENU, resource_type)],
                        True,
                    ),
                    # Without permission
                    (
                        api_name,
                        "POST",
                        [(ACTION_CAN_READ, resource_type), (ACTION_CAN_CREATE, "resource_test")],
                        False,
                    ),
                )
                for api_name, resource_type in IS_AUTHORIZED_METHODS_SIMPLE.items()
            ]
        ),
    )
    def test_is_authorized(self, api_name, method, user_permissions, expected_result, auth_manager):
        user = Mock()
        user.perms = user_permissions
        result = getattr(auth_manager, api_name)(
            method=method,
            user=user,
        )
        assert result == expected_result

    @pytest.mark.parametrize(
        "method, dag_access_entity, dag_details, user_permissions, expected_result",
        [
            # Scenario 1 #
            # With global permissions on Dags
            (
                "GET",
                None,
                None,
                [(ACTION_CAN_READ, RESOURCE_DAG)],
                True,
            ),
            # On specific DAG with global permissions on Dags
            (
                "GET",
                None,
                DagDetails(id="test_dag_id"),
                [(ACTION_CAN_READ, RESOURCE_DAG)],
                True,
            ),
            # With permission on a specific DAG
            (
                "GET",
                None,
                DagDetails(id="test_dag_id"),
                [(ACTION_CAN_READ, "DAG:test_dag_id"), (ACTION_CAN_READ, "DAG:test_dag_id2")],
                True,
            ),
            # Without permission on a specific DAG (wrong method)
            (
                "POST",
                None,
                DagDetails(id="test_dag_id"),
                [(ACTION_CAN_READ, "DAG:test_dag_id")],
                False,
            ),
            # Without permission on a specific DAG
            (
                "GET",
                None,
                DagDetails(id="test_dag_id2"),
                [(ACTION_CAN_READ, "DAG:test_dag_id")],
                False,
            ),
            # Without permission on DAGs
            (
                "GET",
                None,
                None,
                [(ACTION_CAN_READ, "resource_test")],
                False,
            ),
            # Scenario 2 #
            # With global permissions on DAGs
            (
                "GET",
                DagAccessEntity.RUN,
                DagDetails(id="test_dag_id"),
                [(ACTION_CAN_READ, RESOURCE_DAG), (ACTION_CAN_READ, RESOURCE_DAG_RUN)],
                True,
            ),
            # With read permissions on a specific DAG
            (
                "GET",
                DagAccessEntity.TASK_INSTANCE,
                DagDetails(id="test_dag_id"),
                [(ACTION_CAN_READ, "DAG:test_dag_id"), (ACTION_CAN_READ, RESOURCE_TASK_INSTANCE)],
                True,
            ),
            # With edit permissions on a specific DAG and read on the DAG access entity
            (
                "POST",
                DagAccessEntity.RUN,
                DagDetails(id="test_dag_id"),
                [(ACTION_CAN_EDIT, "DAG:test_dag_id"), (ACTION_CAN_CREATE, RESOURCE_DAG_RUN)],
                True,
            ),
            # Without permissions to edit the DAG
            (
                "POST",
                DagAccessEntity.RUN,
                DagDetails(id="test_dag_id"),
                [(ACTION_CAN_CREATE, RESOURCE_DAG_RUN)],
                False,
            ),
            # Without read permissions on a specific DAG
            (
                "GET",
                DagAccessEntity.TASK_LOGS,
                DagDetails(id="test_dag_id"),
                [(ACTION_CAN_READ, RESOURCE_TASK_INSTANCE)],
                False,
            ),
        ],
    )
    def test_is_authorized_dag(
        self, method, dag_access_entity, dag_details, user_permissions, expected_result, auth_manager
    ):
        user = Mock()
        user.perms = user_permissions
        result = auth_manager.is_authorized_dag(
            method=method, dag_access_entity=dag_access_entity, dag_details=dag_details, user=user
        )
        assert result == expected_result

    @pytest.mark.parametrize(
        "user_permissions, expected_result",
        [
            # With permission
            (
                [(ACTION_CAN_READ, RESOURCE_WEBSITE)],
                True,
            ),
            # Without permission
            (
                [(ACTION_CAN_READ, "resource_test"), (ACTION_CAN_CREATE, RESOURCE_WEBSITE)],
                False,
            ),
        ],
    )
    def test_is_authorized_website(self, user_permissions, expected_result, auth_manager):
        user = Mock()
        user.perms = user_permissions
        result = auth_manager.is_authorized_website(user=user)
        assert result == expected_result

    def test_get_security_manager_override_class_return_fab_security_manager_override(self, auth_manager):
        assert auth_manager.get_security_manager_override_class() is FabAirflowSecurityManagerOverride

    def test_get_url_login_when_auth_view_not_defined(self, auth_manager):
        with pytest.raises(AirflowException, match="`auth_view` not defined in the security manager."):
            auth_manager.get_url_login()

    @mock.patch("airflow.auth.managers.fab.fab_auth_manager.url_for")
    def test_get_url_login(self, mock_url_for, auth_manager):
        auth_manager.security_manager.auth_view = Mock()
        auth_manager.security_manager.auth_view.endpoint = "test_endpoint"
        auth_manager.get_url_login()
        mock_url_for.assert_called_once_with("test_endpoint.login")

    @mock.patch("airflow.auth.managers.fab.fab_auth_manager.url_for")
    def test_get_url_login_with_next(self, mock_url_for, auth_manager):
        auth_manager.security_manager.auth_view = Mock()
        auth_manager.security_manager.auth_view.endpoint = "test_endpoint"
        auth_manager.get_url_login(next_url="next_url")
        mock_url_for.assert_called_once_with("test_endpoint.login", next="next_url")

    def test_get_url_logout_when_auth_view_not_defined(self, auth_manager):
        with pytest.raises(AirflowException, match="`auth_view` not defined in the security manager."):
            auth_manager.get_url_logout()

    @mock.patch("airflow.auth.managers.fab.fab_auth_manager.url_for")
    def test_get_url_logout(self, mock_url_for, auth_manager):
        auth_manager.security_manager.auth_view = Mock()
        auth_manager.security_manager.auth_view.endpoint = "test_endpoint"
        auth_manager.get_url_logout()
        mock_url_for.assert_called_once_with("test_endpoint.logout")

    def test_get_url_user_profile_when_auth_view_not_defined(self, auth_manager):
        assert auth_manager.get_url_user_profile() is None

    @mock.patch("airflow.auth.managers.fab.fab_auth_manager.url_for")
    def test_get_url_user_profile(self, mock_url_for, auth_manager):
        expected_url = "test_url"
        mock_url_for.return_value = expected_url
        auth_manager.security_manager.user_view = Mock()
        auth_manager.security_manager.user_view.endpoint = "test_endpoint"
        actual_url = auth_manager.get_url_user_profile()
        mock_url_for.assert_called_once_with("test_endpoint.userinfo")
        assert actual_url == expected_url
