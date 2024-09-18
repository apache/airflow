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
from typing import TYPE_CHECKING
from unittest import mock
from unittest.mock import Mock

import pytest
from flask import Flask

from airflow.exceptions import AirflowConfigException, AirflowException

try:
    from airflow.auth.managers.models.resource_details import AccessView, DagAccessEntity, DagDetails
except ImportError:
    pass

from tests.test_utils.compat import ignore_provider_compatibility_error

with ignore_provider_compatibility_error("2.9.0+", __file__):
    from airflow.providers.fab.auth_manager.fab_auth_manager import FabAuthManager
    from airflow.providers.fab.auth_manager.models import User
    from airflow.providers.fab.auth_manager.security_manager.override import FabAirflowSecurityManagerOverride

from airflow.security.permissions import (
    ACTION_CAN_ACCESS_MENU,
    ACTION_CAN_CREATE,
    ACTION_CAN_DELETE,
    ACTION_CAN_EDIT,
    ACTION_CAN_READ,
    RESOURCE_CONFIG,
    RESOURCE_CONNECTION,
    RESOURCE_DAG,
    RESOURCE_DAG_RUN,
    RESOURCE_DATASET,
    RESOURCE_DOCS,
    RESOURCE_JOB,
    RESOURCE_PLUGIN,
    RESOURCE_PROVIDER,
    RESOURCE_TASK_INSTANCE,
    RESOURCE_TRIGGER,
    RESOURCE_VARIABLE,
    RESOURCE_WEBSITE,
)
from airflow.www.extensions.init_appbuilder import init_appbuilder

if TYPE_CHECKING:
    from airflow.auth.managers.base_auth_manager import ResourceMethod

IS_AUTHORIZED_METHODS_SIMPLE = {
    "is_authorized_configuration": RESOURCE_CONFIG,
    "is_authorized_connection": RESOURCE_CONNECTION,
    "is_authorized_dataset": RESOURCE_DATASET,
    "is_authorized_variable": RESOURCE_VARIABLE,
}


@pytest.fixture
def auth_manager():
    return FabAuthManager(None)


@pytest.fixture
def flask_app():
    return Flask(__name__)


@pytest.fixture
def auth_manager_with_appbuilder(flask_app):
    appbuilder = init_appbuilder(flask_app)
    return FabAuthManager(appbuilder)


@pytest.mark.db_test
class TestFabAuthManager:
    @pytest.mark.parametrize(
        "id,first_name,last_name,username,email,expected",
        [
            (1, "First", "Last", None, None, "First Last"),
            (1, "First", None, "user", None, "First"),
            (1, None, "Last", "user", "email", "Last"),
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
    def test_is_logged_in(self, mock_get_user, auth_manager_with_appbuilder):
        user = Mock()
        user.is_anonymous.return_value = True
        mock_get_user.return_value = user

        assert auth_manager_with_appbuilder.is_logged_in() is False

    @mock.patch.object(FabAuthManager, "get_user")
    def test_is_logged_in_with_inactive_user(self, mock_get_user, auth_manager_with_appbuilder):
        user = Mock()
        user.is_anonymous.return_value = False
        user.is_active.return_value = True
        mock_get_user.return_value = user

        assert auth_manager_with_appbuilder.is_logged_in() is False

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
                    # With permission
                    (
                        api_name,
                        "MENU",
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
            # Without read permissions on a specific DAG
            (
                "GET",
                DagAccessEntity.TASK_INSTANCE,
                DagDetails(id="test_dag_id"),
                [(ACTION_CAN_READ, RESOURCE_TASK_INSTANCE)],
                False,
            ),
            # With read permissions on a specific DAG but not on the DAG run
            (
                "GET",
                DagAccessEntity.TASK_INSTANCE,
                DagDetails(id="test_dag_id"),
                [(ACTION_CAN_READ, "DAG:test_dag_id"), (ACTION_CAN_READ, RESOURCE_TASK_INSTANCE)],
                False,
            ),
            # With read permissions on a specific DAG but not on the DAG run
            (
                "GET",
                DagAccessEntity.TASK_INSTANCE,
                DagDetails(id="test_dag_id"),
                [
                    (ACTION_CAN_READ, "DAG:test_dag_id"),
                    (ACTION_CAN_READ, RESOURCE_TASK_INSTANCE),
                    (ACTION_CAN_READ, RESOURCE_DAG_RUN),
                ],
                True,
            ),
            # With edit permissions on a specific DAG and read on the DAG access entity
            (
                "DELETE",
                DagAccessEntity.TASK,
                DagDetails(id="test_dag_id"),
                [(ACTION_CAN_EDIT, "DAG:test_dag_id"), (ACTION_CAN_DELETE, RESOURCE_TASK_INSTANCE)],
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
            method=method, access_entity=dag_access_entity, details=dag_details, user=user
        )
        assert result == expected_result

    @pytest.mark.parametrize(
        "access_view, user_permissions, expected_result",
        [
            # With permission (jobs)
            (
                AccessView.JOBS,
                [(ACTION_CAN_READ, RESOURCE_JOB)],
                True,
            ),
            # With permission (plugins)
            (
                AccessView.PLUGINS,
                [(ACTION_CAN_READ, RESOURCE_PLUGIN)],
                True,
            ),
            # With permission (providers)
            (
                AccessView.PROVIDERS,
                [(ACTION_CAN_READ, RESOURCE_PROVIDER)],
                True,
            ),
            # With permission (triggers)
            (
                AccessView.TRIGGERS,
                [(ACTION_CAN_READ, RESOURCE_TRIGGER)],
                True,
            ),
            # With permission (website)
            (
                AccessView.WEBSITE,
                [(ACTION_CAN_READ, RESOURCE_WEBSITE)],
                True,
            ),
            # Without permission
            (
                AccessView.WEBSITE,
                [(ACTION_CAN_READ, "resource_test"), (ACTION_CAN_CREATE, RESOURCE_WEBSITE)],
                False,
            ),
            # Without permission
            (
                AccessView.WEBSITE,
                [(ACTION_CAN_READ, RESOURCE_TRIGGER)],
                False,
            ),
            # Docs (positive)
            (
                AccessView.DOCS,
                [(ACTION_CAN_ACCESS_MENU, RESOURCE_DOCS)],
                True,
            ),
            # Without permission
            (
                AccessView.DOCS,
                [(ACTION_CAN_READ, RESOURCE_DOCS)],
                False,
            ),
        ],
    )
    def test_is_authorized_view(self, access_view, user_permissions, expected_result, auth_manager):
        user = Mock()
        user.perms = user_permissions
        result = auth_manager.is_authorized_view(access_view=access_view, user=user)
        assert result == expected_result

    @pytest.mark.parametrize(
        "method, resource_name, user_permissions, expected_result",
        [
            (
                "GET",
                "custom_resource",
                [(ACTION_CAN_READ, "custom_resource")],
                True,
            ),
            (
                "GET",
                "custom_resource",
                [(ACTION_CAN_EDIT, "custom_resource")],
                False,
            ),
            (
                "GET",
                "custom_resource",
                [(ACTION_CAN_READ, "custom_resource2")],
                False,
            ),
            (
                "DUMMY",
                "custom_resource",
                [("DUMMY", "custom_resource")],
                True,
            ),
        ],
    )
    def test_is_authorized_custom_view(
        self,
        method: ResourceMethod | str,
        resource_name: str,
        user_permissions,
        expected_result,
        auth_manager,
    ):
        user = Mock()
        user.perms = user_permissions
        result = auth_manager.is_authorized_custom_view(method=method, resource_name=resource_name, user=user)
        assert result == expected_result

    @pytest.mark.db_test
    def test_security_manager_return_fab_security_manager_override(self, auth_manager_with_appbuilder):
        assert isinstance(auth_manager_with_appbuilder.security_manager, FabAirflowSecurityManagerOverride)

    @pytest.mark.db_test
    def test_security_manager_return_custom_provided(self, flask_app, auth_manager_with_appbuilder):
        class TestSecurityManager(FabAirflowSecurityManagerOverride):
            pass

        flask_app.config["SECURITY_MANAGER_CLASS"] = TestSecurityManager
        assert isinstance(auth_manager_with_appbuilder.security_manager, TestSecurityManager)

    @pytest.mark.db_test
    def test_security_manager_wrong_inheritance_raise_exception(
        self, flask_app, auth_manager_with_appbuilder
    ):
        class TestSecurityManager:
            pass

        flask_app.config["SECURITY_MANAGER_CLASS"] = TestSecurityManager

        with pytest.raises(
            AirflowConfigException,
            match="Your CUSTOM_SECURITY_MANAGER must extend FabAirflowSecurityManagerOverride.",
        ):
            auth_manager_with_appbuilder.security_manager

    @pytest.mark.db_test
    def test_get_url_login_when_auth_view_not_defined(self, auth_manager_with_appbuilder):
        with pytest.raises(AirflowException, match="`auth_view` not defined in the security manager."):
            auth_manager_with_appbuilder.get_url_login()

    @pytest.mark.db_test
    @mock.patch("airflow.providers.fab.auth_manager.fab_auth_manager.url_for")
    def test_get_url_login(self, mock_url_for, auth_manager_with_appbuilder):
        auth_manager_with_appbuilder.security_manager.auth_view = Mock()
        auth_manager_with_appbuilder.security_manager.auth_view.endpoint = "test_endpoint"
        auth_manager_with_appbuilder.get_url_login()
        mock_url_for.assert_called_once_with("test_endpoint.login")

    @pytest.mark.db_test
    @mock.patch("airflow.providers.fab.auth_manager.fab_auth_manager.url_for")
    def test_get_url_login_with_next(self, mock_url_for, auth_manager_with_appbuilder):
        auth_manager_with_appbuilder.security_manager.auth_view = Mock()
        auth_manager_with_appbuilder.security_manager.auth_view.endpoint = "test_endpoint"
        auth_manager_with_appbuilder.get_url_login(next_url="next_url")
        mock_url_for.assert_called_once_with("test_endpoint.login", next="next_url")

    @pytest.mark.db_test
    def test_get_url_logout_when_auth_view_not_defined(self, auth_manager_with_appbuilder):
        with pytest.raises(AirflowException, match="`auth_view` not defined in the security manager."):
            auth_manager_with_appbuilder.get_url_logout()

    @pytest.mark.db_test
    @mock.patch("airflow.providers.fab.auth_manager.fab_auth_manager.url_for")
    def test_get_url_logout(self, mock_url_for, auth_manager_with_appbuilder):
        auth_manager_with_appbuilder.security_manager.auth_view = Mock()
        auth_manager_with_appbuilder.security_manager.auth_view.endpoint = "test_endpoint"
        auth_manager_with_appbuilder.get_url_logout()
        mock_url_for.assert_called_once_with("test_endpoint.logout")

    @pytest.mark.db_test
    def test_get_url_user_profile_when_auth_view_not_defined(self, auth_manager_with_appbuilder):
        assert auth_manager_with_appbuilder.get_url_user_profile() is None

    @pytest.mark.db_test
    @mock.patch("airflow.providers.fab.auth_manager.fab_auth_manager.url_for")
    def test_get_url_user_profile(self, mock_url_for, auth_manager_with_appbuilder):
        expected_url = "test_url"
        mock_url_for.return_value = expected_url
        auth_manager_with_appbuilder.security_manager.user_view = Mock()
        auth_manager_with_appbuilder.security_manager.user_view.endpoint = "test_endpoint"
        actual_url = auth_manager_with_appbuilder.get_url_user_profile()
        mock_url_for.assert_called_once_with("test_endpoint.userinfo")
        assert actual_url == expected_url
