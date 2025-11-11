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

from contextlib import contextmanager, suppress
from itertools import chain
from typing import TYPE_CHECKING
from unittest import mock
from unittest.mock import MagicMock, Mock

import pytest
from flask import g

from airflow.api_fastapi.app import AUTH_MANAGER_FASTAPI_APP_PREFIX, get_auth_manager
from airflow.api_fastapi.common.types import MenuItem
from airflow.exceptions import AirflowConfigException
from airflow.providers.fab.www.app import create_app
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.utils.db import resetdb

from tests_common.test_utils.config import conf_vars
from unit.fab.auth_manager.api_endpoints.api_connexion_utils import create_user, delete_user

with suppress(ImportError):
    from airflow.api_fastapi.auth.managers.models.resource_details import (
        AccessView,
        DagAccessEntity,
        DagDetails,
    )

from airflow.providers.common.compat.security.permissions import (
    RESOURCE_ASSET,
    RESOURCE_ASSET_ALIAS,
    RESOURCE_BACKFILL,
)
from airflow.providers.fab.auth_manager.fab_auth_manager import FabAuthManager
from airflow.providers.fab.auth_manager.security_manager.override import FabAirflowSecurityManagerOverride
from airflow.providers.fab.www.security.permissions import (
    ACTION_CAN_ACCESS_MENU,
    ACTION_CAN_CREATE,
    ACTION_CAN_DELETE,
    ACTION_CAN_EDIT,
    ACTION_CAN_READ,
    RESOURCE_AUDIT_LOG,
    RESOURCE_CONFIG,
    RESOURCE_CONNECTION,
    RESOURCE_DAG,
    RESOURCE_DAG_RUN,
    RESOURCE_DOCS,
    RESOURCE_JOB,
    RESOURCE_PLUGIN,
    RESOURCE_PROVIDER,
    RESOURCE_TASK_INSTANCE,
    RESOURCE_TRIGGER,
    RESOURCE_VARIABLE,
    RESOURCE_WEBSITE,
)

from tests_common.test_utils.dag import sync_dag_to_db
from tests_common.test_utils.version_compat import AIRFLOW_V_3_1_PLUS

if AIRFLOW_V_3_1_PLUS:
    from airflow.providers.fab.www.security.permissions import RESOURCE_HITL_DETAIL

    HITL_ENDPOINT_TESTS = [
        # With global permissions on Dags, but no permission on HITL Detail
        (
            "GET",
            DagAccessEntity.HITL_DETAIL,
            None,
            [(ACTION_CAN_READ, RESOURCE_DAG)],
            False,
        ),
        # With global permissions on Dags, but no permission on HITL Detail
        (
            "PUT",
            DagAccessEntity.HITL_DETAIL,
            None,
            [(ACTION_CAN_READ, RESOURCE_DAG)],
            False,
        ),
        # With global permissions on Dags, with read permission on HITL Detail
        (
            "GET",
            DagAccessEntity.HITL_DETAIL,
            None,
            [(ACTION_CAN_READ, RESOURCE_DAG), (ACTION_CAN_READ, RESOURCE_HITL_DETAIL)],
            True,
        ),
        # With global permissions on Dags, with read permission on HITL Detail, but wrong method
        (
            "PUT",
            DagAccessEntity.HITL_DETAIL,
            None,
            [(ACTION_CAN_READ, RESOURCE_DAG), (ACTION_CAN_READ, RESOURCE_HITL_DETAIL)],
            False,
        ),
        # With global permissions on Dags, with write permission on HITL Detail, but wrong method
        (
            "GET",
            DagAccessEntity.HITL_DETAIL,
            None,
            [(ACTION_CAN_READ, RESOURCE_DAG), (ACTION_CAN_EDIT, RESOURCE_HITL_DETAIL)],
            False,
        ),
        # With global permissions on Dags, with edit permission on HITL Detail
        (
            "PUT",
            DagAccessEntity.HITL_DETAIL,
            None,
            [(ACTION_CAN_READ, RESOURCE_DAG), (ACTION_CAN_EDIT, RESOURCE_HITL_DETAIL)],
            True,
        ),
    ]

if TYPE_CHECKING:
    from airflow.api_fastapi.auth.managers.base_auth_manager import ResourceMethod


IS_AUTHORIZED_METHODS_SIMPLE = {
    "is_authorized_configuration": RESOURCE_CONFIG,
    "is_authorized_connection": RESOURCE_CONNECTION,
    "is_authorized_asset": RESOURCE_ASSET,
    "is_authorized_asset_alias": RESOURCE_ASSET_ALIAS,
    "is_authorized_backfill": RESOURCE_BACKFILL,
    "is_authorized_variable": RESOURCE_VARIABLE,
}


@contextmanager
def user_set(app, user):
    g.user = user
    yield
    g.user = None


@pytest.fixture
def auth_manager():
    return FabAuthManager(None)


@pytest.fixture
def flask_app():
    with conf_vars(
        {
            (
                "core",
                "auth_manager",
            ): "airflow.providers.fab.auth_manager.fab_auth_manager.FabAuthManager",
        }
    ):
        app = create_app(enable_plugins=False)
        with app.app_context():
            yield app


@pytest.fixture
def auth_manager_with_appbuilder(flask_app):
    return get_auth_manager()


@pytest.mark.db_test
class TestFabAuthManager:
    @mock.patch("flask_login.utils._get_user")
    def test_get_user(self, mock_current_user, minimal_app_for_auth_api, auth_manager):
        user = Mock()
        user.is_anonymous.return_value = True
        mock_current_user.return_value = user
        with minimal_app_for_auth_api.app_context():
            assert auth_manager.get_user() == user

    @mock.patch("flask_login.utils._get_user")
    def test_get_user_from_flask_g(self, mock_current_user, minimal_app_for_auth_api, auth_manager):
        session_user = Mock()
        session_user.is_anonymous = True
        mock_current_user.return_value = session_user

        flask_g_user = Mock()
        flask_g_user.is_anonymous = False
        with minimal_app_for_auth_api.app_context():
            with user_set(minimal_app_for_auth_api, flask_g_user):
                assert auth_manager.get_user() == flask_g_user

    def test_deserialize_user(self, flask_app, auth_manager_with_appbuilder):
        user = create_user(flask_app, "test")
        result = auth_manager_with_appbuilder.deserialize_user({"sub": str(user.id)})
        assert user.get_id() == result.get_id()

    def test_serialize_user(self, flask_app, auth_manager_with_appbuilder):
        user = create_user(flask_app, "test")
        result = auth_manager_with_appbuilder.serialize_user(user)
        assert result == {"sub": str(user.id)}

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
            # Without permission on DAGs
            (
                "GET",
                None,
                None,
                [(ACTION_CAN_READ, "resource_test")],
                False,
            ),
            # With specific DAG permissions but no specific DAG requested
            (
                "GET",
                None,
                None,
                [(ACTION_CAN_READ, "DAG:test_dag_id")],
                True,
            ),
            # With multiple specific DAG permissions, no specific DAG requested
            (
                "GET",
                None,
                None,
                [(ACTION_CAN_READ, "DAG:test_dag_id"), (ACTION_CAN_READ, "DAG:test_dag_id2")],
                True,
            ),
            # With specific DAG permissions and wrong method
            (
                "POST",
                None,
                None,
                [(ACTION_CAN_READ, "DAG:test_dag_id")],
                False,
            ),
            # With correct POST permissions
            (
                "POST",
                None,
                None,
                [(ACTION_CAN_CREATE, RESOURCE_DAG)],
                True,
            ),
            # Mixed permissions - some DAG, some non-DAG
            (
                "GET",
                None,
                None,
                [(ACTION_CAN_READ, "DAG:test_dag_id"), (ACTION_CAN_READ, "resource_test")],
                True,
            ),
            # DAG sub-entity with specific DAG permissions but no specific DAG requested
            (
                "GET",
                DagAccessEntity.RUN,
                None,
                [(ACTION_CAN_READ, "DAG:test_dag_id"), (ACTION_CAN_READ, RESOURCE_DAG_RUN)],
                True,
            ),
            # DAG sub-entity access with no DAG permissions, no specific DAG requested
            (
                "GET",
                DagAccessEntity.RUN,
                None,
                [(ACTION_CAN_READ, RESOURCE_DAG_RUN)],
                False,
            ),
            # DAG sub-entity with specific DAG permissions but missing sub-entity permission
            (
                "GET",
                DagAccessEntity.TASK_INSTANCE,
                None,
                [(ACTION_CAN_READ, "DAG:test_dag_id")],
                False,
            ),
            # Multiple DAG access entities with proper permissions
            (
                "DELETE",
                DagAccessEntity.TASK,
                None,
                [(ACTION_CAN_EDIT, "DAG:test_dag_id"), (ACTION_CAN_DELETE, RESOURCE_TASK_INSTANCE)],
                True,
            ),
            # User with specific DAG permissions but wrong method for sub-entity
            (
                "POST",
                DagAccessEntity.RUN,
                None,
                [(ACTION_CAN_READ, "DAG:test_dag_id"), (ACTION_CAN_READ, RESOURCE_DAG_RUN)],
                False,
            ),
            # Scenario 2 #
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
            # Scenario 3 #
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
            # With read permissions on a specific DAG and on the DAG run
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
            # With edit permissions on a specific DAG and delete on the DAG access entity
            (
                "DELETE",
                DagAccessEntity.TASK,
                DagDetails(id="test_dag_id"),
                [(ACTION_CAN_EDIT, "DAG:test_dag_id"), (ACTION_CAN_DELETE, RESOURCE_TASK_INSTANCE)],
                True,
            ),
            # With edit permissions on a specific DAG and create on the DAG access entity
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
            # Use deprecated prefix "DAG Run" to assign permissions specifically on dag runs
            (
                "GET",
                DagAccessEntity.RUN,
                DagDetails(id="test_dag_id"),
                [(ACTION_CAN_READ, "DAG:test_dag_id"), (ACTION_CAN_READ, "DAG Run:test_dag_id")],
                True,
            ),
        ],
    )
    def test_is_authorized_dag(
        self,
        method,
        dag_access_entity,
        dag_details,
        user_permissions,
        expected_result,
        auth_manager_with_appbuilder,
    ):
        user = Mock()
        user.perms = user_permissions
        user.id = 1
        result = auth_manager_with_appbuilder.is_authorized_dag(
            method=method, access_entity=dag_access_entity, details=dag_details, user=user
        )
        assert result == expected_result

    @pytest.mark.skipif(
        AIRFLOW_V_3_1_PLUS is not True, reason="HITL test will be skipped if Airflow version < 3.1.0"
    )
    @pytest.mark.parametrize(
        "method, dag_access_entity, dag_details, user_permissions, expected_result",
        HITL_ENDPOINT_TESTS if AIRFLOW_V_3_1_PLUS else [],
    )
    @mock.patch.object(FabAuthManager, "get_authorized_dag_ids")
    def test_is_authorized_dag_hitl_detail(
        self,
        mock_get_authorized_dag_ids,
        method,
        dag_access_entity,
        dag_details,
        user_permissions,
        expected_result,
        auth_manager_with_appbuilder,
    ):
        dag_permissions = [perm[1] for perm in user_permissions if perm[1].startswith("DAG:")]
        dag_ids = {perm.replace("DAG:", "") for perm in dag_permissions}
        mock_get_authorized_dag_ids.return_value = dag_ids

        user = Mock()
        user.perms = user_permissions
        user.id = 1
        result = auth_manager_with_appbuilder.is_authorized_dag(
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

    @pytest.mark.parametrize(
        "menu_items, user_permissions, expected_result",
        [
            (
                [MenuItem.ASSETS, MenuItem.DAGS],
                [(ACTION_CAN_ACCESS_MENU, RESOURCE_ASSET), (ACTION_CAN_ACCESS_MENU, RESOURCE_DAG)],
                [MenuItem.ASSETS, MenuItem.DAGS],
            ),
            (
                [MenuItem.ASSETS, MenuItem.DAGS],
                [(ACTION_CAN_READ, RESOURCE_ASSET), (ACTION_CAN_READ, RESOURCE_DAG)],
                [],
            ),
            (
                [MenuItem.AUDIT_LOG, MenuItem.VARIABLES],
                [(ACTION_CAN_ACCESS_MENU, RESOURCE_AUDIT_LOG), (ACTION_CAN_READ, RESOURCE_VARIABLE)],
                [MenuItem.AUDIT_LOG],
            ),
            (
                [],
                [],
                [],
            ),
        ],
    )
    def test_filter_authorized_menu_items(
        self,
        menu_items: list[MenuItem],
        user_permissions,
        expected_result,
        auth_manager,
    ):
        user = Mock()
        user.perms = user_permissions
        result = auth_manager.filter_authorized_menu_items(menu_items, user=user)
        assert result == expected_result

    def test_get_authorized_connections(self, auth_manager):
        session = Mock()
        session.execute.return_value.scalars.return_value.all.return_value = ["conn1", "conn2"]
        result = auth_manager.get_authorized_connections(user=Mock(), method="GET", session=session)
        assert result == {"conn1", "conn2"}

    @pytest.mark.parametrize(
        "method, user_permissions, expected_results",
        [
            # Scenario 1
            # With global read permissions on Dags
            (
                "GET",
                [(ACTION_CAN_READ, RESOURCE_DAG)],
                {"test_dag1", "test_dag2", "Connections"},
            ),
            # Scenario 2
            # With global edit permissions on Dags
            (
                "PUT",
                [(ACTION_CAN_EDIT, RESOURCE_DAG)],
                {"test_dag1", "test_dag2", "Connections"},
            ),
            # Scenario 3
            # With DAG-specific permissions
            (
                "GET",
                [(ACTION_CAN_READ, "DAG:test_dag1")],
                {"test_dag1"},
            ),
            # Scenario 4
            # With no permissions
            (
                "GET",
                [],
                set(),
            ),
            # Scenario 5
            # With read permissions but edit is requested
            (
                "PUT",
                [(ACTION_CAN_READ, RESOURCE_DAG)],
                set(),
            ),
            # Scenario 7
            # With read permissions but edit is requested
            (
                "PUT",
                [(ACTION_CAN_READ, "DAG:test_dag1")],
                set(),
            ),
            # Scenario 8
            # With DAG-specific permissions
            (
                "PUT",
                [(ACTION_CAN_EDIT, "DAG:test_dag1"), (ACTION_CAN_EDIT, "DAG:test_dag2")],
                {"test_dag1", "test_dag2"},
            ),
            # Scenario 9
            # With non-DAG related permissions
            (
                "GET",
                [(ACTION_CAN_READ, "DAG:test_dag1"), (ACTION_CAN_READ, RESOURCE_CONNECTION)],
                {"test_dag1"},
            ),
        ],
    )
    def test_get_authorized_dag_ids(
        self, method, user_permissions, expected_results, auth_manager_with_appbuilder, flask_app, dag_maker
    ):
        with dag_maker("test_dag1"):
            EmptyOperator(task_id="task1")
        if AIRFLOW_V_3_1_PLUS:
            sync_dag_to_db(dag_maker.dag)
        with dag_maker("test_dag2"):
            EmptyOperator(task_id="task2")
        if AIRFLOW_V_3_1_PLUS:
            sync_dag_to_db(dag_maker.dag)
        with dag_maker("Connections"):
            EmptyOperator(task_id="task3")
        if AIRFLOW_V_3_1_PLUS:
            sync_dag_to_db(dag_maker.dag)
        dag_maker.session.commit()
        dag_maker.session.close()

        user = create_user(
            flask_app,
            username="username",
            role_name="test",
            permissions=user_permissions,
        )

        auth_manager_with_appbuilder.security_manager.sync_perm_for_dag("test_dag1")
        auth_manager_with_appbuilder.security_manager.sync_perm_for_dag("test_dag2")

        results = auth_manager_with_appbuilder.get_authorized_dag_ids(user=user, method=method)
        assert results == expected_results

        delete_user(flask_app, "username")

    def test_get_authorized_pools(self, auth_manager):
        session = Mock()
        session.execute.return_value.scalars.return_value.all.return_value = ["pool1", "pool2"]
        result = auth_manager.get_authorized_pools(user=Mock(), method="GET", session=session)
        assert result == {"pool1", "pool2"}

    def test_get_authorized_variables(self, auth_manager):
        session = Mock()
        session.execute.return_value.scalars.return_value.all.return_value = ["var1", "var2"]
        result = auth_manager.get_authorized_variables(user=Mock(), method="GET", session=session)
        assert result == {"var1", "var2"}

    def test_security_manager_return_fab_security_manager_override(self, auth_manager_with_appbuilder):
        assert isinstance(auth_manager_with_appbuilder.security_manager, FabAirflowSecurityManagerOverride)

    def test_security_manager_return_custom_provided(self, flask_app, auth_manager_with_appbuilder):
        class TestSecurityManager(FabAirflowSecurityManagerOverride):
            pass

        flask_app.config["SECURITY_MANAGER_CLASS"] = TestSecurityManager
        # Invalidate the cache
        del auth_manager_with_appbuilder.__dict__["security_manager"]
        assert isinstance(auth_manager_with_appbuilder.security_manager, TestSecurityManager)

    def test_security_manager_wrong_inheritance_raise_exception(
        self, flask_app, auth_manager_with_appbuilder
    ):
        class TestSecurityManager:
            pass

        flask_app.config["SECURITY_MANAGER_CLASS"] = TestSecurityManager
        # Invalidate the cache
        del auth_manager_with_appbuilder.__dict__["security_manager"]
        with pytest.raises(
            AirflowConfigException,
            match="Your CUSTOM_SECURITY_MANAGER must extend FabAirflowSecurityManagerOverride.",
        ):
            auth_manager_with_appbuilder.security_manager

    def test_get_url_login(self, auth_manager):
        result = auth_manager.get_url_login()
        assert result == f"{AUTH_MANAGER_FASTAPI_APP_PREFIX}/login/"

    def test_get_url_logout(self, auth_manager):
        result = auth_manager.get_url_logout()
        assert result == f"{AUTH_MANAGER_FASTAPI_APP_PREFIX}/logout"

    @mock.patch.object(FabAuthManager, "_is_authorized", return_value=True)
    def test_get_extra_menu_items(self, _, auth_manager_with_appbuilder, flask_app):
        result = auth_manager_with_appbuilder.get_extra_menu_items(user=Mock())
        assert len(result) == 5
        assert all(item.href.startswith(AUTH_MANAGER_FASTAPI_APP_PREFIX) for item in result)

    def test_get_db_manager(self, auth_manager):
        result = auth_manager.get_db_manager()
        assert result == "airflow.providers.fab.auth_manager.models.db.FABDBManager"


@pytest.mark.db_test
@pytest.mark.parametrize("skip_init", [False, True])
@conf_vars(
    {("database", "external_db_managers"): "airflow.providers.fab.auth_manager.models.db.FABDBManager"}
)
@mock.patch("airflow.providers.fab.auth_manager.models.db.FABDBManager")
@mock.patch("airflow.utils.db.create_global_lock", new=MagicMock)
@mock.patch("airflow.utils.db.drop_airflow_models")
@mock.patch("airflow.utils.db.drop_airflow_moved_tables")
@mock.patch("airflow.utils.db.initdb")
@mock.patch("airflow.settings.engine.connect")
def test_resetdb(
    mock_connect,
    mock_init,
    mock_drop_moved,
    mock_drop_airflow,
    mock_fabdb_manager,
    skip_init,
):
    session_mock = MagicMock()
    resetdb(session_mock, skip_init=skip_init)
    mock_drop_airflow.assert_called_once_with(mock_connect.return_value)
    mock_drop_moved.assert_called_once_with(mock_connect.return_value)
    if skip_init:
        mock_init.assert_not_called()
    else:
        mock_init.assert_called_once_with(session=session_mock)
