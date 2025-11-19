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

from typing import TYPE_CHECKING
from unittest.mock import ANY, Mock, patch

import pytest

from tests_common.test_utils.version_compat import AIRFLOW_V_3_0_PLUS

if not AIRFLOW_V_3_0_PLUS:
    pytest.skip("AWS auth manager is only compatible with Airflow >= 3.0.0", allow_module_level=True)

from airflow.api_fastapi.app import AUTH_MANAGER_FASTAPI_APP_PREFIX
from airflow.api_fastapi.auth.managers.models.resource_details import (
    AccessView,
    BackfillDetails,
    ConfigurationDetails,
    ConnectionDetails,
    DagAccessEntity,
    DagDetails,
    PoolDetails,
    VariableDetails,
)
from airflow.api_fastapi.common.types import MenuItem
from airflow.providers.amazon.aws.auth_manager.avp.entities import AvpEntities
from airflow.providers.amazon.aws.auth_manager.aws_auth_manager import AwsAuthManager
from airflow.providers.amazon.aws.auth_manager.user import AwsAuthManagerUser

from tests_common.test_utils.config import conf_vars

if TYPE_CHECKING:
    from airflow.api_fastapi.auth.managers.base_auth_manager import ResourceMethod
    from airflow.api_fastapi.auth.managers.models.resource_details import (
        AssetAliasDetails,
        AssetDetails,
    )
else:
    from airflow.providers.common.compat.assets import AssetAliasDetails, AssetDetails


mock = Mock()

SAML_METADATA_PARSED = {
    "idp": {
        "entityId": "https://portal.sso.us-east-1.amazonaws.com/saml/assertion/<assertion>",
        "singleSignOnService": {
            "url": "https://portal.sso.us-east-1.amazonaws.com/saml/assertion/<assertion>",
            "binding": "urn:oasis:names:tc:SAML:2.0:bindings:HTTP-Redirect",
        },
        "singleLogoutService": {
            "url": "https://portal.sso.us-east-1.amazonaws.com/saml/logout/<assertion>",
            "binding": "urn:oasis:names:tc:SAML:2.0:bindings:HTTP-Redirect",
        },
        "x509cert": "<cert>",
    },
    "security": {"authnRequestsSigned": False},
    "sp": {"NameIDFormat": "urn:oasis:names:tc:SAML:2.0:nameid-format:transient"},
}


@pytest.fixture
def auth_manager():
    with conf_vars(
        {
            (
                "core",
                "auth_manager",
            ): "airflow.providers.amazon.aws.auth_manager.aws_auth_manager.AwsAuthManager",
        }
    ):
        return AwsAuthManager()


@pytest.fixture
def test_user():
    return AwsAuthManagerUser(user_id="test_user_id", groups=[], username="test_username")


class TestAwsAuthManager:
    def test_avp_facade(self, auth_manager):
        assert hasattr(auth_manager, "avp_facade")

    @pytest.mark.parametrize(
        ("details", "user", "expected_user", "expected_entity_id"),
        [
            (None, mock, ANY, None),
            (ConfigurationDetails(section="test"), mock, mock, "test"),
        ],
    )
    @patch.object(AwsAuthManager, "avp_facade")
    def test_is_authorized_configuration(
        self,
        mock_avp_facade,
        details,
        user,
        expected_user,
        expected_entity_id,
        auth_manager,
    ):
        is_authorized = Mock(return_value=True)
        mock_avp_facade.is_authorized = is_authorized

        method: ResourceMethod = "GET"
        result = auth_manager.is_authorized_configuration(method=method, details=details, user=user)

        is_authorized.assert_called_once_with(
            method=method,
            entity_type=AvpEntities.CONFIGURATION,
            user=expected_user,
            entity_id=expected_entity_id,
        )
        assert result

    @pytest.mark.parametrize(
        ("details", "user", "expected_user", "expected_entity_id"),
        [
            (None, mock, ANY, None),
            (ConnectionDetails(conn_id="conn_id"), mock, mock, "conn_id"),
        ],
    )
    @patch.object(AwsAuthManager, "avp_facade")
    def test_is_authorized_connection(
        self,
        mock_avp_facade,
        details,
        user,
        expected_user,
        expected_entity_id,
        auth_manager,
    ):
        is_authorized = Mock(return_value=True)
        mock_avp_facade.is_authorized = is_authorized

        method: ResourceMethod = "GET"
        result = auth_manager.is_authorized_connection(method=method, details=details, user=user)

        is_authorized.assert_called_once_with(
            method=method,
            entity_type=AvpEntities.CONNECTION,
            user=expected_user,
            entity_id=expected_entity_id,
        )
        assert result

    @pytest.mark.parametrize(
        ("access_entity", "details", "user", "expected_user", "expected_entity_id", "expected_context"),
        [
            (None, None, mock, ANY, None, None),
            (None, DagDetails(id="dag_1"), mock, mock, "dag_1", None),
            (
                DagAccessEntity.CODE,
                DagDetails(id="dag_1"),
                mock,
                mock,
                "dag_1",
                {
                    "dag_entity": {
                        "string": "CODE",
                    },
                },
            ),
        ],
    )
    @patch.object(AwsAuthManager, "avp_facade")
    def test_is_authorized_dag(
        self,
        mock_avp_facade,
        access_entity,
        details,
        user,
        expected_user,
        expected_entity_id,
        expected_context,
        auth_manager,
    ):
        is_authorized = Mock(return_value=True)
        mock_avp_facade.is_authorized = is_authorized

        method: ResourceMethod = "GET"
        result = auth_manager.is_authorized_dag(
            method=method, access_entity=access_entity, details=details, user=user
        )

        is_authorized.assert_called_once_with(
            method=method,
            entity_type=AvpEntities.DAG,
            user=expected_user,
            entity_id=expected_entity_id,
            context=expected_context,
        )
        assert result

    @pytest.mark.parametrize(
        ("details", "user", "expected_user", "expected_entity_id"),
        [
            (None, mock, ANY, None),
            (BackfillDetails(id=1), mock, mock, 1),
        ],
    )
    @patch.object(AwsAuthManager, "avp_facade")
    def test_is_authorized_backfill(
        self,
        mock_avp_facade,
        details,
        user,
        expected_user,
        expected_entity_id,
        auth_manager,
    ):
        is_authorized = Mock(return_value=True)
        mock_avp_facade.is_authorized = is_authorized

        method: ResourceMethod = "GET"
        result = auth_manager.is_authorized_backfill(method=method, details=details, user=user)

        is_authorized.assert_called_once_with(
            method=method, entity_type=AvpEntities.BACKFILL, user=expected_user, entity_id=expected_entity_id
        )
        assert result

    @pytest.mark.parametrize(
        ("details", "user", "expected_user", "expected_entity_id"),
        [
            (None, mock, ANY, None),
            (AssetDetails(id="1"), mock, mock, "1"),
        ],
    )
    @patch.object(AwsAuthManager, "avp_facade")
    def test_is_authorized_asset(
        self,
        mock_avp_facade,
        details,
        user,
        expected_user,
        expected_entity_id,
        auth_manager,
    ):
        is_authorized = Mock(return_value=True)
        mock_avp_facade.is_authorized = is_authorized

        method: ResourceMethod = "GET"
        result = auth_manager.is_authorized_asset(method=method, details=details, user=user)

        is_authorized.assert_called_once_with(
            method=method, entity_type=AvpEntities.ASSET, user=expected_user, entity_id=expected_entity_id
        )
        assert result

    @pytest.mark.parametrize(
        ("details", "user", "expected_user", "expected_entity_id"),
        [
            (None, mock, ANY, None),
            (AssetAliasDetails(id="1"), mock, mock, "1"),
        ],
    )
    @patch.object(AwsAuthManager, "avp_facade")
    def test_is_authorized_asset_alias(
        self,
        mock_avp_facade,
        details,
        user,
        expected_user,
        expected_entity_id,
        auth_manager,
    ):
        is_authorized = Mock(return_value=True)
        mock_avp_facade.is_authorized = is_authorized

        method: ResourceMethod = "GET"
        result = auth_manager.is_authorized_asset_alias(method=method, details=details, user=user)

        is_authorized.assert_called_once_with(
            method=method,
            entity_type=AvpEntities.ASSET_ALIAS,
            user=expected_user,
            entity_id=expected_entity_id,
        )
        assert result

    @pytest.mark.parametrize(
        ("details", "user", "expected_user", "expected_entity_id"),
        [
            (None, mock, ANY, None),
            (PoolDetails(name="pool1"), mock, mock, "pool1"),
        ],
    )
    @patch.object(AwsAuthManager, "avp_facade")
    def test_is_authorized_pool(
        self,
        mock_avp_facade,
        details,
        user,
        expected_user,
        expected_entity_id,
        auth_manager,
    ):
        is_authorized = Mock(return_value=True)
        mock_avp_facade.is_authorized = is_authorized

        method: ResourceMethod = "GET"
        result = auth_manager.is_authorized_pool(method=method, details=details, user=user)

        is_authorized.assert_called_once_with(
            method=method, entity_type=AvpEntities.POOL, user=expected_user, entity_id=expected_entity_id
        )
        assert result

    @pytest.mark.parametrize(
        ("details", "user", "expected_user", "expected_entity_id"),
        [
            (None, mock, ANY, None),
            (VariableDetails(key="var1"), mock, mock, "var1"),
        ],
    )
    @patch.object(AwsAuthManager, "avp_facade")
    def test_is_authorized_variable(
        self,
        mock_avp_facade,
        details,
        user,
        expected_user,
        expected_entity_id,
        auth_manager,
    ):
        is_authorized = Mock(return_value=True)
        mock_avp_facade.is_authorized = is_authorized

        method: ResourceMethod = "GET"
        result = auth_manager.is_authorized_variable(method=method, details=details, user=user)

        is_authorized.assert_called_once_with(
            method=method, entity_type=AvpEntities.VARIABLE, user=expected_user, entity_id=expected_entity_id
        )
        assert result

    @pytest.mark.parametrize(
        ("access_view", "user", "expected_user"),
        [
            (AccessView.CLUSTER_ACTIVITY, mock, ANY),
            (AccessView.PLUGINS, mock, mock),
        ],
    )
    @patch.object(AwsAuthManager, "avp_facade")
    def test_is_authorized_view(self, mock_avp_facade, access_view, user, expected_user, auth_manager):
        is_authorized = Mock(return_value=True)
        mock_avp_facade.is_authorized = is_authorized

        result = auth_manager.is_authorized_view(access_view=access_view, user=user)

        is_authorized.assert_called_once_with(
            method="GET", entity_type=AvpEntities.VIEW, user=expected_user, entity_id=access_view.value
        )
        assert result

    def test_filter_authorized_menu_items(self, auth_manager):
        batch_is_authorized_output = [
            {
                "request": {
                    "principal": {"entityType": "Airflow::User", "entityId": "test_user_id"},
                    "action": {"actionType": "Airflow::Action", "actionId": "Menu.MENU"},
                    "resource": {"entityType": "Airflow::Menu", "entityId": MenuItem.CONNECTIONS.value},
                },
                "decision": "DENY",
            },
            {
                "request": {
                    "principal": {"entityType": "Airflow::User", "entityId": "test_user_id"},
                    "action": {"actionType": "Airflow::Action", "actionId": "Menu.MENU"},
                    "resource": {"entityType": "Airflow::Menu", "entityId": MenuItem.VARIABLES.value},
                },
                "decision": "ALLOW",
            },
            {
                "request": {
                    "principal": {"entityType": "Airflow::User", "entityId": "test_user_id"},
                    "action": {"actionType": "Airflow::Action", "actionId": "Menu.MENU"},
                    "resource": {"entityType": "Airflow::Menu", "entityId": MenuItem.ASSETS.value},
                },
                "decision": "DENY",
            },
            {
                "request": {
                    "principal": {"entityType": "Airflow::User", "entityId": "test_user_id"},
                    "action": {"actionType": "Airflow::Action", "actionId": "Menu.MENU"},
                    "resource": {"entityType": "Airflow::Menu", "entityId": MenuItem.DAGS.value},
                },
                "decision": "ALLOW",
            },
        ]
        auth_manager.avp_facade.get_batch_is_authorized_results = Mock(
            return_value=batch_is_authorized_output
        )

        result = auth_manager.filter_authorized_menu_items(
            [MenuItem.CONNECTIONS, MenuItem.VARIABLES, MenuItem.ASSETS, MenuItem.DAGS],
            user=AwsAuthManagerUser(user_id="test_user_id", groups=[]),
        )

        auth_manager.avp_facade.get_batch_is_authorized_results.assert_called_once_with(
            requests=[
                {
                    "method": "MENU",
                    "entity_type": AvpEntities.MENU,
                    "entity_id": MenuItem.CONNECTIONS.value,
                },
                {
                    "method": "MENU",
                    "entity_type": AvpEntities.MENU,
                    "entity_id": MenuItem.VARIABLES.value,
                },
                {
                    "method": "MENU",
                    "entity_type": AvpEntities.MENU,
                    "entity_id": MenuItem.ASSETS.value,
                },
                {
                    "method": "MENU",
                    "entity_type": AvpEntities.MENU,
                    "entity_id": MenuItem.DAGS.value,
                },
            ],
            user=ANY,
        )
        assert result == [MenuItem.VARIABLES, MenuItem.DAGS]

    @patch.object(AwsAuthManager, "avp_facade")
    def test_batch_is_authorized_connection(
        self,
        mock_avp_facade,
        auth_manager,
    ):
        batch_is_authorized = Mock(return_value=True)
        mock_avp_facade.batch_is_authorized = batch_is_authorized

        result = auth_manager.batch_is_authorized_connection(
            requests=[
                {"method": "GET"},
                {"method": "PUT", "details": ConnectionDetails(conn_id="test")},
            ],
            user=mock,
        )

        batch_is_authorized.assert_called_once_with(
            requests=[
                {
                    "method": "GET",
                    "entity_type": AvpEntities.CONNECTION,
                    "entity_id": None,
                },
                {
                    "method": "PUT",
                    "entity_type": AvpEntities.CONNECTION,
                    "entity_id": "test",
                },
            ],
            user=ANY,
        )
        assert result

    @patch.object(AwsAuthManager, "avp_facade")
    def test_batch_is_authorized_dag(
        self,
        mock_avp_facade,
        auth_manager,
    ):
        batch_is_authorized = Mock(return_value=True)
        mock_avp_facade.batch_is_authorized = batch_is_authorized

        result = auth_manager.batch_is_authorized_dag(
            requests=[
                {"method": "GET"},
                {"method": "GET", "details": DagDetails(id="dag_1")},
            ]
            + [
                {"method": "GET", "details": DagDetails(id="dag_1"), "access_entity": dag_access_entity}
                for dag_access_entity in (
                    DagAccessEntity.AUDIT_LOG,
                    DagAccessEntity.CODE,
                    DagAccessEntity.DEPENDENCIES,
                    DagAccessEntity.RUN,
                    DagAccessEntity.TASK,
                    DagAccessEntity.TASK_INSTANCE,
                    DagAccessEntity.TASK_LOGS,
                    DagAccessEntity.VERSION,
                    DagAccessEntity.WARNING,
                    DagAccessEntity.XCOM,
                )
            ],
            user=mock,
        )

        batch_is_authorized.assert_called_once_with(
            requests=[
                {
                    "method": "GET",
                    "entity_type": AvpEntities.DAG,
                    "entity_id": None,
                    "context": None,
                },
                {
                    "method": "GET",
                    "entity_type": AvpEntities.DAG,
                    "entity_id": "dag_1",
                    "context": None,
                },
            ]
            + [
                {
                    "method": "GET",
                    "entity_type": AvpEntities.DAG,
                    "entity_id": "dag_1",
                    "context": {"dag_entity": {"string": dag_entity}},
                }
                for dag_entity in (
                    DagAccessEntity.AUDIT_LOG.value,
                    DagAccessEntity.CODE.value,
                    DagAccessEntity.DEPENDENCIES.value,
                    DagAccessEntity.RUN.value,
                    DagAccessEntity.TASK.value,
                    DagAccessEntity.TASK_INSTANCE.value,
                    DagAccessEntity.TASK_LOGS.value,
                    DagAccessEntity.VERSION.value,
                    DagAccessEntity.WARNING.value,
                    DagAccessEntity.XCOM.value,
                )
            ],
            user=ANY,
        )
        assert result

    @patch.object(AwsAuthManager, "avp_facade")
    def test_batch_is_authorized_pool(
        self,
        mock_avp_facade,
        auth_manager,
    ):
        batch_is_authorized = Mock(return_value=True)
        mock_avp_facade.batch_is_authorized = batch_is_authorized

        result = auth_manager.batch_is_authorized_pool(
            requests=[
                {"method": "GET"},
                {"method": "PUT", "details": PoolDetails(name="test")},
            ],
            user=mock,
        )

        batch_is_authorized.assert_called_once_with(
            requests=[
                {
                    "method": "GET",
                    "entity_type": AvpEntities.POOL,
                    "entity_id": None,
                },
                {
                    "method": "PUT",
                    "entity_type": AvpEntities.POOL,
                    "entity_id": "test",
                },
            ],
            user=ANY,
        )
        assert result

    @patch.object(AwsAuthManager, "avp_facade")
    def test_batch_is_authorized_variable(
        self,
        mock_avp_facade,
        auth_manager,
    ):
        batch_is_authorized = Mock(return_value=True)
        mock_avp_facade.batch_is_authorized = batch_is_authorized

        result = auth_manager.batch_is_authorized_variable(
            requests=[
                {"method": "GET"},
                {"method": "PUT", "details": VariableDetails(key="test")},
            ],
            user=mock,
        )

        batch_is_authorized.assert_called_once_with(
            requests=[
                {
                    "method": "GET",
                    "entity_type": AvpEntities.VARIABLE,
                    "entity_id": None,
                },
                {
                    "method": "PUT",
                    "entity_type": AvpEntities.VARIABLE,
                    "entity_id": "test",
                },
            ],
            user=ANY,
        )
        assert result

    @pytest.mark.parametrize(
        ("get_authorized_method", "avp_entity", "entities_parameter"),
        [
            ("filter_authorized_connections", AvpEntities.CONNECTION.value, "conn_ids"),
            ("filter_authorized_dag_ids", AvpEntities.DAG.value, "dag_ids"),
            ("filter_authorized_pools", AvpEntities.POOL.value, "pool_names"),
            ("filter_authorized_variables", AvpEntities.VARIABLE.value, "variable_keys"),
        ],
    )
    @pytest.mark.parametrize(
        ("method", "user", "expected_result"),
        [
            ("GET", AwsAuthManagerUser(user_id="test_user_id1", groups=[]), {"entity_1"}),
            ("PUT", AwsAuthManagerUser(user_id="test_user_id1", groups=[]), set()),
            ("GET", AwsAuthManagerUser(user_id="test_user_id2", groups=[]), set()),
            ("PUT", AwsAuthManagerUser(user_id="test_user_id2", groups=[]), {"entity_2"}),
        ],
    )
    def test_filter_authorized(
        self,
        get_authorized_method,
        avp_entity,
        entities_parameter,
        method,
        user,
        auth_manager,
        test_user,
        expected_result,
    ):
        entity_ids = {"entity_1", "entity_2"}
        # test_user_id1 has GET permissions on entity_1
        # test_user_id2 has PUT permissions on entity_2
        batch_is_authorized_output = [
            {
                "request": {
                    "principal": {"entityType": "Airflow::User", "entityId": "test_user_id1"},
                    "action": {"actionType": "Airflow::Action", "actionId": f"{avp_entity}.GET"},
                    "resource": {"entityType": f"Airflow::{avp_entity}", "entityId": "entity_1"},
                },
                "decision": "ALLOW",
            },
            {
                "request": {
                    "principal": {"entityType": "Airflow::User", "entityId": "test_user_id1"},
                    "action": {"actionType": "Airflow::Action", "actionId": f"{avp_entity}.PUT"},
                    "resource": {"entityType": f"Airflow::{avp_entity}", "entityId": "entity_1"},
                },
                "decision": "DENY",
            },
            {
                "request": {
                    "principal": {"entityType": "Airflow::User", "entityId": "test_user_id1"},
                    "action": {"actionType": "Airflow::Action", "actionId": f"{avp_entity}.GET"},
                    "resource": {"entityType": f"Airflow::{avp_entity}", "entityId": "entity_2"},
                },
                "decision": "DENY",
            },
            {
                "request": {
                    "principal": {"entityType": "Airflow::User", "entityId": "test_user_id1"},
                    "action": {"actionType": "Airflow::Action", "actionId": f"{avp_entity}.PUT"},
                    "resource": {"entityType": f"Airflow::{avp_entity}", "entityId": "entity_2"},
                },
                "decision": "DENY",
            },
            {
                "request": {
                    "principal": {"entityType": "Airflow::User", "entityId": "test_user_id2"},
                    "action": {"actionType": "Airflow::Action", "actionId": f"{avp_entity}.GET"},
                    "resource": {"entityType": f"Airflow::{avp_entity}", "entityId": "entity_1"},
                },
                "decision": "DENY",
            },
            {
                "request": {
                    "principal": {"entityType": "Airflow::User", "entityId": "test_user_id2"},
                    "action": {"actionType": "Airflow::Action", "actionId": f"{avp_entity}.PUT"},
                    "resource": {"entityType": f"Airflow::{avp_entity}", "entityId": "entity_1"},
                },
                "decision": "DENY",
            },
            {
                "request": {
                    "principal": {"entityType": "Airflow::User", "entityId": "test_user_id2"},
                    "action": {"actionType": "Airflow::Action", "actionId": f"{avp_entity}.GET"},
                    "resource": {"entityType": f"Airflow::{avp_entity}", "entityId": "entity_2"},
                },
                "decision": "DENY",
            },
            {
                "request": {
                    "principal": {"entityType": "Airflow::User", "entityId": "test_user_id2"},
                    "action": {"actionType": "Airflow::Action", "actionId": f"{avp_entity}.PUT"},
                    "resource": {"entityType": f"Airflow::{avp_entity}", "entityId": "entity_2"},
                },
                "decision": "ALLOW",
            },
        ]
        auth_manager.avp_facade.get_batch_is_authorized_results = Mock(
            return_value=batch_is_authorized_output
        )

        params = {
            entities_parameter: entity_ids,
            "method": method,
            "user": user,
        }
        result = getattr(auth_manager, get_authorized_method)(**params)

        auth_manager.avp_facade.get_batch_is_authorized_results.assert_called()
        assert result == expected_result

    def test_get_url_login(self, auth_manager):
        result = auth_manager.get_url_login()
        assert result == f"{AUTH_MANAGER_FASTAPI_APP_PREFIX}/login"

    def test_get_cli_commands_return_cli_commands(self, auth_manager):
        assert len(auth_manager.get_cli_commands()) > 0
