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
from flask import session
from flask_appbuilder.menu import MenuItem

from airflow.providers.amazon.version_compat import AIRFLOW_V_3_0_PLUS

if not AIRFLOW_V_3_0_PLUS:
    pytest.skip("AWS auth manager is only compatible with Airflow >= 3.0.0", allow_module_level=True)

from airflow.auth.managers.models.resource_details import (
    AccessView,
    ConfigurationDetails,
    ConnectionDetails,
    DagAccessEntity,
    DagDetails,
    PoolDetails,
    VariableDetails,
)
from airflow.providers.amazon.aws.auth_manager.avp.entities import AvpEntities
from airflow.providers.amazon.aws.auth_manager.aws_auth_manager import AwsAuthManager
from airflow.providers.amazon.aws.auth_manager.user import AwsAuthManagerUser
from airflow.security.permissions import (
    RESOURCE_AUDIT_LOG,
    RESOURCE_CLUSTER_ACTIVITY,
    RESOURCE_CONNECTION,
    RESOURCE_VARIABLE,
)

from tests_common.test_utils.config import conf_vars

if TYPE_CHECKING:
    from airflow.auth.managers.base_auth_manager import ResourceMethod
    from airflow.auth.managers.models.resource_details import AssetDetails
    from airflow.security.permissions import RESOURCE_ASSET
else:
    from airflow.providers.common.compat.assets import AssetDetails
    from airflow.providers.common.compat.security.permissions import RESOURCE_ASSET


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
        with patch.object(AwsAuthManager, "_check_avp_schema_version"):
            return AwsAuthManager()


@pytest.fixture
def test_user():
    return AwsAuthManagerUser(user_id="test_user_id", groups=[], username="test_username")


class TestAwsAuthManager:
    def test_avp_facade(self, auth_manager):
        assert hasattr(auth_manager, "avp_facade")

    @pytest.mark.db_test
    @patch.object(AwsAuthManager, "is_logged_in")
    def test_get_user(self, mock_is_logged_in, auth_manager, app, test_user):
        mock_is_logged_in.return_value = True

        with app.test_request_context():
            session["aws_user"] = test_user
            result = auth_manager.get_user()

        assert result == test_user

    @patch.object(AwsAuthManager, "is_logged_in")
    def test_get_user_return_none_when_not_logged_in(self, mock_is_logged_in, auth_manager):
        mock_is_logged_in.return_value = False
        result = auth_manager.get_user()

        assert result is None

    @pytest.mark.db_test
    def test_is_logged_in(self, auth_manager, app, test_user):
        with app.test_request_context():
            session["aws_user"] = test_user
            result = auth_manager.is_logged_in()

        assert result

    @pytest.mark.db_test
    def test_is_logged_in_return_false_when_no_user_in_session(self, auth_manager, app, test_user):
        with app.test_request_context():
            result = auth_manager.is_logged_in()

        assert result is False

    @pytest.mark.parametrize(
        "details, user, expected_user, expected_entity_id",
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
        "details, user, expected_user, expected_entity_id",
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
        "access_entity, details, user, expected_user, expected_entity_id, expected_context",
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
        "details, user, expected_user, expected_entity_id",
        [
            (None, mock, ANY, None),
            (AssetDetails(uri="uri"), mock, mock, "uri"),
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
        "details, user, expected_user, expected_entity_id",
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
        "details, user, expected_user, expected_entity_id",
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
        "access_view, user, expected_user",
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

    @patch.object(AwsAuthManager, "avp_facade")
    def test_batch_is_authorized_connection(
        self,
        mock_avp_facade,
        auth_manager,
    ):
        batch_is_authorized = Mock(return_value=True)
        mock_avp_facade.batch_is_authorized = batch_is_authorized

        result = auth_manager.batch_is_authorized_connection(
            requests=[{"method": "GET"}, {"method": "GET", "details": ConnectionDetails(conn_id="conn_id")}],
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
                    "method": "GET",
                    "entity_type": AvpEntities.CONNECTION,
                    "entity_id": "conn_id",
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
                {"method": "GET", "details": DagDetails(id="dag_1"), "access_entity": DagAccessEntity.CODE},
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
                {
                    "method": "GET",
                    "entity_type": AvpEntities.DAG,
                    "entity_id": "dag_1",
                    "context": {
                        "dag_entity": {
                            "string": DagAccessEntity.CODE.value,
                        },
                    },
                },
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
            requests=[{"method": "GET"}, {"method": "GET", "details": PoolDetails(name="pool1")}],
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
                    "method": "GET",
                    "entity_type": AvpEntities.POOL,
                    "entity_id": "pool1",
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
            requests=[{"method": "GET"}, {"method": "GET", "details": VariableDetails(key="var1")}],
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
                    "method": "GET",
                    "entity_type": AvpEntities.VARIABLE,
                    "entity_id": "var1",
                },
            ],
            user=ANY,
        )
        assert result

    @patch.object(AwsAuthManager, "get_user")
    def test_filter_permitted_menu_items(self, mock_get_user, auth_manager, test_user):
        batch_is_authorized_output = [
            {
                "request": {
                    "principal": {"entityType": "Airflow::User", "entityId": "test_user_id"},
                    "action": {"actionType": "Airflow::Action", "actionId": "Menu.MENU"},
                    "resource": {"entityType": "Airflow::Menu", "entityId": "Connections"},
                },
                "decision": "DENY",
            },
            {
                "request": {
                    "principal": {"entityType": "Airflow::User", "entityId": "test_user_id"},
                    "action": {"actionType": "Airflow::Action", "actionId": "Menu.MENU"},
                    "resource": {"entityType": "Airflow::Menu", "entityId": "Variables"},
                },
                "decision": "ALLOW",
            },
            {
                "request": {
                    "principal": {"entityType": "Airflow::User", "entityId": "test_user_id"},
                    "action": {"actionType": "Airflow::Action", "actionId": "Menu.MENU"},
                    "resource": {"entityType": "Airflow::Menu", "entityId": RESOURCE_ASSET},
                },
                "decision": "DENY",
            },
            {
                "request": {
                    "principal": {"entityType": "Airflow::User", "entityId": "test_user_id"},
                    "action": {"actionType": "Airflow::Action", "actionId": "Menu.MENU"},
                    "resource": {"entityType": "Airflow::Menu", "entityId": "Cluster Activity"},
                },
                "decision": "DENY",
            },
            {
                "request": {
                    "principal": {"entityType": "Airflow::User", "entityId": "test_user_id"},
                    "action": {"actionType": "Airflow::Action", "actionId": "Menu.MENU"},
                    "resource": {"entityType": "Airflow::Menu", "entityId": "Audit Logs"},
                },
                "decision": "ALLOW",
            },
            {
                "request": {
                    "principal": {"entityType": "Airflow::User", "entityId": "test_user_id"},
                    "action": {"actionType": "Airflow::Action", "actionId": "Menu.MENU"},
                    "resource": {"entityType": "Airflow::Menu", "entityId": "CustomPage"},
                },
                "decision": "ALLOW",
            },
        ]
        auth_manager.avp_facade.get_batch_is_authorized_results = Mock(
            return_value=batch_is_authorized_output
        )

        mock_get_user.return_value = test_user

        result = auth_manager.filter_permitted_menu_items(
            [
                MenuItem("Category1", childs=[MenuItem(RESOURCE_CONNECTION), MenuItem(RESOURCE_VARIABLE)]),
                MenuItem("Category2", childs=[MenuItem(RESOURCE_ASSET)]),
                MenuItem(RESOURCE_CLUSTER_ACTIVITY),
                MenuItem(RESOURCE_AUDIT_LOG),
                MenuItem("CustomPage"),
            ]
        )

        """
        return {
            "method": "MENU",
            "entity_type": AvpEntities.MENU,
            "entity_id": resource_name,
        }
        """

        auth_manager.avp_facade.get_batch_is_authorized_results.assert_called_once_with(
            requests=[
                {
                    "method": "MENU",
                    "entity_type": AvpEntities.MENU,
                    "entity_id": "Connections",
                },
                {
                    "method": "MENU",
                    "entity_type": AvpEntities.MENU,
                    "entity_id": "Variables",
                },
                {
                    "method": "MENU",
                    "entity_type": AvpEntities.MENU,
                    "entity_id": RESOURCE_ASSET,
                },
                {"method": "MENU", "entity_type": AvpEntities.MENU, "entity_id": "Cluster Activity"},
                {"method": "MENU", "entity_type": AvpEntities.MENU, "entity_id": "Audit Logs"},
                {
                    "method": "MENU",
                    "entity_type": AvpEntities.MENU,
                    "entity_id": "CustomPage",
                },
            ],
            user=test_user,
        )
        assert len(result) == 3
        assert result[0].name == "Category1"
        assert len(result[0].childs) == 1
        assert result[0].childs[0].name == RESOURCE_VARIABLE
        assert result[1].name == RESOURCE_AUDIT_LOG
        assert result[2].name == "CustomPage"

    @patch.object(AwsAuthManager, "get_user")
    def test_filter_permitted_menu_items_logged_out(self, mock_get_user, auth_manager):
        mock_get_user.return_value = None
        result = auth_manager.filter_permitted_menu_items(
            [
                MenuItem(RESOURCE_AUDIT_LOG),
            ]
        )

        assert result == []

    @pytest.mark.parametrize(
        "methods, user",
        [
            (None, AwsAuthManagerUser(user_id="test_user_id", groups=[])),
            (["PUT", "GET"], AwsAuthManagerUser(user_id="test_user_id", groups=[])),
        ],
    )
    def test_filter_permitted_dag_ids(self, methods, user, auth_manager, test_user):
        dag_ids = {"dag_1", "dag_2"}
        batch_is_authorized_output = [
            {
                "request": {
                    "principal": {"entityType": "Airflow::User", "entityId": "test_user_id"},
                    "action": {"actionType": "Airflow::Action", "actionId": "Dag.GET"},
                    "resource": {"entityType": "Airflow::Dag", "entityId": "dag_1"},
                },
                "decision": "DENY",
            },
            {
                "request": {
                    "principal": {"entityType": "Airflow::User", "entityId": "test_user_id"},
                    "action": {"actionType": "Airflow::Action", "actionId": "Dag.PUT"},
                    "resource": {"entityType": "Airflow::Dag", "entityId": "dag_1"},
                },
                "decision": "DENY",
            },
            {
                "request": {
                    "principal": {"entityType": "Airflow::User", "entityId": "test_user_id"},
                    "action": {"actionType": "Airflow::Action", "actionId": "Dag.GET"},
                    "resource": {"entityType": "Airflow::Dag", "entityId": "dag_2"},
                },
                "decision": "DENY",
            },
            {
                "request": {
                    "principal": {"entityType": "Airflow::User", "entityId": "test_user_id"},
                    "action": {"actionType": "Airflow::Action", "actionId": "Dag.PUT"},
                    "resource": {"entityType": "Airflow::Dag", "entityId": "dag_2"},
                },
                "decision": "ALLOW",
            },
        ]
        auth_manager.avp_facade.get_batch_is_authorized_results = Mock(
            return_value=batch_is_authorized_output
        )

        result = auth_manager.filter_permitted_dag_ids(
            dag_ids=dag_ids,
            methods=methods,
            user=user,
        )

        auth_manager.avp_facade.get_batch_is_authorized_results.assert_called()
        assert result == {"dag_2"}

    def test_get_url_login(self, auth_manager):
        result = auth_manager.get_url_login()
        assert result == "http://localhost:9091/auth/login"

    def test_get_cli_commands_return_cli_commands(self, auth_manager):
        assert len(auth_manager.get_cli_commands()) > 0
