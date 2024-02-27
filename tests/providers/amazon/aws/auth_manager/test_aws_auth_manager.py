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
from flask import Flask, session
from flask_appbuilder.menu import MenuItem

from airflow.auth.managers.models.resource_details import (
    AccessView,
    ConfigurationDetails,
    ConnectionDetails,
    DagAccessEntity,
    DagDetails,
    DatasetDetails,
    PoolDetails,
    VariableDetails,
)
from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.auth_manager.avp.entities import AvpEntities
from airflow.providers.amazon.aws.auth_manager.aws_auth_manager import AwsAuthManager
from airflow.providers.amazon.aws.auth_manager.security_manager.aws_security_manager_override import (
    AwsSecurityManagerOverride,
)
from airflow.providers.amazon.aws.auth_manager.user import AwsAuthManagerUser
from airflow.security.permissions import (
    RESOURCE_AUDIT_LOG,
    RESOURCE_CLUSTER_ACTIVITY,
    RESOURCE_CONNECTION,
    RESOURCE_DATASET,
    RESOURCE_VARIABLE,
)
from airflow.www.extensions.init_appbuilder import init_appbuilder
from tests.test_utils.config import conf_vars

if TYPE_CHECKING:
    from airflow.auth.managers.base_auth_manager import ResourceMethod

mock = Mock()


@pytest.fixture
def auth_manager():
    with conf_vars(
        {
            (
                "core",
                "auth_manager",
            ): "airflow.providers.amazon.aws.auth_manager.aws_auth_manager.AwsAuthManager",
            ("aws_auth_manager", "enable"): "True",
        }
    ):
        return AwsAuthManager(None)


@pytest.fixture
def auth_manager_with_appbuilder():
    flask_app = Flask(__name__)
    appbuilder = init_appbuilder(flask_app)
    with conf_vars(
        {
            (
                "core",
                "auth_manager",
            ): "airflow.providers.amazon.aws.auth_manager.aws_auth_manager.AwsAuthManager",
            ("aws_auth_manager", "enable"): "True",
        }
    ):
        return AwsAuthManager(appbuilder)


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
            (None, None, ANY, None),
            (ConfigurationDetails(section="test"), mock, mock, "test"),
        ],
    )
    @patch.object(AwsAuthManager, "avp_facade")
    @patch.object(AwsAuthManager, "get_user")
    def test_is_authorized_configuration(
        self,
        mock_get_user,
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

        if not user:
            mock_get_user.assert_called_once()
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
            (None, None, ANY, None),
            (ConnectionDetails(conn_id="conn_id"), mock, mock, "conn_id"),
        ],
    )
    @patch.object(AwsAuthManager, "avp_facade")
    @patch.object(AwsAuthManager, "get_user")
    def test_is_authorized_connection(
        self,
        mock_get_user,
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

        if not user:
            mock_get_user.assert_called_once()
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
            (None, None, None, ANY, None, None),
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
    @patch.object(AwsAuthManager, "get_user")
    def test_is_authorized_dag(
        self,
        mock_get_user,
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

        if not user:
            mock_get_user.assert_called_once()
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
            (None, None, ANY, None),
            (DatasetDetails(uri="uri"), mock, mock, "uri"),
        ],
    )
    @patch.object(AwsAuthManager, "avp_facade")
    @patch.object(AwsAuthManager, "get_user")
    def test_is_authorized_dataset(
        self,
        mock_get_user,
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
        result = auth_manager.is_authorized_dataset(method=method, details=details, user=user)

        if not user:
            mock_get_user.assert_called_once()
        is_authorized.assert_called_once_with(
            method=method, entity_type=AvpEntities.DATASET, user=expected_user, entity_id=expected_entity_id
        )
        assert result

    @pytest.mark.parametrize(
        "details, user, expected_user, expected_entity_id",
        [
            (None, None, ANY, None),
            (PoolDetails(name="pool1"), mock, mock, "pool1"),
        ],
    )
    @patch.object(AwsAuthManager, "avp_facade")
    @patch.object(AwsAuthManager, "get_user")
    def test_is_authorized_pool(
        self,
        mock_get_user,
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

        if not user:
            mock_get_user.assert_called_once()
        is_authorized.assert_called_once_with(
            method=method, entity_type=AvpEntities.POOL, user=expected_user, entity_id=expected_entity_id
        )
        assert result

    @pytest.mark.parametrize(
        "details, user, expected_user, expected_entity_id",
        [
            (None, None, ANY, None),
            (VariableDetails(key="var1"), mock, mock, "var1"),
        ],
    )
    @patch.object(AwsAuthManager, "avp_facade")
    @patch.object(AwsAuthManager, "get_user")
    def test_is_authorized_variable(
        self,
        mock_get_user,
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

        if not user:
            mock_get_user.assert_called_once()
        is_authorized.assert_called_once_with(
            method=method, entity_type=AvpEntities.VARIABLE, user=expected_user, entity_id=expected_entity_id
        )
        assert result

    @pytest.mark.parametrize(
        "access_view, user, expected_user",
        [
            (AccessView.CLUSTER_ACTIVITY, None, ANY),
            (AccessView.PLUGINS, mock, mock),
        ],
    )
    @patch.object(AwsAuthManager, "avp_facade")
    @patch.object(AwsAuthManager, "get_user")
    def test_is_authorized_view(
        self, mock_get_user, mock_avp_facade, access_view, user, expected_user, auth_manager
    ):
        is_authorized = Mock(return_value=True)
        mock_avp_facade.is_authorized = is_authorized

        result = auth_manager.is_authorized_view(access_view=access_view, user=user)

        if not user:
            mock_get_user.assert_called_once()
        is_authorized.assert_called_once_with(
            method="GET", entity_type=AvpEntities.VIEW, user=expected_user, entity_id=access_view.value
        )
        assert result

    @patch.object(AwsAuthManager, "avp_facade")
    @patch.object(AwsAuthManager, "get_user")
    def test_batch_is_authorized_connection(
        self,
        mock_get_user,
        mock_avp_facade,
        auth_manager,
    ):
        batch_is_authorized = Mock(return_value=True)
        mock_avp_facade.batch_is_authorized = batch_is_authorized

        result = auth_manager.batch_is_authorized_connection(
            requests=[{"method": "GET"}, {"method": "GET", "details": ConnectionDetails(conn_id="conn_id")}]
        )

        mock_get_user.assert_called_once()
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
    @patch.object(AwsAuthManager, "get_user")
    def test_batch_is_authorized_dag(
        self,
        mock_get_user,
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
            ]
        )

        mock_get_user.assert_called_once()
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
    @patch.object(AwsAuthManager, "get_user")
    def test_batch_is_authorized_pool(
        self,
        mock_get_user,
        mock_avp_facade,
        auth_manager,
    ):
        batch_is_authorized = Mock(return_value=True)
        mock_avp_facade.batch_is_authorized = batch_is_authorized

        result = auth_manager.batch_is_authorized_pool(
            requests=[{"method": "GET"}, {"method": "GET", "details": PoolDetails(name="pool1")}]
        )

        mock_get_user.assert_called_once()
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
    @patch.object(AwsAuthManager, "get_user")
    def test_batch_is_authorized_variable(
        self,
        mock_get_user,
        mock_avp_facade,
        auth_manager,
    ):
        batch_is_authorized = Mock(return_value=True)
        mock_avp_facade.batch_is_authorized = batch_is_authorized

        result = auth_manager.batch_is_authorized_variable(
            requests=[{"method": "GET"}, {"method": "GET", "details": VariableDetails(key="var1")}]
        )

        mock_get_user.assert_called_once()
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
                    "action": {"actionType": "Airflow::Action", "actionId": "Connection.GET"},
                    "resource": {"entityType": "Airflow::Connection", "entityId": "*"},
                },
                "decision": "DENY",
            },
            {
                "request": {
                    "principal": {"entityType": "Airflow::User", "entityId": "test_user_id"},
                    "action": {"actionType": "Airflow::Action", "actionId": "Variable.GET"},
                    "resource": {"entityType": "Airflow::Variable", "entityId": "*"},
                },
                "decision": "ALLOW",
            },
            {
                "request": {
                    "principal": {"entityType": "Airflow::User", "entityId": "test_user_id"},
                    "action": {"actionType": "Airflow::Action", "actionId": "Dataset.GET"},
                    "resource": {"entityType": "Airflow::Dataset", "entityId": "*"},
                },
                "decision": "DENY",
            },
            {
                "request": {
                    "principal": {"entityType": "Airflow::User", "entityId": "test_user_id"},
                    "action": {"actionType": "Airflow::Action", "actionId": "View.GET"},
                    "resource": {"entityType": "Airflow::View", "entityId": "CLUSTER_ACTIVITY"},
                },
                "decision": "DENY",
            },
            {
                "request": {
                    "principal": {"entityType": "Airflow::User", "entityId": "test_user_id"},
                    "action": {"actionType": "Airflow::Action", "actionId": "Dag.GET"},
                    "resource": {"entityType": "Airflow::Dag", "entityId": "*"},
                    "context": {
                        "contextMap": {
                            "dag_entity": {
                                "string": "AUDIT_LOG",
                            }
                        }
                    },
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
                MenuItem("Category2", childs=[MenuItem(RESOURCE_DATASET)]),
                MenuItem(RESOURCE_CLUSTER_ACTIVITY),
                MenuItem(RESOURCE_AUDIT_LOG),
            ]
        )

        auth_manager.avp_facade.get_batch_is_authorized_results.assert_called_once_with(
            requests=[
                {
                    "method": "GET",
                    "entity_type": AvpEntities.CONNECTION,
                },
                {
                    "method": "GET",
                    "entity_type": AvpEntities.VARIABLE,
                },
                {
                    "method": "GET",
                    "entity_type": AvpEntities.DATASET,
                },
                {
                    "method": "GET",
                    "entity_type": AvpEntities.VIEW,
                    "entity_id": AccessView.CLUSTER_ACTIVITY.value,
                },
                {
                    "method": "GET",
                    "entity_type": AvpEntities.DAG,
                    "context": {
                        "dag_entity": {
                            "string": DagAccessEntity.AUDIT_LOG.value,
                        },
                    },
                },
            ],
            user=test_user,
        )
        assert len(result) == 2
        assert result[0].name == "Category1"
        assert len(result[0].childs) == 1
        assert result[0].childs[0].name == RESOURCE_VARIABLE
        assert result[1].name == RESOURCE_AUDIT_LOG

    @patch.object(AwsAuthManager, "get_user")
    def test_filter_permitted_menu_items_logged_out(self, mock_get_user, auth_manager):
        mock_get_user.return_value = None
        result = auth_manager.filter_permitted_menu_items(
            [
                MenuItem(RESOURCE_AUDIT_LOG),
            ]
        )

        assert result == []

    @patch.object(AwsAuthManager, "get_user")
    def test_filter_permitted_menu_items_wrong_menu_item(self, mock_get_user, auth_manager, test_user):
        mock_get_user.return_value = test_user
        with pytest.raises(AirflowException, match="Unknown resource name"):
            auth_manager.filter_permitted_menu_items(
                [
                    MenuItem("Test"),
                ]
            )

    @patch("airflow.providers.amazon.aws.auth_manager.aws_auth_manager.url_for")
    def test_get_url_login(self, mock_url_for, auth_manager):
        auth_manager.get_url_login()
        mock_url_for.assert_called_once_with("AwsAuthManagerAuthenticationViews.login")

    @patch("airflow.providers.amazon.aws.auth_manager.aws_auth_manager.url_for")
    def test_get_url_logout(self, mock_url_for, auth_manager):
        auth_manager.get_url_logout()
        mock_url_for.assert_called_once_with("AwsAuthManagerAuthenticationViews.logout")

    @pytest.mark.db_test
    def test_security_manager_return_default_security_manager(self, auth_manager_with_appbuilder):
        assert isinstance(auth_manager_with_appbuilder.security_manager, AwsSecurityManagerOverride)

    def test_get_cli_commands_return_cli_commands(self, auth_manager):
        assert len(auth_manager.get_cli_commands()) > 0
