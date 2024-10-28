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

import json
from unittest import mock
from unittest.mock import Mock

import pytest

from airflow.security.permissions import (
    ACTION_CAN_READ,
    RESOURCE_ADMIN_MENU,
    RESOURCE_BROWSE_MENU,
    RESOURCE_DOCS_MENU,
    RESOURCE_VARIABLE,
)
from airflow.www import app as application

pytestmark = pytest.mark.skip_if_database_isolation_mode


@pytest.fixture
def app():
    return application.create_app(testing=True)


@pytest.fixture
def app_builder(app):
    return app.appbuilder


@pytest.fixture
def security_manager(app_builder):
    return app_builder.sm


@pytest.mark.db_test
class TestAirflowSecurityManagerV2:
    @pytest.mark.parametrize(
        "action_name, resource_name, auth_manager_methods, expected",
        [
            (ACTION_CAN_READ, RESOURCE_VARIABLE, {"is_authorized_variable": True}, True),
            (
                ACTION_CAN_READ,
                RESOURCE_VARIABLE,
                {"is_authorized_variable": False},
                False,
            ),
            (ACTION_CAN_READ, RESOURCE_DOCS_MENU, {"is_authorized_view": True}, True),
            (ACTION_CAN_READ, RESOURCE_DOCS_MENU, {"is_authorized_view": False}, False),
            (
                ACTION_CAN_READ,
                RESOURCE_ADMIN_MENU,
                {
                    "is_authorized_view": False,
                    "is_authorized_variable": False,
                    "is_authorized_connection": True,
                    "is_authorized_dag": False,
                    "is_authorized_configuration": False,
                    "is_authorized_pool": False,
                },
                True,
            ),
            (
                ACTION_CAN_READ,
                RESOURCE_ADMIN_MENU,
                {
                    "is_authorized_view": False,
                    "is_authorized_variable": False,
                    "is_authorized_connection": False,
                    "is_authorized_dag": False,
                    "is_authorized_configuration": False,
                    "is_authorized_pool": False,
                },
                False,
            ),
            (
                ACTION_CAN_READ,
                RESOURCE_BROWSE_MENU,
                {
                    "is_authorized_dag": False,
                    "is_authorized_view": False,
                },
                False,
            ),
            (
                ACTION_CAN_READ,
                RESOURCE_BROWSE_MENU,
                {
                    "is_authorized_dag": False,
                    "is_authorized_view": True,
                },
                True,
            ),
            (
                "can_not_a_default_action",
                "custom_resource",
                {"is_authorized_custom_view": False},
                False,
            ),
            (
                "can_not_a_default_action",
                "custom_resource",
                {"is_authorized_custom_view": True},
                True,
            ),
        ],
    )
    @mock.patch("airflow.www.security_manager.get_auth_manager")
    def test_has_access(
        self,
        mock_get_auth_manager,
        security_manager,
        action_name,
        resource_name,
        auth_manager_methods,
        expected,
    ):
        user = Mock()
        auth_manager = Mock()
        for method_name, method_return in auth_manager_methods.items():
            method = Mock(return_value=method_return)
            getattr(auth_manager, method_name).side_effect = method
            mock_get_auth_manager.return_value = auth_manager
        result = security_manager.has_access(action_name, resource_name, user=user)
        assert result == expected
        if len(auth_manager_methods) > 1 and not expected:
            for method_name in auth_manager_methods:
                getattr(auth_manager, method_name).assert_called()

    @mock.patch("airflow.utils.session.create_session")
    @mock.patch("airflow.www.security_manager.get_auth_manager")
    def test_manager_does_not_create_extra_db_sessions(
        self,
        _,
        mock_create_session,
        security_manager,
    ):
        """
        Test that the Security Manager doesn't create extra DB sessions and
        instead uses the session already available through the appbuilder
        object that is attached to it.
        """
        with mock.patch.object(
            security_manager.appbuilder, "session"
        ) as mock_appbuilder_session:
            action_name = ACTION_CAN_READ
            resource_pk = "PK"
            user = Mock()
            for func in security_manager._auth_manager_is_authorized_map.values():
                try:
                    func(action_name, resource_pk, user)
                except json.JSONDecodeError:
                    # The resource-retrieving function expects a "composite"
                    # PK as a JSON string. Provide a mocked one.
                    func(action_name, "[1, 1, 1, 1]", user)
                mock_create_session.assert_not_called()

        # The Security Manager's `appbuilder.session` object should have been
        # put to use by many of the functions tested above.
        assert len(mock_appbuilder_session.method_calls) > 0
