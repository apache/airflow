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

from unittest.mock import Mock, patch

import pytest

from airflow.security.permissions import ACTION_CAN_READ, RESOURCE_DAG

from tests_common.test_utils.compat import ignore_provider_compatibility_error

permissions = [(ACTION_CAN_READ, RESOURCE_DAG)]

with ignore_provider_compatibility_error("2.9.0+", __file__):
    from airflow.api_connexion.exceptions import PermissionDenied
    from airflow.providers.fab.auth_manager.decorators.auth import _has_access_fab, _requires_access_fab

from airflow.www import app as application  # noqa: E402


@pytest.fixture(scope="module")
def app():
    return application.create_app(testing=True)


@pytest.fixture
def mock_sm():
    return Mock()


@pytest.fixture
def mock_appbuilder(mock_sm):
    appbuilder = Mock()
    appbuilder.sm = mock_sm
    return appbuilder


@pytest.fixture
def mock_auth_manager(mock_sm):
    auth_manager = Mock()
    auth_manager.security_manager = mock_sm
    return auth_manager


@pytest.fixture
def mock_app(mock_appbuilder):
    app = Mock()
    app.appbuilder = mock_appbuilder
    return app


mock_call = Mock()

permissions = [(ACTION_CAN_READ, RESOURCE_DAG)]


@_has_access_fab(permissions)
def decorated_has_access_fab():
    mock_call()


@pytest.mark.db_test
class TestFabAuthManagerDecorators:
    def setup_method(self) -> None:
        mock_call.reset_mock()

    @patch("airflow.providers.fab.auth_manager.decorators.auth.get_auth_manager")
    def test_requires_access_fab_sync_resource_permissions(
        self, mock_get_auth_manager, mock_sm, mock_appbuilder, mock_auth_manager, app
    ):
        app.appbuilder = mock_appbuilder
        mock_appbuilder.update_perms = True
        mock_get_auth_manager.return_value = mock_auth_manager

        with app.test_request_context():

            @_requires_access_fab()
            def decorated_requires_access_fab():
                pass

        mock_sm.sync_resource_permissions.assert_called_once()

    @patch("airflow.providers.fab.auth_manager.decorators.auth.check_authentication")
    @patch("airflow.providers.fab.auth_manager.decorators.auth.get_auth_manager")
    def test_requires_access_fab_access_denied(
        self, mock_get_auth_manager, mock_check_authentication, mock_sm, mock_auth_manager, app
    ):
        mock_sm.check_authorization.return_value = False
        mock_get_auth_manager.return_value = mock_auth_manager

        with app.test_request_context():

            @_requires_access_fab(permissions)
            def decorated_requires_access_fab():
                pass

            with pytest.raises(PermissionDenied):
                decorated_requires_access_fab()

        mock_check_authentication.assert_called_once()
        mock_sm.check_authorization.assert_called_once()
        mock_call.assert_not_called()

    @patch("airflow.providers.fab.auth_manager.decorators.auth.check_authentication")
    @patch("airflow.providers.fab.auth_manager.decorators.auth.get_auth_manager")
    def test_requires_access_fab_access_granted(
        self, mock_get_auth_manager, mock_check_authentication, mock_sm, mock_auth_manager, app
    ):
        mock_sm.check_authorization.return_value = True
        mock_get_auth_manager.return_value = mock_auth_manager

        with app.test_request_context():

            @_requires_access_fab(permissions)
            def decorated_requires_access_fab():
                mock_call()

            decorated_requires_access_fab()

        mock_check_authentication.assert_called_once()
        mock_sm.check_authorization.assert_called_once()
        mock_call.assert_called_once()

    @patch("airflow.providers.fab.auth_manager.decorators.auth._has_access")
    def test_has_access_fab_with_no_dags(self, mock_has_access, mock_sm, mock_appbuilder, app):
        app.appbuilder = mock_appbuilder
        with app.test_request_context():
            decorated_has_access_fab()

        mock_sm.check_authorization.assert_called_once_with(permissions, None)
        mock_has_access.assert_called_once()

    @patch("airflow.providers.fab.auth_manager.decorators.auth.render_template")
    @patch("airflow.providers.fab.auth_manager.decorators.auth._has_access")
    def test_has_access_fab_with_multiple_dags_render_error(
        self, mock_has_access, mock_render_template, mock_sm, mock_appbuilder, app
    ):
        app.appbuilder = mock_appbuilder
        with app.test_request_context() as mock_context:
            mock_context.request.args = {"dag_id": "dag1"}
            mock_context.request.form = {"dag_id": "dag2"}
            decorated_has_access_fab()

        mock_sm.check_authorization.assert_not_called()
        mock_has_access.assert_not_called()
        mock_render_template.assert_called_once()
