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

from unittest import mock

import pytest

from airflow import settings
from airflow.api_fastapi.auth.managers.base_auth_manager import BaseAuthManager
from airflow.providers.fab.auth_manager.fab_auth_manager import FabAuthManager
from airflow.providers.fab.www.app import create_app
from airflow.providers.fab.www.session import (
    AirflowDatabaseSessionInterface,
    AirflowSecureCookieSessionInterface,
)

from tests_common.test_utils.config import conf_vars

FAB_AUTH_MANAGER = "airflow.providers.fab.auth_manager.fab_auth_manager.FabAuthManager"


@pytest.mark.db_test
class TestCreateApp:
    def test_flask_app_binds_to_the_metadata_engine(self):
        """A second engine would bypass a ``create_metadata_engine`` override in local settings."""
        with conf_vars(
            {("core", "auth_manager"): "airflow.providers.fab.auth_manager.fab_auth_manager.FabAuthManager"}
        ):
            flask_app = create_app(enable_plugins=False)

        with flask_app.app_context():
            db = flask_app.extensions["sqlalchemy"]
            assert db.engine is settings.engine

    @pytest.mark.parametrize(
        ("auth_manager_cls", "enable_plugins", "called"),
        [
            pytest.param(FabAuthManager, False, True, id="fab-app"),
            pytest.param(FabAuthManager, True, True, id="plugins-app"),
            pytest.param(BaseAuthManager, True, False, id="plugins-app-without-fab-auth-manager"),
        ],
    )
    @conf_vars({("core", "auth_manager"): FAB_AUTH_MANAGER})
    @mock.patch("airflow.providers.fab.www.app.init_session_token_generation", autospec=True)
    @mock.patch("airflow.providers.fab.www.app.init_session_max_lifetime", autospec=True)
    @mock.patch("airflow.providers.fab.www.app.init_airflow_session_interface", autospec=True)
    @mock.patch("airflow.providers.fab.www.app.get_auth_manager", autospec=True)
    def test_fab_session_settings_apply_wherever_fab_login_sessions_are_used(
        self,
        mock_get_auth_manager,
        mock_init_airflow_session_interface,
        mock_init_session_max_lifetime,
        mock_init_session_token_generation,
        auth_manager_cls,
        enable_plugins,
        called,
    ):
        """The plugins app serves the FAB login views too."""
        mock_get_auth_manager.return_value = mock.Mock(spec=auth_manager_cls)
        flask_app = create_app(enable_plugins=enable_plugins)

        if called:
            mock_init_airflow_session_interface.assert_called_once_with(flask_app, mock.ANY)
            mock_init_session_max_lifetime.assert_called_once_with(flask_app)
            mock_init_session_token_generation.assert_called_once_with(flask_app)
        else:
            mock_init_airflow_session_interface.assert_not_called()
            mock_init_session_max_lifetime.assert_not_called()
            mock_init_session_token_generation.assert_not_called()

    @pytest.mark.parametrize(
        ("session_backend", "expected_interface"),
        [
            pytest.param("database", AirflowDatabaseSessionInterface, id="database"),
            pytest.param("securecookie", AirflowSecureCookieSessionInterface, id="securecookie"),
        ],
    )
    def test_plugins_app_uses_the_configured_session_backend(self, session_backend, expected_interface):
        """Both apps run in the API server, so the plugins app is created next to the FAB app."""
        with conf_vars(
            {("core", "auth_manager"): FAB_AUTH_MANAGER, ("fab", "session_backend"): session_backend}
        ):
            fab_app = create_app(enable_plugins=False)
            plugins_app = create_app(enable_plugins=True)

        assert isinstance(fab_app.session_interface, expected_interface)
        assert isinstance(plugins_app.session_interface, expected_interface)
