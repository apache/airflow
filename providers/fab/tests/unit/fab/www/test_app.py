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
    @mock.patch("airflow.providers.fab.www.app.get_auth_manager", autospec=True)
    def test_sessions_checked_wherever_fab_login_sessions_are_read(
        self,
        mock_get_auth_manager,
        mock_init_session_token_generation,
        auth_manager_cls,
        enable_plugins,
        called,
    ):
        """The plugins app reads the FAB login session too, with the ``securecookie`` backend."""
        mock_get_auth_manager.return_value = mock.Mock(spec=auth_manager_cls)
        flask_app = create_app(enable_plugins=enable_plugins)

        if called:
            mock_init_session_token_generation.assert_called_once_with(flask_app)
        else:
            mock_init_session_token_generation.assert_not_called()
