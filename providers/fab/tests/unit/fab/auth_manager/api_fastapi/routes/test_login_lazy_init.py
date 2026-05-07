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

from unittest.mock import MagicMock, patch

import pytest
from fastapi import HTTPException

from airflow.providers.fab.auth_manager.api_fastapi.routes.login import _get_flask_app
from airflow.providers.fab.auth_manager.fab_auth_manager import FabAuthManager


class TestGetFlaskAppLazyInit:
    @patch("airflow.providers.fab.auth_manager.api_fastapi.routes.login.get_auth_manager")
    @patch("airflow.providers.fab.www.app.create_app")
    def test_get_flask_app_lazy_init(self, mock_create_app, mock_get_auth_manager):
        """Test that _get_flask_app lazily initializes the flask app if it's None."""
        mock_fab_auth_manager = MagicMock(spec=FabAuthManager)
        mock_fab_auth_manager.flask_app = None
        mock_get_auth_manager.return_value = mock_fab_auth_manager

        mock_flask_app = MagicMock()
        mock_create_app.return_value = mock_flask_app

        # First call
        result = _get_flask_app()

        assert result == mock_flask_app
        mock_create_app.assert_called_once_with(enable_plugins=False)
        assert mock_fab_auth_manager.flask_app == mock_flask_app

        # Reset mock and set flask_app (simulate it being stored)
        mock_create_app.reset_mock()

        # Second call
        result = _get_flask_app()
        assert result == mock_flask_app
        mock_create_app.assert_not_called()

    @patch("airflow.providers.fab.auth_manager.api_fastapi.routes.login.get_auth_manager")
    @patch("airflow.providers.fab.www.app.create_app")
    def test_get_flask_app_lazy_init_error(self, mock_create_app, mock_get_auth_manager):
        """Test that _get_flask_app raises HTTPException if create_app fails."""
        mock_fab_auth_manager = MagicMock(spec=FabAuthManager)
        mock_fab_auth_manager.flask_app = None
        mock_get_auth_manager.return_value = mock_fab_auth_manager

        mock_create_app.side_effect = Exception("Config error")

        with pytest.raises(HTTPException) as exc:
            _get_flask_app()

        assert exc.value.status_code == 500
        assert "Failed to initialize Flask app context" in exc.value.detail
