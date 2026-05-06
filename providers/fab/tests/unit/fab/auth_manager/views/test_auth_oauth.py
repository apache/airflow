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
from flask_appbuilder.security.views import AuthOAuthView

from airflow.providers.fab.auth_manager.views.auth_oauth import CustomAuthOAuthView


class TestCustomAuthOAuthView:
    """Test CustomAuthOAuthView."""

    def test_oauth_authorized_keeps_callback_route_exposed(self):
        """Test oauth callback route is exposed on the custom view."""
        assert hasattr(CustomAuthOAuthView.oauth_authorized, "_urls")
        assert any(
            route == "/oauth-authorized/<provider>"
            for route, _methods in CustomAuthOAuthView.oauth_authorized._urls
        )

    @pytest.mark.parametrize("backend", ["database", "securecookie"])
    @mock.patch("airflow.providers.fab.auth_manager.views.auth_oauth.conf")
    def test_oauth_authorized_marks_session_modified(self, mock_conf, backend):
        """Test that oauth_authorized marks session as modified regardless of backend."""
        mock_conf.get.return_value = backend
        mock_session = mock.MagicMock()
        view = CustomAuthOAuthView()

        with (
            mock.patch("airflow.providers.fab.auth_manager.views.auth_oauth.session", new=mock_session),
            mock.patch.object(AuthOAuthView, "oauth_authorized", return_value=mock.Mock()) as mock_parent,
        ):
            view.oauth_authorized("test_provider")

            mock_parent.assert_called_once_with("test_provider")
            assert mock_session.modified is True
            mock_conf.get.assert_called_once_with("fab", "SESSION_BACKEND", fallback="securecookie")

    @mock.patch("airflow.providers.fab.auth_manager.views.auth_oauth.conf")
    def test_oauth_authorized_returns_parent_response(self, mock_conf):
        """Test that oauth_authorized returns the response from parent method."""
        mock_conf.get.return_value = "database"
        view = CustomAuthOAuthView()
        mock_response = mock.Mock()

        with (
            mock.patch("airflow.providers.fab.auth_manager.views.auth_oauth.session", new=mock.MagicMock()),
            mock.patch.object(AuthOAuthView, "oauth_authorized", return_value=mock_response),
        ):
            result = view.oauth_authorized("google")
            assert result == mock_response
