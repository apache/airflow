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

from airflow.providers.fab.auth_manager.views.auth_oauth import CustomAuthOAuthView


class TestCustomAuthOAuthView:
    """Test CustomAuthOAuthView."""

    @mock.patch("airflow.providers.fab.auth_manager.views.auth_oauth.conf")
    @mock.patch("airflow.providers.fab.auth_manager.views.auth_oauth.session")
    def test_oauth_authorized_marks_session_modified_database_backend(self, mock_session, mock_conf):
        """Test that oauth_authorized marks session as modified for database backend."""
        # Setup
        mock_conf.get.return_value = "database"
        view = CustomAuthOAuthView()

        # Mock parent's oauth_authorized to return a mock response
        with mock.patch.object(
            view.__class__.__bases__[0], "oauth_authorized", return_value=mock.Mock()
        ) as mock_parent:
            # Execute
            view.oauth_authorized("test_provider")

            # Verify parent was called
            mock_parent.assert_called_once_with("test_provider")

            # Verify session.modified was set to True
            assert mock_session.modified is True

            # Verify conf.get was called to check backend type
            mock_conf.get.assert_called_once_with("fab", "SESSION_BACKEND", fallback="securecookie")

    @mock.patch("airflow.providers.fab.auth_manager.views.auth_oauth.conf")
    @mock.patch("airflow.providers.fab.auth_manager.views.auth_oauth.session")
    def test_oauth_authorized_marks_session_modified_securecookie_backend(self, mock_session, mock_conf):
        """Test that oauth_authorized marks session as modified for securecookie backend."""
        # Setup
        mock_conf.get.return_value = "securecookie"
        view = CustomAuthOAuthView()

        # Mock parent's oauth_authorized to return a mock response
        with mock.patch.object(
            view.__class__.__bases__[0], "oauth_authorized", return_value=mock.Mock()
        ) as mock_parent:
            # Execute
            view.oauth_authorized("azure")

            # Verify parent was called
            mock_parent.assert_called_once_with("azure")

            # Verify session.modified was set to True
            assert mock_session.modified is True

    @mock.patch("airflow.providers.fab.auth_manager.views.auth_oauth.conf")
    @mock.patch("airflow.providers.fab.auth_manager.views.auth_oauth.session")
    def test_oauth_authorized_returns_parent_response(self, mock_session, mock_conf):
        """Test that oauth_authorized returns the response from parent method."""
        # Setup
        mock_conf.get.return_value = "database"
        view = CustomAuthOAuthView()
        mock_response = mock.Mock()

        # Mock parent's oauth_authorized to return mock response
        with mock.patch.object(
            view.__class__.__bases__[0], "oauth_authorized", return_value=mock_response
        ):
            # Execute
            result = view.oauth_authorized("google")

            # Verify the response is returned unchanged
            assert result == mock_response
