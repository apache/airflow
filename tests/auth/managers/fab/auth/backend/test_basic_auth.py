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
from flask import Response
from flask_appbuilder.const import AUTH_LDAP

from airflow.api.auth.backend.basic_auth import requires_authentication
from airflow.www import app as application


@pytest.fixture
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
def mock_app(mock_appbuilder):
    app = Mock()
    app.appbuilder = mock_appbuilder
    return app


@pytest.fixture
def mock_authorization():
    authorization = Mock()
    authorization.username = "username"
    authorization.password = "password"
    return authorization


mock_call = Mock()


@requires_authentication
def function_decorated():
    mock_call()


@pytest.mark.db_test
class TestBasicAuth:
    def setup_method(self) -> None:
        mock_call.reset_mock()

    def test_requires_authentication_with_no_header(self, app):
        with app.test_request_context() as mock_context:
            mock_context.request.authorization = None
            result = function_decorated()

        assert type(result) is Response
        assert result.status_code == 401

    @patch("airflow.auth.managers.fab.api.auth.backend.basic_auth.get_airflow_app")
    @patch("airflow.auth.managers.fab.api.auth.backend.basic_auth.login_user")
    def test_requires_authentication_with_ldap(
        self, mock_login_user, mock_get_airflow_app, app, mock_app, mock_sm, mock_authorization
    ):
        mock_get_airflow_app.return_value = mock_app
        mock_sm.auth_type = AUTH_LDAP
        user = Mock()
        mock_sm.auth_user_ldap.return_value = user

        with app.test_request_context() as mock_context:
            mock_context.request.authorization = mock_authorization
            function_decorated()

        mock_sm.auth_user_ldap.assert_called_once_with(
            mock_authorization.username, mock_authorization.password
        )
        mock_login_user.assert_called_once_with(user, remember=False)
        mock_call.assert_called_once()

    @patch("airflow.auth.managers.fab.api.auth.backend.basic_auth.get_airflow_app")
    @patch("airflow.auth.managers.fab.api.auth.backend.basic_auth.login_user")
    def test_requires_authentication_with_db(
        self, mock_login_user, mock_get_airflow_app, app, mock_app, mock_sm, mock_authorization
    ):
        mock_get_airflow_app.return_value = mock_app
        user = Mock()
        mock_sm.auth_user_db.return_value = user

        with app.test_request_context() as mock_context:
            mock_context.request.authorization = mock_authorization
            function_decorated()

        mock_sm.auth_user_db.assert_called_once_with(mock_authorization.username, mock_authorization.password)
        mock_login_user.assert_called_once_with(user, remember=False)
        mock_call.assert_called_once()
