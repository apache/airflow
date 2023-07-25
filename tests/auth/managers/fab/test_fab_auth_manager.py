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
from unittest.mock import Mock

import pytest

from airflow import AirflowException
from airflow.auth.managers.fab.fab_auth_manager import FabAuthManager
from airflow.auth.managers.fab.security_manager_override import FabAirflowSecurityManagerOverride
from airflow.www.fab_security.sqla.models import User
from airflow.www.security import ApplessAirflowSecurityManager


@pytest.fixture
def auth_manager():
    auth_manager = FabAuthManager()
    auth_manager.security_manager = ApplessAirflowSecurityManager()
    return auth_manager


class TestFabAuthManager:
    @pytest.mark.parametrize(
        "first_name,last_name,expected",
        [
            ("First", "Last", "First Last"),
            ("First", None, "First"),
            (None, "Last", "Last"),
        ],
    )
    @mock.patch("flask_login.utils._get_user")
    def test_get_user_name(self, mock_current_user, first_name, last_name, expected, auth_manager):
        user = User()
        user.first_name = first_name
        user.last_name = last_name
        mock_current_user.return_value = user

        assert auth_manager.get_user_name() == expected

    @mock.patch("flask_login.utils._get_user")
    def test_is_logged_in(self, mock_current_user, auth_manager):
        user = Mock()
        user.is_anonymous.return_value = True
        mock_current_user.return_value = user

        assert auth_manager.is_logged_in() is False

    def test_get_security_manager_override_class_return_fab_security_manager_override(self, auth_manager):
        assert auth_manager.get_security_manager_override_class() is FabAirflowSecurityManagerOverride

    def test_get_url_login_when_auth_view_not_defined(self, auth_manager):
        with pytest.raises(AirflowException, match="`auth_view` not defined in the security manager."):
            auth_manager.get_url_login()

    @mock.patch("airflow.auth.managers.fab.fab_auth_manager.url_for")
    def test_get_url_login(self, mock_url_for, auth_manager):
        auth_manager.security_manager.auth_view = Mock()
        auth_manager.security_manager.auth_view.endpoint = "test_endpoint"
        auth_manager.get_url_login()
        mock_url_for.assert_called_once_with("test_endpoint.login")

    @mock.patch("airflow.auth.managers.fab.fab_auth_manager.url_for")
    def test_get_url_login_with_next(self, mock_url_for, auth_manager):
        auth_manager.security_manager.auth_view = Mock()
        auth_manager.security_manager.auth_view.endpoint = "test_endpoint"
        auth_manager.get_url_login(next_url="next_url")
        mock_url_for.assert_called_once_with("test_endpoint.login", next="next_url")
