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

from airflow.providers.fab.auth_manager.api.auth.backend.session import requires_authentication
from airflow.www import app as application

from tests_common.test_utils.compat import AIRFLOW_V_2_9_PLUS

pytestmark = [
    pytest.mark.skipif(not AIRFLOW_V_2_9_PLUS, reason="Tests for Airflow 2.9.0+ only"),
]


@pytest.fixture
def app():
    return application.create_app(testing=True)


mock_call = Mock()


@requires_authentication
def function_decorated():
    mock_call()


@pytest.mark.db_test
class TestSessionAuth:
    def setup_method(self) -> None:
        mock_call.reset_mock()

    @patch("airflow.providers.fab.auth_manager.api.auth.backend.session.get_auth_manager")
    def test_requires_authentication_when_not_authenticated(self, mock_get_auth_manager, app):
        auth_manager = Mock()
        auth_manager.is_logged_in.return_value = False
        mock_get_auth_manager.return_value = auth_manager
        with app.test_request_context() as mock_context:
            mock_context.request.authorization = None
            result = function_decorated()

        assert type(result) is Response
        assert result.status_code == 401

    @patch("airflow.providers.fab.auth_manager.api.auth.backend.session.get_auth_manager")
    def test_requires_authentication_when_authenticated(self, mock_get_auth_manager, app):
        auth_manager = Mock()
        auth_manager.is_logged_in.return_value = True
        mock_get_auth_manager.return_value = auth_manager
        with app.test_request_context() as mock_context:
            mock_context.request.authorization = None
            function_decorated()

        mock_call.assert_called_once()
