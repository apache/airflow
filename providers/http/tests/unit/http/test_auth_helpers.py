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

import pytest

from unittest.mock import Mock, patch

from requests.auth import AuthBase
from aiohttp import BasicAuth

from airflow.sdk.definitions.connection import Connection
from airflow.providers.http.auth_helpers import deserialize_auth_type, resolve_auth_type, serialize_auth_type

class TestAuthType(AuthBase):
    pass

class TestHttpHelpers:

    @pytest.fixture
    def mock_connection(self):
        connection = Mock(spec=Connection)
        connection.login = "login"
        connection.password = "password"
        return connection

    def test_serialize_auth_type(self):
        assert serialize_auth_type(TestAuthType) == "tests.unit.http.test_helpers.TestAuthType"

    def test_serialize_none_auth_type(self):
        assert serialize_auth_type(None) is None

    def test_deserialize_auth_type(self):
        assert deserialize_auth_type("tests.unit.http.test_helpers.TestAuthType") is TestAuthType

    def test_deserialize_none_auth_type(self):
        assert deserialize_auth_type(None) is None

    def test_resolve_auth_type(self):
        assert resolve_auth_type(TestAuthType, "foo") is TestAuthType

    @patch("airflow.providers.http.helpers.BaseHook.get_connection")
    def test_resolve_auth_type_no_auth(self, mock_get_connection, mock_connection):
        mock_connection.login = False
        mock_connection.password = False
        mock_get_connection.return_value = mock_connection
        assert resolve_auth_type(None, "mock_conn") is None

    @patch("airflow.providers.http.helpers.BaseHook.get_connection")
    def test_resolve_auth_type_with_basic_auth(self, mock_get_connection, mock_connection):
        mock_get_connection.return_value = mock_connection
        assert resolve_auth_type(None, "mock_conn") is BasicAuth

    @patch("airflow.providers.http.helpers.BaseHook.get_connection")
    def test_resolve_auth_type_with_login(self, mock_get_connection, mock_connection):
        mock_connection.password = False
        mock_get_connection.return_value = mock_connection
        assert resolve_auth_type(None, "mock_conn") is BasicAuth

    @patch("airflow.providers.http.helpers.BaseHook.get_connection")
    def test_resolve_auth_type_with_password(self, mock_get_connection, mock_connection):
        mock_connection.login = False
        mock_get_connection.return_value = mock_connection
        assert resolve_auth_type(None, "mock_conn") is BasicAuth
