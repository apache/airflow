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
from unittest.mock import MagicMock, Mock, patch

import pytest

from airflow.models import Connection
from airflow.providers.weaviate.hooks.weaviate import WeaviateHook

TEST_CONN_ID = "test_weaviate_conn"


@pytest.fixture
def weaviate_hook():
    """
    Fixture to create a WeaviateHook instance for testing.
    """
    mock_conn = Mock()

    # Patch the WeaviateHook get_connection method to return the mock connection
    with patch.object(WeaviateHook, "get_connection", return_value=mock_conn):
        hook = WeaviateHook(conn_id=TEST_CONN_ID)
    return hook


@pytest.fixture
def mock_auth_api_key():
    with mock.patch("airflow.providers.weaviate.hooks.weaviate.AuthApiKey") as m:
        yield m


@pytest.fixture
def mock_auth_bearer_token():
    with mock.patch("airflow.providers.weaviate.hooks.weaviate.AuthBearerToken") as m:
        yield m


@pytest.fixture
def mock_auth_client_credentials():
    with mock.patch("airflow.providers.weaviate.hooks.weaviate.AuthClientCredentials") as m:
        yield m


@pytest.fixture
def mock_auth_client_password():
    with mock.patch("airflow.providers.weaviate.hooks.weaviate.AuthClientPassword") as m:
        yield m


class TestWeaviateHook:
    """
    Test the WeaviateHook Hook.
    """

    @pytest.fixture(autouse=True)
    def setup_method(self, monkeypatch):
        """Set up the test method."""
        self.weaviate_api_key1 = "weaviate_api_key1"
        self.weaviate_api_key2 = "weaviate_api_key2"
        self.api_key = "api_key"
        self.weaviate_client_credentials = "weaviate_client_credentials"
        self.client_secret = "client_secret"
        self.scope = "scope1 scope2"
        self.client_password = "client_password"
        self.client_bearer_token = "client_bearer_token"
        self.host = "http://localhost:8080"
        conns = (
            Connection(
                conn_id=self.weaviate_api_key1,
                host=self.host,
                conn_type="weaviate",
                extra={"api_key": self.api_key},
            ),
            Connection(
                conn_id=self.weaviate_api_key2,
                host=self.host,
                conn_type="weaviate",
                extra={"token": self.api_key},
            ),
            Connection(
                conn_id=self.weaviate_client_credentials,
                host=self.host,
                conn_type="weaviate",
                extra={"client_secret": self.client_secret, "scope": self.scope},
            ),
            Connection(
                conn_id=self.client_password,
                host=self.host,
                conn_type="weaviate",
                login="login",
                password="password",
            ),
            Connection(
                conn_id=self.client_bearer_token,
                host=self.host,
                conn_type="weaviate",
                extra={
                    "access_token": self.client_bearer_token,
                    "expires_in": 30,
                    "refresh_token": "refresh_token",
                },
            ),
        )
        for conn in conns:
            monkeypatch.setenv(f"AIRFLOW_CONN_{conn.conn_id.upper()}", conn.get_uri())

    @mock.patch("airflow.providers.weaviate.hooks.weaviate.WeaviateClient")
    def test_get_conn_with_api_key_in_extra(self, mock_client, mock_auth_api_key):
        hook = WeaviateHook(conn_id=self.weaviate_api_key1)
        hook.get_conn()
        mock_auth_api_key.assert_called_once_with(self.api_key)
        mock_client.assert_called_once_with(
            url=self.host, auth_client_secret=mock_auth_api_key(api_key=self.api_key), additional_headers={}
        )

    @mock.patch("airflow.providers.weaviate.hooks.weaviate.WeaviateClient")
    def test_get_conn_with_token_in_extra(self, mock_client, mock_auth_api_key):
        # when token is passed in extra
        hook = WeaviateHook(conn_id=self.weaviate_api_key2)
        hook.get_conn()
        mock_auth_api_key.assert_called_once_with(self.api_key)
        mock_client.assert_called_once_with(
            url=self.host, auth_client_secret=mock_auth_api_key(api_key=self.api_key), additional_headers={}
        )

    @mock.patch("airflow.providers.weaviate.hooks.weaviate.WeaviateClient")
    def test_get_conn_with_access_token_in_extra(self, mock_client, mock_auth_bearer_token):
        hook = WeaviateHook(conn_id=self.client_bearer_token)
        hook.get_conn()
        mock_auth_bearer_token.assert_called_once_with(
            self.client_bearer_token, expires_in=30, refresh_token="refresh_token"
        )
        mock_client.assert_called_once_with(
            url=self.host,
            auth_client_secret=mock_auth_bearer_token(
                access_token=self.client_bearer_token, expires_in=30, refresh_token="refresh_token"
            ),
            additional_headers={},
        )

    @mock.patch("airflow.providers.weaviate.hooks.weaviate.WeaviateClient")
    def test_get_conn_with_client_secret_in_extra(self, mock_client, mock_auth_client_credentials):
        hook = WeaviateHook(conn_id=self.weaviate_client_credentials)
        hook.get_conn()
        mock_auth_client_credentials.assert_called_once_with(
            client_secret=self.client_secret, scope=self.scope
        )
        mock_client.assert_called_once_with(
            url=self.host,
            auth_client_secret=mock_auth_client_credentials(api_key=self.client_secret, scope=self.scope),
            additional_headers={},
        )

    @mock.patch("airflow.providers.weaviate.hooks.weaviate.WeaviateClient")
    def test_get_conn_with_client_password_in_extra(self, mock_client, mock_auth_client_password):
        hook = WeaviateHook(conn_id=self.client_password)
        hook.get_conn()
        mock_auth_client_password.assert_called_once_with(username="login", password="password", scope=None)
        mock_client.assert_called_once_with(
            url=self.host,
            auth_client_secret=mock_auth_client_password(username="login", password="password", scope=None),
            additional_headers={},
        )


def test_create_class(weaviate_hook):
    """
    Test the create_class method of WeaviateHook.
    """
    # Mock the Weaviate Client
    mock_client = MagicMock()
    weaviate_hook.get_conn = MagicMock(return_value=mock_client)

    # Define test class JSON
    test_class_json = {
        "class": "TestClass",
        "description": "Test class for unit testing",
    }

    # Test the create_class method
    weaviate_hook.create_class(test_class_json)

    # Assert that the create_class method was called with the correct arguments
    mock_client.schema.create_class.assert_called_once_with(test_class_json)


def test_create_schema(weaviate_hook):
    """
    Test the create_schema method of WeaviateHook.
    """
    # Mock the Weaviate Client
    mock_client = MagicMock()
    weaviate_hook.get_conn = MagicMock(return_value=mock_client)

    # Define test schema JSON
    test_schema_json = {
        "classes": [
            {
                "class": "TestClass",
                "description": "Test class for unit testing",
            }
        ]
    }

    # Test the create_schema method
    weaviate_hook.create_schema(test_schema_json)

    # Assert that the create_schema method was called with the correct arguments
    mock_client.schema.create.assert_called_once_with(test_schema_json)


def test_batch_data(weaviate_hook):
    """
    Test the batch_data method of WeaviateHook.
    """
    # Mock the Weaviate Client
    mock_client = MagicMock()
    weaviate_hook.get_conn = MagicMock(return_value=mock_client)

    # Define test data
    test_class_name = "TestClass"
    test_data = [{"name": "John"}, {"name": "Jane"}]

    # Test the batch_data method
    weaviate_hook.batch_data(test_class_name, test_data)

    # Assert that the batch_data method was called with the correct arguments
    mock_client.batch.configure.assert_called_once()
    mock_batch_context = mock_client.batch.__enter__.return_value
    assert mock_batch_context.add_data_object.call_count == len(test_data)
