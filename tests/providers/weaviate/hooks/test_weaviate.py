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

from contextlib import ExitStack
from unittest.mock import MagicMock, Mock, call, patch

import pandas as pd
import pytest
import requests
import weaviate
from weaviate import ObjectAlreadyExistsException

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
    with patch("airflow.providers.weaviate.hooks.weaviate.AuthApiKey") as m:
        yield m


@pytest.fixture
def mock_auth_bearer_token():
    with patch("airflow.providers.weaviate.hooks.weaviate.AuthBearerToken") as m:
        yield m


@pytest.fixture
def mock_auth_client_credentials():
    with patch("airflow.providers.weaviate.hooks.weaviate.AuthClientCredentials") as m:
        yield m


@pytest.fixture
def mock_auth_client_password():
    with patch("airflow.providers.weaviate.hooks.weaviate.AuthClientPassword") as m:
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

    @patch("airflow.providers.weaviate.hooks.weaviate.WeaviateClient")
    def test_get_conn_with_api_key_in_extra(self, mock_client, mock_auth_api_key):
        hook = WeaviateHook(conn_id=self.weaviate_api_key1)
        hook.get_conn()
        mock_auth_api_key.assert_called_once_with(self.api_key)
        mock_client.assert_called_once_with(
            url=self.host, auth_client_secret=mock_auth_api_key(api_key=self.api_key), additional_headers={}
        )

    @patch("airflow.providers.weaviate.hooks.weaviate.WeaviateClient")
    def test_get_conn_with_token_in_extra(self, mock_client, mock_auth_api_key):
        # when token is passed in extra
        hook = WeaviateHook(conn_id=self.weaviate_api_key2)
        hook.get_conn()
        mock_auth_api_key.assert_called_once_with(self.api_key)
        mock_client.assert_called_once_with(
            url=self.host, auth_client_secret=mock_auth_api_key(api_key=self.api_key), additional_headers={}
        )

    @patch("airflow.providers.weaviate.hooks.weaviate.WeaviateClient")
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

    @patch("airflow.providers.weaviate.hooks.weaviate.WeaviateClient")
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

    @patch("airflow.providers.weaviate.hooks.weaviate.WeaviateClient")
    def test_get_conn_with_client_password_in_extra(self, mock_client, mock_auth_client_password):
        hook = WeaviateHook(conn_id=self.client_password)
        hook.get_conn()
        mock_auth_client_password.assert_called_once_with(username="login", password="password", scope=None)
        mock_client.assert_called_once_with(
            url=self.host,
            auth_client_secret=mock_auth_client_password(username="login", password="password", scope=None),
            additional_headers={},
        )

    @patch("airflow.providers.weaviate.hooks.weaviate.generate_uuid5")
    def test_create_object(self, mock_gen_uuid, weaviate_hook):
        """
        Test the create_object method of WeaviateHook.
        """
        mock_client = MagicMock()
        weaviate_hook.get_conn = MagicMock(return_value=mock_client)
        return_value = weaviate_hook.create_object({"name": "Test"}, "TestClass")
        mock_gen_uuid.assert_called_once()
        mock_client.data_object.create.assert_called_once_with(
            {"name": "Test"}, "TestClass", uuid=mock_gen_uuid.return_value
        )
        assert return_value

    def test_create_object_already_exists_return_none(self, weaviate_hook):
        """
        Test the create_object method of WeaviateHook.
        """
        mock_client = MagicMock()
        weaviate_hook.get_conn = MagicMock(return_value=mock_client)
        mock_client.data_object.create.side_effect = ObjectAlreadyExistsException
        return_value = weaviate_hook.create_object({"name": "Test"}, "TestClass")
        assert return_value is None

    def test_get_object(self, weaviate_hook):
        """
        Test the get_object method of WeaviateHook.
        """
        mock_client = MagicMock()
        weaviate_hook.get_conn = MagicMock(return_value=mock_client)
        weaviate_hook.get_object(class_name="TestClass", uuid="uuid")
        mock_client.data_object.get.assert_called_once_with(class_name="TestClass", uuid="uuid")

    def test_get_of_get_or_create_object(self, weaviate_hook):
        """
        Test the get part of get_or_create_object method of WeaviateHook.
        """
        mock_client = MagicMock()
        weaviate_hook.get_conn = MagicMock(return_value=mock_client)
        weaviate_hook.get_or_create_object(data_object={"name": "Test"}, class_name="TestClass")
        mock_client.data_object.get.assert_called_once_with(
            class_name="TestClass",
            consistency_level=None,
            tenant=None,
        )

    @patch("airflow.providers.weaviate.hooks.weaviate.generate_uuid5")
    def test_create_of_get_or_create_object(self, mock_gen_uuid, weaviate_hook):
        """
        Test the create part of get_or_create_object method of WeaviateHook.
        """
        mock_client = MagicMock()
        weaviate_hook.get_conn = MagicMock(return_value=mock_client)
        weaviate_hook.get_object = MagicMock(return_value=None)
        mock_create_object = MagicMock()
        weaviate_hook.create_object = mock_create_object
        weaviate_hook.get_or_create_object(data_object={"name": "Test"}, class_name="TestClass")
        mock_create_object.assert_called_once_with(
            {"name": "Test"},
            "TestClass",
            uuid=mock_gen_uuid.return_value,
            consistency_level=None,
            tenant=None,
            vector=None,
        )

    def test_create_of_get_or_create_object_raises_valueerror(self, weaviate_hook):
        """
        Test that if data_object is None or class_name is None, ValueError is raised.
        """
        mock_client = MagicMock()
        weaviate_hook.get_conn = MagicMock(return_value=mock_client)
        weaviate_hook.get_object = MagicMock(return_value=None)
        mock_create_object = MagicMock()
        weaviate_hook.create_object = mock_create_object
        with pytest.raises(ValueError):
            weaviate_hook.get_or_create_object(data_object=None, class_name="TestClass")
        with pytest.raises(ValueError):
            weaviate_hook.get_or_create_object(data_object={"name": "Test"}, class_name=None)

    def test_get_all_objects(self, weaviate_hook):
        """
        Test the get_all_objects method of WeaviateHook.
        """
        mock_client = MagicMock()
        weaviate_hook.get_conn = MagicMock(return_value=mock_client)
        objects = [
            {"deprecations": None, "objects": [{"name": "Test1", "id": 2}, {"name": "Test2", "id": 3}]},
            {"deprecations": None, "objects": []},
        ]
        mock_get_object = MagicMock()
        weaviate_hook.get_object = mock_get_object
        mock_get_object.side_effect = objects

        return_value = weaviate_hook.get_all_objects(class_name="TestClass")
        assert weaviate_hook.get_object.call_args_list == [
            call(after=None, class_name="TestClass"),
            call(after=3, class_name="TestClass"),
        ]
        assert return_value == [{"name": "Test1", "id": 2}, {"name": "Test2", "id": 3}]

    def test_get_all_objects_returns_dataframe(self, weaviate_hook):
        """
        Test the get_all_objects method of WeaviateHook can return a dataframe.
        """
        mock_client = MagicMock()
        weaviate_hook.get_conn = MagicMock(return_value=mock_client)
        objects = [
            {"deprecations": None, "objects": [{"name": "Test1", "id": 2}, {"name": "Test2", "id": 3}]},
            {"deprecations": None, "objects": []},
        ]
        mock_get_object = MagicMock()
        weaviate_hook.get_object = mock_get_object
        mock_get_object.side_effect = objects

        return_value = weaviate_hook.get_all_objects(class_name="TestClass", as_dataframe=True)
        assert weaviate_hook.get_object.call_args_list == [
            call(after=None, class_name="TestClass"),
            call(after=3, class_name="TestClass"),
        ]
        import pandas

        assert isinstance(return_value, pandas.DataFrame)

    def test_delete_object(self, weaviate_hook):
        """
        Test the delete_object method of WeaviateHook.
        """
        mock_client = MagicMock()
        weaviate_hook.get_conn = MagicMock(return_value=mock_client)
        weaviate_hook.delete_object(uuid="uuid", class_name="TestClass")
        mock_client.data_object.delete.assert_called_once_with("uuid", class_name="TestClass")

    def test_update_object(self, weaviate_hook):
        """
        Test the update_object method of WeaviateHook.
        """
        mock_client = MagicMock()
        weaviate_hook.get_conn = MagicMock(return_value=mock_client)
        weaviate_hook.update_object(
            uuid="uuid", class_name="TestClass", data_object={"name": "Test"}, tenant="2d"
        )
        mock_client.data_object.update.assert_called_once_with(
            {"name": "Test"}, "TestClass", "uuid", tenant="2d"
        )

    def test_validate_object(self, weaviate_hook):
        """
        Test the validate_object method of WeaviateHook.
        """
        mock_client = MagicMock()
        weaviate_hook.get_conn = MagicMock(return_value=mock_client)
        weaviate_hook.validate_object(class_name="TestClass", data_object={"name": "Test"}, uuid="2d")
        mock_client.data_object.validate.assert_called_once_with({"name": "Test"}, "TestClass", uuid="2d")

    def test_replace_object(self, weaviate_hook):
        """
        Test the replace_object method of WeaviateHook.
        """
        mock_client = MagicMock()
        weaviate_hook.get_conn = MagicMock(return_value=mock_client)
        weaviate_hook.replace_object(
            uuid="uuid", class_name="TestClass", data_object={"name": "Test"}, tenant="2d"
        )
        mock_client.data_object.replace.assert_called_once_with(
            {"name": "Test"}, "TestClass", "uuid", tenant="2d"
        )

    def test_object_exists(self, weaviate_hook):
        """
        Test the object_exists method of WeaviateHook.
        """
        mock_client = MagicMock()
        weaviate_hook.get_conn = MagicMock(return_value=mock_client)
        weaviate_hook.object_exists(class_name="TestClass", uuid="2d")
        mock_client.data_object.exists.assert_called_once_with("2d", class_name="TestClass")


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


@pytest.mark.parametrize(
    argnames=["data", "expected_length"],
    argvalues=[
        ([{"name": "John"}, {"name": "Jane"}], 2),
        (pd.DataFrame.from_dict({"name": ["John", "Jane"]}), 2),
    ],
    ids=("data as list of dicts", "data as dataframe"),
)
def test_batch_data(data, expected_length, weaviate_hook):
    """
    Test the batch_data method of WeaviateHook.
    """
    # Mock the Weaviate Client
    mock_client = MagicMock()
    weaviate_hook.get_conn = MagicMock(return_value=mock_client)

    # Define test data
    test_class_name = "TestClass"

    # Test the batch_data method
    weaviate_hook.batch_data(test_class_name, data)

    # Assert that the batch_data method was called with the correct arguments
    mock_client.batch.configure.assert_called_once()
    mock_batch_context = mock_client.batch.__enter__.return_value
    assert mock_batch_context.add_data_object.call_count == expected_length


@patch("airflow.providers.weaviate.hooks.weaviate.WeaviateHook.get_conn")
def test_batch_data_retry(get_conn, weaviate_hook):
    """Test to ensure retrying working as expected"""
    data = [{"name": "chandler"}, {"name": "joey"}, {"name": "ross"}]
    response = requests.Response()
    response.status_code = 429
    error = requests.exceptions.HTTPError()
    error.response = response
    side_effect = [None, error, None, error, None]
    get_conn.return_value.batch.__enter__.return_value.add_data_object.side_effect = side_effect
    weaviate_hook.batch_data("TestClass", data)
    assert get_conn.return_value.batch.__enter__.return_value.add_data_object.call_count == len(side_effect)


@pytest.mark.parametrize(
    argnames=["get_schema_value", "existing", "expected_value"],
    argvalues=[
        ({"classes": [{"class": "A"}, {"class": "B"}]}, "ignore", [{"class": "C"}]),
        ({"classes": [{"class": "A"}, {"class": "B"}]}, "replace", [{"class": "B"}, {"class": "C"}]),
        ({"classes": [{"class": "A"}, {"class": "B"}]}, "fail", {}),
        ({"classes": [{"class": "A"}, {"class": "B"}]}, "invalid_option", {}),
    ],
    ids=["ignore", "replace", "fail", "invalid_option"],
)
@patch("airflow.providers.weaviate.hooks.weaviate.WeaviateHook.delete_classes")
@patch("airflow.providers.weaviate.hooks.weaviate.WeaviateHook.create_schema")
@patch("airflow.providers.weaviate.hooks.weaviate.WeaviateHook.get_schema")
def test_upsert_schema_scenarios(
    get_schema, create_schema, delete_classes, get_schema_value, existing, expected_value, weaviate_hook
):
    schema_json = {
        "B": {"class": "B"},
        "C": {"class": "C"},
    }
    with ExitStack() as stack:
        delete_classes.return_value = None
        if existing in ["fail", "invalid_option"]:
            stack.enter_context(pytest.raises(ValueError))
        get_schema.return_value = get_schema_value
        weaviate_hook.create_or_replace_classes(schema_json=schema_json, existing=existing)
        create_schema.assert_called_once_with({"classes": expected_value})
        if existing == "replace":
            delete_classes.assert_called_once_with(class_names=["B"])


@patch("builtins.open")
@patch("json.load")
@patch("airflow.providers.weaviate.hooks.weaviate.WeaviateHook.create_schema")
@patch("airflow.providers.weaviate.hooks.weaviate.WeaviateHook.get_schema")
def test_upsert_schema_json_file_param(get_schema, create_schema, load, open, weaviate_hook):
    """Test if schema_json is path to a json file"""
    get_schema.return_value = {"classes": [{"class": "A"}, {"class": "B"}]}
    load.return_value = {
        "B": {"class": "B"},
        "C": {"class": "C"},
    }
    weaviate_hook.create_or_replace_classes(schema_json="/tmp/some_temp_file.json", existing="ignore")
    create_schema.assert_called_once_with({"classes": [{"class": "C"}]})


@patch("airflow.providers.weaviate.hooks.weaviate.WeaviateHook.get_client")
def test_delete_schema(get_client, weaviate_hook):
    class_names = ["class_a", "class_b"]
    get_client.return_value.schema.delete_class.side_effect = [
        weaviate.UnexpectedStatusCodeException("something failed", requests.Response()),
        None,
    ]
    error_list = weaviate_hook.delete_classes(class_names, if_error="continue")
    assert error_list == ["class_a"]

    get_client.return_value.schema.delete_class.side_effect = weaviate.UnexpectedStatusCodeException(
        "something failed", requests.Response()
    )
    with pytest.raises(weaviate.UnexpectedStatusCodeException):
        weaviate_hook.delete_classes("class_a", if_error="stop")


@pytest.mark.parametrize(
    argnames=["classes_to_test", "expected_result"],
    argvalues=[
        (
            [
                {
                    "class": "Author",
                    "description": "Authors info",
                    "properties": [
                        {
                            "name": "last_name",
                            "description": "Last name of the author",
                            "dataType": ["text"],
                        },
                    ],
                },
            ],
            True,
        ),
        (
            [
                {
                    "class": "Author",
                    "description": "Authors info",
                    "properties": [
                        {
                            "name": "last_name",
                            "description": "Last name of the author",
                            "dataType": ["text"],
                        },
                    ],
                },
            ],
            True,
        ),
        (
            [
                {
                    "class": "Author",
                    "description": "Authors info",
                    "properties": [
                        {
                            "name": "invalid_property",
                            "description": "Last name of the author",
                            "dataType": ["text"],
                        }
                    ],
                },
            ],
            False,
        ),
        (
            [
                {
                    "class": "invalid_class",
                    "description": "Authors info",
                    "properties": [
                        {
                            "name": "last_name",
                            "description": "Last name of the author",
                            "dataType": ["text"],
                        },
                    ],
                },
            ],
            False,
        ),
        (
            [
                {
                    "class": "Author",
                    "description": "Authors info",
                    "properties": [
                        {
                            "name": "last_name",
                            "description": "Last name of the author",
                            "dataType": ["text"],
                        },
                        {
                            "name": "name",
                            "description": "Name of the author",
                            "dataType": ["text"],
                            "extra_key": "some_value",
                        },
                    ],
                },
            ],
            True,
        ),
    ],
    ids=(
        "property_level_check",
        "class_level_check",
        "invalid_property",
        "invalid_class",
        "swapped_properties",
    ),
)
@patch("airflow.providers.weaviate.hooks.weaviate.WeaviateHook.get_schema")
def test_contains_schema(get_schema, classes_to_test, expected_result, weaviate_hook):
    get_schema.return_value = {
        "classes": [
            {
                "class": "Author",
                "description": "Authors info",
                "properties": [
                    {
                        "name": "name",
                        "description": "Name of the author",
                        "dataType": ["text"],
                        "extra_key": "some_value",
                    },
                    {
                        "name": "last_name",
                        "description": "Last name of the author",
                        "dataType": ["text"],
                        "extra_key": "some_value",
                    },
                ],
            },
            {
                "class": "Article",
                "description": "An article written by an Author",
                "properties": [
                    {
                        "name": "name",
                        "description": "Name of the author",
                        "dataType": ["text"],
                        "extra_key": "some_value",
                    },
                    {
                        "name": "last_name",
                        "description": "Last name of the author",
                        "dataType": ["text"],
                        "extra_key": "some_value",
                    },
                ],
            },
        ]
    }
    assert weaviate_hook.check_subset_of_schema(classes_to_test) == expected_result
