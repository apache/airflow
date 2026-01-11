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
from unittest.mock import MagicMock, Mock

import pandas as pd
import pytest
from packaging.version import Version

weaviate = pytest.importorskip("weaviate")
from weaviate import ObjectAlreadyExistsException  # noqa: E402

from airflow.models import Connection  # noqa: E402
from airflow.providers.weaviate.hooks.weaviate import WeaviateHook  # noqa: E402

TEST_CONN_ID = "test_weaviate_conn"


# Weaviate 4.10.0 uses HTTPX internal, so we need to mock httpx rather than requests.
USE_HTTPX = Version(weaviate.__version__) >= Version("4.10.0")

if USE_HTTPX:
    import httpx
else:
    import requests

NON_RETRIABLE_UNEXPECTED_STATUS_CODE = 501


@pytest.fixture
def weaviate_hook():
    """
    Fixture to create a WeaviateHook instance for testing.
    """
    mock_conn = Mock()

    # Patch the WeaviateHook get_connection method to return the mock connection
    with mock.patch.object(WeaviateHook, "get_connection", return_value=mock_conn):
        hook = WeaviateHook(conn_id=TEST_CONN_ID)
    return hook


@pytest.fixture
def mock_auth_api_key():
    with mock.patch("airflow.providers.weaviate.hooks.weaviate.Auth.api_key") as m:
        yield m


@pytest.fixture
def mock_auth_bearer_token():
    with mock.patch("airflow.providers.weaviate.hooks.weaviate.Auth.bearer_token") as m:
        yield m


@pytest.fixture
def mock_auth_client_credentials():
    with mock.patch("airflow.providers.weaviate.hooks.weaviate.Auth.client_credentials") as m:
        yield m


@pytest.fixture
def mock_auth_client_password():
    with mock.patch("airflow.providers.weaviate.hooks.weaviate.Auth.client_password") as m:
        yield m


class MockFetchObjectReturn:
    def __init__(self, *, objects):
        self.objects = objects


class MockObject:
    def __init__(self, *, properties: dict, uuid: str) -> None:
        self.properties = properties
        self.uuid = uuid
        self.collection = "collection"
        self.metadata = "metadata"
        self.references = "references"
        self.vector = "vector"

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, MockObject):
            return False
        return self.properties == other.properties and self.uuid == other.uuid

    def __hash__(self):
        return hash((self.properties, self.uuid))


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
        self.host = "localhost"
        self.port = 8000
        self.grpc_host = "localhost"
        self.grpc_port = 50051
        conns = (
            Connection(
                conn_id=self.weaviate_api_key1,
                host=self.host,
                port=self.port,
                conn_type="weaviate",
                extra={"api_key": self.api_key, "grpc_host": self.grpc_host, "grpc_port": self.grpc_port},
            ),
            Connection(
                conn_id=self.weaviate_api_key2,
                host=self.host,
                port=self.port,
                conn_type="weaviate",
                extra={"token": self.api_key, "grpc_host": self.grpc_host, "grpc_port": self.grpc_port},
            ),
            Connection(
                conn_id=self.weaviate_client_credentials,
                host=self.host,
                port=self.port,
                conn_type="weaviate",
                extra={
                    "client_secret": self.client_secret,
                    "scope": self.scope,
                    "grpc_host": self.grpc_host,
                    "grpc_port": self.grpc_port,
                },
            ),
            Connection(
                conn_id=self.client_password,
                host=self.host,
                port=self.port,
                conn_type="weaviate",
                login="login",
                password="password",
                extra={"grpc_host": self.grpc_host, "grpc_port": self.grpc_port},
            ),
            Connection(
                conn_id=self.client_bearer_token,
                host=self.host,
                port=self.port,
                conn_type="weaviate",
                extra={
                    "access_token": self.client_bearer_token,
                    "expires_in": 30,
                    "refresh_token": "refresh_token",
                    "grpc_host": self.grpc_host,
                    "grpc_port": self.grpc_port,
                },
            ),
        )
        for conn in conns:
            monkeypatch.setenv(f"AIRFLOW_CONN_{conn.conn_id.upper()}", conn.get_uri())

    @mock.patch("airflow.providers.weaviate.hooks.weaviate.weaviate.connect_to_custom")
    def test_get_conn_with_api_key_in_extra(self, mock_connect_to_custom, mock_auth_api_key):
        hook = WeaviateHook(conn_id=self.weaviate_api_key1)
        hook.get_conn()
        mock_auth_api_key.assert_called_once_with(api_key=self.api_key)
        mock_connect_to_custom.assert_called_once_with(
            http_host=self.host,
            http_port=8000,
            http_secure=False,
            grpc_host="localhost",
            grpc_port=50051,
            grpc_secure=False,
            auth_credentials=mock_auth_api_key(api_key=self.api_key),
            headers={},
        )

    @mock.patch("airflow.providers.weaviate.hooks.weaviate.weaviate.connect_to_custom")
    def test_get_conn_with_token_in_extra(self, mock_connect_to_custom, mock_auth_api_key):
        # when token is passed in extra
        hook = WeaviateHook(conn_id=self.weaviate_api_key2)
        hook.get_conn()
        mock_auth_api_key.assert_called_once_with(api_key=self.api_key)
        mock_connect_to_custom.assert_called_once_with(
            http_host=self.host,
            http_port=8000,
            http_secure=False,
            grpc_host="localhost",
            grpc_port=50051,
            grpc_secure=False,
            auth_credentials=mock_auth_api_key(api_key=self.api_key),
            headers={},
        )

    @mock.patch("airflow.providers.weaviate.hooks.weaviate.weaviate.connect_to_custom")
    def test_get_conn_with_access_token_in_extra(self, mock_connect_to_custom, mock_auth_bearer_token):
        hook = WeaviateHook(conn_id=self.client_bearer_token)
        hook.get_conn()
        mock_auth_bearer_token.assert_called_once_with(
            access_token=self.client_bearer_token, expires_in=30, refresh_token="refresh_token"
        )
        mock_connect_to_custom.assert_called_once_with(
            http_host=self.host,
            http_port=8000,
            http_secure=False,
            grpc_host="localhost",
            grpc_port=50051,
            grpc_secure=False,
            auth_credentials=mock_auth_bearer_token(
                access_token=self.client_bearer_token, expires_in=30, refresh_token="refresh_token"
            ),
            headers={},
        )

    @mock.patch("airflow.providers.weaviate.hooks.weaviate.weaviate.connect_to_custom")
    def test_get_conn_with_client_secret_in_extra(self, mock_connect_to_custom, mock_auth_client_credentials):
        hook = WeaviateHook(conn_id=self.weaviate_client_credentials)
        hook.get_conn()
        mock_auth_client_credentials.assert_called_once_with(
            client_secret=self.client_secret, scope=self.scope
        )
        mock_connect_to_custom.assert_called_once_with(
            http_host=self.host,
            http_port=8000,
            http_secure=False,
            grpc_host="localhost",
            grpc_port=50051,
            grpc_secure=False,
            auth_credentials=mock_auth_client_credentials(api_key=self.client_secret, scope=self.scope),
            headers={},
        )

    @mock.patch("airflow.providers.weaviate.hooks.weaviate.weaviate.connect_to_custom")
    def test_get_conn_with_client_password_in_extra(self, mock_connect_to_custom, mock_auth_client_password):
        hook = WeaviateHook(conn_id=self.client_password)
        hook.get_conn()
        mock_auth_client_password.assert_called_once_with(username="login", password="password", scope=None)
        mock_connect_to_custom.assert_called_once_with(
            http_host=self.host,
            http_port=8000,
            http_secure=False,
            grpc_host="localhost",
            grpc_port=50051,
            grpc_secure=False,
            auth_credentials=mock_auth_client_password(username="login", password="password", scope=None),
            headers={},
        )

    @mock.patch("airflow.providers.weaviate.hooks.weaviate.generate_uuid5")
    def test_create_object(self, mock_gen_uuid, weaviate_hook):
        """
        Test the create_object method of WeaviateHook.
        """
        mock_collection = MagicMock()
        weaviate_hook.get_collection = MagicMock(return_value=mock_collection)

        return_value = weaviate_hook.create_object({"name": "Test"}, "TestCollection")

        mock_gen_uuid.assert_called_once()
        mock_collection.data.insert.assert_called_once_with(
            properties={"name": "Test"}, uuid=mock_gen_uuid.return_value
        )
        assert return_value

    def test_create_object_already_exists_return_none(self, weaviate_hook):
        """
        Test the create_object method of WeaviateHook.
        """
        mock_collection = MagicMock()
        weaviate_hook.get_collection = MagicMock(return_value=mock_collection)
        mock_collection.data.insert.side_effect = ObjectAlreadyExistsException

        return_value = weaviate_hook.create_object({"name": "Test"}, "TestCollection")

        assert return_value is None

    def test_get_object(self, weaviate_hook):
        """
        Test the get_object method of WeaviateHook.
        """
        mock_collection = MagicMock()
        weaviate_hook.get_collection = MagicMock(return_value=mock_collection)

        weaviate_hook.get_object(collection_name="TestCollection", uuid="uuid")

        mock_collection.query.fetch_objects.assert_called_once_with(uuid="uuid")

    def test_get_of_get_or_create_object(self, weaviate_hook):
        """
        Test the get part of get_or_create_object method of WeaviateHook.
        """
        mock_collection = MagicMock()
        weaviate_hook.get_collection = MagicMock(return_value=mock_collection)

        weaviate_hook.get_or_create_object(data_object={"name": "Test"}, collection_name="TestCollection")

        mock_collection.query.fetch_objects.assert_called_once_with()

    @mock.patch("airflow.providers.weaviate.hooks.weaviate.generate_uuid5")
    def test_create_of_get_or_create_object(self, mock_gen_uuid, weaviate_hook):
        """
        Test the create part of get_or_create_object method of WeaviateHook.
        """
        mock_collection = MagicMock()
        weaviate_hook.get_collection = MagicMock(return_value=mock_collection)
        weaviate_hook.get_object = MagicMock(return_value=None)
        mock_create_object = MagicMock()
        weaviate_hook.create_object = mock_create_object

        weaviate_hook.get_or_create_object(data_object={"name": "Test"}, collection_name="TestCollection")

        mock_create_object.assert_called_once_with(
            data_object={"name": "Test"},
            collection_name="TestCollection",
            uuid=mock_gen_uuid.return_value,
            vector=None,
        )

    def test_create_of_get_or_create_object_raises_valueerror(self, weaviate_hook):
        """
        Test that if data_object is None or collection_name is None, ValueError is raised.
        """
        mock_collection = MagicMock()
        weaviate_hook.get_collection = MagicMock(return_value=mock_collection)
        weaviate_hook.get_object = MagicMock(return_value=None)
        mock_create_object = MagicMock()

        weaviate_hook.create_object = mock_create_object

        with pytest.raises(
            ValueError, match="data_object and collection are required to create a new object"
        ):
            weaviate_hook.get_or_create_object(data_object=None, collection_name="TestCollection")
        with pytest.raises(
            ValueError, match="data_object and collection are required to create a new object"
        ):
            weaviate_hook.get_or_create_object(data_object={"name": "Test"}, collection_name=None)

    def test_get_all_objects(self, weaviate_hook):
        """
        Test the get_all_objects method of WeaviateHook.
        """
        mock_collection = MagicMock()
        weaviate_hook.get_collection = MagicMock(return_value=mock_collection)
        objects = [
            MockFetchObjectReturn(
                objects=[
                    MockObject(properties={"name": "Test1", "id": 2}, uuid="u1"),
                    MockObject(properties={"name": "Test2", "id": 3}, uuid="u2"),
                ]
            ),
            MockFetchObjectReturn(objects=[]),
        ]
        mock_get_object = MagicMock()
        weaviate_hook.get_object = mock_get_object
        mock_get_object.side_effect = objects

        return_value = weaviate_hook.get_all_objects(collection_name="TestCollection")

        assert weaviate_hook.get_object.call_args_list == [
            mock.call(after=None, collection_name="TestCollection"),
            mock.call(after="u2", collection_name="TestCollection"),
        ]
        assert return_value == [
            MockObject(properties={"name": "Test1", "id": 2}, uuid="u1"),
            MockObject(properties={"name": "Test2", "id": 3}, uuid="u2"),
        ]

    def test_get_all_objects_returns_dataframe(self, weaviate_hook):
        """
        Test the get_all_objects method of WeaviateHook can return a dataframe.
        """
        mock_collection = MagicMock()
        weaviate_hook.get_collection = MagicMock(return_value=mock_collection)
        objects = [
            MockFetchObjectReturn(
                objects=[
                    MockObject(properties={"name": "Test1", "id": 2}, uuid="u1"),
                    MockObject(properties={"name": "Test2", "id": 3}, uuid="u2"),
                ]
            ),
            MockFetchObjectReturn(objects=[]),
        ]
        mock_get_object = MagicMock()
        weaviate_hook.get_object = mock_get_object
        mock_get_object.side_effect = objects

        return_value = weaviate_hook.get_all_objects(collection_name="TestCollection", as_dataframe=True)

        assert weaviate_hook.get_object.call_args_list == [
            mock.call(after=None, collection_name="TestCollection"),
            mock.call(after="u2", collection_name="TestCollection"),
        ]
        import pandas

        assert isinstance(return_value, pandas.DataFrame)

    def test_delete_object(self, weaviate_hook):
        """
        Test the delete_object method of WeaviateHook.
        """
        mock_collection = MagicMock()
        weaviate_hook.get_collection = MagicMock(return_value=mock_collection)

        weaviate_hook.delete_object(collection_name="TestCollection", uuid="uuid")

        mock_collection.data.delete_by_id.assert_called_once_with(uuid="uuid")

    def test_update_object(self, weaviate_hook):
        """
        Test the update_object method of WeaviateHook.
        """
        mock_collection = MagicMock()
        weaviate_hook.get_collection = MagicMock(return_value=mock_collection)

        weaviate_hook.update_object(
            uuid="uuid", collection_name="TestCollection", properties={"name": "Test"}
        )

        mock_collection.data.update.assert_called_once_with(properties={"name": "Test"}, uuid="uuid")

    def test_replace_object(self, weaviate_hook):
        """
        Test the replace_object method of WeaviateHook.
        """
        mock_collection = MagicMock()
        weaviate_hook.get_collection = MagicMock(return_value=mock_collection)

        weaviate_hook.replace_object(
            uuid="uuid", collection_name="TestCollection", properties={"name": "Test"}
        )

        mock_collection.data.replace.assert_called_once_with(
            properties={"name": "Test"}, uuid="uuid", references=None
        )

    def test_object_exists(self, weaviate_hook):
        """
        Test the object_exists method of WeaviateHook.
        """
        mock_collection = MagicMock()
        weaviate_hook.get_collection = MagicMock(return_value=mock_collection)

        weaviate_hook.object_exists(collection_name="TestCollection", uuid="2d")

        mock_collection.data.exists.assert_called_once_with(uuid="2d")


def test_create_collection(weaviate_hook):
    """
    Test the create_collection method of WeaviateHook.
    """
    # Mock the Weaviate Client
    mock_client = MagicMock()
    weaviate_hook.get_conn = MagicMock(return_value=mock_client)

    # Test the create_collection method
    weaviate_hook.create_collection("TestCollection", description="Test class for unit testing")

    # Assert that the create_collection method was called with the correct arguments
    mock_client.collections.create.assert_called_once_with(
        name="TestCollection", description="Test class for unit testing"
    )


@pytest.mark.parametrize(
    argnames=("data", "expected_length"),
    argvalues=[
        (
            [
                {
                    "from_uuid": "0fe86eae-45f7-456c-b19f-04fc59e9ce41",
                    "to_uuid": "360b6f5b-ed23-413c-a6e8-cb864a52e712",
                    "from_property": "hasCategory",
                },
                {
                    "from_uuid": "34ccb2e1-1cfc-46e5-94d2-48c335e52c29",
                    "to_uuid": "a775ef49-a8ab-480d-ac85-b70197654072",
                    "from_property": "hasCategory",
                },
            ],
            2,
        ),
        (
            pd.DataFrame.from_dict(
                {
                    "from_uuid": [
                        "0fe86eae-45f7-456c-b19f-04fc59e9ce41",
                        "34ccb2e1-1cfc-46e5-94d2-48c335e52c29",
                    ],
                    "to_uuid": [
                        "360b6f5b-ed23-413c-a6e8-cb864a52e712",
                        "a775ef49-a8ab-480d-ac85-b70197654072",
                    ],
                    "from_property": ["hasCategory", "hasCategory"],
                }
            ),
            2,
        ),
    ],
    ids=("batch create link data as list of dicts", "batch create link data as dataframe"),
)
def test_batch_create_links(data, expected_length, weaviate_hook):
    """
    Test the batch_create_links method of WeaviateHook.
    """
    # Mock the Weaviate Collection
    mock_collection = MagicMock()
    weaviate_hook.get_collection = MagicMock(return_value=mock_collection)

    # Define test data
    test_collection_name = "TestCollection"

    # Test the batch_data method
    weaviate_hook.batch_create_links(test_collection_name, data, to_uuid_col="to_uuid")

    mock_batch_context = mock_collection.batch.dynamic.return_value.__enter__.return_value
    assert mock_batch_context.add_reference.call_count == expected_length


def test_batch_create_links_retry(weaviate_hook):
    """Test to ensure retrying working as expected"""
    # Mock the Weaviate Collection
    mock_collection = MagicMock()
    weaviate_hook.get_collection = MagicMock(return_value=mock_collection)

    data = [
        {
            "from_uuid": "0fe86eae-45f7-456c-b19f-04fc59e9ce41",
            "to": "360b6f5b-ed23-413c-a6e8-cb864a52e712",
            "from_property": "hasCategory",
        },
        {
            "from_uuid": "34ccb2e1-1cfc-46e5-94d2-48c335e52c29",
            "to": "a775ef49-a8ab-480d-ac85-b70197654072",
            "from_property": "hasCategory",
        },
    ]
    if USE_HTTPX:
        response = httpx.Response(status_code=429)
        too_many_requests_error = weaviate.UnexpectedStatusCodeException(
            "Weaviate returned an unexpected status code",
            response=response,
        )
    else:
        response = requests.Response()
        response.status_code = 429
        too_many_requests_error = requests.exceptions.HTTPError(response=response)
    side_effect = [None, too_many_requests_error, too_many_requests_error, None]

    mock_collection.batch.dynamic.return_value.__enter__.return_value.add_reference.side_effect = side_effect

    weaviate_hook.batch_create_links("TestCollection", data)

    assert mock_collection.batch.dynamic.return_value.__enter__.return_value.add_reference.call_count == len(
        side_effect
    )


@pytest.mark.parametrize(
    argnames=("data", "expected_length"),
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
    # Mock the Weaviate Collection
    mock_collection = MagicMock()
    weaviate_hook.get_collection = MagicMock(return_value=mock_collection)

    # Define test data
    test_collection_name = "TestCollection"

    # Test the batch_data method
    weaviate_hook.batch_data(test_collection_name, data)

    mock_batch_context = mock_collection.batch.dynamic.return_value.__enter__.return_value
    assert mock_batch_context.add_object.call_count == expected_length


def test_batch_data_retry(weaviate_hook):
    """Test to ensure retrying working as expected"""
    # Mock the Weaviate Collection
    mock_collection = MagicMock()
    weaviate_hook.get_collection = MagicMock(return_value=mock_collection)

    data = [{"name": "chandler"}, {"name": "joey"}, {"name": "ross"}]
    if USE_HTTPX:
        response = httpx.Response(status_code=429)
        too_many_requests_error = weaviate.UnexpectedStatusCodeException(
            "Weaviate returned an unexpected status code",
            response=response,
        )
    else:
        response = requests.Response()
        response.status_code = 429
        too_many_requests_error = requests.exceptions.HTTPError(response=response)
    side_effect = [None, too_many_requests_error, None, too_many_requests_error, None]

    mock_collection.batch.dynamic.return_value.__enter__.return_value.add_object.side_effect = side_effect

    weaviate_hook.batch_data("TestCollection", data)

    assert mock_collection.batch.dynamic.return_value.__enter__.return_value.add_object.call_count == len(
        side_effect
    )


@mock.patch("airflow.providers.weaviate.hooks.weaviate.WeaviateHook.get_conn")
def test_delete_by_property_retry(get_conn, weaviate_hook):
    from weaviate.classes.query import Filter

    mock_collection = MagicMock()
    weaviate_hook.get_collection = MagicMock(return_value=mock_collection)

    get_conn.return_value.collections.get.return_value = mock_collection

    if USE_HTTPX:
        response = httpx.Response(status_code=429)
        too_many_requests_error = weaviate.UnexpectedStatusCodeException(
            "Weaviate returned an unexpected status code",
            response=response,
        )
    else:
        response = requests.Response()
        response.status_code = 429
        too_many_requests_error = requests.exceptions.HTTPError(response=response)
    side_effect = [too_many_requests_error, too_many_requests_error, None]

    mock_collection.data.delete_many.side_effect = side_effect

    weaviate_hook.delete_by_property(
        collection_names="collection_a",
        filter_criteria=Filter.by_property("name").equal("John"),
        if_error="continue",
    )

    assert mock_collection.data.delete_many.call_count == len(side_effect)


@mock.patch("airflow.providers.weaviate.hooks.weaviate.WeaviateHook.get_collection")
def test_delete_by_property_get_exception(mock_get_collection, weaviate_hook):
    from weaviate.classes.query import Filter
    from weaviate.exceptions import WeaviateDeleteManyError

    collection_names = ["collection_a", "collection_b", "collection_c"]

    mock_collection_b = MagicMock()
    mock_collection_b.data.delete_many.return_value = None

    mock_collection_c = MagicMock()
    mock_collection_c.data.delete_many.return_value = None
    mock_collection_c.data.delete_many.side_effect = WeaviateDeleteManyError(
        "A delete many request to Weaviate fails in any way"
    )

    mock_get_collection.side_effect = [
        weaviate.UnexpectedStatusCodeException(
            "something failed",
            requests.Response()
            if not USE_HTTPX
            else httpx.Response(status_code=NON_RETRIABLE_UNEXPECTED_STATUS_CODE),
        ),
        mock_collection_b,
        mock_collection_c,
    ]

    # Test when if_error='continue' – expect failed collections list
    error_list = weaviate_hook.delete_by_property(
        collection_names=collection_names,
        filter_criteria=Filter.by_property("name").equal("John"),
        if_error="continue",
    )
    assert error_list == ["collection_a", "collection_c"]

    mock_get_collection.reset_mock()
    mock_get_collection.side_effect = weaviate.UnexpectedStatusCodeException(
        "something failed",
        requests.Response()
        if not USE_HTTPX
        else httpx.Response(status_code=NON_RETRIABLE_UNEXPECTED_STATUS_CODE),
    )

    with pytest.raises(weaviate.UnexpectedStatusCodeException):
        weaviate_hook.delete_by_property(
            collection_names="collection_a",
            filter_criteria=Filter.by_property("name").equal("John"),
            if_error="stop",
        )


@mock.patch("airflow.providers.weaviate.hooks.weaviate.WeaviateHook.get_conn")
def test_delete_collections(get_conn, weaviate_hook):
    collection_names = ["collection_a", "collection_b"]
    get_conn.return_value.collections.delete.side_effect = [
        weaviate.UnexpectedStatusCodeException(
            "something failed",
            httpx.Response(status_code=NON_RETRIABLE_UNEXPECTED_STATUS_CODE)
            if USE_HTTPX
            else requests.Response()
            if not USE_HTTPX
            else httpx.Response(status_code=NON_RETRIABLE_UNEXPECTED_STATUS_CODE),
        ),
        None,
    ]
    error_list = weaviate_hook.delete_collections(collection_names, if_error="continue")
    assert error_list == ["collection_a"]

    get_conn.return_value.collections.delete.side_effect = weaviate.UnexpectedStatusCodeException(
        "something failed",
        httpx.Response(status_code=NON_RETRIABLE_UNEXPECTED_STATUS_CODE)
        if USE_HTTPX
        else requests.Response()
        if not USE_HTTPX
        else httpx.Response(status_code=NON_RETRIABLE_UNEXPECTED_STATUS_CODE),
    )
    with pytest.raises(weaviate.UnexpectedStatusCodeException):
        weaviate_hook.delete_collections("class_a", if_error="stop")


@mock.patch("airflow.providers.weaviate.hooks.weaviate.WeaviateHook.get_conn")
def test_http_errors_of_delete_collections(get_conn, weaviate_hook):
    collection_names = ["collection_a", "collection_b"]
    if USE_HTTPX:
        resp = httpx.Response(status_code=429)
        too_many_requests_error = weaviate.UnexpectedStatusCodeException(
            "Weaviate returned an unexpected status code",
            response=resp,
        )
        connection_error = httpx.ConnectError("Connection error")
    else:
        resp = requests.Response()
        resp.status_code = 429
        too_many_requests_error = requests.exceptions.HTTPError(response=resp)
        connection_error = requests.exceptions.ConnectionError
    get_conn.return_value.collections.delete.side_effect = [
        too_many_requests_error,
        None,
        connection_error,
        None,
    ]
    error_list = weaviate_hook.delete_collections(collection_names, if_error="continue")
    assert error_list == []
    assert get_conn.return_value.collections.delete.call_count == 4


@mock.patch("weaviate.util.generate_uuid5")
def test___generate_uuids(generate_uuid5, weaviate_hook):
    df = pd.DataFrame.from_dict({"name": ["ross", "bob"], "age": ["12", "22"], "gender": ["m", "m"]})
    with pytest.raises(ValueError, match=r"Columns last_name don't exist in dataframe"):
        weaviate_hook._generate_uuids(
            df=df, collection_name="test", unique_columns=["name", "age", "gender", "last_name"]
        )

    df = pd.DataFrame.from_dict(
        {"id": [1, 2], "name": ["ross", "bob"], "age": ["12", "22"], "gender": ["m", "m"]}
    )
    with pytest.raises(
        ValueError, match=r"Property 'id' already in dataset. Consider renaming or specify 'uuid_column'"
    ):
        weaviate_hook._generate_uuids(df=df, collection_name="test", unique_columns=["name", "age", "gender"])

    with pytest.raises(
        ValueError,
        match=r"Property age already in dataset. Consider renaming or specify a different 'uuid_column'.",
    ):
        weaviate_hook._generate_uuids(
            df=df, uuid_column="age", collection_name="test", unique_columns=["name", "age", "gender"]
        )


@mock.patch("airflow.providers.weaviate.hooks.weaviate.WeaviateHook.delete_object")
def test__delete_objects(delete_object, weaviate_hook):
    if USE_HTTPX:
        resp_429 = httpx.Response(status_code=429)
        http_429_exception = weaviate.UnexpectedStatusCodeException(
            "Weaviate returned an unexpected status code",
            response=resp_429,
        )
        resp_404 = httpx.Response(status_code=404)
        http_404_exception = weaviate.UnexpectedStatusCodeException(
            message="object not found", response=resp_404
        )
    else:
        resp_429 = requests.Response()
        resp_429.status_code = 429
        http_429_exception = requests.exceptions.HTTPError(response=resp_429)
        resp_404 = requests.Response()
        resp_404.status_code = 404
        http_404_exception = weaviate.exceptions.UnexpectedStatusCodeException(
            message="object not found", response=resp_404
        )

    delete_object.side_effect = [http_404_exception, None, http_429_exception, http_429_exception, None]
    weaviate_hook._delete_objects(uuids=["1", "2", "3"], collection_name="test")
    assert delete_object.call_count == 5


def test__prepare_document_to_uuid_map(weaviate_hook):
    input_data = [
        {"id": "1", "name": "ross", "age": "12", "gender": "m"},
        {"id": "2", "name": "bob", "age": "22", "gender": "m"},
        {"id": "3", "name": "joy", "age": "15", "gender": "f"},
    ]
    grouped_data = weaviate_hook._prepare_document_to_uuid_map(
        data=input_data, group_key="gender", get_value=lambda x: x["name"]
    )
    assert grouped_data == {"m": {"ross", "bob"}, "f": {"joy"}}


@mock.patch("airflow.providers.weaviate.hooks.weaviate.WeaviateHook._prepare_document_to_uuid_map")
@mock.patch("airflow.providers.weaviate.hooks.weaviate.WeaviateHook._get_documents_to_uuid_map")
def test___get_segregated_documents(_get_documents_to_uuid_map, _prepare_document_to_uuid_map, weaviate_hook):
    _get_documents_to_uuid_map.return_value = {
        "abc.doc": {"uuid1", "uuid2", "uuid2"},
        "xyz.doc": {"uuid4", "uuid5"},
        "dfg.doc": {"uuid8", "uuid0", "uuid12"},
    }
    _prepare_document_to_uuid_map.return_value = {
        "abc.doc": {"uuid1", "uuid56", "uuid2"},
        "xyz.doc": {"uuid4", "uuid5"},
        "hjk.doc": {"uuid8", "uuid0", "uuid12"},
    }
    (
        _,
        changed_documents,
        unchanged_docs,
        new_documents,
    ) = weaviate_hook._get_segregated_documents(
        data=pd.DataFrame(),
        document_column="doc_key",
        uuid_column="id",
        collection_name="doc",
    )
    assert changed_documents == {"abc.doc"}
    assert unchanged_docs == {"xyz.doc"}
    assert new_documents == {"hjk.doc"}


@mock.patch("airflow.providers.weaviate.hooks.weaviate.WeaviateHook._get_segregated_documents")
@mock.patch("airflow.providers.weaviate.hooks.weaviate.WeaviateHook._generate_uuids")
def test_error_option_of_create_or_replace_document_objects(
    _generate_uuids, _get_segregated_documents, weaviate_hook
):
    df = pd.DataFrame.from_dict(
        {
            "id": ["1", "2", "3"],
            "name": ["ross", "bob", "joy"],
            "age": ["12", "22", "15"],
            "gender": ["m", "m", "f"],
            "doc": ["abc.xml", "zyx.html", "zyx.html"],
        }
    )

    _get_segregated_documents.return_value = ({}, {"abc.xml"}, {}, {"zyx.html"})
    _generate_uuids.return_value = (df, "id")
    with pytest.raises(ValueError, match="Documents abc.xml already exists. You can either skip or replace"):
        weaviate_hook.create_or_replace_document_objects(
            data=df, document_column="doc", collection_name="test", existing="error"
        )


@mock.patch("airflow.providers.weaviate.hooks.weaviate.WeaviateHook._delete_objects")
@mock.patch("airflow.providers.weaviate.hooks.weaviate.WeaviateHook.batch_data")
@mock.patch("airflow.providers.weaviate.hooks.weaviate.WeaviateHook._get_segregated_documents")
@mock.patch("airflow.providers.weaviate.hooks.weaviate.WeaviateHook._generate_uuids")
def test_skip_option_of_create_or_replace_document_objects(
    _generate_uuids, _get_segregated_documents, batch_data, _delete_objects, weaviate_hook
):
    df = pd.DataFrame.from_dict(
        {
            "id": ["1", "2", "3"],
            "name": ["ross", "bob", "joy"],
            "age": ["12", "22", "15"],
            "gender": ["m", "m", "f"],
            "doc": ["abc.xml", "zyx.html", "zyx.html"],
        }
    )

    collection_name = "test"
    documents_to_uuid_map, changed_documents, unchanged_documents, new_documents = (
        {},
        {"abc.xml"},
        {},
        {"zyx.html"},
    )
    _get_segregated_documents.return_value = (
        documents_to_uuid_map,
        changed_documents,
        unchanged_documents,
        new_documents,
    )
    _generate_uuids.return_value = (df, "id")

    weaviate_hook.create_or_replace_document_objects(
        data=df, collection_name=collection_name, existing="skip", document_column="doc"
    )

    pd.testing.assert_frame_equal(
        batch_data.call_args_list[0].kwargs["data"], df[df["doc"].isin(new_documents)]
    )


@mock.patch("airflow.providers.weaviate.hooks.weaviate.WeaviateHook._delete_all_documents_objects")
@mock.patch("airflow.providers.weaviate.hooks.weaviate.WeaviateHook.batch_data")
@mock.patch("airflow.providers.weaviate.hooks.weaviate.WeaviateHook._get_segregated_documents")
@mock.patch("airflow.providers.weaviate.hooks.weaviate.WeaviateHook._generate_uuids")
def test_replace_option_of_create_or_replace_document_objects(
    _generate_uuids, _get_segregated_documents, batch_data, _delete_all_documents_objects, weaviate_hook
):
    df = pd.DataFrame.from_dict(
        {
            "id": ["1", "2", "3"],
            "name": ["ross", "bob", "joy"],
            "age": ["12", "22", "15"],
            "gender": ["m", "m", "f"],
            "doc": ["abc.xml", "zyx.html", "zyx.html"],
        }
    )

    collection_name = "test"
    documents_to_uuid_map, changed_documents, unchanged_documents, new_documents = (
        {"abc.xml": {"uuid"}},
        {"abc.xml"},
        {},
        {"zyx.html"},
    )
    batch_data.return_value = []
    _get_segregated_documents.return_value = (
        documents_to_uuid_map,
        changed_documents,
        unchanged_documents,
        new_documents,
    )
    _generate_uuids.return_value = (df, "id")
    weaviate_hook.create_or_replace_document_objects(
        data=df, collection_name=collection_name, existing="replace", document_column="doc"
    )
    _delete_all_documents_objects.assert_called_with(
        document_keys=list(changed_documents),
        total_objects_count=1,
        document_column="doc",
        collection_name="test",
        batch_delete_error=[],
        verbose=False,
    )
    pd.testing.assert_frame_equal(
        batch_data.call_args_list[0].kwargs["data"],
        df[df["doc"].isin(changed_documents.union(new_documents))],
    )


@mock.patch("airflow.providers.weaviate.hooks.weaviate.weaviate.connect_to_custom")
@pytest.mark.parametrize(
    ("http_secure", "port", "expected"),
    [
        (False, None, 80),
        (True, None, 443),
        (False, 8000, 8000),
        (True, 8000, 8000),
    ],
)
def test_get_conn_http_port_logic(connect_to_custom, http_secure, port, expected):
    from airflow.models import Connection

    conn = Connection(
        conn_id="weaviate_http_port_logic",
        conn_type="weaviate",
        host="localhost",
        port=port,
        extra={"http_secure": http_secure},
    )

    with mock.patch.object(WeaviateHook, "get_connection", return_value=conn):
        hook = WeaviateHook(conn_id="weaviate_http_port_logic")
        hook.get_conn()

    # Assert: http_port honors provided port, otherwise 80/443 depending on http_secure
    kwargs = connect_to_custom.call_args.kwargs
    assert kwargs["http_host"] == "localhost"
    assert kwargs["http_port"] == expected
    assert kwargs["http_secure"] == http_secure
