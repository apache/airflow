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

from unittest.mock import MagicMock, Mock, patch

import pytest

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


def test_create_class(weaviate_hook):
    """
    Test the create_class method of WeaviateHook.
    """
    # Mock the Weaviate Client
    mock_client = MagicMock()
    weaviate_hook.get_client = MagicMock(return_value=mock_client)

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
    weaviate_hook.get_client = MagicMock(return_value=mock_client)

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
    weaviate_hook.get_client = MagicMock(return_value=mock_client)

    # Define test data
    test_class_name = "TestClass"
    test_data = [{"name": "John"}, {"name": "Jane"}]

    # Test the batch_data method
    weaviate_hook.batch_data(test_class_name, test_data)

    # Assert that the batch_data method was called with the correct arguments
    mock_client.batch.configure.assert_called_once()
    mock_batch_context = mock_client.batch.__enter__.return_value
    assert mock_batch_context.add_data_object.call_count == len(test_data)
