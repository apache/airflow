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

from airflow.providers.pinecone.hooks.pinecone import PineconeHook


class TestPineconeHook:
    def setup_method(self):
        """Set up the test environment, mocking necessary connections and initializing the
        PineconeHook object."""
        with patch("airflow.models.Connection.get_connection_from_secrets") as mock_get_connection:
            mock_conn = Mock()
            mock_conn.host = "pinecone.io"
            mock_conn.login = "test_user"
            mock_conn.password = "test_password"
            mock_get_connection.return_value = mock_conn
            self.pinecone_hook = PineconeHook()
            self.index_name = "test_index"

    @patch("airflow.providers.pinecone.hooks.pinecone.pinecone.Index")
    def test_upsert(self, mock_index):
        """Test the upsert_data_async method of PineconeHook for correct data insertion asynchronously."""
        data = [("id1", [1.0, 2.0, 3.0], {"meta": "data"})]
        mock_upsert = Mock()
        mock_index.return_value.upsert = mock_upsert
        self.pinecone_hook.upsert(self.index_name, data)
        mock_upsert.assert_called_once_with(vectors=data, namespace="", batch_size=None, show_progress=True)

    @patch("airflow.providers.pinecone.hooks.pinecone.PineconeHook.list_indexes")
    def test_list_indexes(self, mock_list_indexes):
        """Test that the list_indexes method of PineconeHook is called correctly."""
        self.pinecone_hook.list_indexes()
        mock_list_indexes.assert_called_once()

    @patch("airflow.providers.pinecone.hooks.pinecone.PineconeHook.create_index")
    def test_create_index(self, mock_create_index):
        """Test that the create_index method of PineconeHook is called with correct arguments."""
        self.pinecone_hook.create_index(index_name=self.index_name, dimension=128)
        mock_create_index.assert_called_once_with(index_name="test_index", dimension=128)

    @patch("airflow.providers.pinecone.hooks.pinecone.PineconeHook.describe_index")
    def test_describe_index(self, mock_describe_index):
        """Test that the describe_index method of PineconeHook is called with correct arguments."""
        self.pinecone_hook.describe_index(index_name=self.index_name)
        mock_describe_index.assert_called_once_with(index_name=self.index_name)

    @patch("airflow.providers.pinecone.hooks.pinecone.PineconeHook.delete_index")
    def test_delete_index(self, mock_delete_index):
        """Test that the delete_index method of PineconeHook is called with the correct index name."""
        self.pinecone_hook.delete_index(index_name="test_index")
        mock_delete_index.assert_called_once_with(index_name="test_index")

    @patch("airflow.providers.pinecone.hooks.pinecone.PineconeHook.create_collection")
    def test_create_collection(self, mock_create_collection):
        """
        Test that the create_collection method of PineconeHook is called correctly.
        """
        self.pinecone_hook.create_collection(collection_name="test_collection")
        mock_create_collection.assert_called_once_with(collection_name="test_collection")

    @patch("airflow.providers.pinecone.hooks.pinecone.PineconeHook.configure_index")
    def test_configure_index(self, mock_configure_index):
        """
        Test that the configure_index method of PineconeHook is called correctly.
        """
        self.pinecone_hook.configure_index(index_configuration={})
        mock_configure_index.assert_called_once_with(index_configuration={})

    @patch("airflow.providers.pinecone.hooks.pinecone.PineconeHook.describe_collection")
    def test_describe_collection(self, mock_describe_collection):
        """
        Test that the describe_collection method of PineconeHook is called correctly.
        """
        self.pinecone_hook.describe_collection(collection_name="test_collection")
        mock_describe_collection.assert_called_once_with(collection_name="test_collection")

    @patch("airflow.providers.pinecone.hooks.pinecone.PineconeHook.list_collections")
    def test_list_collections(self, mock_list_collections):
        """
        Test that the list_collections method of PineconeHook is called correctly.
        """
        self.pinecone_hook.list_collections()
        mock_list_collections.assert_called_once()

    @patch("airflow.providers.pinecone.hooks.pinecone.PineconeHook.query_vector")
    def test_query_vector(self, mock_query_vector):
        """
        Test that the query_vector method of PineconeHook is called correctly.
        """
        self.pinecone_hook.query_vector(vector=[1.0, 2.0, 3.0])
        mock_query_vector.assert_called_once_with(vector=[1.0, 2.0, 3.0])

    def test__chunks(self):
        """
        Test that the _chunks method of PineconeHook behaves as expected.
        """
        data = list(range(10))
        chunked_data = list(self.pinecone_hook._chunks(data, 3))
        assert chunked_data == [(0, 1, 2), (3, 4, 5), (6, 7, 8), (9,)]

    @patch("airflow.providers.pinecone.hooks.pinecone.PineconeHook.upsert_data_async")
    def test_upsert_data_async_correctly(self, mock_upsert_data_async):
        """
        Test that the upsert_data_async method of PineconeHook is called correctly.
        """
        data = [("id1", [1.0, 2.0, 3.0], {"meta": "data"})]
        self.pinecone_hook.upsert_data_async(index_name="test_index", data=data)
        mock_upsert_data_async.assert_called_once_with(index_name="test_index", data=data)

    @patch("airflow.providers.pinecone.hooks.pinecone.PineconeHook.describe_index_stats")
    def test_describe_index_stats(self, mock_describe_index_stats):
        """
        Test that the describe_index_stats method of PineconeHook is called correctly.
        """
        self.pinecone_hook.describe_index_stats(index_name="test_index")
        mock_describe_index_stats.assert_called_once_with(index_name="test_index")
