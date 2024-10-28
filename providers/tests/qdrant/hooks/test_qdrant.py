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

qdrant_client = pytest.importorskip("qdrant_client")

from airflow.providers.qdrant.hooks.qdrant import QdrantHook


class TestQdrantHook:
    def setup_method(self):
        """Set up the test connection for the QdrantHook."""
        with patch(
            "airflow.models.Connection.get_connection_from_secrets"
        ) as mock_get_connection:
            mock_conn = Mock()
            mock_conn.host = "localhost"
            mock_conn.port = 6333
            mock_conn.extra_dejson = {}
            mock_conn.password = "some_test_api_key"
            mock_get_connection.return_value = mock_conn
            self.qdrant_hook = QdrantHook()

            self.collection_name = "test_collection"

    @patch("airflow.providers.qdrant.hooks.qdrant.QdrantHook.conn")
    def test_verify_connection(self, mock_conn):
        """Test the verify_connection of the QdrantHook."""
        self.qdrant_hook.verify_connection()

        mock_conn.get_collections.assert_called_once()

    @patch("airflow.providers.qdrant.hooks.qdrant.QdrantHook.conn")
    def test_upsert(self, conn):
        """Test the upsert method of the QdrantHook with appropriate arguments."""
        vectors = [[0.732, 0.611, 0.289], [0.217, 0.526, 0.416], [0.326, 0.483, 0.376]]
        ids = [32, 21, "b626f6a9-b14d-4af9-b7c3-43d8deb719a6"]
        payloads = [
            {"meta": "data"},
            {"meta": "data_2"},
            {"meta": "data_3", "extra": "data"},
        ]
        parallel = 2
        self.qdrant_hook.conn.upsert(
            collection_name=self.collection_name,
            vectors=vectors,
            ids=ids,
            payloads=payloads,
            parallel=parallel,
        )
        conn.upsert.assert_called_once_with(
            collection_name=self.collection_name,
            vectors=vectors,
            ids=ids,
            payloads=payloads,
            parallel=parallel,
        )

    @patch("airflow.providers.qdrant.hooks.qdrant.QdrantHook.conn")
    def test_list_collections(self, conn):
        """Test that the list_collections is called correctly."""
        self.qdrant_hook.conn.list_collections()
        conn.list_collections.assert_called_once()

    @patch("airflow.providers.qdrant.hooks.qdrant.QdrantHook.conn")
    def test_create_collection(self, conn):
        """Test that the create_collection is called with correct arguments."""

        from qdrant_client.models import Distance, VectorParams

        self.qdrant_hook.conn.create_collection(
            collection_name=self.collection_name,
            vectors_config=VectorParams(size=384, distance=Distance.COSINE),
        )
        conn.create_collection.assert_called_once_with(
            collection_name=self.collection_name,
            vectors_config=VectorParams(size=384, distance=Distance.COSINE),
        )

    @patch("airflow.providers.qdrant.hooks.qdrant.QdrantHook.conn")
    def test_delete(self, conn):
        """Test that the delete is called with correct arguments."""

        self.qdrant_hook.conn.delete(
            collection_name=self.collection_name, points_selector=[32, 21], wait=False
        )

        conn.delete.assert_called_once_with(
            collection_name=self.collection_name, points_selector=[32, 21], wait=False
        )

    @patch("airflow.providers.qdrant.hooks.qdrant.QdrantHook.conn")
    def test_search(self, conn):
        """Test that the search is called with correct arguments."""

        self.qdrant_hook.conn.search(
            collection_name=self.collection_name,
            query_vector=[1.0, 2.0, 3.0],
            limit=10,
            with_vectors=True,
        )

        conn.search.assert_called_once_with(
            collection_name=self.collection_name,
            query_vector=[1.0, 2.0, 3.0],
            limit=10,
            with_vectors=True,
        )

    @patch("airflow.providers.qdrant.hooks.qdrant.QdrantHook.conn")
    def test_get_collection(self, conn):
        """Test that the get_collection is called with correct arguments."""

        self.qdrant_hook.conn.get_collection(collection_name=self.collection_name)

        conn.get_collection.assert_called_once_with(collection_name=self.collection_name)

    @patch("airflow.providers.qdrant.hooks.qdrant.QdrantHook.conn")
    def test_delete_collection(self, conn):
        """Test that the delete_collection is called with correct arguments."""

        self.qdrant_hook.conn.delete_collection(collection_name=self.collection_name)

        conn.delete_collection.assert_called_once_with(
            collection_name=self.collection_name
        )
