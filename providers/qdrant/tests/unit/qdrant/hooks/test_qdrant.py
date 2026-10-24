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

import json
from unittest.mock import Mock, patch

import pytest
from qdrant_client.models import ScoredPoint

from airflow.providers.qdrant.hooks.qdrant import QdrantHook

qdrant_client = pytest.importorskip("qdrant_client")


class TestQdrantHook:
    def setup_method(self):
        """Set up the test connection for the QdrantHook."""
        with patch("airflow.models.Connection.get_connection_from_secrets") as mock_get_connection:
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
        payloads = [{"meta": "data"}, {"meta": "data_2"}, {"meta": "data_3", "extra": "data"}]
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
            collection_name=self.collection_name, query_vector=[1.0, 2.0, 3.0], limit=10, with_vectors=True
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

        conn.delete_collection.assert_called_once_with(collection_name=self.collection_name)

    @patch("airflow.providers.qdrant.hooks.qdrant.QdrantHook.conn")
    def test_search_returns_json_serializable_dicts(self, conn):
        """``ScoredPoint`` objects are not XCom-serializable, so ``search`` returns plain dicts."""
        conn.query_points.return_value = Mock(
            points=[
                ScoredPoint(
                    id="5c56c793-69f3-4fbf-87e6-c4bf54c28c26",
                    version=1,
                    score=0.9,
                    payload={"text": "hi"},
                    vector=[0.1, 0.2],
                ),
                ScoredPoint(id=7, version=1, score=0.7, payload={"text": "yo"}),
            ]
        )

        results = self.qdrant_hook.search(collection_name=self.collection_name, query=[0.1, 0.2], limit=5)

        assert [(r["id"], r["score"], r["payload"]) for r in results] == [
            ("5c56c793-69f3-4fbf-87e6-c4bf54c28c26", 0.9, {"text": "hi"}),
            (7, 0.7, {"text": "yo"}),
        ]
        assert results[0]["vector"] == [0.1, 0.2]
        assert json.loads(json.dumps(results)) == results

    @patch("airflow.providers.qdrant.hooks.qdrant.QdrantHook.conn")
    def test_search_uses_query_points_and_forwards_arguments(self, conn):
        """``search`` calls ``query_points`` (modern API) with all arguments forwarded.

        Guards against a regression to the deprecated ``search()`` API and against
        silently dropping optional parameters.
        """
        conn.query_points.return_value = Mock(points=[])
        query_filter = Mock(name="filter")
        search_params = Mock(name="params")

        self.qdrant_hook.search(
            collection_name=self.collection_name,
            query=[1.0, 2.0, 3.0],
            query_filter=query_filter,
            search_params=search_params,
            limit=7,
            offset=2,
            with_payload=["title"],
            with_vectors=False,
            score_threshold=0.5,
        )

        conn.search.assert_not_called()
        conn.query_points.assert_called_once_with(
            collection_name=self.collection_name,
            query=[1.0, 2.0, 3.0],
            query_filter=query_filter,
            search_params=search_params,
            limit=7,
            offset=2,
            with_payload=["title"],
            with_vectors=False,
            score_threshold=0.5,
        )

    @patch("airflow.providers.qdrant.hooks.qdrant.QdrantHook.conn")
    def test_search_forwards_extra_kwargs(self, conn):
        """Extra ``**kwargs`` (e.g. ``using`` for named vectors) reach ``query_points``.

        Keeps the hook forward-compatible with future ``query_points`` parameters
        (hybrid search, named vectors) without needing to enumerate them here.
        """
        conn.query_points.return_value = Mock(points=[])

        self.qdrant_hook.search(
            collection_name=self.collection_name,
            query=[0.1, 0.2, 0.3],
            using="text-embedding",
        )

        _, kwargs = conn.query_points.call_args
        assert kwargs["using"] == "text-embedding"
