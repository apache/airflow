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

from airflow.providers.common.compat.sdk import Context
from airflow.providers.qdrant.hooks.qdrant import QdrantHook
from airflow.providers.qdrant.operators.qdrant import QdrantIngestOperator, QdrantSearchOperator

qdrant_client = pytest.importorskip("qdrant_client")


class TestQdrantIngestOperator:
    @pytest.mark.db_test
    def test_operator_execution(self, dag_maker):
        """
        Test the execution of the QdrantIngestOperator.
        Ensures that the upsert method on the hook is correctly called.
        """
        with dag_maker(dag_id="test_dag") as dummy_dag:
            vectors = [[0.732, 0.611, 0.289], [0.217, 0.526, 0.416], [0.326, 0.483, 0.376]]
            ids = [32, 21, "b626f6a9-b14d-4af9-b7c3-43d8deb719a6"]
            payload = [{"meta": "data"}, {"meta": "data_2"}, {"meta": "data_3", "extra": "data"}]

            task = QdrantIngestOperator(
                task_id="ingest_vectors",
                collection_name="test_collection",
                vectors=vectors,
                ids=ids,
                payload=payload,
                wait=False,
                max_retries=1,
                parallel=3,
                dag=dummy_dag,
            )

        with patch(
            "airflow.providers.qdrant.operators.qdrant.QdrantIngestOperator.hook"
        ) as mock_hook_instance:
            task.execute(context={})
            mock_hook_instance.conn.upload_collection.assert_called_once_with(
                collection_name="test_collection",
                vectors=vectors,
                ids=ids,
                payload=payload,
                batch_size=64,
                wait=False,
                max_retries=1,
                parallel=3,
                method=None,
            )


class TestQdrantSearchOperator:
    """Unit tests for QdrantSearchOperator."""

    COLLECTION = "test_collection"
    QUERY = [0.1, 0.2, 0.3]

    def test_execute_returns_hook_search_result(self):
        """``execute`` returns whatever ``QdrantHook.search`` returns.

        The operator is a thin XCom-safe delegate to the hook, so this guards the
        contract that whatever list-of-dicts the hook produces is what lands in XCom.
        """
        op = QdrantSearchOperator(
            task_id="search",
            collection_name=self.COLLECTION,
            query=self.QUERY,
            limit=5,
        )
        mock_hook = Mock(spec=QdrantHook)
        mock_hook.search.return_value = [
            {"id": "a", "score": 0.9, "payload": {"text": "hi"}},
        ]
        op.hook = mock_hook

        result = op.execute(context=Context())

        assert result == [{"id": "a", "score": 0.9, "payload": {"text": "hi"}}]

    def test_execute_delegates_default_arguments_to_hook(self):
        """``execute`` forwards each constructor field to ``hook.search`` at its default.

        Catches drift between the operator constructor and the hook signature.
        """
        op = QdrantSearchOperator(
            task_id="search",
            collection_name=self.COLLECTION,
            query=self.QUERY,
        )
        mock_hook = Mock(spec=QdrantHook)
        mock_hook.search.return_value = []
        op.hook = mock_hook

        op.execute(context=Context())

        mock_hook.search.assert_called_once_with(
            collection_name=self.COLLECTION,
            query=self.QUERY,
            query_filter=None,
            search_params=None,
            limit=10,
            offset=None,
            with_payload=True,
            with_vectors=False,
            score_threshold=None,
        )

    def test_execute_forwards_all_optional_arguments(self):
        """Every optional constructor arg reaches ``hook.search`` on the right keyword."""
        query_filter = Mock(name="filter")
        search_params = Mock(name="params")
        op = QdrantSearchOperator(
            task_id="search",
            collection_name=self.COLLECTION,
            query=self.QUERY,
            query_filter=query_filter,
            search_params=search_params,
            limit=7,
            offset=2,
            with_payload=["title"],
            with_vectors=True,
            score_threshold=0.5,
        )
        mock_hook = Mock(spec=QdrantHook)
        mock_hook.search.return_value = []
        op.hook = mock_hook

        op.execute(context=Context())

        mock_hook.search.assert_called_once_with(
            collection_name=self.COLLECTION,
            query=self.QUERY,
            query_filter=query_filter,
            search_params=search_params,
            limit=7,
            offset=2,
            with_payload=["title"],
            with_vectors=True,
            score_threshold=0.5,
        )

    def test_execute_forwards_query_points_kwargs(self):
        """Arguments the operator does not name, such as ``using`` for named vectors, reach the hook."""
        op = QdrantSearchOperator(
            task_id="search",
            collection_name=self.COLLECTION,
            query=self.QUERY,
            query_points_kwargs={"using": "text-embedding"},
        )
        mock_hook = Mock(spec=QdrantHook)
        mock_hook.search.return_value = []
        op.hook = mock_hook

        op.execute(context=Context())

        assert mock_hook.search.call_args.kwargs["using"] == "text-embedding"

    def test_default_conn_id_matches_hook(self):
        """The operator's default ``conn_id`` matches ``QdrantHook.default_conn_name``.

        Prevents a silent split where the operator points at a different connection
        than the hook if the default is ever renamed on one side but not the other.
        """
        op = QdrantSearchOperator(
            task_id="search",
            collection_name=self.COLLECTION,
            query=self.QUERY,
        )
        assert op.conn_id == QdrantHook.default_conn_name


class TestQdrantSearchOperatorAgainstLocalQdrant:
    """Ingest and search through the real client, with Qdrant's in-process engine instead of a server."""

    @pytest.fixture
    def client(self):
        from qdrant_client import QdrantClient

        return QdrantClient(":memory:")

    @staticmethod
    def _run(op, client):
        hook = QdrantHook()
        hook.__dict__["conn"] = client
        op.__dict__["hook"] = hook
        return op.execute(context=Context())

    def test_finds_what_the_ingest_operator_wrote(self, client):
        from qdrant_client import models

        client.create_collection(
            "docs", vectors_config=models.VectorParams(size=4, distance=models.Distance.COSINE)
        )
        ingest = QdrantIngestOperator(
            task_id="ingest",
            collection_name="docs",
            vectors=[[1, 0, 0, 0], [0.9, 0.1, 0, 0], [0, 1, 0, 0]],
            ids=[1, 2, 3],
            payload=[{"year": 2024}, {"year": 2025}, {"year": 2025}],
        )
        self._run(ingest, client)

        results = self._run(
            QdrantSearchOperator(
                task_id="search",
                collection_name="docs",
                query=[1, 0, 0, 0],
                query_filter=models.Filter(
                    must=[models.FieldCondition(key="year", match=models.MatchValue(value=2025))]
                ),
                limit=1,
            ),
            client,
        )

        assert [(r["id"], r["payload"]) for r in results] == [(2, {"year": 2025})]
        assert json.loads(json.dumps(results)) == results

    def test_named_vectors_are_searched_through_query_points_kwargs(self, client):
        from qdrant_client import models

        client.create_collection(
            "multi",
            vectors_config={
                "text": models.VectorParams(size=3, distance=models.Distance.COSINE),
                "image": models.VectorParams(size=2, distance=models.Distance.COSINE),
            },
        )
        client.upsert(
            "multi",
            points=[
                models.PointStruct(
                    id=1, vector={"text": [1, 0, 0], "image": [0, 1]}, payload={"name": "cat"}
                ),
                models.PointStruct(
                    id=2, vector={"text": [0, 1, 0], "image": [1, 0]}, payload={"name": "dog"}
                ),
            ],
        )

        results = self._run(
            QdrantSearchOperator(
                task_id="search",
                collection_name="multi",
                query=[1, 0],
                limit=1,
                with_vectors=True,
                query_points_kwargs={"using": "image"},
            ),
            client,
        )

        assert results[0]["payload"] == {"name": "dog"}
        assert json.loads(json.dumps(results)) == results
