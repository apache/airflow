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
from qdrant_client import QdrantClient, models

from airflow.providers.common.compat.sdk import Context
from airflow.providers.qdrant.hooks.qdrant import QdrantHook
from airflow.providers.qdrant.operators.qdrant import QdrantIngestOperator, QdrantSearchOperator

qdrant_client = pytest.importorskip("qdrant_client")

DEFAULT_ARGUMENTS = {
    "query_filter": None,
    "search_params": None,
    "limit": 10,
    "offset": None,
    "with_payload": True,
    "with_vectors": False,
    "score_threshold": None,
}
ALL_OPTIONAL_ARGUMENTS = {
    "query_filter": models.Filter(
        must=[models.FieldCondition(key="year", match=models.MatchValue(value=2025))]
    ),
    "search_params": models.SearchParams(hnsw_ef=128),
    "limit": 7,
    "offset": 2,
    "with_payload": ["title"],
    "with_vectors": True,
    "score_threshold": 0.5,
}


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

    @pytest.mark.parametrize(
        ("optional_kwargs", "expected"),
        [
            pytest.param({}, DEFAULT_ARGUMENTS, id="defaults"),
            pytest.param(ALL_OPTIONAL_ARGUMENTS, ALL_OPTIONAL_ARGUMENTS, id="all optional arguments"),
            pytest.param(
                {"query_points_kwargs": {"using": "text-embedding"}},
                {**DEFAULT_ARGUMENTS, "using": "text-embedding"},
                id="query_points_kwargs",
            ),
        ],
    )
    def test_execute_forwards_constructor_arguments_to_hook(self, optional_kwargs, expected):
        """Every constructor argument reaches ``hook.search`` on the right keyword.

        Catches drift between the operator constructor and the hook signature.
        """
        op = QdrantSearchOperator(
            task_id="search",
            collection_name=self.COLLECTION,
            query=self.QUERY,
            **optional_kwargs,
        )
        mock_hook = Mock(spec=QdrantHook)
        mock_hook.search.return_value = []
        op.hook = mock_hook

        op.execute(context=Context())

        mock_hook.search.assert_called_once_with(
            collection_name=self.COLLECTION, query=self.QUERY, **expected
        )

    def test_template_fields_are_rendered(self):
        op = QdrantSearchOperator(
            task_id="search",
            conn_id="{{ params.conn_id }}",
            collection_name="{{ params.collection }}",
            query="{{ params.point_id }}",
            query_filter={"must": [{"key": "year", "match": {"value": "{{ params.year }}"}}]},
            limit="{{ params.k }}",
        )

        params = {"conn_id": "qdrant_prod", "collection": "docs", "point_id": "p1", "year": 2025, "k": 3}
        op.render_template_fields(context={"params": params})

        assert op.conn_id == "qdrant_prod"
        assert op.collection_name == "docs"
        assert op.query == "p1"
        assert op.query_filter == {"must": [{"key": "year", "match": {"value": "2025"}}]}
        assert op.limit == "3"

    def test_a_templated_limit_reaches_the_hook_as_an_int(self):
        """Without native rendering, ``limit`` renders as a str, which the gRPC client rejects."""
        op = QdrantSearchOperator(
            task_id="search", collection_name=self.COLLECTION, query=self.QUERY, limit="{{ params.k }}"
        )
        op.render_template_fields(context={"params": {"k": 3}})
        mock_hook = Mock(spec=QdrantHook)
        mock_hook.search.return_value = []
        op.hook = mock_hook

        op.execute(context=Context())

        assert mock_hook.search.call_args.kwargs["limit"] == 3

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
        return QdrantClient(":memory:")

    @staticmethod
    def _run(op, client):
        hook = QdrantHook()
        hook.__dict__["conn"] = client
        op.__dict__["hook"] = hook
        return op.execute(context=Context())

    def test_finds_what_the_ingest_operator_wrote(self, client):
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
        assert results[0]["vector"] == {"text": [0.0, 1.0, 0.0], "image": [1.0, 0.0]}
        assert json.loads(json.dumps(results)) == results
