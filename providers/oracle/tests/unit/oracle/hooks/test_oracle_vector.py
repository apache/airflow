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

from array import array
from unittest import mock

import pytest

from airflow.providers.oracle import vector
from airflow.providers.oracle.hooks.oracle_vector import OracleVectorDocument, OracleVectorHook
from airflow.providers.oracle.vector import OracleVectorFormat


class FakeCursor:
    def __init__(self, rows=None):
        self.rows = rows or []
        self.sql = None
        self.binds = None
        self.executed_rows = []
        self.executed_batches = []
        self.rowcount = 0
        self.rowfactory = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, sql, binds=None):
        self.sql = sql
        self.binds = binds or {}
        self.executed_rows.append(self.binds)
        self.rowfactory = None

    def executemany(self, sql, rows):
        self.sql = sql
        self.executed_batches.append(list(rows))
        self.rowcount = len(rows)

    def fetchall(self):
        return self.rows

    def fetchone(self):
        return self.rows[0] if self.rows else None

    def __iter__(self):
        if self.rowfactory is None:
            return iter(self.rows)
        return (self.rowfactory(*row) for row in self.rows)


class FakeConnection:
    def __init__(self, cursor):
        self._cursor = cursor
        self.committed = False
        self.closed = False
        self.version = "23.4.0.0.0"

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.closed = True
        return False

    def cursor(self):
        return self._cursor

    def commit(self):
        self.committed = True


class RecordingOracleVectorHook(OracleVectorHook):
    def __init__(self):
        self.statements = []
        self.first_rows = []

    def run(self, sql, *args, **kwargs):
        self.statements.append(sql)

    def get_first(self, sql, parameters=None):
        self.statements.append((sql, parameters))
        return self.first_rows.pop(0) if self.first_rows else None


def test_create_vector_table_emits_expected_ddl():
    hook = RecordingOracleVectorHook()
    hook.create_vector_table(table_name="docs", embedding_dimension=3, if_not_exists=False)
    sql = hook.statements[-1]
    assert "CREATE TABLE" in sql
    assert "docs" in sql
    assert "embedding VECTOR(3, FLOAT32) NOT NULL" in sql
    assert "metadata JSON" in sql


def test_create_vector_table_uses_if_not_exists_by_default():
    hook = RecordingOracleVectorHook()

    hook.create_vector_table(table_name="docs", embedding_dimension=3)

    assert hook.statements[-1].startswith("CREATE TABLE IF NOT EXISTS docs")


def test_create_vector_table_overwrites_with_if_exists_drop():
    hook = RecordingOracleVectorHook()

    hook.create_vector_table(table_name="docs", embedding_dimension=3, overwrite=True, if_not_exists=False)

    assert hook.statements[0] == "DROP TABLE IF EXISTS docs"
    assert hook.statements[1].startswith("CREATE TABLE docs")


@pytest.mark.parametrize(
    ("embedding_format", "expected"),
    [
        ("int8", "INT8"),
        (OracleVectorFormat.FLOAT64, "FLOAT64"),
        (OracleVectorFormat.BINARY, "BINARY"),
        (OracleVectorFormat.FLEXIBLE, "*"),
    ],
)
def test_create_vector_table_accepts_supported_embedding_formats(embedding_format, expected):
    hook = RecordingOracleVectorHook()
    hook.create_vector_table(
        table_name="docs",
        embedding_dimension=3,
        embedding_format=embedding_format,
        if_not_exists=False,
    )
    assert f"embedding VECTOR(3, {expected}) NOT NULL" in hook.statements[-1]


def test_create_vector_table_rejects_unsupported_embedding_format():
    hook = RecordingOracleVectorHook()
    with pytest.raises(ValueError, match="Unsupported Oracle vector format"):
        hook.create_vector_table(
            table_name="docs",
            embedding_dimension=3,
            embedding_format="FLOAT16",
            if_not_exists=False,
        )


@pytest.mark.parametrize(
    ("embedding_format", "expected_format"),
    [
        (OracleVectorFormat.FLOAT32, "FLOAT32"),
        (OracleVectorFormat.FLOAT64, "FLOAT64"),
        (OracleVectorFormat.INT8, "INT8"),
    ],
)
def test_create_vector_table_creates_sparse_vector_column(embedding_format, expected_format):
    hook = RecordingOracleVectorHook()

    hook.create_vector_table(
        table_name="docs",
        embedding_dimension=3,
        embedding_format=embedding_format,
        sparse=True,
        if_not_exists=False,
    )

    assert f"embedding VECTOR(3, {expected_format}, SPARSE) NOT NULL" in hook.statements[-1]


@pytest.mark.parametrize("embedding_format", [OracleVectorFormat.BINARY, OracleVectorFormat.FLEXIBLE])
def test_create_vector_table_rejects_sparse_vector_with_unsupported_format(embedding_format):
    hook = RecordingOracleVectorHook()

    with pytest.raises(ValueError, match="Sparse vectors support"):
        hook.create_vector_table(
            table_name="docs",
            embedding_dimension=3,
            embedding_format=embedding_format,
            sparse=True,
        )


def test_create_vector_table_rejects_overwrite_and_if_not_exists():
    hook = RecordingOracleVectorHook()
    with pytest.raises(ValueError, match="overwrite=True cannot be combined with if_not_exists=True"):
        hook.create_vector_table(table_name="docs", embedding_dimension=3, overwrite=True, if_not_exists=True)


def test_add_texts_rejects_mismatched_lengths():
    hook = RecordingOracleVectorHook()
    with pytest.raises(ValueError, match=r"zip\(\) argument 2 is shorter than argument 1"):
        hook.add_texts(table_name="docs", texts=["a", "b"], embeddings=[[1, 2, 3]])


@mock.patch("airflow.providers.oracle.hooks.oracle_vector.uuid4", side_effect=["generated-1", "generated-2"])
def test_add_texts_generates_ids_and_uses_empty_metadata(mock_uuid4, monkeypatch):
    cursor = FakeCursor()
    conn = FakeConnection(cursor)
    hook = RecordingOracleVectorHook()
    monkeypatch.setattr(hook, "get_conn", lambda: conn)

    ids = hook.add_texts(
        table_name="docs",
        texts=["hello", "world"],
        embeddings=[[1, 2, 3], [4, 5, 6]],
    )

    assert ids == ["generated-1", "generated-2"]
    assert mock_uuid4.call_count == 2
    assert cursor.executed_batches == [
        [
            {
                "id": "generated-1",
                "text": "hello",
                "metadata": "{}",
                "embedding": array("f", [1.0, 2.0, 3.0]),
            },
            {
                "id": "generated-2",
                "text": "world",
                "metadata": "{}",
                "embedding": array("f", [4.0, 5.0, 6.0]),
            },
        ]
    ]


def test_add_documents_executes_insert_and_commits(monkeypatch):
    cursor = FakeCursor()
    conn = FakeConnection(cursor)
    hook = RecordingOracleVectorHook()
    monkeypatch.setattr(hook, "get_conn", lambda: conn)
    ids = hook.add_documents(
        table_name="docs",
        documents=[
            OracleVectorDocument(id="d1", text="hello", metadata={"source": "unit"}, embedding=[1, 2, 3]),
            OracleVectorDocument(id="d2", text="world", metadata={"source": "unit"}, embedding=[4, 5, 6]),
        ],
    )
    assert ids == ["d1", "d2"]
    assert "INSERT INTO" in cursor.sql
    assert cursor.executed_batches == [
        [
            {
                "id": "d1",
                "text": "hello",
                "metadata": '{"source":"unit"}',
                "embedding": array("f", [1.0, 2.0, 3.0]),
            },
            {
                "id": "d2",
                "text": "world",
                "metadata": '{"source":"unit"}',
                "embedding": array("f", [4.0, 5.0, 6.0]),
            },
        ]
    ]
    assert conn.committed


def test_add_documents_preserves_sparse_embeddings(monkeypatch):
    cursor = FakeCursor()
    conn = FakeConnection(cursor)
    hook = RecordingOracleVectorHook()
    monkeypatch.setattr(hook, "get_conn", lambda: conn)
    embedding = vector.oracledb.SparseVector(5, [1, 3], array("f", [4.0, 5.0]))

    hook.add_documents(
        table_name="docs",
        documents=[OracleVectorDocument(id="d1", text="hello", embedding=embedding)],
    )

    assert cursor.executed_batches[0][0]["embedding"] is embedding


def test_add_texts_mutate_on_duplicate_uses_merge(monkeypatch):
    cursor = FakeCursor()
    conn = FakeConnection(cursor)
    hook = RecordingOracleVectorHook()
    monkeypatch.setattr(hook, "get_conn", lambda: conn)
    hook.add_texts(
        table_name="docs",
        texts=["hello"],
        embeddings=[[1, 2, 3]],
        ids=["d1"],
        mutate_on_duplicate=True,
    )
    assert "MERGE INTO" in cursor.sql
    assert "WHEN MATCHED THEN UPDATE" in cursor.sql
    assert cursor.executed_batches == [
        [
            {
                "id": "d1",
                "text": "hello",
                "metadata": "{}",
                "embedding": array("f", [1.0, 2.0, 3.0]),
            }
        ]
    ]


def test_similarity_search_by_vector_emits_vector_distance_and_binds(monkeypatch):
    cursor = FakeCursor()
    conn = FakeConnection(cursor)
    cursor.rows = [("d1", "hello", {"source": "unit"}, 0.1)]
    hook = RecordingOracleVectorHook()
    monkeypatch.setattr(hook, "get_conn", lambda: conn)
    results = hook.similarity_search_by_vector(
        table_name="docs",
        embedding=[1, 2, 3],
        k=1,
        distance="COSINE",
        filter={"source": {"$eq": "unit"}},
        include_score=True,
    )
    assert "VECTOR_DISTANCE" in cursor.sql
    assert "COSINE" in cursor.sql
    assert "JSON_SERIALIZE" not in cursor.sql
    assert cursor.binds["query_embedding"] == array("f", [1.0, 2.0, 3.0])
    assert cursor.binds["k"] == 1
    assert results[0].id == "d1"
    assert results[0].metadata == {"source": "unit"}
    assert results[0].distance == 0.1


def test_similarity_search_by_vector_preserves_sparse_query_embedding(monkeypatch):
    cursor = FakeCursor()
    conn = FakeConnection(cursor)
    hook = RecordingOracleVectorHook()
    monkeypatch.setattr(hook, "get_conn", lambda: conn)
    embedding = vector.oracledb.SparseVector(5, [1, 3], array("f", [4.0, 5.0]))

    hook.similarity_search_by_vector(table_name="docs", embedding=embedding, k=1)

    assert cursor.binds["query_embedding"] is embedding


def test_get_by_ids_can_include_embedding(monkeypatch):
    cursor = FakeCursor()
    conn = FakeConnection(cursor)
    cursor.rows = [("d1", "hello", {"source": "unit"}, [1, 2, 3])]
    hook = RecordingOracleVectorHook()
    monkeypatch.setattr(hook, "get_conn", lambda: conn)
    results = hook.get_by_ids(table_name="docs", ids=["d1"], include_embedding=True)
    assert "IN (:id_0)" in cursor.sql
    assert "JSON_SERIALIZE" not in cursor.sql
    assert cursor.binds == {"id_0": "d1"}
    assert results[0].metadata == {"source": "unit"}
    assert results[0].embedding == [1.0, 2.0, 3.0]


def test_get_by_ids_returns_sparse_embedding(monkeypatch):
    cursor = FakeCursor()
    conn = FakeConnection(cursor)
    cursor.rows = [
        (
            "d1",
            "hello",
            {"source": "unit"},
            vector.oracledb.SparseVector(5, [1, 3], array("f", [4.0, 5.0])),
        )
    ]
    hook = RecordingOracleVectorHook()
    monkeypatch.setattr(hook, "get_conn", lambda: conn)

    results = hook.get_by_ids(table_name="docs", ids=["d1"], include_embedding=True)

    assert isinstance(results[0].embedding, vector.oracledb.SparseVector)
    assert results[0].embedding.num_dimensions == 5
    assert list(results[0].embedding.indices) == [1, 3]
    assert list(results[0].embedding.values) == [4.0, 5.0]


def test_delete_executes_delete(monkeypatch):
    cursor = FakeCursor()
    conn = FakeConnection(cursor)
    hook = RecordingOracleVectorHook()
    monkeypatch.setattr(hook, "get_conn", lambda: conn)
    deleted = hook.delete_documents(table_name="docs", ids=["d1", "d2"])
    assert deleted == 2
    assert "DELETE FROM" in cursor.sql
    assert len(cursor.executed_batches[0]) == 2


def test_create_hnsw_index_emits_ddl():
    hook = RecordingOracleVectorHook()
    hook.create_vector_index(
        table_name="docs",
        index_name="docs_hnsw_idx",
        index_type="HNSW",
        distance="COSINE",
        accuracy=90,
        parallel=2,
        neighbors=32,
        ef_construction=200,
    )
    sql = hook.statements[-1]
    assert "CREATE VECTOR INDEX IF NOT EXISTS" in sql
    assert "ORGANIZATION INMEMORY NEIGHBOR GRAPH" in sql
    assert "DISTANCE COSINE" in sql
    assert "neighbors 32" in sql
    assert "efConstruction 200" in sql


def test_create_ivf_index_emits_ddl():
    hook = RecordingOracleVectorHook()
    hook.create_vector_index(
        table_name="docs",
        index_name="docs_ivf_idx",
        index_type="IVF",
        neighbor_partitions=10,
    )
    sql = hook.statements[-1]
    assert "ORGANIZATION NEIGHBOR PARTITIONS" in sql
    assert "type IVF" in sql
    assert "neighbor partitions 10" in sql


def test_drop_vector_index_uses_if_exists_ddl():
    hook = RecordingOracleVectorHook()

    hook.drop_vector_index(index_name="docs_hnsw_idx")

    assert hook.statements == ["DROP INDEX IF EXISTS docs_hnsw_idx"]


def test_create_index_validates_parameter_combinations():
    hook = RecordingOracleVectorHook()
    with pytest.raises(ValueError, match="neighbors and ef_construction are only valid for HNSW indexes"):
        hook.create_vector_index(table_name="docs", index_name="idx", index_type="IVF", neighbors=10)
    with pytest.raises(ValueError, match="neighbor_partitions is only valid for IVF indexes"):
        hook.create_vector_index(
            table_name="docs", index_name="idx", index_type="HNSW", neighbor_partitions=10
        )
