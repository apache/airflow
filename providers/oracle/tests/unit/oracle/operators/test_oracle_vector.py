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
from airflow.providers.oracle.hooks.oracle_vector import OracleVectorSearchResult
from airflow.providers.oracle.operators.oracle_vector import (
    OracleAddVectorDocumentsOperator,
    OracleCreateVectorIndexOperator,
    OracleCreateVectorTableOperator,
    OracleDeleteVectorDocumentsOperator,
    OracleVectorSearchOperator,
)
from airflow.providers.oracle.vector import OracleVectorFormat


@mock.patch("airflow.providers.oracle.operators.oracle_vector.OracleVectorHook")
def test_create_vector_table_operator_calls_hook(mock_hook_class):
    op = OracleCreateVectorTableOperator(
        task_id="t",
        table_name="docs",
        embedding_dimension=3,
        embedding_format=OracleVectorFormat.INT8,
        sparse=True,
    )
    op.execute({})
    mock_hook_class.return_value.create_vector_table.assert_called_once_with(
        table_name="docs",
        embedding_dimension=3,
        id_column="id",
        text_column="text",
        metadata_column="metadata",
        embedding_column="embedding",
        embedding_format=OracleVectorFormat.INT8,
        sparse=True,
        if_not_exists=True,
        overwrite=False,
    )


@mock.patch("airflow.providers.oracle.operators.oracle_vector.OracleVectorHook")
def test_add_documents_operator_calls_hook(mock_hook_class):
    mock_hook_class.return_value.add_documents.return_value = ["d1"]
    op = OracleAddVectorDocumentsOperator(
        task_id="t",
        table_name="docs",
        documents=[{"id": "d1", "text": "hello", "embedding": [1, 2, 3]}],
        embedding_format=OracleVectorFormat.FLOAT64,
    )
    assert op.execute({}) == ["d1"]
    mock_hook_class.return_value.add_documents.assert_called_once_with(
        table_name="docs",
        documents=[{"id": "d1", "text": "hello", "embedding": [1, 2, 3]}],
        id_column="id",
        text_column="text",
        metadata_column="metadata",
        embedding_column="embedding",
        embedding_format=OracleVectorFormat.FLOAT64,
        batch_size=1000,
        mutate_on_duplicate=False,
        embedding_provider_config=None,
    )


def test_add_documents_operator_requires_source():
    with pytest.raises(ValueError, match="Either documents or documents_callable must be supplied"):
        OracleAddVectorDocumentsOperator(task_id="t", table_name="docs")


@mock.patch("airflow.providers.oracle.operators.oracle_vector.OracleVectorHook")
def test_search_operator_returns_serializable_results(mock_hook_class):
    mock_hook_class.return_value.similarity_search_by_vector.return_value = [
        OracleVectorSearchResult(id="d1", text="hello", metadata={"source": "unit"}, distance=0.1)
    ]
    op = OracleVectorSearchOperator(
        task_id="t",
        table_name="docs",
        embedding=[1, 2, 3],
        embedding_format=OracleVectorFormat.FLOAT64,
    )
    assert op.execute({}) == [
        {"id": "d1", "text": "hello", "metadata": {"source": "unit"}, "distance": 0.1, "embedding": None}
    ]
    mock_hook_class.return_value.similarity_search_by_vector.assert_called_once_with(
        table_name="docs",
        embedding=[1, 2, 3],
        k=4,
        distance=mock.ANY,
        filter=None,
        id_column="id",
        text_column="text",
        metadata_column="metadata",
        embedding_column="embedding",
        embedding_format=OracleVectorFormat.FLOAT64,
        include_score=True,
        include_embedding=False,
    )


@mock.patch("airflow.providers.oracle.operators.oracle_vector.OracleVectorHook")
def test_search_operator_serializes_sparse_embeddings(mock_hook_class):
    mock_hook_class.return_value.similarity_search_by_vector.return_value = [
        OracleVectorSearchResult(
            id="d1",
            text="hello",
            metadata={"source": "unit"},
            embedding=vector.oracledb.SparseVector(5, [1, 3], array("f", [4.0, 5.0])),
        )
    ]
    op = OracleVectorSearchOperator(task_id="t", table_name="docs", embedding=[1, 2, 3])

    assert op.execute({}) == [
        {
            "id": "d1",
            "text": "hello",
            "metadata": {"source": "unit"},
            "distance": None,
            "embedding": {"num_dimensions": 5, "indices": [1, 3], "values": [4.0, 5.0]},
        }
    ]


@mock.patch("airflow.providers.oracle.operators.oracle_vector.OracleVectorHook")
def test_create_index_operator_calls_hook(mock_hook_class):
    op = OracleCreateVectorIndexOperator(
        task_id="t",
        table_name="docs",
        index_name="docs_idx",
        index_type="HNSW",
        distance="COSINE",
    )
    op.execute({})
    mock_hook_class.return_value.create_vector_index.assert_called_once()


@mock.patch("airflow.providers.oracle.operators.oracle_vector.OracleVectorHook")
def test_delete_operator_calls_hook(mock_hook_class):
    mock_hook_class.return_value.delete_documents.return_value = 2
    op = OracleDeleteVectorDocumentsOperator(task_id="t", table_name="docs", ids=["d1", "d2"])
    assert op.execute({}) == 2
    mock_hook_class.return_value.delete_documents.assert_called_once_with(
        table_name="docs", ids=["d1", "d2"], id_column="id"
    )
