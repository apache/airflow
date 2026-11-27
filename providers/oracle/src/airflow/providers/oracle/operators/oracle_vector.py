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

"""Oracle AI Vector Search operators."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from typing import TYPE_CHECKING, Any

from airflow.models import BaseOperator
from airflow.providers.oracle.hooks.oracle_vector import OracleVectorDocument, OracleVectorHook
from airflow.providers.oracle.vector import (
    OracleVectorDistance,
    OracleVectorFormat,
    OracleVectorIndexType,
    VectorInput,
    vector_to_result,
)

if TYPE_CHECKING:
    from airflow.providers.common.compat.sdk import Context


class OracleCreateVectorTableOperator(BaseOperator):
    """Create an Oracle vector table."""

    template_fields: Sequence[str] = ("table_name", "oracle_conn_id")

    def __init__(
        self,
        *,
        table_name: str,
        embedding_dimension: int,
        oracle_conn_id: str = "oracle_default",
        id_column: str = "id",
        text_column: str = "text",
        metadata_column: str = "metadata",
        embedding_column: str = "embedding",
        embedding_format: OracleVectorFormat | str = OracleVectorFormat.FLOAT32,
        sparse: bool = False,
        if_not_exists: bool = True,
        overwrite: bool = False,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.table_name = table_name
        self.embedding_dimension = embedding_dimension
        self.oracle_conn_id = oracle_conn_id
        self.id_column = id_column
        self.text_column = text_column
        self.metadata_column = metadata_column
        self.embedding_column = embedding_column
        self.embedding_format = embedding_format
        self.sparse = sparse
        self.if_not_exists = if_not_exists
        self.overwrite = overwrite

    def execute(self, context: Context) -> None:
        hook = OracleVectorHook(oracle_conn_id=self.oracle_conn_id)
        hook.create_vector_table(
            table_name=self.table_name,
            embedding_dimension=self.embedding_dimension,
            id_column=self.id_column,
            text_column=self.text_column,
            metadata_column=self.metadata_column,
            embedding_column=self.embedding_column,
            embedding_format=self.embedding_format,
            sparse=self.sparse,
            if_not_exists=self.if_not_exists,
            overwrite=self.overwrite,
        )


class OracleAddVectorDocumentsOperator(BaseOperator):
    """Add documents to an Oracle vector table."""

    template_fields: Sequence[str] = ("table_name", "oracle_conn_id")

    def __init__(
        self,
        *,
        table_name: str,
        documents: Sequence[OracleVectorDocument | Mapping[str, Any]] | None = None,
        documents_callable: Callable[[Context], Sequence[OracleVectorDocument | Mapping[str, Any]]]
        | None = None,
        oracle_conn_id: str = "oracle_default",
        id_column: str = "id",
        text_column: str = "text",
        metadata_column: str = "metadata",
        embedding_column: str = "embedding",
        embedding_format: OracleVectorFormat | str = OracleVectorFormat.FLOAT32,
        batch_size: int = 1000,
        mutate_on_duplicate: bool = False,
        embedding_provider_config: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        if documents is None and documents_callable is None:
            raise ValueError("Either documents or documents_callable must be supplied")
        if documents is not None and documents_callable is not None:
            raise ValueError("Only one of documents or documents_callable may be supplied")
        self.table_name = table_name
        self.documents = documents
        self.documents_callable = documents_callable
        self.oracle_conn_id = oracle_conn_id
        self.id_column = id_column
        self.text_column = text_column
        self.metadata_column = metadata_column
        self.embedding_column = embedding_column
        self.embedding_format = embedding_format
        self.batch_size = batch_size
        self.mutate_on_duplicate = mutate_on_duplicate
        self.embedding_provider_config = embedding_provider_config

    def execute(self, context: Context) -> list[str]:
        documents = self.documents_callable(context) if self.documents_callable else self.documents
        hook = OracleVectorHook(oracle_conn_id=self.oracle_conn_id)
        return hook.add_documents(
            table_name=self.table_name,
            documents=documents or [],
            id_column=self.id_column,
            text_column=self.text_column,
            metadata_column=self.metadata_column,
            embedding_column=self.embedding_column,
            embedding_format=self.embedding_format,
            batch_size=self.batch_size,
            mutate_on_duplicate=self.mutate_on_duplicate,
            embedding_provider_config=self.embedding_provider_config,
        )


class OracleVectorSearchOperator(BaseOperator):
    """Run Oracle vector similarity search and return XCom-safe dictionaries."""

    template_fields: Sequence[str] = ("table_name", "oracle_conn_id", "query")

    def __init__(
        self,
        *,
        table_name: str,
        query: str | None = None,
        embedding: VectorInput | None = None,
        oracle_conn_id: str = "oracle_default",
        embedding_provider_config: dict[str, Any] | None = None,
        k: int = 4,
        distance: OracleVectorDistance | str = OracleVectorDistance.EUCLIDEAN,
        filter: dict[str, Any] | None = None,
        id_column: str = "id",
        text_column: str = "text",
        metadata_column: str = "metadata",
        embedding_column: str = "embedding",
        embedding_format: OracleVectorFormat | str = OracleVectorFormat.FLOAT32,
        include_score: bool = True,
        include_embedding: bool = False,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        if query is None and embedding is None:
            raise ValueError("At least one of query or embedding must be supplied")
        self.table_name = table_name
        self.query = query
        self.embedding = embedding
        self.oracle_conn_id = oracle_conn_id
        self.embedding_provider_config = embedding_provider_config
        self.k = k
        self.distance = distance
        self.filter = filter
        self.id_column = id_column
        self.text_column = text_column
        self.metadata_column = metadata_column
        self.embedding_column = embedding_column
        self.embedding_format = embedding_format
        self.include_score = include_score
        self.include_embedding = include_embedding

    def execute(self, context: Context) -> list[dict[str, Any]]:
        hook = OracleVectorHook(oracle_conn_id=self.oracle_conn_id)
        if self.query is not None:
            results = hook.similarity_search(
                table_name=self.table_name,
                query=self.query,
                embedding=self.embedding,
                embedding_provider_config=self.embedding_provider_config,
                k=self.k,
                distance=self.distance,
                filter=self.filter,
                id_column=self.id_column,
                text_column=self.text_column,
                metadata_column=self.metadata_column,
                embedding_column=self.embedding_column,
                embedding_format=self.embedding_format,
                include_score=self.include_score,
                include_embedding=self.include_embedding,
            )
        else:
            results = hook.similarity_search_by_vector(
                table_name=self.table_name,
                embedding=self.embedding or [],
                k=self.k,
                distance=self.distance,
                filter=self.filter,
                id_column=self.id_column,
                text_column=self.text_column,
                metadata_column=self.metadata_column,
                embedding_column=self.embedding_column,
                embedding_format=self.embedding_format,
                include_score=self.include_score,
                include_embedding=self.include_embedding,
            )
        return [
            {
                "id": result.id,
                "text": result.text,
                "metadata": result.metadata,
                "distance": result.distance,
                "embedding": vector_to_result(result.embedding) if result.embedding is not None else None,
            }
            for result in results
        ]


class OracleCreateVectorIndexOperator(BaseOperator):
    """Create an Oracle HNSW or IVF vector index."""

    template_fields: Sequence[str] = ("table_name", "oracle_conn_id", "index_name")

    def __init__(
        self,
        *,
        table_name: str,
        index_name: str,
        oracle_conn_id: str = "oracle_default",
        index_type: OracleVectorIndexType | str = OracleVectorIndexType.HNSW,
        embedding_column: str = "embedding",
        distance: OracleVectorDistance | str = OracleVectorDistance.EUCLIDEAN,
        accuracy: int | None = None,
        parallel: int | None = None,
        neighbors: int | None = None,
        ef_construction: int | None = None,
        neighbor_partitions: int | None = None,
        if_not_exists: bool = True,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.table_name = table_name
        self.index_name = index_name
        self.oracle_conn_id = oracle_conn_id
        self.index_type = index_type
        self.embedding_column = embedding_column
        self.distance = distance
        self.accuracy = accuracy
        self.parallel = parallel
        self.neighbors = neighbors
        self.ef_construction = ef_construction
        self.neighbor_partitions = neighbor_partitions
        self.if_not_exists = if_not_exists

    def execute(self, context: Context) -> None:
        hook = OracleVectorHook(oracle_conn_id=self.oracle_conn_id)
        hook.create_vector_index(
            table_name=self.table_name,
            index_name=self.index_name,
            index_type=self.index_type,
            embedding_column=self.embedding_column,
            distance=self.distance,
            accuracy=self.accuracy,
            parallel=self.parallel,
            neighbors=self.neighbors,
            ef_construction=self.ef_construction,
            neighbor_partitions=self.neighbor_partitions,
            if_not_exists=self.if_not_exists,
        )


class OracleDeleteVectorDocumentsOperator(BaseOperator):
    """Delete vector documents by id."""

    template_fields: Sequence[str] = ("table_name", "oracle_conn_id")

    def __init__(
        self,
        *,
        table_name: str,
        ids: Sequence[str],
        oracle_conn_id: str = "oracle_default",
        id_column: str = "id",
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.table_name = table_name
        self.ids = ids
        self.oracle_conn_id = oracle_conn_id
        self.id_column = id_column

    def execute(self, context: Context) -> int:
        hook = OracleVectorHook(oracle_conn_id=self.oracle_conn_id)
        return hook.delete_documents(table_name=self.table_name, ids=self.ids, id_column=self.id_column)
