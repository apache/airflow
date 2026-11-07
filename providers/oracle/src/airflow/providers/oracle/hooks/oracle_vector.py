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

"""Oracle AI Vector Search hook."""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from functools import partial
from typing import Any, cast
from uuid import uuid4

from airflow.providers.oracle.hooks.oracle import OracleHook
from airflow.providers.oracle.vector import (
    OracleVectorDistance,
    OracleVectorFormat,
    OracleVectorIndexType,
    VectorInput,
    VectorValue,
    _coerce_json_dict,
    _ensure_json_serializable,
    _materialize_iterable,
    _normalize_distance,
    _normalize_index_type,
    _normalize_vector_format,
    _OracleJsonFilterBuilder,
    _quote_identifier,
    _validate_positive_int,
    _vector_to_bind_value,
)


@dataclass(frozen=True)
class OracleVectorDocument:
    """Document payload accepted by OracleVectorHook ingestion APIs."""

    id: str
    text: str
    metadata: dict[str, Any] | None = None
    embedding: VectorInput | None = None


@dataclass(frozen=True)
class OracleVectorSearchResult:
    """Oracle vector search result."""

    id: str | None
    text: str
    metadata: dict[str, Any]
    distance: float | None = None
    embedding: VectorValue | None = None


class OracleVectorHook(OracleHook):
    """
    Hook for Oracle AI Vector Search operations.

    The hook is connection-scoped. Table names are explicit on methods so
    Airflow operators can expose them as templated task arguments.
    """

    def __init__(self, *args: Any, oracle_conn_id: str = "oracle_default", **kwargs: Any) -> None:
        super().__init__(*args, oracle_conn_id=oracle_conn_id, fetch_lobs=False, **kwargs)

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------
    def _insert_or_merge_sql(
        self,
        *,
        table_name: str,
        id_column: str,
        text_column: str,
        metadata_column: str,
        embedding_column: str,
        mutate_on_duplicate: bool,
    ) -> str:
        quoted_table_name = _quote_identifier(table_name, allow_schema=True)
        quoted_id_column = _quote_identifier(id_column)
        quoted_text_column = _quote_identifier(text_column)
        quoted_metadata_column = _quote_identifier(metadata_column)
        quoted_embedding_column = _quote_identifier(embedding_column)
        if mutate_on_duplicate:
            return f"""
                MERGE INTO {quoted_table_name} tgt
                USING (
                    SELECT :id AS id_value, :text AS text_value, :metadata AS metadata_value, :embedding AS embedding_value
                    FROM dual
                ) src
                ON (tgt.{quoted_id_column} = src.id_value)
                WHEN MATCHED THEN UPDATE SET
                    tgt.{quoted_text_column} = src.text_value,
                    tgt.{quoted_metadata_column} = src.metadata_value,
                    tgt.{quoted_embedding_column} = src.embedding_value
                WHEN NOT MATCHED THEN INSERT (
                    {quoted_id_column}, {quoted_text_column}, {quoted_metadata_column}, {quoted_embedding_column}
                )
                VALUES (src.id_value, src.text_value, src.metadata_value, src.embedding_value)
            """
        return f"""
            INSERT INTO {quoted_table_name} (
                {quoted_id_column}, {quoted_text_column}, {quoted_metadata_column}, {quoted_embedding_column}
            )
            VALUES (:id, :text, :metadata, :embedding)
        """

    def _execute_rows(
        self,
        sql: str,
        rows: Sequence[Mapping[str, Any]],
        *,
        batch_size: int,
    ) -> int:
        if not rows:
            return 0
        total = 0
        with self.get_conn() as conn:
            with conn.cursor() as cursor:
                # Keep manual batching while Airflow supports python-oracledb versions before 3.4,
                # which added Cursor.executemany(batch_size=...).
                for start in range(0, len(rows), batch_size):
                    batch = list(rows[start : start + batch_size])
                    cursor.executemany(sql, batch)
                    if cursor.rowcount > 0:
                        total += cursor.rowcount
                    else:
                        total += len(batch)
            conn.commit()
        return total

    def _row_to_result(
        self,
        *row: Any,
        include_score: bool,
        include_embedding: bool,
    ) -> OracleVectorSearchResult:
        idx = 0
        doc_id = None if row[idx] is None else str(row[idx])
        idx += 1
        text = str(row[idx])
        idx += 1
        metadata = _coerce_json_dict(row[idx])
        idx += 1
        distance = None
        if include_score:
            distance = None if row[idx] is None else float(row[idx])
            idx += 1
        embedding = None
        if include_embedding:
            embedding = row[idx]
        return OracleVectorSearchResult(
            id=doc_id, text=text, metadata=metadata, distance=distance, embedding=embedding
        )

    # ------------------------------------------------------------------
    # Table management
    # ------------------------------------------------------------------
    def create_vector_table(
        self,
        *,
        table_name: str,
        embedding_dimension: int,
        id_column: str = "id",
        text_column: str = "text",
        metadata_column: str = "metadata",
        embedding_column: str = "embedding",
        embedding_format: OracleVectorFormat | str = OracleVectorFormat.FLOAT32,
        sparse: bool = False,
        if_not_exists: bool = True,
        overwrite: bool = False,
    ) -> None:
        """Create an Oracle vector table."""
        _validate_positive_int("embedding_dimension", embedding_dimension)
        embedding_format = _normalize_vector_format(embedding_format)
        if sparse and embedding_format not in {
            OracleVectorFormat.FLOAT32,
            OracleVectorFormat.FLOAT64,
            OracleVectorFormat.INT8,
        }:
            raise ValueError("Sparse vectors support FLOAT32, FLOAT64, and INT8 formats only")
        quoted_table_name = _quote_identifier(table_name, allow_schema=True)
        if overwrite and if_not_exists:
            raise ValueError("overwrite=True cannot be combined with if_not_exists=True")
        if overwrite:
            self.run(f"DROP TABLE IF EXISTS {quoted_table_name}")

        quoted_id_column = _quote_identifier(id_column)
        quoted_text_column = _quote_identifier(text_column)
        quoted_metadata_column = _quote_identifier(metadata_column)
        quoted_embedding_column = _quote_identifier(embedding_column)
        if_not_exists_sql = " IF NOT EXISTS" if if_not_exists else ""
        sparse_sql = ", SPARSE" if sparse else ""
        sql = f"""CREATE TABLE{if_not_exists_sql} {quoted_table_name} (
            {quoted_id_column} VARCHAR2(512) PRIMARY KEY,
            {quoted_text_column} CLOB NOT NULL,
            {quoted_metadata_column} JSON,
            {quoted_embedding_column} VECTOR({embedding_dimension}, {embedding_format.value}{sparse_sql}) NOT NULL
        )"""
        self.run(sql)

    # ------------------------------------------------------------------
    # Ingestion and retrieval
    # ------------------------------------------------------------------
    def add_texts(
        self,
        *,
        table_name: str,
        texts: Iterable[str],
        embeddings: Iterable[VectorInput] | None = None,
        metadatas: Iterable[dict[str, Any] | None] | None = None,
        ids: Iterable[str] | None = None,
        id_column: str = "id",
        text_column: str = "text",
        metadata_column: str = "metadata",
        embedding_column: str = "embedding",
        embedding_format: OracleVectorFormat | str = OracleVectorFormat.FLOAT32,
        batch_size: int = 1000,
        mutate_on_duplicate: bool = False,
        embedding_provider_config: dict[str, Any] | None = None,
    ) -> list[str]:
        """Insert or upsert texts and embeddings into a vector table."""
        _validate_positive_int("batch_size", batch_size)
        text_list = _materialize_iterable("texts", texts) or []
        embedding_list = _materialize_iterable("embeddings", embeddings)
        metadata_list = _materialize_iterable("metadatas", metadatas)
        id_list = _materialize_iterable("ids", ids)
        embedding_format = _normalize_vector_format(embedding_format)

        if embedding_provider_config is not None:
            raise NotImplementedError(
                "DB-side embedding generation is not yet supported. Pass client-side embeddings instead."
            )
        if embedding_list is None:
            raise ValueError("embeddings is required for client-side ingestion")

        if id_list is None:
            id_list = [str(uuid4()) for _ in text_list]
        if metadata_list is None:
            metadata_list = [{} for _ in text_list]

        rows = [
            {
                "id": str(id_),
                "text": str(text),
                "metadata": _ensure_json_serializable(metadata),
                "embedding": _vector_to_bind_value(embedding, embedding_format),
            }
            for text, embedding, metadata, id_ in zip(
                text_list, embedding_list, metadata_list, id_list, strict=True
            )
        ]
        self._execute_rows(
            self._insert_or_merge_sql(
                table_name=table_name,
                id_column=id_column,
                text_column=text_column,
                metadata_column=metadata_column,
                embedding_column=embedding_column,
                mutate_on_duplicate=mutate_on_duplicate,
            ),
            rows,
            batch_size=batch_size,
        )
        return [str(item) for item in id_list]

    def add_documents(
        self,
        *,
        table_name: str,
        documents: Iterable[OracleVectorDocument | Mapping[str, Any]],
        id_column: str = "id",
        text_column: str = "text",
        metadata_column: str = "metadata",
        embedding_column: str = "embedding",
        embedding_format: OracleVectorFormat | str = OracleVectorFormat.FLOAT32,
        batch_size: int = 1000,
        mutate_on_duplicate: bool = False,
        embedding_provider_config: dict[str, Any] | None = None,
    ) -> list[str]:
        """Insert or upsert structured document objects into a vector table."""
        docs = list(documents)
        texts: list[str] = []
        embeddings: list[VectorInput | None] = []
        metadatas: list[dict[str, Any] | None] = []
        ids: list[str] = []
        for doc in docs:
            if isinstance(doc, OracleVectorDocument):
                ids.append(doc.id)
                texts.append(doc.text)
                metadatas.append(doc.metadata)
                embeddings.append(doc.embedding)
            elif isinstance(doc, Mapping):
                ids.append(str(doc["id"]))
                texts.append(str(doc["text"]))
                metadatas.append(doc.get("metadata"))
                embeddings.append(doc.get("embedding"))
            else:
                raise ValueError("Each document must be OracleVectorDocument or a mapping")
        if any(item is None for item in embeddings) and embedding_provider_config is None:
            raise ValueError(
                "Each document must include an embedding when embedding_provider_config is not supplied"
            )
        return self.add_texts(
            table_name=table_name,
            texts=texts,
            embeddings=cast("Iterable[VectorInput]", embeddings),
            metadatas=metadatas,
            ids=ids,
            id_column=id_column,
            text_column=text_column,
            metadata_column=metadata_column,
            embedding_column=embedding_column,
            embedding_format=embedding_format,
            batch_size=batch_size,
            mutate_on_duplicate=mutate_on_duplicate,
            embedding_provider_config=embedding_provider_config,
        )

    def delete_documents(self, *, table_name: str, ids: Sequence[str], id_column: str = "id") -> int:
        """Delete documents by id and return row count."""
        if not ids:
            return 0
        quoted_table_name = _quote_identifier(table_name, allow_schema=True)
        quoted_id_column = _quote_identifier(id_column)
        rows = [{"id": str(item)} for item in ids]
        sql = f"DELETE FROM {quoted_table_name} WHERE {quoted_id_column} = :id"
        return self._execute_rows(sql, rows, batch_size=1000)

    def get_by_ids(
        self,
        *,
        table_name: str,
        ids: Sequence[str],
        id_column: str = "id",
        text_column: str = "text",
        metadata_column: str = "metadata",
        include_embedding: bool = False,
        embedding_column: str = "embedding",
    ) -> list[OracleVectorSearchResult]:
        """Fetch documents by id."""
        if not ids:
            return []
        quoted_table_name = _quote_identifier(table_name, allow_schema=True)
        quoted_id_column = _quote_identifier(id_column)
        quoted_text_column = _quote_identifier(text_column)
        quoted_metadata_column = _quote_identifier(metadata_column)
        quoted_embedding_column = _quote_identifier(embedding_column)
        placeholders = ", ".join(f":id_{i}" for i in range(len(ids)))
        binds = {f"id_{i}": str(value) for i, value in enumerate(ids)}
        select_columns = [
            quoted_id_column,
            quoted_text_column,
            quoted_metadata_column,
        ]
        if include_embedding:
            select_columns.append(quoted_embedding_column)
        sql = (
            f"SELECT {', '.join(select_columns)} FROM {quoted_table_name} "
            f"WHERE {quoted_id_column} IN ({placeholders})"
        )
        with self.get_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql, binds)
                cursor.rowfactory = partial(
                    self._row_to_result,
                    include_score=False,
                    include_embedding=include_embedding,
                )
                return list(cursor)

    # ------------------------------------------------------------------
    # Search
    # ------------------------------------------------------------------
    def similarity_search(
        self,
        *,
        table_name: str,
        query: str,
        embedding: VectorInput | None = None,
        embedding_provider_config: dict[str, Any] | None = None,
        k: int = 4,
        distance: OracleVectorDistance | str = OracleVectorDistance.EUCLIDEAN,
        filter: dict[str, Any] | None = None,
        id_column: str = "id",
        text_column: str = "text",
        metadata_column: str = "metadata",
        embedding_column: str = "embedding",
        embedding_format: OracleVectorFormat | str = OracleVectorFormat.FLOAT32,
        include_score: bool = False,
        include_embedding: bool = False,
    ) -> list[OracleVectorSearchResult]:
        """
        Search by query text when the caller supplies the query embedding.

        DB-side embedding generation is not yet supported, so the caller must provide the embedding used for the search. The
        query text is retained for API compatibility and logging.
        """
        if embedding_provider_config is not None:
            raise NotImplementedError(
                "DB-side query embedding generation is not yet supported. "
                "Pass a client-side query embedding instead."
            )
        if embedding is None:
            raise ValueError("embedding is required for similarity_search")
        self.log.debug("Running vector search for query text of length %s", len(query or ""))
        return self.similarity_search_by_vector(
            table_name=table_name,
            embedding=embedding,
            k=k,
            distance=distance,
            filter=filter,
            id_column=id_column,
            text_column=text_column,
            metadata_column=metadata_column,
            embedding_column=embedding_column,
            embedding_format=embedding_format,
            include_score=include_score,
            include_embedding=include_embedding,
        )

    def similarity_search_by_vector(
        self,
        *,
        table_name: str,
        embedding: VectorInput,
        k: int = 4,
        distance: OracleVectorDistance | str = OracleVectorDistance.EUCLIDEAN,
        filter: dict[str, Any] | None = None,
        id_column: str = "id",
        text_column: str = "text",
        metadata_column: str = "metadata",
        embedding_column: str = "embedding",
        embedding_format: OracleVectorFormat | str = OracleVectorFormat.FLOAT32,
        include_score: bool = False,
        include_embedding: bool = False,
    ) -> list[OracleVectorSearchResult]:
        """Run Oracle VECTOR_DISTANCE search by query vector."""
        _validate_positive_int("k", k)
        distance = _normalize_distance(distance)
        quoted_table_name = _quote_identifier(table_name, allow_schema=True)
        quoted_id_column = _quote_identifier(id_column)
        quoted_text_column = _quote_identifier(text_column)
        quoted_metadata_column = _quote_identifier(metadata_column)
        quoted_embedding_column = _quote_identifier(embedding_column)
        filter_builder = _OracleJsonFilterBuilder(quoted_metadata_column)
        where_clause, filter_binds = filter_builder.build(filter)
        where_sql = f"WHERE {where_clause}" if where_clause else ""
        score_sql = f"VECTOR_DISTANCE({quoted_embedding_column}, :query_embedding, {distance.value})"
        select_columns = [
            quoted_id_column,
            quoted_text_column,
            quoted_metadata_column,
        ]
        if include_score:
            select_columns.append(f"{score_sql} AS distance")
        if include_embedding:
            select_columns.append(quoted_embedding_column)
        sql = f"""
            SELECT {", ".join(select_columns)}
            FROM {quoted_table_name}
            {where_sql}
            ORDER BY {score_sql}
            FETCH FIRST :k ROWS ONLY
        """
        binds = {
            "query_embedding": _vector_to_bind_value(embedding, embedding_format),
            "k": int(k),
            **filter_binds,
        }
        with self.get_conn() as conn:
            with conn.cursor() as cursor:
                # self.log.info("Running Oracle vector search SQL:\n%s", sql)
                cursor.execute(sql, binds)
                cursor.rowfactory = partial(
                    self._row_to_result,
                    include_score=include_score,
                    include_embedding=include_embedding,
                )
                return list(cursor)

    # ------------------------------------------------------------------
    # Index management
    # ------------------------------------------------------------------
    def create_vector_index(
        self,
        *,
        table_name: str,
        index_name: str,
        index_type: OracleVectorIndexType | str = OracleVectorIndexType.HNSW,
        embedding_column: str = "embedding",
        distance: OracleVectorDistance | str = OracleVectorDistance.EUCLIDEAN,
        accuracy: int | None = None,
        parallel: int | None = None,
        neighbors: int | None = None,
        ef_construction: int | None = None,
        neighbor_partitions: int | None = None,
        if_not_exists: bool = True,
    ) -> None:
        """Create an HNSW or IVF vector index."""
        index_type = _normalize_index_type(index_type)
        distance = _normalize_distance(distance)
        _validate_positive_int("accuracy", accuracy, minimum=1, maximum=100)
        _validate_positive_int("parallel", parallel)
        _validate_positive_int("neighbors", neighbors, minimum=2, maximum=2048)
        _validate_positive_int("ef_construction", ef_construction, minimum=1, maximum=65535)
        _validate_positive_int("neighbor_partitions", neighbor_partitions, minimum=1, maximum=10_000_000)
        if index_type == OracleVectorIndexType.HNSW and neighbor_partitions is not None:
            raise ValueError("neighbor_partitions is only valid for IVF indexes")
        if index_type == OracleVectorIndexType.IVF and (neighbors is not None or ef_construction is not None):
            raise ValueError("neighbors and ef_construction are only valid for HNSW indexes")

        quoted_index_name = _quote_identifier(index_name)
        quoted_table_name = _quote_identifier(table_name, allow_schema=True)
        quoted_embedding_column = _quote_identifier(embedding_column)
        if_not_exists_sql = " IF NOT EXISTS" if if_not_exists else ""
        parts = [
            f"CREATE VECTOR INDEX{if_not_exists_sql} {quoted_index_name} "
            f"ON {quoted_table_name} ({quoted_embedding_column})"
        ]
        if index_type == OracleVectorIndexType.HNSW:
            parts.append("ORGANIZATION INMEMORY NEIGHBOR GRAPH")
        else:
            parts.append("ORGANIZATION NEIGHBOR PARTITIONS")
        if accuracy is not None:
            parts.append(f"WITH TARGET ACCURACY {accuracy}")
        parts.append(f"DISTANCE {distance.value}")
        parameters: list[str] = [f"type {index_type.value}"]
        if neighbors is not None:
            parameters.append(f"neighbors {neighbors}")
        if ef_construction is not None:
            parameters.append(f"efConstruction {ef_construction}")
        if neighbor_partitions is not None:
            parameters.append(f"neighbor partitions {neighbor_partitions}")
        parts.append("PARAMETERS (" + ", ".join(parameters) + ")")
        if parallel is not None:
            parts.append(f"PARALLEL {parallel}")
        self.run("\n".join(parts))

    def drop_vector_index(self, *, index_name: str) -> None:
        """Drop a vector index."""
        self.run(f"DROP INDEX IF EXISTS {_quote_identifier(index_name)}")

    # ------------------------------------------------------------------
    # APIs planned for a future release.
    # ------------------------------------------------------------------
    def max_marginal_relevance_search_by_vector(self, **_: Any) -> list[OracleVectorSearchResult]:
        raise NotImplementedError("MMR search is deferred to a follow-up PR")

    def load_onnx_model(self, **_: Any) -> None:
        raise NotImplementedError("ONNX model loading is not yet supported")

    def drop_onnx_model(self, **_: Any) -> None:
        raise NotImplementedError("ONNX model lifecycle support is not yet supported")

    def generate_embedding(self, **_: Any) -> list[float]:
        raise NotImplementedError("DB-side embedding generation is not yet supported")

    def generate_embeddings(self, **_: Any) -> list[list[float]]:
        raise NotImplementedError("DB-side embedding generation is not yet supported")
