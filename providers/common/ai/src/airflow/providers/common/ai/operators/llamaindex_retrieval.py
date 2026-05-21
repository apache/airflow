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
"""Operator for semantic retrieval via a persisted LlamaIndex index."""

from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING, Any

from airflow.providers.common.compat.sdk import (
    AirflowOptionalProviderFeatureException,
    BaseOperator,
)

if TYPE_CHECKING:
    from airflow.sdk import Context
    from llama_index.core.base.embeddings.base import BaseEmbedding


class RetrievalOperator(BaseOperator):
    """
    Retrieve relevant document chunks from a persisted LlamaIndex index.

    Loads a previously persisted vector store index (from
    ``EmbeddingOperator(persist_dir=...)``) and performs similarity search
    against the provided query. Output is a list of chunks with text,
    score, metadata, and node id, ready for downstream synthesis via
    :class:`~airflow.providers.common.ai.operators.llm.LLMOperator`.

    Passes the embedding model **directly** to
    ``load_index_from_storage(..., embed_model=...)`` -- no LlamaIndex
    ``Settings`` mutation, so concurrent tasks in the same worker don't
    race on shared state.

    :param query: The query string. Supports Jinja templating.
    :param index_persist_dir: Local path or storage URI (``s3://``,
        ``gs://``, ...) pointing at the persisted LlamaIndex index.
        Resolved via :class:`~airflow.sdk.ObjectStoragePath` when a URI
        scheme is present.
    :param persist_conn_id: Airflow connection ID for cloud-storage
        credentials when ``index_persist_dir`` is a URI.
    :param embed_model: Either:

        * a string model name (e.g. ``"text-embedding-3-small"``) -- the
          operator constructs an :class:`~.LlamaIndexHook`-backed
          ``OpenAIEmbedding`` from ``llm_conn_id``, or
        * a pre-built ``BaseEmbedding`` instance -- bypass the hook for
          non-OpenAI vendors. Must match the embedding model used when
          the index was originally built.

    :param llm_conn_id: Airflow connection ID for the embedding API. Used
        only when ``embed_model`` is a string (or omitted entirely).
    :param top_k: Number of top results to retrieve.
    """

    template_fields: Sequence[str] = (
        "query",
        "index_persist_dir",
        "persist_conn_id",
        "llm_conn_id",
    )

    def __init__(
        self,
        *,
        query: str,
        index_persist_dir: str,
        persist_conn_id: str | None = None,
        embed_model: str | BaseEmbedding | None = None,
        llm_conn_id: str = "llamaindex_default",
        top_k: int = 5,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.query = query
        self.index_persist_dir = index_persist_dir
        self.persist_conn_id = persist_conn_id
        self.embed_model = embed_model
        self.llm_conn_id = llm_conn_id
        self.top_k = top_k

    def execute(self, context: Context) -> dict[str, Any]:
        try:
            from llama_index.core import StorageContext, load_index_from_storage
        except ImportError as e:
            raise AirflowOptionalProviderFeatureException(e)

        embed_model = self._resolve_embed_model()
        storage_context = self._open_storage_context(StorageContext)
        index = load_index_from_storage(storage_context, embed_model=embed_model)

        retriever = index.as_retriever(similarity_top_k=self.top_k)
        results = retriever.retrieve(self.query)
        self.log.info("Retrieved %d chunks for query: %s", len(results), self.query[:100])

        chunks = [
            {
                "text": node_with_score.node.get_content(),
                "score": node_with_score.score,
                "metadata": node_with_score.node.metadata,
                "node_id": node_with_score.node.node_id,
            }
            for node_with_score in results
        ]

        return {
            "query": self.query,
            "chunks": chunks,
        }

    def _resolve_embed_model(self) -> BaseEmbedding:
        """String / ``None`` -> hook; anything else -> pre-built instance."""
        if self.embed_model is None or isinstance(self.embed_model, str):
            from airflow.providers.common.ai.hooks.llamaindex import LlamaIndexHook

            return LlamaIndexHook(
                llm_conn_id=self.llm_conn_id,
                embed_model=self.embed_model,
            ).get_embedding_model()
        return self.embed_model

    def _open_storage_context(self, storage_context_cls: Any) -> Any:
        """Open a ``StorageContext`` from a local path or storage URI."""
        if "://" in self.index_persist_dir:
            from airflow.sdk import ObjectStoragePath

            source = ObjectStoragePath(self.index_persist_dir, conn_id=self.persist_conn_id)
            if not source.is_dir():
                raise FileNotFoundError(
                    f"Persisted LlamaIndex index not found at '{self.index_persist_dir}'. "
                    "Did you run EmbeddingOperator with the same persist_dir first?"
                )
            return storage_context_cls.from_defaults(
                persist_dir=str(source),
                fs=source.fs,
            )

        from pathlib import Path

        if not Path(self.index_persist_dir).is_dir():
            raise FileNotFoundError(
                f"Persisted LlamaIndex index not found at '{self.index_persist_dir}'. "
                "Did you run EmbeddingOperator with the same persist_dir first?"
            )
        return storage_context_cls.from_defaults(persist_dir=self.index_persist_dir)
