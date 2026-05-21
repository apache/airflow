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
"""Operator for document chunking and embedding via LlamaIndex."""

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


class LlamaIndexEmbeddingOperator(BaseOperator):
    """
    Chunk documents and produce embedding vectors using LlamaIndex.

    Bridges document loading (e.g.
    :class:`~airflow.providers.common.ai.operators.document_loader.DocumentLoaderOperator`
    output) and vector storage (pgvector, Pinecone, Weaviate, ...). Input is
    ``list[dict]`` with ``text`` and ``metadata`` keys; output includes the
    embedding vectors ready for downstream storage ingest.

    The operator passes the embedding model **directly** to
    ``VectorStoreIndex(..., embed_model=...)`` -- it does not mutate
    LlamaIndex's global ``Settings`` singleton, so concurrent tasks in the
    same worker don't race on shared state.

    :param documents: List of dicts with ``text`` and ``metadata`` keys,
        typically from ``DocumentLoaderOperator`` or a ``@task``. Bind via
        ``my_loader.output`` (XCom direct), **not** via Jinja -- ``list[dict]``
        does not survive Jinja stringification.
    :param embed_model: Either:

        * a string model name (e.g. ``"text-embedding-3-small"``) -- the
          operator constructs an :class:`~.LlamaIndexHook`-backed
          ``OpenAIEmbedding`` from ``llm_conn_id``, or
        * a pre-built ``BaseEmbedding`` instance -- bypass the hook
          entirely for non-OpenAI vendors (e.g.
          ``CohereEmbedding(...)``, ``BedrockEmbedding(...)``).

    :param llm_conn_id: Airflow connection ID for the embedding API. Used
        only when ``embed_model`` is a string (or omitted entirely, falling
        back to ``extra["embed_model"]`` on the connection).
    :param chunk_size: Chunk size for the sentence splitter.
    :param chunk_overlap: Overlap between chunks.
    :param persist_dir: Optional path to persist the index. Accepts local
        paths and storage URIs (``s3://``, ``gs://``, ...) resolved via
        :class:`~airflow.sdk.ObjectStoragePath`.
    :param persist_conn_id: Airflow connection ID for cloud-storage
        credentials when ``persist_dir`` is a URI.
    """

    template_fields: Sequence[str] = (
        "llm_conn_id",
        "persist_dir",
        "persist_conn_id",
    )

    def __init__(
        self,
        *,
        documents: list[dict[str, Any]],
        embed_model: str | BaseEmbedding | None = None,
        llm_conn_id: str = "llamaindex_default",
        chunk_size: int = 512,
        chunk_overlap: int = 50,
        persist_dir: str | None = None,
        persist_conn_id: str | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.documents = documents
        self.embed_model = embed_model
        self.llm_conn_id = llm_conn_id
        self.chunk_size = chunk_size
        self.chunk_overlap = chunk_overlap
        self.persist_dir = persist_dir
        self.persist_conn_id = persist_conn_id

    def execute(self, context: Context) -> dict[str, Any]:
        try:
            from llama_index.core import Document, StorageContext, VectorStoreIndex
            from llama_index.core.node_parser import SentenceSplitter
        except ImportError as e:
            raise AirflowOptionalProviderFeatureException(e)

        embed_model = self._resolve_embed_model()

        llama_docs = [
            Document(text=doc["text"], metadata=doc.get("metadata", {})) for doc in self.documents
        ]

        splitter = SentenceSplitter(chunk_size=self.chunk_size, chunk_overlap=self.chunk_overlap)
        nodes = splitter.get_nodes_from_documents(llama_docs)
        self.log.info("Split %d documents into %d chunks", len(llama_docs), len(nodes))

        # ``VectorStoreIndex(...)`` populates each node's ``.embedding`` as a
        # side effect of building the index; capture the index so the
        # variable isn't discarded (also lets future enhancements query it
        # before persistence).
        index = VectorStoreIndex(nodes, embed_model=embed_model, show_progress=False)

        if self.persist_dir:
            self._persist(index)

        chunks = [
            {
                "text": node.text,
                "metadata": node.metadata,
                # ``node.embedding`` is populated by ``VectorStoreIndex`` for
                # every node since we forced an in-memory build above.
                "vector": node.embedding,
            }
            for node in nodes
        ]

        return {
            "document_count": len(llama_docs),
            "chunk_count": len(nodes),
            "persist_dir": self.persist_dir,
            "chunks": chunks,
        }

    def _resolve_embed_model(self) -> BaseEmbedding:
        """
        Return a ready-to-use ``BaseEmbedding``.

        If ``embed_model`` is a string or ``None``, build one via
        ``LlamaIndexHook`` (OpenAI from the configured Airflow connection).
        Anything else is treated as a pre-built ``BaseEmbedding`` instance
        (user brought their own) and returned as-is. Avoids
        ``isinstance(.., BaseEmbedding)`` so the check doesn't trigger an
        otherwise-unnecessary ``llama_index`` import.
        """
        if self.embed_model is None or isinstance(self.embed_model, str):
            from airflow.providers.common.ai.hooks.llamaindex import LlamaIndexHook

            return LlamaIndexHook(
                llm_conn_id=self.llm_conn_id,
                embed_model=self.embed_model,
            ).get_embedding_model()
        return self.embed_model

    def _persist(self, index: Any) -> None:
        """Persist the index to ``persist_dir``; cloud URIs go through ObjectStoragePath."""
        if "://" in self.persist_dir:  # type: ignore[operator]
            from airflow.sdk import ObjectStoragePath

            target = ObjectStoragePath(self.persist_dir, conn_id=self.persist_conn_id)
            target.mkdir(parents=True, exist_ok=True)
            index.storage_context.persist(persist_dir=str(target), fs=target.fs)
        else:
            import os

            os.makedirs(self.persist_dir, exist_ok=True)  # type: ignore[arg-type]
            index.storage_context.persist(persist_dir=self.persist_dir)
        self.log.info("Index persisted to %s", self.persist_dir)
