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

import os
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, cast

from airflow.providers.common.compat.sdk import (
    AirflowOptionalProviderFeatureException,
    BaseOperator,
)

if TYPE_CHECKING:
    from llama_index.core.base.embeddings.base import BaseEmbedding
    from llama_index.core.schema import TextNode

    from airflow.sdk import Context


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
        typically from ``DocumentLoaderOperator`` or a ``@task``. Templated,
        so binding via ``my_loader.output`` (XCom direct) resolves to the
        native ``list[dict]`` before ``execute`` runs.
    :param embed_model: Either:

        * a string model name (e.g. ``"text-embedding-3-small"``) -- the
          operator constructs an :class:`~.LlamaIndexHook`-backed
          ``OpenAIEmbedding`` from ``llm_conn_id`` / ``embed_conn_id``, or
        * a pre-built ``BaseEmbedding`` instance -- bypass the hook
          entirely for non-OpenAI vendors (e.g.
          ``CohereEmbedding(...)``, ``BedrockEmbedding(...)``).

        Templated, so it works with both literal strings and ``@task``
        output that builds a custom embedder.

    :param llm_conn_id: Airflow connection ID for the embedding API. Falls
        back to :attr:`LlamaIndexHook.default_conn_name` when ``None``.
    :param embed_conn_id: Optional separate Airflow connection ID for the
        embedding provider. Falls back to ``llm_conn_id`` when ``None``.
    :param chunk_size: Chunk size for the sentence splitter.
    :param chunk_overlap: Overlap between chunks.
    :param persist_dir: Optional path to persist the index. Accepts local
        paths and storage URIs (``s3://``, ``gs://``, ...) resolved via
        :class:`~airflow.sdk.ObjectStoragePath`.
    :param persist_conn_id: Airflow connection ID for cloud-storage
        credentials when ``persist_dir`` is a URI.
    """

    template_fields: Sequence[str] = (
        "documents",
        "embed_model",
        "llm_conn_id",
        "embed_conn_id",
        "persist_dir",
        "persist_conn_id",
    )

    def __init__(
        self,
        *,
        documents: list[dict[str, Any]],
        embed_model: str | BaseEmbedding | None = None,
        llm_conn_id: str | None = None,
        embed_conn_id: str | None = None,
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
        self.embed_conn_id = embed_conn_id
        self.chunk_size = chunk_size
        self.chunk_overlap = chunk_overlap
        self.persist_dir = persist_dir
        self.persist_conn_id = persist_conn_id

    def execute(self, context: Context) -> dict[str, Any]:
        try:
            from llama_index.core import Document, VectorStoreIndex
            from llama_index.core.node_parser import SentenceSplitter
        except ImportError as e:
            raise AirflowOptionalProviderFeatureException(e)

        embed_model = self._resolve_embed_model()

        llama_docs = [Document(text=doc["text"], metadata=doc.get("metadata", {})) for doc in self.documents]

        splitter = SentenceSplitter(chunk_size=self.chunk_size, chunk_overlap=self.chunk_overlap)
        nodes = splitter.get_nodes_from_documents(llama_docs)
        self.log.info("Split %d documents into %d chunks", len(llama_docs), len(nodes))

        # ``VectorStoreIndex(...)`` populates each node's ``.embedding`` as a
        # side effect of building the index; capture the index so the
        # variable isn't discarded.
        index = VectorStoreIndex(nodes, embed_model=embed_model, show_progress=False)

        if self.persist_dir:
            self._persist(index, self.persist_dir)

        # ``SentenceSplitter`` always returns ``TextNode`` instances, but the
        # base ``get_nodes_from_documents`` signature is typed as
        # ``list[BaseNode]`` (which has no ``.text``). Cast so mypy doesn't
        # flag the ``.text`` access; ``node.embedding`` is populated by
        # ``VectorStoreIndex`` for every node above.
        text_nodes = cast("list[TextNode]", nodes)
        chunks = [
            {
                "text": node.text,
                "metadata": node.metadata,
                "vector": node.embedding,
            }
            for node in text_nodes
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

        Three cases:

        * ``None`` or ``str`` -- build an ``OpenAIEmbedding`` via
          ``LlamaIndexHook`` (the framework's documented ``default``
          behaviour).
        * Has ``get_text_embedding`` / ``_get_query_embedding`` -- treat as
          a pre-built ``BaseEmbedding`` (duck-typed to avoid forcing a
          ``llama_index`` import here).
        * Anything else -- ``TypeError`` with a clear pointer.
        """
        if self.embed_model is None or isinstance(self.embed_model, str):
            from airflow.providers.common.ai.hooks.llamaindex import LlamaIndexHook

            return LlamaIndexHook(
                llm_conn_id=self.llm_conn_id,
                embed_conn_id=self.embed_conn_id,
                embed_model=self.embed_model,
            ).get_embedding_model()

        # ``BaseEmbedding`` always exposes these two methods (see
        # ``llama_index.core.base.embeddings.base``). Duck-typing avoids
        # importing ``llama_index`` here and also catches the case where an
        # unresolved ``XComArg`` slips through.
        if hasattr(self.embed_model, "get_text_embedding") and hasattr(
            self.embed_model, "_get_query_embedding"
        ):
            return self.embed_model

        raise TypeError(
            "embed_model must be a string model name, a LlamaIndex "
            f"``BaseEmbedding`` instance, or None. Got {type(self.embed_model).__name__!r}."
        )

    def _persist(self, index: Any, persist_dir: str) -> None:
        """Persist the index to ``persist_dir``; cloud URIs go through ObjectStoragePath."""
        if "://" in persist_dir:
            from airflow.sdk import ObjectStoragePath

            target = ObjectStoragePath(persist_dir, conn_id=self.persist_conn_id)
            target.mkdir(parents=True, exist_ok=True)
            # ``str(target)`` returns ``s3://<conn_id>@<bucket>/...`` when
            # ``conn_id`` is set (see ``task-sdk/.../io/path.py``), which
            # fsspec misinterprets. Pass the raw user URI as the path string
            # and the authenticated filesystem separately.
            index.storage_context.persist(persist_dir=persist_dir, fs=target.fs)
        else:
            os.makedirs(persist_dir, exist_ok=True)
            index.storage_context.persist(persist_dir=persist_dir)
        self.log.info("Index persisted to %s", persist_dir)
