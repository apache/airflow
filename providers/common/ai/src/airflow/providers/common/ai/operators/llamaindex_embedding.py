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
from typing import TYPE_CHECKING, Any

from airflow.providers.common.compat.sdk import BaseOperator

if TYPE_CHECKING:
    from airflow.sdk import Context


class EmbeddingOperator(BaseOperator):
    """
    Chunk documents and produce embedding vectors using LlamaIndex.

    Bridges document loading (Airflow provider hooks returning text) and
    vector storage (pgvector, Pinecone, Weaviate ingest operators). Input
    is ``list[dict]`` with ``text`` and ``metadata`` keys; output includes
    the embedding vectors ready for downstream storage.

    :param documents: List of dicts with ``text`` and ``metadata`` keys,
        typically from ``DocumentLoaderOperator`` or a ``@task``.
    :param llm_conn_id: Airflow connection ID for the embedding API.
    :param embed_model: Embedding model name (default: ``text-embedding-3-small``).
    :param chunk_size: Chunk size for the sentence splitter (default: 512).
    :param chunk_overlap: Overlap between chunks (default: 50).
    :param persist_dir: Optional directory path to persist the LlamaIndex
        index for later retrieval.
    """

    template_fields: Sequence[str] = ("documents", "llm_conn_id", "persist_dir")

    def __init__(
        self,
        *,
        documents: list[dict[str, Any]],
        llm_conn_id: str = "pydanticai_default",
        embed_model: str = "text-embedding-3-small",
        chunk_size: int = 512,
        chunk_overlap: int = 50,
        persist_dir: str | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.documents = documents
        self.llm_conn_id = llm_conn_id
        self.embed_model = embed_model
        self.chunk_size = chunk_size
        self.chunk_overlap = chunk_overlap
        self.persist_dir = persist_dir

    def execute(self, context: Context) -> dict[str, Any]:
        from llama_index.core import Document, StorageContext, VectorStoreIndex
        from llama_index.core.node_parser import SentenceSplitter

        from airflow.providers.common.ai.hooks.llamaindex import LlamaIndexHook

        hook = LlamaIndexHook(llm_conn_id=self.llm_conn_id, embed_model=self.embed_model)
        hook.configure_settings()

        llama_docs = [Document(text=doc["text"], metadata=doc.get("metadata", {})) for doc in self.documents]

        splitter = SentenceSplitter(chunk_size=self.chunk_size, chunk_overlap=self.chunk_overlap)
        nodes = splitter.get_nodes_from_documents(llama_docs)
        self.log.info("Split %d documents into %d chunks", len(llama_docs), len(nodes))

        storage_context = StorageContext.from_defaults()
        VectorStoreIndex(nodes, storage_context=storage_context, show_progress=False)

        if self.persist_dir:
            os.makedirs(self.persist_dir, exist_ok=True)
            storage_context.persist(persist_dir=self.persist_dir)
            self.log.info("Index persisted to %s", self.persist_dir)

        chunks = []
        for node in nodes:
            chunk: dict[str, Any] = {
                "text": node.text,
                "metadata": node.metadata,
            }
            if node.embedding:
                chunk["vector"] = node.embedding
            chunks.append(chunk)

        return {
            "document_count": len(llama_docs),
            "chunk_count": len(nodes),
            "persist_dir": self.persist_dir,
            "chunks": chunks,
        }
