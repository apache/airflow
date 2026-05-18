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

from airflow.providers.common.compat.sdk import BaseOperator

if TYPE_CHECKING:
    from airflow.sdk import Context


class RetrievalOperator(BaseOperator):
    """
    Retrieve relevant document chunks from a persisted LlamaIndex index.

    Loads a previously persisted vector store index and performs similarity
    search against the provided query. The output is a list of chunks with
    text, score, metadata, and source information ready for downstream
    synthesis via ``LLMOperator``.

    :param query: The query string to search for. Supports Jinja templating.
    :param index_persist_dir: Path to the persisted LlamaIndex index directory.
    :param llm_conn_id: Airflow connection ID for the embedding API
        (needed to embed the query vector).
    :param embed_model: Embedding model name (default: ``text-embedding-3-small``).
    :param top_k: Number of top results to retrieve (default: 5).
    """

    template_fields: Sequence[str] = ("query", "index_persist_dir", "llm_conn_id")

    def __init__(
        self,
        *,
        query: str,
        index_persist_dir: str,
        llm_conn_id: str = "pydanticai_default",
        embed_model: str = "text-embedding-3-small",
        top_k: int = 5,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.query = query
        self.index_persist_dir = index_persist_dir
        self.llm_conn_id = llm_conn_id
        self.embed_model = embed_model
        self.top_k = top_k

    def execute(self, context: Context) -> dict[str, Any]:
        from llama_index.core import StorageContext, load_index_from_storage

        from airflow.providers.common.ai.hooks.llamaindex import LlamaIndexHook

        hook = LlamaIndexHook(llm_conn_id=self.llm_conn_id, embed_model=self.embed_model)
        hook.configure_settings()

        storage_context = StorageContext.from_defaults(persist_dir=self.index_persist_dir)
        index = load_index_from_storage(storage_context)

        retriever = index.as_retriever(similarity_top_k=self.top_k)
        results = retriever.retrieve(self.query)
        self.log.info("Retrieved %d chunks for query: %s", len(results), self.query[:100])

        chunks = []
        for node_with_score in results:
            node = node_with_score.node
            chunks.append(
                {
                    "text": node.get_content(),
                    "score": node_with_score.score,
                    "metadata": node.metadata,
                    "source": node.node_id,
                }
            )

        return {
            "question": self.query,
            "chunks": chunks,
        }
