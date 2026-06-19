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
"""Example DAGs demonstrating LlamaIndexHook + LlamaIndex operator usage.

Each DAG covers a single pattern. The docs reference these via
``.. exampleinclude::`` so the runnable snippets stay in sync.
"""

from __future__ import annotations

from airflow.providers.common.ai.operators.document_loader import DocumentLoaderOperator
from airflow.providers.common.ai.operators.llamaindex_embedding import LlamaIndexEmbeddingOperator
from airflow.providers.common.ai.operators.llamaindex_retrieval import LlamaIndexRetrievalOperator
from airflow.providers.common.compat.sdk import dag, task


# [START howto_hook_llamaindex_embed]
@dag(schedule=None, tags=["example"])
def example_llamaindex_embed():
    """Chunk + embed a directory of documents and persist the index locally."""

    load = DocumentLoaderOperator(
        task_id="load",
        source_path="/opt/airflow/data/library/**/*",
        file_extensions=[".pdf", ".md", ".txt"],
    )

    embed = LlamaIndexEmbeddingOperator(
        task_id="embed",
        documents=load.output,  # XCom direct -- never via Jinja (list[dict])
        embed_model="text-embedding-3-small",
        llm_conn_id="llamaindex_default",
        chunk_size=512,
        chunk_overlap=50,
        persist_dir="/opt/airflow/data/library_index",
    )

    load >> embed


# [END howto_hook_llamaindex_embed]

example_llamaindex_embed()


# [START howto_hook_llamaindex_retrieve]
@dag(schedule=None, tags=["example"])
def example_llamaindex_retrieve():
    """Load a persisted index and run similarity search."""

    retrieve = LlamaIndexRetrievalOperator(
        task_id="retrieve",
        query="{{ params.query }}",
        index_persist_dir="/opt/airflow/data/library_index",
        embed_model="text-embedding-3-small",
        llm_conn_id="llamaindex_default",
        top_k=5,
    )

    retrieve


# [END howto_hook_llamaindex_retrieve]

example_llamaindex_retrieve()


# [START howto_hook_llamaindex_cloud_persist]
@dag(schedule=None, tags=["example"])
def example_llamaindex_cloud_persist():
    """Persist the index directly to S3 -- no separate upload step."""

    load = DocumentLoaderOperator(
        task_id="load",
        source_path="s3://my-bucket/library/",
        source_conn_id="aws_default",
        file_extensions=[".pdf"],
    )

    embed = LlamaIndexEmbeddingOperator(
        task_id="embed",
        documents=load.output,
        embed_model="text-embedding-3-small",
        llm_conn_id="llamaindex_default",
        persist_dir="s3://my-bucket/indexes/library/",
        persist_conn_id="aws_default",
    )

    load >> embed


# [END howto_hook_llamaindex_cloud_persist]

example_llamaindex_cloud_persist()


# [START howto_hook_llamaindex_byo_embed_model]
@dag(schedule=None, tags=["example"])
def example_llamaindex_byo_embed_model():
    """Use a non-OpenAI embedding by instantiating the LlamaIndex class directly.

    LlamaIndex doesn't ship a universal init helper, so the operator accepts
    a pre-built ``BaseEmbedding`` instance and bypasses the hook entirely.
    Install the matching extra:
    ``pip install llama-index-embeddings-cohere``.
    """

    @task
    def build_cohere_embedder():
        from llama_index.embeddings.cohere import CohereEmbedding

        from airflow.providers.common.compat.sdk import BaseHook

        conn = BaseHook.get_connection("cohere_default")
        return CohereEmbedding(model_name="embed-english-v3.0", cohere_api_key=conn.password)

    @task
    def empty_doc_list() -> list[dict]:
        return [{"text": "Cohere demo content", "metadata": {}}]

    embed = LlamaIndexEmbeddingOperator(
        task_id="embed",
        documents=empty_doc_list(),
        embed_model=build_cohere_embedder(),
        persist_dir="/opt/airflow/data/cohere_index",
    )

    embed


# [END howto_hook_llamaindex_byo_embed_model]

example_llamaindex_byo_embed_model()
