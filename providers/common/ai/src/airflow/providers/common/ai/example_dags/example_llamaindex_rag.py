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
"""Example DAGs demonstrating RAG pipelines with LlamaIndex operators.

Three patterns:

1. Full RAG pipeline -- load -> embed -> retrieve -> answer in one DAG.
2. Separate index/query DAGs -- production-shaped split (scheduled
   indexing job + on-demand query DAG).
3. Multi-source RAG -- combine multiple loaders with source metadata.

The ``LLMOperator`` synthesis step uses a ``pydanticai_default`` connection
because :class:`~airflow.providers.common.ai.operators.llm.LLMOperator` is
pydantic-ai-backed; the LlamaIndex operators use ``llamaindex_default``.
The two connection types are intentional -- they back different frameworks.
"""

from __future__ import annotations

from airflow.providers.common.ai.operators.document_loader import DocumentLoaderOperator
from airflow.providers.common.ai.operators.llamaindex_embedding import LlamaIndexEmbeddingOperator
from airflow.providers.common.ai.operators.llamaindex_retrieval import LlamaIndexRetrievalOperator
from airflow.providers.common.ai.operators.llm import LLMOperator
from airflow.providers.common.compat.sdk import dag, task

# ---------------------------------------------------------------------------
# 1. Full RAG pipeline: load -> embed -> retrieve -> answer
# ---------------------------------------------------------------------------


# [START howto_llamaindex_rag_pipeline]
@dag(schedule=None, tags=["example"])
def example_llamaindex_rag_pipeline():
    """End-to-end RAG pipeline in a single DAG.

    1. Parse local text files into document dicts.
    2. Chunk and embed the documents, persisting the index to disk.
    3. Retrieve relevant chunks for a user question.
    4. Synthesize an answer using the retrieved context.
    """
    load = DocumentLoaderOperator(
        task_id="load_docs",
        source_path="/opt/airflow/data/knowledge_base/",
        file_extensions=[".txt", ".md", ".pdf"],
    )

    embed = LlamaIndexEmbeddingOperator(
        task_id="embed_docs",
        documents=load.output,
        embed_model="text-embedding-3-small",
        llm_conn_id="llamaindex_default",
        chunk_size=512,
        chunk_overlap=50,
        persist_dir="/opt/airflow/data/indexes/kb_index",
    )

    retrieve = LlamaIndexRetrievalOperator(
        task_id="retrieve",
        query="What are the main components of Apache Airflow?",
        index_persist_dir="/opt/airflow/data/indexes/kb_index",
        embed_model="text-embedding-3-small",
        llm_conn_id="llamaindex_default",
        top_k=5,
    )

    @task
    def format_context(retrieval_result: dict) -> str:
        chunks = retrieval_result["chunks"]
        return "\n\n---\n\n".join(chunk["text"] for chunk in chunks)

    context = format_context(retrieve.output)

    answer = LLMOperator(
        task_id="answer",
        prompt=(
            "Using the context below, answer the question: "
            "What are the main components of Apache Airflow?\n\n"
            "Context:\n{{ ti.xcom_pull(task_ids='format_context') }}"
        ),
        llm_conn_id="pydanticai_default",
        system_prompt="Answer based only on the provided context. Cite sources when possible.",
    )

    embed >> retrieve >> context >> answer


# [END howto_llamaindex_rag_pipeline]

example_llamaindex_rag_pipeline()


# ---------------------------------------------------------------------------
# 2. Production-shaped split: scheduled indexing + on-demand query
# ---------------------------------------------------------------------------


# [START howto_llamaindex_index_dag]
@dag(schedule="@weekly", tags=["example"])
def example_llamaindex_index_pdf():
    """Weekly indexing DAG -- keep the vector index fresh as PDFs arrive.

    The companion query DAG (below) reads the persisted index on demand.
    """
    load = DocumentLoaderOperator(
        task_id="load_pdfs",
        source_path="/opt/airflow/data/reports/*.pdf",
    )

    build_index = LlamaIndexEmbeddingOperator(
        task_id="build_index",
        documents=load.output,
        embed_model="text-embedding-3-small",
        llm_conn_id="llamaindex_default",
        chunk_size=1024,
        chunk_overlap=100,
        persist_dir="/opt/airflow/data/indexes/reports_index",
    )

    load >> build_index


# [END howto_llamaindex_index_dag]

example_llamaindex_index_pdf()


# [START howto_llamaindex_query_dag]
@dag(
    schedule=None,
    params={"question": "Summarize the key findings from the latest quarterly report."},
    tags=["example"],
)
def example_llamaindex_query():
    """On-demand query DAG -- retrieve from a pre-built index and synthesize.

    Trigger manually or via API with a ``question`` parameter.
    """
    retrieve = LlamaIndexRetrievalOperator(
        task_id="retrieve",
        query="{{ params.question }}",
        index_persist_dir="/opt/airflow/data/indexes/reports_index",
        embed_model="text-embedding-3-small",
        llm_conn_id="llamaindex_default",
        top_k=5,
    )

    @task
    def format_context(retrieval_result: dict) -> str:
        chunks = retrieval_result["chunks"]
        numbered = [f"[{i + 1}] {chunk['text']}" for i, chunk in enumerate(chunks)]
        return "\n\n".join(numbered)

    context = format_context(retrieve.output)

    synthesize = LLMOperator(
        task_id="synthesize",
        prompt=(
            "Question: {{ params.question }}\n\n"
            "Relevant excerpts:\n{{ ti.xcom_pull(task_ids='format_context') }}\n\n"
            "Provide a detailed answer with references to the excerpt numbers."
        ),
        llm_conn_id="pydanticai_default",
        system_prompt=(
            "You are a research assistant. Answer the question using only the "
            "provided excerpts. Reference excerpt numbers in square brackets."
        ),
    )

    context >> synthesize


# [END howto_llamaindex_query_dag]

example_llamaindex_query()


# ---------------------------------------------------------------------------
# 3. Multi-source RAG: combine CSV product data with text documentation
# ---------------------------------------------------------------------------


# [START howto_llamaindex_multi_source]
@dag(schedule=None, tags=["example"])
def example_llamaindex_multi_source():
    """Combine multiple loaders with source-tagging metadata.

    Shows how ``DocumentLoaderOperator`` handles different file formats and
    how ``metadata_fields`` tags documents by source for filtered retrieval
    downstream.
    """
    load_products = DocumentLoaderOperator(
        task_id="load_products",
        source_path="/opt/airflow/data/products.csv",
        metadata_fields={"source": "product_catalog", "department": "engineering"},
    )

    load_docs = DocumentLoaderOperator(
        task_id="load_docs",
        source_path="/opt/airflow/data/documentation/",
        file_extensions=[".md", ".txt"],
        metadata_fields={"source": "documentation"},
    )

    @task
    def merge_documents(products: list[dict], docs: list[dict]) -> list[dict]:
        return products + docs

    merged = merge_documents(load_products.output, load_docs.output)

    embed_all = LlamaIndexEmbeddingOperator(
        task_id="embed_all",
        documents=merged,
        embed_model="text-embedding-3-small",
        llm_conn_id="llamaindex_default",
        persist_dir="/opt/airflow/data/indexes/multi_source_index",
    )

    embed_all


# [END howto_llamaindex_multi_source]

example_llamaindex_multi_source()
