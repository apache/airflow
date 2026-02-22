#
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
"""
Example DAG demonstrating RAG (Retrieval-Augmented Generation) failure analysis.

This DAG simulates a typical RAG pipeline and shows how to use Airflow to detect
and diagnose common failure modes, such as chunking errors, retriever bias, and
hallucination risks. The failure taxonomy follows the WFGY ProblemMap
(https://github.com/onestardao/WFGY).
"""

from __future__ import annotations

import logging
import random
from datetime import datetime

from airflow.sdk import dag, task, task_group
from airflow.sdk.exceptions import AirflowFailException

logger = logging.getLogger(__name__)


@dag(
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["example", "rag", "llm", "monitoring"],
)
def example_rag_failure_analysis():
    """
    ### RAG Failure Analysis Pipeline
    This DAG illustrates a RAG pipeline with built-in health checks for each stage.
    """

    @task_group(group_id="ingestion_and_chunking")
    def ingestion():
        """Simulates ingestion and chunking with failure detection."""

        @task
        def fetch_documents() -> list[str]:
            """Simulates fetching documents from a source."""
            logger.info("Fetching documents from source...")
            return ["doc1", "doc2", "doc3"]

        @task
        def chunk_documents(docs: list[str]) -> list[str]:
            """
            Simulates chunking with a check for 'Small Chunk Sensitivity'.
            Failure mode: Too small chunks lose context.
            """
            logger.info("Chunking documents...")
            chunks = []
            for doc in docs:
                # Simulating a failure mode: Chunk size analysis
                chunk_size = random.randint(50, 500)
                if chunk_size < 100:
                    logger.warning(
                        "FAILURE DETECTED: Small Chunk Sensitivity. "
                        "Chunk for %s is only %d characters. Context might be lost.",
                        doc,
                        chunk_size,
                    )
                chunks.append(f"chunk_{doc}_{chunk_size}")
            return chunks

        return chunk_documents(fetch_documents())

    @task_group(group_id="vector_store_maintenance")
    def vector_store(chunks):
        """Simulates vector store updates and index health checks."""

        @task
        def embed_and_index(chunks: list[str]) -> None:
            """
            Simulates embedding and indexing.
            Failure mode: Index Skew / Partially rebuilt stores.
            """
            logger.info("Embedding %d chunks...", len(chunks))
            # Simulate an indexing lag or partial update
            if random.random() < 0.1:
                logger.error("FAILURE DETECTED: Index Skew. Some chunks failed to index.")
                raise AirflowFailException("Index integrity check failed.")
            logger.info("Indexing complete.")

        return embed_and_index(chunks)

    @task_group(group_id="retrieval_and_generation")
    def retrieval_gen():
        """Simulates retrieval and LLM generation with semantic checks."""

        @task
        def retrieve_context(query: str = "What is Airflow?") -> list[str]:
            """
            Simulates retrieval.
            Failure mode: Retriever Bias / Distance Mismatch.
            """
            logger.info("Retrieving context for query: %s", query)
            # Simulate a Retriever Bias failure mode
            # (e.g., retrieving shipping policy instead of warranty)
            retrieved_docs = ["Airflow Guide", "Shipping Policy"]

            if "Shipping Policy" in retrieved_docs:
                logger.warning(
                    "FAILURE DETECTED: Retriever Bias. Non-relevant document retrieved for query: %s", query
                )
            return retrieved_docs

        @task
        def generate_answer(context: list[str]) -> str:
            """
            Simulates LLM generation.
            Failure mode: Hallucination / Prompt Drift.
            """
            logger.info("Generating answer using context...")
            # Simulate a semantic check on the output
            answer = "Airflow is a platform to programmatically author, schedule and monitor workflows."

            # Simple simulation of a guardrail check
            if "platform" not in answer:
                logger.error("FAILURE DETECTED: Hallucination. Answer lacks core definition.")

            return answer

        @task
        def output_faithfulness_check(answer: str, context: list[str]) -> None:
            """
            A diagnostic task that validates the final answer against retrieved context.

            Checks for hallucination and context mismatch by scoring how well the
            generated answer is grounded in the retrieved documents.
            """
            logger.info("Running output faithfulness check...")
            # Perform automated checks (e.g., faithfulness, relevance)
            faithfulness_score = random.uniform(0.7, 1.0)
            if faithfulness_score < 0.8:
                logger.warning(
                    "FAILURE DETECTED: Context Mismatch. LLM answer faithfulness score: %.2f",
                    faithfulness_score,
                )
            else:
                logger.info("Output faithfulness check passed.")

        context = retrieve_context()
        answer = generate_answer(context)
        return output_faithfulness_check(answer, context)

    # Define high-level dependencies
    chunks_out = ingestion()
    vec_store_out = vector_store(chunks_out)
    retrieval_out = retrieval_gen()

    vec_store_out >> retrieval_out


example_rag_failure_analysis()
