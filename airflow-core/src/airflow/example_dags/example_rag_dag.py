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
and diagnose common failure modes from the WFGY 16-problem ProblemMap, such as
chunking errors, retriever bias, and hallucination risks.
"""

from __future__ import annotations

import logging
import random
from datetime import datetime

from airflow.decorators import dag, task, task_group
from airflow.exceptions import AirflowFailException

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
        def fetch_documents():
            """Simulates fetching documents from a source."""
            logger.info("Fetching documents from source...")
            return ["doc1", "doc2", "doc3"]

        @task
        def chunk_documents(docs):
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

        chunk_documents(fetch_documents())

    @task_group(group_id="vector_store_maintenance")
    def vector_store():
        """Simulates vector store updates and index health checks."""

        @task
        def embed_and_index(chunks):
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

        embed_and_index(ingestion())

    @task_group(group_id="retrieval_and_generation")
    def retrieval_gen():
        """Simulates retrieval and LLM generation with semantic checks."""

        @task
        def retrieve_context(query="What is Airflow?"):
            """
            Simulates retrieval.
            Failure mode: Retriever Bias / Distance Mismatch.
            """
            logger.info("Retrieving context for query: %s", query)
            # Simulate a 16-problem Failure: Retriever Bias
            # (e.g., retrieving shipping policy instead of warranty)
            retrieved_docs = ["Airflow Guide", "Shipping Policy"]

            if "Shipping Policy" in retrieved_docs:
                logger.warning(
                    "FAILURE DETECTED: Retriever Bias. Non-relevant document retrieved for query: %s", query
                )
            return retrieved_docs

        @task
        def generate_answer(context):
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
        def semantic_firewall(answer, context):
            """
            A diagnostic task that validates the final output against context.
            This corresponds to the 'Semantic Firewall' concept in the WFGY framework.
            """
            logger.info("Running Semantic Firewall check...")
            # Perform automated checks (e.g., faithfulness, relevance)
            faithfulness_score = random.uniform(0.7, 1.0)
            if faithfulness_score < 0.8:
                logger.warning(
                    "FAILURE DETECTED: Context Mismatch. LLM answer faithfulness score: %.2f",
                    faithfulness_score,
                )
            else:
                logger.info("Semantic Firewall check passed.")

        context = retrieve_context()
        answer = generate_answer(context)
        semantic_firewall(answer, context)

    # Define high-level dependencies
    ingestion() >> vector_store() >> retrieval_gen()


example_rag_failure_analysis()
