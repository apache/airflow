.. Licensed to the Apache Software Foundation (ASF) under one
   or more contributor license agreements.  See the NOTICE file
   distributed with this work for additional information
   regarding copyright ownership.  The ASF licenses this file
   to you under the Apache License, Version 2.0 (the
   "License"); you may not use this file except in compliance
   with the License.  You may obtain a copy of the License at

..   http://www.apache.org/licenses/LICENSE-2.0

.. Unless required by applicable law or agreed to in writing,
   software distributed under the License is distributed on an
   "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
   KIND, either express or implied.  See the License for the
   specific language governing permissions and limitations
   under the License.

RAG Failure Analysis Guide
==========================

As Retrieval-Augmented Generation (RAG) and Large Language Model (LLM) pipelines become more complex, debugging them requires moving beyond simple prompt tuning. When a RAG system fails, the issue often originates in earlier stages of the pipeline orchestrated by Airflow.

This guide introduces a structured approach to RAG failure analysis using the **WFGY 16-problem ProblemMap**, a checklist of typical failure modes tailored for Airflow orchestrators.

The 16-Problem Framework
------------------------

The WFGY ProblemMap categorizes RAG failures into several key areas. Understanding where your DAG fits into these categories helps localize issues quickly.

1. **Ingestion & Chunking Problems**
   - *Small Chunk Sensitivity*: Chunks are too small to retain context.
   - *Chunk Boundary Clipping*: Semantic meaning is lost at arbitrary cut-off points.
   - *Table/Image Neglect*: Critical data in non-textual formats is skipped.

2. **Retrieval & Embedding Failures**
   - *Retriever Bias*: The search algorithm consistently misses specific topics.
   - *Embedding Distance Mismatch*: High cosine similarity but low semantic relevance.
   - *Top-K Dilution*: Including too many non-relevant chunks confuses the LLM.

3. **Vector Store Integrity**
   - *Index Skew*: Part of the vector store is out of date compared to the source.
   - *Empty Index*: Startup failures where queries are run against an uninitialized store.

4. **Reasoning & Generation Issues**
   - *Hallucination*: The LLM generates plausible but factually incorrect information not present in the context.
   - *Question Drift*: In multi-turn conversations, the original intent is lost.
   - *Prompt Injection / Guardrail Breaches*.

Debugging Patterns in Airflow
-----------------------------

Airflow provides several mechanisms to detect and log these 16 failure modes during DAG execution.

Attaching Sensors and Health Checks
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Instead of treating your RAG pipeline as a "black box," you can insert diagnostic tasks or sensors to validate each step.

.. code-block:: python

    @task
    def semantic_firewall(answer, context):
        """
        A diagnostic task that validates the final output against context.
        """
        # Perform faithfulness and relevance checks
        if not is_faithful(answer, context):
            raise AirflowFailException("Faithfulness check failed: Hallucination detected.")


    @task
    def chunk_integrity_check(chunks):
        """
        Detects 'Small Chunk Sensitivity' failure mode.
        """
        avg_size = sum(len(c) for c in chunks) / len(chunks)
        if avg_size < 150:
            logger.warning("Potential failure: Average chunk size is too small.")

Enhanced Logging
~~~~~~~~~~~~~~~~

Use Airflow's task logging to capture specific failure metadata. This makes it easier to use Airflow's UI to see exactly where a retrieval went wrong.

.. code-block:: python

    logger.info("FAILURE DETECTED: Retriever Bias. Query: '%s' returned Doc ID: %s", query, top_doc_id)

Example DAG
-----------

For a full demonstration, see the :ref:`example_rag_dag` provided in the Airflow example dags.

Further Reading
---------------

- `WFGY 16-problem ProblemMap <https://github.com/onestardao/WFGY>`_
- `RAG Debugging Best Practices <https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html>`_
