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

.. _howto/rag-pipelines:

Document and RAG pipelines
==========================

A retrieval pipeline in this provider is three ordinary tasks. :doc:`operators/document_loader`
parses files (text, CSV, JSON, PDF, DOCX) into a list of ``{"text", "metadata"}`` dicts with
no AI framework involved. :doc:`operators/llamaindex_embedding` chunks those documents and
produces embedding vectors. :doc:`operators/llamaindex_retrieval` pulls the closest chunks back
out for a question, ready to drop into an :doc:`LLMOperator <operators/llm>` prompt. Each step
is a task, so indexing can run on a schedule while querying runs on demand.

The LlamaIndex operators read their embedding and language models from a ``llamaindex``
connection. :doc:`hooks/llamaindex` returns those LlamaIndex objects for use in a plain
``@task`` when the operators do not fit.

:doc:`use_cases/ask_questions_over_pdfs` and :doc:`use_cases/compare_10k_filings` show the
whole shape end to end.

.. toctree::
    :hidden:
    :titlesonly:

    Load documents <operators/document_loader>
    Embed documents <operators/llamaindex_embedding>
    Retrieve context <operators/llamaindex_retrieval>
    LlamaIndex connection <connections/llamaindex>
    Using LlamaIndex directly <hooks/llamaindex>
