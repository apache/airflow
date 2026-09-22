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

.. _howto/installation:

Installation
============

The provider needs Airflow 3.0 or later. Install it with the extra that matches the
model vendor your connection will point at:

.. code-block:: bash

    pip install "apache-airflow-providers-common-ai[openai]"

Quote the package name so the brackets survive your shell. Swap ``openai`` for
``anthropic``, ``google`` or ``bedrock`` as needed, or combine several extras with a
comma. The minimum versions of ``apache-airflow`` and ``pydantic-ai-slim`` are listed in
the requirements table on the :doc:`landing page <index>`.

Choosing extras
---------------

The provider's extras split into a few groups:

* **Model providers** — ``openai``, ``anthropic``, ``google``, ``bedrock``, ``typesafe``:
  pick the one matching your ``llm_conn_id`` connection. ``typesafe`` differs from the rest
  in kind: it installs a classifier model that answers typed questions and cannot write
  text (see :doc:`classifier_models`). The first four mirror the identically named
  ``pydantic-ai-slim`` optional dependency groups, and ``typesafe`` adds the ``typesafe-sdk``
  the built-in adapter talks to; pydantic-ai supports more model providers
  than these, each under its own extra name, so check the
  `pydantic-ai install docs <https://ai.pydantic.dev/install/#slim-install>`__ for the full list.
* **Agent tooling** — ``mcp``, ``skills``, ``code-mode``, ``shields``, ``modal``: MCP servers,
  Agent Skills, code-mode tool execution, shield capabilities (input/output guards, tool
  guards, cost tracking), and the hosted Modal backend for :doc:`sandboxed execution <sandbox/index>`.
* **Document loading** — ``pdf``, ``docx``, ``avro``, ``parquet``: file formats for
  document pipelines.
* **Retrieval / SQL** — ``sql``, ``common.sql``, ``langchain``, ``llamaindex``: RAG and
  SQL-schema tooling.
* **Git-backed content** — ``git``: pulling Agent Skills or documents from a git connection.

The ``Optional dependencies`` table on the :doc:`landing page <index>` lists the exact
package each extra installs.

Features gated on the Airflow version
-------------------------------------

The provider runs on Airflow 3.0, but some features need a newer core:

.. list-table::
   :header-rows: 1
   :widths: 60 40

   * - Feature
     - Needs
   * - :doc:`Approval gates <approval_gates>` and :doc:`HITL review <hitl_review>`
     - Airflow 3.1
   * - :doc:`Retry policies <retry_policies>`
     - Airflow 3.3
   * - :doc:`Durable execution <durable_execution>` without configuring
       ``[common.ai] durable_cache_path`` (the task state store)
     - Airflow 3.3

Next steps
----------

* :doc:`quickstart` creates a connection and runs a first ``@task.llm``.
* :doc:`connections/pydantic_ai` covers every connection field and the vendor-specific
  connection types.
* :doc:`installing-providers-from-sources` covers verifying and installing a release
  artifact by hand.
