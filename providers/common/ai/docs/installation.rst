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

The provider needs Airflow 2.11 or later. Install it with the extra that matches the
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

* **Model providers** (``openai``, ``anthropic``, ``google``, ``bedrock``, ``typesafe``):
  pick the one matching your ``llm_conn_id`` connection (:doc:`model_providers` maps
  vendors to extras, prefixes and connection types). ``typesafe`` differs from the rest
  in kind: it installs a decision model that answers typed questions and cannot write
  text (see :doc:`decision_models`). Other decision models, behind the System One API,
  need no extra. The first four mirror the identically named
  ``pydantic-ai-slim`` optional dependency groups, and ``typesafe`` adds the ``typesafe-sdk``
  the built-in adapter talks to; pydantic-ai supports more model providers
  than these, each under its own extra name, so check the
  `pydantic-ai install docs <https://ai.pydantic.dev/install/#slim-install>`__ for the full list.
* **Agent tooling** (``mcp``, ``skills``, ``code-mode``, ``shields``, ``modal``,
  ``opensandbox``): MCP servers, Agent Skills, code-mode tool execution, shield
  capabilities (input/output guards, tool guards, cost tracking), and the hosted Modal and
  self-hosted OpenSandbox backends for :doc:`sandboxed execution <sandbox/index>`.
* **Document loading** (``pdf``, ``docx``, ``avro``, ``parquet``): file formats for
  document pipelines.
* **Retrieval / SQL** (``sql``, ``common.sql``, ``langchain``, ``llamaindex``): RAG and
  SQL-schema tooling.
* **Git-backed content** (``git``): pulling Agent Skills or documents from a git connection.

The ``Optional dependencies`` table on the :doc:`landing page <index>` lists the exact
package each extra installs.

Features gated on the Airflow version
-------------------------------------

The provider runs on Airflow 2.11, but some features need a newer Airflow version:

.. list-table::
   :header-rows: 1
   :widths: 60 40

   * - Feature
     - Needs
   * - The ``skills`` and ``git`` extras (``apache-airflow-providers-git`` needs Airflow 3)
     - Airflow 3.0
   * - :doc:`Approval gates <approval_gates>` and :doc:`HITL review <hitl_review>`
     - Airflow 3.1
   * - The **Model** field in the connection form; on older Airflow versions put the model in
       **Extra**, for example ``{"model": "openai:gpt-5"}``
     - Airflow 3.2
   * - :doc:`Retry policies <retry_policies>`
     - Airflow 3.3
   * - :doc:`Durable execution <durable_execution>` without configuring
       ``[common.ai] durable_cache_path`` (the task state store)
     - Airflow 3.3
   * - :doc:`Tool approval <tool_approval>` that pauses the task; on older Airflow versions a tool
       marked for approval fails the task
     - Airflow 3.3
   * - A :doc:`structured output <structured_output>` reaching downstream tasks as the
       Pydantic model; on older Airflow versions it arrives as a ``dict``
     - Airflow 3.3

Airflow 2.11
------------

On Airflow 2.11 the operators, decorators, hooks and toolsets run as they do on Airflow
3.0, apart from the table above. Three things differ from an Airflow 3 install:

* The examples in these docs import ``dag``, ``task`` and ``Param`` from ``airflow.sdk``.
  On Airflow 2 import ``dag`` and ``task`` from ``airflow.decorators`` and ``Param`` from
  ``airflow.models.param``; the provider's own imports stay the same.
* Install Airflow with its constraints file as usual, then add the provider without it. The
  Airflow 2.11 constraints pin ``apache-airflow-providers-common-compat`` and
  ``apache-airflow-providers-common-sql`` to releases older than this provider needs.
  Installing Airflow 2.11.0 without its constraints can also pull in a ``universal-pathlib``
  0.3 release, which Airflow 2's ``ObjectStoragePath`` rejects; 2.11.1 and later cap it.
  Leave out the ``skills`` and ``git`` extras: they need Airflow 3, and without
  constraints ``pip`` upgrades Airflow to satisfy them.
* Python 3.10 to 3.12: the provider needs 3.10 or later, and Airflow 2.11 supports up to 3.12.

Next steps
----------

* :doc:`quickstart` creates a connection and runs a first ``@task.llm``.
* :doc:`connections/pydantic_ai` covers every connection field and the vendor-specific
  connection types.
* :doc:`installing-providers-from-sources` covers verifying and installing a release
  artifact by hand.
