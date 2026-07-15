
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

``apache-airflow-providers-common-ai``
##################################################

The ``common.ai`` provider is the vendor-neutral way to put LLM and agent steps in a Dag.

When to use this provider
--------------------------

.. list-table::
   :header-rows: 1
   :widths: 40 30 30

   * - Use case
     - Use
     - Package
   * - Portable generation, classification, extraction, branching, or a
       worker-run agent with toolsets
     - ``common.ai``
     - ``apache-airflow-providers-common-ai``
   * - A vendor's native Embeddings, Responses, or Batch API
     - The vendor's own provider
     - e.g. :doc:`apache-airflow-providers-openai:index`,
       :doc:`apache-airflow-providers-anthropic:index`,
       :doc:`apache-airflow-providers-cohere:index`
   * - A vendor-managed, server-side agent session (e.g. Anthropic Managed Agents)
     - The vendor's own provider
     - e.g. :doc:`apache-airflow-providers-anthropic:index`

``common.ai`` is built on `pydantic-ai <https://ai.pydantic.dev/>`__, so the model vendor
(OpenAI, Anthropic, Google, Bedrock, …) is picked by the connection ``llm_conn_id`` points
at — switching providers later is a connection change, not a Dag rewrite. Existing LangChain
tools aren't locked out either: with the ``langchain`` extra installed, they can be wrapped
in ``LangChainToolset`` and dropped straight into a common.ai agent (see :doc:`toolsets`).
The AI step is orchestrated by Airflow: the model calls, the agent loop, and any tools all
run in the Airflow worker, where they get retries, logging, and observability like any other
task.

Use it when a Dag needs:

* **Generation, classification, summarization, or structured extraction** —
  :doc:`LLMOperator and @task.llm <operators/llm>`, with Pydantic-typed output pushed to XCom.
* **Branching on a model's decision** — :doc:`LLMBranchOperator <operators/llm_branch>`.
* **Agents with tools** — :doc:`AgentOperator <operators/agent>` runs a multi-turn agent loop
  in the worker, calling Airflow-defined :doc:`toolsets <toolsets>` (SQL, hooks, MCP servers),
  with optional human-in-the-loop review and durable step replay — if the task retries after
  a failure, completed steps are replayed from cache instead of re-executing.
* **Document pipelines** — loading, file analysis, embeddings, and retrieval for RAG
  (see :doc:`operators/index`).

Use a vendor's own provider instead when the Dag needs that vendor's **native API surface** —
a service the vendor runs for you, which no vendor-neutral operator wraps:

* :doc:`apache-airflow-providers-openai:index` — the Embeddings, Responses, and Batch APIs.
* :doc:`apache-airflow-providers-anthropic:index` — the Claude Message Batches API, and
  Managed Agents sessions where the agent loop runs on Anthropic's infrastructure rather
  than in the Airflow worker.
* :doc:`apache-airflow-providers-cohere:index` — Cohere's own Embed API.
* :doc:`apache-airflow-providers-google:index` — Vertex AI's Batch Prediction jobs
  (``CreateBatchPredictionJobOperator``), a managed batch service like OpenAI's Batch API.
* :doc:`apache-airflow-providers-amazon:index` — Bedrock's Batch Inference
  (``BedrockBatchInferenceOperator``), and Bedrock AgentCore's managed agent runtime
  (``BedrockCreateAgentRuntimeOperator`` / ``BedrockInvokeAgentRuntimeOperator``), where the
  agent loop runs on AWS's infrastructure rather than in the Airflow worker.

As a rule of thumb: if Airflow should *run* the AI step (and the model should stay
swappable), use ``common.ai``; if the Dag *submits work to* a vendor-managed service and
waits for the result, use that vendor's provider.

For example, this ``LLMOperator`` call is unchanged whether ``llm_conn_id`` points at an
OpenAI, Anthropic, or other pydantic-ai-supported connection:

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_llm.py
    :language: python
    :start-after: [START howto_operator_llm_basic]
    :end-before: [END howto_operator_llm_basic]

Choosing extras
----------------

The provider's extras split into a few groups:

* **Model providers** — ``openai``, ``anthropic``, ``google``, ``bedrock``: pick the one
  matching your ``llm_conn_id`` connection. Each extra name mirrors the identically named
  ``pydantic-ai-slim`` optional dependency group; pydantic-ai supports more model providers
  than these four, each under its own extra name, so check the
  `pydantic-ai install docs <https://ai.pydantic.dev/install/#slim-install>`__ for the full list.
* **Agent tooling** — ``mcp``, ``skills``, ``code-mode``: MCP servers, Agent Skills, and
  code-mode tool execution.
* **Document loading** — ``pdf``, ``docx``, ``avro``, ``parquet``: file formats for
  document pipelines.
* **Retrieval / SQL** — ``sql``, ``common.sql``, ``langchain``, ``llamaindex``: RAG and
  SQL-schema tooling.
* **Git-backed content** — ``git``: pulling Agent Skills or documents from a git connection.

See the Optional dependencies table below for the exact package each extra installs.

.. toctree::
    :hidden:
    :maxdepth: 1
    :caption: Basics

    Home <self>
    Changelog <changelog>
    Security <security>

.. toctree::
    :hidden:
    :maxdepth: 1
    :caption: Guides

    Quick start <quickstart>
    Connection types <connections/pydantic_ai>
    MCP connection <connections/mcp>
    Hooks <hooks/index>
    Toolsets <toolsets>
    Operators <operators/index>
    Examples <examples>
    Retry Policies <retry_policies>
    HITL Review <hitl_review>
    Observability <observability>

.. toctree::
    :hidden:
    :maxdepth: 1
    :caption: References

    Configuration <configurations-ref>
    Python API <_api/airflow/providers/common/ai/index>

.. toctree::
    :hidden:
    :maxdepth: 1
    :caption: System tests

    System Tests <_api/tests/system/common/ai/index>

.. toctree::
    :hidden:
    :maxdepth: 1
    :caption: Resources

    PyPI Repository <https://pypi.org/project/apache-airflow-providers-common-ai/>
    Installing from sources <installing-providers-from-sources>

.. THE REMAINDER OF THE FILE IS AUTOMATICALLY GENERATED. IT WILL BE OVERWRITTEN AT RELEASE TIME!


.. toctree::
    :hidden:
    :maxdepth: 1
    :caption: Commits

    Detailed list of commits <commits>


apache-airflow-providers-common-ai package
------------------------------------------------------

AI/LLM hooks and operators for Airflow pipelines using `pydantic-ai <https://ai.pydantic.dev/>`__.


Release: 0.6.0

Provider package
----------------

This package is for the ``common.ai`` provider.
All classes for this package are included in the ``airflow.providers.common.ai`` python package.

Installation
------------

You can install this package on top of an existing Airflow installation via
``pip install apache-airflow-providers-common-ai``.
For the minimum Airflow version supported, see ``Requirements`` below.

Requirements
------------

The minimum Apache Airflow version supported by this provider distribution is ``3.0.0``.

==========================================  ==================
PIP package                                 Version required
==========================================  ==================
``apache-airflow``                          ``>=3.0.0``
``apache-airflow-providers-common-compat``  ``>=1.15.0``
``apache-airflow-providers-standard``       ``>=1.12.1``
``pydantic-ai-slim``                        ``>=2.0.0``
==========================================  ==================

Optional cross provider package dependencies
--------------------------------------------

Those are dependencies that might be needed in order to use all the features of the package.
You need to install the specified provider distributions in order to use them.

You can install such cross-provider dependencies when installing from PyPI. For example:

.. code-block:: bash

    pip install apache-airflow-providers-common-ai[common.sql]


============================================================================================================  ==============
Dependent package                                                                                             Extra
============================================================================================================  ==============
`apache-airflow-providers-common-sql <https://airflow.apache.org/docs/apache-airflow-providers-common-sql>`_  ``common.sql``
`apache-airflow-providers-git <https://airflow.apache.org/docs/apache-airflow-providers-git>`_                ``git``
============================================================================================================  ==============

Optional dependencies
---------------------

These extras install optional third-party libraries that enable additional features of the provider.
Install them when installing from PyPI. For example:

.. code-block:: bash

    pip install apache-airflow-providers-common-ai[anthropic]


==============  =======================================================================================================================================
Extra           Dependencies
==============  =======================================================================================================================================
``anthropic``   ``pydantic-ai-slim[anthropic]``
``bedrock``     ``pydantic-ai-slim[bedrock]``
``google``      ``pydantic-ai-slim[google]``
``openai``      ``pydantic-ai-slim[openai]``
``mcp``         ``pydantic-ai-slim[mcp]``
``code-mode``   ``pydantic-ai-harness[codemode]>=0.3.0``
``skills``      ``apache-airflow-providers-git>=0.4.0``, ``pydantic-ai-skills>=0.11.0``
``avro``        ``fastavro>=1.10.0; python_version < "3.14"``, ``fastavro>=1.12.1; python_version >= "3.14"``
``parquet``     ``pyarrow>=18.0.0; python_version < '3.14'``, ``pyarrow>=22.0.0; python_version >= '3.14'``
``sql``         ``apache-airflow-providers-common-sql``, ``sqlglot>=30.0.0``
``common.sql``  ``apache-airflow-providers-common-sql``
``langchain``   ``langchain>=1.0.0``
``llamaindex``  ``dataclasses-json>=0.6.7``, ``llama-index-core>=0.13.0``, ``llama-index-embeddings-openai>=0.6.0``, ``llama-index-llms-openai>=0.6.0``
``pdf``         ``pypdf>=4.0.0``
``docx``        ``python-docx>=1.0.0``
``git``         ``apache-airflow-providers-git``
==============  =======================================================================================================================================

Downloading official packages
-----------------------------

You can download officially released packages and verify their checksums and signatures from the
`Official Apache Download site <https://downloads.apache.org/airflow/providers/>`_

* `The apache-airflow-providers-common-ai 0.6.0 sdist package <https://downloads.apache.org/airflow/providers/apache_airflow_providers_common_ai-0.6.0.tar.gz>`_ (`asc <https://downloads.apache.org/airflow/providers/apache_airflow_providers_common_ai-0.6.0.tar.gz.asc>`__, `sha512 <https://downloads.apache.org/airflow/providers/apache_airflow_providers_common_ai-0.6.0.tar.gz.sha512>`__)
* `The apache-airflow-providers-common-ai 0.6.0 wheel package <https://downloads.apache.org/airflow/providers/apache_airflow_providers_common_ai-0.6.0-py3-none-any.whl>`_ (`asc <https://downloads.apache.org/airflow/providers/apache_airflow_providers_common_ai-0.6.0-py3-none-any.whl.asc>`__, `sha512 <https://downloads.apache.org/airflow/providers/apache_airflow_providers_common_ai-0.6.0-py3-none-any.whl.sha512>`__)
