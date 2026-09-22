
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
   * - Many prompts through a batch API at half the price, with retry-safe re-attachment
       and results landed on object storage
     - ``common.ai``
     - ``apache-airflow-providers-common-ai`` (:doc:`operators/llm_batch`)
   * - A vendor's native Embeddings or Responses API, or a batch of raw provider request
       bodies (multi-turn, images, non-chat endpoints)
     - The vendor's own provider
     - e.g. :doc:`apache-airflow-providers-openai:index`,
       :doc:`apache-airflow-providers-anthropic:index`,
       :doc:`apache-airflow-providers-cohere:index`
   * - A vendor-managed, server-side agent session (e.g. Anthropic Managed Agents)
     - The vendor's own provider
     - e.g. :doc:`apache-airflow-providers-anthropic:index`

``common.ai`` is built on `pydantic-ai <https://ai.pydantic.dev/>`__: the connection picks the
model vendor, and Airflow runs the AI step like any other task. :doc:`concepts` explains the
ideas behind the provider in one page; :doc:`operators/index` lists what each operator is for.

As a rule of thumb: if Airflow should *run* the AI step (and the model should stay
swappable), use ``common.ai``; if the Dag *submits work to* a vendor-managed service and
waits for the result, use that vendor's provider.

For example, this ``LLMOperator`` call is unchanged whether ``llm_conn_id`` points at an
OpenAI, Anthropic, or other pydantic-ai-supported connection:

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_llm.py
    :language: python
    :start-after: [START howto_operator_llm_basic]
    :end-before: [END howto_operator_llm_basic]

Getting started
---------------

* :doc:`installation` — which extra to install for your model vendor.
* :doc:`quickstart` — a connection and a first ``@task.llm`` in three steps.
* :doc:`concepts` — connections, operators, toolsets, hooks and XCom in one page.

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
    :caption: Getting started

    Installation <installation>
    Quick start <quickstart>
    Core concepts <concepts>
    Structured output <structured_output>

.. toctree::
    :hidden:
    :maxdepth: 1
    :caption: Models and providers

    Pydantic AI connection <connections/pydantic_ai>
    Azure OpenAI <connections/pydantic_ai_azure>
    AWS Bedrock <connections/pydantic_ai_bedrock>
    Google Vertex AI <connections/pydantic_ai_vertex>
    Self-hosted models <self_hosted_models>
    Classifier models <classifier_models>
    Provider fallback <provider_fallback>
    PydanticAIHook <hooks/pydantic_ai>
    LangChainHook <hooks/langchain>

.. toctree::
    :hidden:
    :maxdepth: 1
    :caption: Operators

    Choosing an operator <operators/index>
    LLMOperator <operators/llm>
    LLMBranchOperator <operators/llm_branch>
    LLMFileAnalysisOperator <operators/llm_file_analysis>
    LLMSQLQueryOperator <operators/llm_sql>
    LLMSchemaCompareOperator <operators/llm_schema_compare>
    LLMBatchOperator <operators/llm_batch>
    AgentOperator <operators/agent>
    Approval gates <approval_gates>

.. toctree::
    :hidden:
    :maxdepth: 1
    :caption: Toolsets

    Choosing a toolset <toolsets/index>
    HookToolset <toolsets/hook>
    SQLToolset <toolsets/sql>
    DataFusionToolset <toolsets/datafusion>
    LoggingToolset <toolsets/logging>
    MCPToolset <toolsets/mcp>
    AgentSkillsToolset <toolsets/skills>
    Managed agent toolsets <toolsets/managed_agent>
    LangChain bridge <toolsets/langchain>

.. toctree::
    :hidden:
    :maxdepth: 1
    :caption: Running agents

    Message history <message_history>
    Guardrails <guardrails>
    Code mode <code_mode>
    Sandboxed execution <sandbox/index>
    HITL review <hitl_review>

.. toctree::
    :hidden:
    :maxdepth: 1
    :caption: Document and RAG pipelines

    DocumentLoaderOperator <operators/document_loader>
    LlamaIndexEmbeddingOperator <operators/llamaindex_embedding>
    LlamaIndexRetrievalOperator <operators/llamaindex_retrieval>
    LlamaIndex connection <connections/llamaindex>
    LlamaIndexHook <hooks/llamaindex>

.. toctree::
    :hidden:
    :maxdepth: 1
    :caption: Reliability and operations

    Durable execution <durable_execution>
    Retry policies <retry_policies>
    Observability <observability>
    Securing agent tools <agent_security>
    Troubleshooting <troubleshooting>

.. toctree::
    :hidden:
    :maxdepth: 1
    :caption: Examples

    Examples by scenario <examples>
    End-to-end pipelines <end_to_end_pipelines>

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


Release: 0.9.0

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
``pydantic-ai-slim``                        ``>=2.33.0``
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
``anthropic``   ``pydantic-ai-slim[anthropic]>=2.33.0``, ``anthropic>=1.0.0``
``bedrock``     ``pydantic-ai-slim[bedrock]>=2.33.0``
``google``      ``pydantic-ai-slim[google]>=2.33.0``
``openai``      ``pydantic-ai-slim[openai]>=2.33.0``, ``openai>=2.47.0``
``typesafe``    ``typesafe-sdk>=0.6.0``
``mcp``         ``pydantic-ai-slim[mcp]>=2.33.0``
``modal``       ``modal>=1.5.0``
``code-mode``   ``pydantic-ai-harness[codemode]>=0.3.0``
``shields``     ``pydantic-ai-shields>=0.3.4``
``skills``      ``apache-airflow-providers-git>=0.4.0``, ``pydantic-ai-skills>=1.2.0``
``avro``        ``fastavro>=1.10.0; python_version < "3.14"``, ``fastavro>=1.12.1; python_version >= "3.14"``
``parquet``     ``pyarrow>=18.0.0; python_version < '3.14'``, ``pyarrow>=22.0.0; python_version >= '3.14'``
``sql``         ``apache-airflow-providers-common-sql>=1.33.0``, ``sqlglot>=30.0.0``
``common.sql``  ``apache-airflow-providers-common-sql>=1.33.0``
``langchain``   ``langchain>=1.0.0``
``llamaindex``  ``dataclasses-json>=0.6.7``, ``llama-index-core>=0.14.5``, ``llama-index-embeddings-openai>=0.6.0``, ``llama-index-llms-openai>=0.6.8``
``pdf``         ``pypdf>=4.0.0``
``docx``        ``python-docx>=1.0.0``
``git``         ``apache-airflow-providers-git``
==============  =======================================================================================================================================

Downloading official packages
-----------------------------

You can download officially released packages and verify their checksums and signatures from the
`Official Apache Download site <https://downloads.apache.org/airflow/providers/>`_

* `The apache-airflow-providers-common-ai 0.9.0 sdist package <https://downloads.apache.org/airflow/providers/apache_airflow_providers_common_ai-0.9.0.tar.gz>`_ (`asc <https://downloads.apache.org/airflow/providers/apache_airflow_providers_common_ai-0.9.0.tar.gz.asc>`__, `sha512 <https://downloads.apache.org/airflow/providers/apache_airflow_providers_common_ai-0.9.0.tar.gz.sha512>`__)
* `The apache-airflow-providers-common-ai 0.9.0 wheel package <https://downloads.apache.org/airflow/providers/apache_airflow_providers_common_ai-0.9.0-py3-none-any.whl>`_ (`asc <https://downloads.apache.org/airflow/providers/apache_airflow_providers_common_ai-0.9.0-py3-none-any.whl.asc>`__, `sha512 <https://downloads.apache.org/airflow/providers/apache_airflow_providers_common_ai-0.9.0-py3-none-any.whl.sha512>`__)
