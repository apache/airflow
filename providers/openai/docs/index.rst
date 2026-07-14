
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

``apache-airflow-providers-openai``
======================================

The ``openai`` provider gives Dags direct access to OpenAI's own APIs — this page compares
that choice against ``common.ai``.

When to use this provider
--------------------------

Use ``openai`` when a Dag needs OpenAI's native API surface — thin wrappers over
OpenAI-specific endpoints and options, built on ``OpenAIHook``, the underlying client the
operators below share:

* ``OpenAIEmbeddingOperator`` — call the Embeddings API directly, e.g. to feed a vector
  store.
* ``OpenAIResponseOperator`` — call the
  `Responses API <https://platform.openai.com/docs/api-reference/responses>`__ with
  OpenAI-specific parameters.
* ``OpenAITriggerBatchOperator`` — submit a
  `Batch API <https://platform.openai.com/docs/guides/batch>`__ job for asynchronous bulk
  processing and wait for it to complete; OpenAI prices Batch API calls at roughly half the
  cost of the equivalent synchronous call, in exchange for a turnaround of up to ~24 hours.

Use :doc:`apache-airflow-providers-common-ai:index` instead when the AI step should be run by
Airflow itself and stay vendor-neutral:

* Generation, classification, or structured extraction with ``LLMOperator`` — it works with
  OpenAI models via a connection, and switching to another model provider later is a
  connection change, not a Dag rewrite.
* Agents whose loop runs in the Airflow worker with ``AgentOperator`` — Airflow-defined
  toolsets (SQL, hooks, MCP servers), human-in-the-loop review, and durable step replay.
* Document-to-vector-store pipelines with its document loader, embedding, and retrieval
  operators, which are not tied to OpenAI's embedding models.

In short: pick ``openai`` to reach an OpenAI-only endpoint; pick ``common.ai`` to keep the
Dag portable across model providers.

For example, calling the Responses API directly:

.. exampleinclude:: /../../openai/tests/system/openai/example_openai.py
    :language: python
    :start-after: [START howto_operator_openai_response]
    :end-before: [END howto_operator_openai_response]

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
    Connection types <connections>
    Operators <operators/openai>


.. toctree::
    :hidden:
    :maxdepth: 1
    :caption: Resources

    Python API <_api/airflow/providers/openai/index>
    PyPI Repository <https://pypi.org/project/apache-airflow-providers-openai/>
    Installing from sources <installing-providers-from-sources>

.. toctree::
    :hidden:
    :maxdepth: 1
    :caption: System tests

    System Tests <_api/tests/system/openai/index>

.. THE REMAINDER OF THE FILE IS AUTOMATICALLY GENERATED. IT WILL BE OVERWRITTEN AT RELEASE TIME!


.. toctree::
    :hidden:
    :maxdepth: 1
    :caption: Commits

    Detailed list of commits <commits>


apache-airflow-providers-openai package
------------------------------------------------------

`OpenAI <https://platform.openai.com/docs/introduction>`__ provider for Apache Airflow.
Enables interaction with OpenAI APIs for text generation, embeddings,
and other AI-powered workflows directly from Airflow DAGs.


Release: 1.8.0

Provider package
----------------

This package is for the ``openai`` provider.
All classes for this package are included in the ``airflow.providers.openai`` python package.

Installation
------------

You can install this package on top of an existing Airflow installation via
``pip install apache-airflow-providers-openai``.
For the minimum Airflow version supported, see ``Requirements`` below.

Requirements
------------

The minimum Apache Airflow version supported by this provider distribution is ``2.11.0``.

==========================================  ==================
PIP package                                 Version required
==========================================  ==================
``apache-airflow``                          ``>=2.11.0``
``apache-airflow-providers-common-compat``  ``>=1.12.0``
``openai``                                  ``>=2.37.0``
==========================================  ==================

Downloading official packages
-----------------------------

You can download officially released packages and verify their checksums and signatures from the
`Official Apache Download site <https://downloads.apache.org/airflow/providers/>`_

* `The apache-airflow-providers-openai 1.8.0 sdist package <https://downloads.apache.org/airflow/providers/apache_airflow_providers_openai-1.8.0.tar.gz>`_ (`asc <https://downloads.apache.org/airflow/providers/apache_airflow_providers_openai-1.8.0.tar.gz.asc>`__, `sha512 <https://downloads.apache.org/airflow/providers/apache_airflow_providers_openai-1.8.0.tar.gz.sha512>`__)
* `The apache-airflow-providers-openai 1.8.0 wheel package <https://downloads.apache.org/airflow/providers/apache_airflow_providers_openai-1.8.0-py3-none-any.whl>`_ (`asc <https://downloads.apache.org/airflow/providers/apache_airflow_providers_openai-1.8.0-py3-none-any.whl.asc>`__, `sha512 <https://downloads.apache.org/airflow/providers/apache_airflow_providers_openai-1.8.0-py3-none-any.whl.sha512>`__)
