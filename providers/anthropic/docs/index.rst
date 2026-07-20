
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

``apache-airflow-providers-anthropic``
======================================

The ``anthropic`` provider gives Dags direct access to Anthropic's own APIs — this page
compares that choice against ``common.ai``.

When to use this provider
--------------------------

Use ``anthropic`` when a Dag needs Anthropic's native API surface — services Anthropic runs
for you, which no vendor-neutral operator wraps:

* ``AnthropicBatchOperator`` and ``AnthropicBatchSensor`` — submit a Claude
  `Message Batches <https://docs.claude.com/en/docs/build-with-claude/batch-processing>`__
  job for asynchronous bulk processing and wait for it to complete; Message Batches run at
  50% of standard cost, with most completing within an hour and a 24-hour SLA.
* ``AnthropicAgentSessionOperator`` — start a Managed Agents session in which the agent loop
  runs server-side on Anthropic's infrastructure; the Airflow task only kicks off the session
  and waits for its outcome.

Use :doc:`apache-airflow-providers-common-ai:index` instead when the AI step should be run by
Airflow itself and stay vendor-neutral:

* Generation, classification, or structured extraction with ``LLMOperator`` — it works with
  Claude via a connection, and switching to another model provider later is a connection
  change, not a Dag rewrite.
* Agents whose loop runs **in the Airflow worker** with ``AgentOperator`` — Airflow-defined
  toolsets (SQL, hooks, MCP servers), human-in-the-loop review, and durable step replay.

Both ``AgentOperator`` and ``AnthropicAgentSessionOperator`` run agents with Claude models;
the difference is where the loop executes and who controls the tools. ``AgentOperator``
orchestrates the loop in the worker and calls tools you define in the Dag.
``AnthropicAgentSessionOperator`` hands the whole session to Anthropic's managed service —
the agent runs autonomously against a pre-created agent and environment, without
Airflow-side tool execution.

For example, running the Message Batches API directly:

.. exampleinclude:: /../tests/system/anthropic/example_anthropic_batch.py
    :language: python
    :start-after: [START howto_operator_anthropic_batch]
    :end-before: [END howto_operator_anthropic_batch]

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
    Operators <operators/anthropic>


.. toctree::
    :hidden:
    :maxdepth: 1
    :caption: Resources

    Python API <_api/airflow/providers/anthropic/index>
    PyPI Repository <https://pypi.org/project/apache-airflow-providers-anthropic/>
    Installing from sources <installing-providers-from-sources>

.. toctree::
    :hidden:
    :maxdepth: 1
    :caption: System tests

    System Tests <_api/tests/system/anthropic/index>

.. THE REMAINDER OF THE FILE IS AUTOMATICALLY GENERATED. IT WILL BE OVERWRITTEN AT RELEASE TIME!


.. toctree::
    :hidden:
    :maxdepth: 1
    :caption: Commits

    Detailed list of commits <commits>


apache-airflow-providers-anthropic package
------------------------------------------------------

`Anthropic <https://docs.claude.com/>`__ provider for Apache Airflow.
Wraps the official Anthropic Python SDK to run the Claude Message Batches API
asynchronously from Airflow, plus direct message and token-counting helpers.


Release: 0.1.0

Provider package
----------------

This package is for the ``anthropic`` provider.
All classes for this package are included in the ``airflow.providers.anthropic`` python package.

Installation
------------

You can install this package on top of an existing Airflow installation via
``pip install apache-airflow-providers-anthropic``.
For the minimum Airflow version supported, see ``Requirements`` below.

Requirements
------------

The minimum Apache Airflow version supported by this provider distribution is ``3.0.0``.

==========================================  ==================
PIP package                                 Version required
==========================================  ==================
``apache-airflow``                          ``>=3.0.0``
``apache-airflow-providers-common-compat``  ``>=1.12.0``
``anthropic``                               ``>=0.101.0``
==========================================  ==================

Optional dependencies
---------------------

These extras install optional third-party libraries that enable additional features of the provider.
Install them when installing from PyPI. For example:

.. code-block:: bash

    pip install apache-airflow-providers-anthropic[bedrock]


===========  ===============================
Extra        Dependencies
===========  ===============================
``bedrock``  ``anthropic[bedrock]>=0.101.0``
``vertex``   ``anthropic[vertex]>=0.101.0``
``aws``      ``anthropic[aws]>=0.101.0``
===========  ===============================

Downloading official packages
-----------------------------

You can download officially released packages and verify their checksums and signatures from the
`Official Apache Download site <https://downloads.apache.org/airflow/providers/>`_

* `The apache-airflow-providers-anthropic 0.1.0 sdist package <https://downloads.apache.org/airflow/providers/apache_airflow_providers_anthropic-0.1.0.tar.gz>`_ (`asc <https://downloads.apache.org/airflow/providers/apache_airflow_providers_anthropic-0.1.0.tar.gz.asc>`__, `sha512 <https://downloads.apache.org/airflow/providers/apache_airflow_providers_anthropic-0.1.0.tar.gz.sha512>`__)
* `The apache-airflow-providers-anthropic 0.1.0 wheel package <https://downloads.apache.org/airflow/providers/apache_airflow_providers_anthropic-0.1.0-py3-none-any.whl>`_ (`asc <https://downloads.apache.org/airflow/providers/apache_airflow_providers_anthropic-0.1.0-py3-none-any.whl.asc>`__, `sha512 <https://downloads.apache.org/airflow/providers/apache_airflow_providers_anthropic-0.1.0-py3-none-any.whl.sha512>`__)
