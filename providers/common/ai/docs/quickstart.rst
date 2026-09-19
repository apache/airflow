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

.. _howto/quickstart:

Quick start
===========

This guide installs the provider, configures a connection, and runs a first
LLM task.

Before you start: this assumes a working :doc:`apache-airflow:installation/index`
(Airflow 3.0+) already exists, you have an API key for the LLM provider you
plan to use, and step 3 below makes a real, billed API call to that provider.

1. Install
----------

Install the provider together with the extra matching the model SDK you plan
to use — ``openai``, ``anthropic``, ``google``, or ``bedrock`` (see
:doc:`index` for the full list of available extras). Replace ``<extra>``
below with the one you need:

.. code-block:: bash

    pip install "apache-airflow-providers-common-ai[<extra>]"

2. Configure the connection
----------------------------

Every LLM call goes through a Pydantic AI connection (``conn_type`` ``pydanticai``,
default connection id ``pydanticai_default``). The model is set in ``provider:model``
format and the API key goes in the password field. See :ref:`howto/connection:pydanticai`
for the full reference, including providers that
don't need an API key (Bedrock, Vertex AI).

The quickest way to set one up is an environment variable. Replace
``openai:gpt-5.6-sol`` with a model you have access to and ``sk-...`` with your
actual API key:

.. code-block:: bash

    export AIRFLOW_CONN_PYDANTICAI_DEFAULT='{"conn_type": "pydanticai", "password": "sk-...", "extra": {"model": "openai:gpt-5.6-sol"}}'

Or add it through the Airflow UI (``Admin > Connections``) or the CLI (``airflow connections add``).

3. Write your first Dag
------------------------

The ``@task.llm`` decorator turns a function that returns a prompt string into
a task that sends that prompt to the LLM and returns its response:

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_quickstart.py
    :language: python
    :start-after: [START howto_quickstart_llm]
    :end-before: [END howto_quickstart_llm]

Run it like any other Dag (``airflow dags test quickstart_llm``) and the
``summarize`` task pushes the LLM's response to XCom.

Structured output
^^^^^^^^^^^^^^^^^^

Need typed data instead of a string? Set ``output_type`` to a Pydantic
``BaseModel`` and the model instance is pushed to XCom unchanged. See the
"Structured Output" section of the :ref:`howto/operator:llm` guide for the
full example and its XCom-deserialization requirements.

Where to go next
-----------------

- :doc:`operators/index` — the full set of operators and ``@task`` decorators
  (file analysis, SQL, branching, schema comparison).
- :doc:`toolsets` — give an agent tools built from Airflow hooks, SQL
  databases, or MCP servers.
- :ref:`howto/operator:agent` — run a multi-turn agent that reasons and calls
  tools instead of a single prompt-response call.
- :doc:`observability` — trace LLM and tool calls with OpenTelemetry.
