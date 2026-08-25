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

Go from zero to a submitted Claude batch in three steps: install the provider,
configure a connection, and write a Dag around ``AnthropicBatchOperator``. This
provider is built around the Message Batches API and Managed Agent sessions;
for interactive, single-call LLM tasks, use ``apache-airflow-providers-common-ai`` instead.

1. Install
----------

.. code-block:: bash

    pip install apache-airflow-providers-anthropic

2. Configure the connection
---------------------------

Batches run through an Anthropic connection (``conn_type`` ``anthropic``,
default connection id ``anthropic_default``). For the first-party API, put
your Anthropic API key in the password field. See :ref:`howto/connection:anthropic`
for the full reference, including the ``platform`` extra for running on
Amazon Bedrock, Google Vertex AI, Claude Platform on AWS or Microsoft Foundry,
and for keyless auth via Workload Identity Federation.

The quickest way to set one up is an environment variable:

.. code-block:: bash

    export AIRFLOW_CONN_ANTHROPIC_DEFAULT='{"conn_type": "anthropic", "password": "sk-ant-..."}'

Or add it through the Airflow UI (``Admin > Connections``) or the CLI (``airflow connections add``).

3. Write your first Dag
-----------------------

``AnthropicBatchOperator`` submits a Message Batch — a list of
``messages.create`` requests — and waits for it to reach a terminal status.
A task retry resubmits a **new** batch, so set ``retries=0``:


.. exampleinclude:: /../../anthropic/src/airflow/providers/anthropic/example_dags/example_batch.py
    :language: python
    :start-after: [START quickstart_batch]
    :end-before: [END quickstart_batch]


Run it like any other Dag (``airflow dags test quickstart_batch``). The task
pushes the batch ID to XCom under key ``batch_id`` as soon as it submits, and
returns it once the batch reaches ``ended``. Pull the per-request results with
:meth:`~airflow.providers.anthropic.hooks.anthropic.AnthropicHook.stream_batch_results`
and write them to object storage — results can be very large and must not be
pushed to XCom.

Where to go next
----------------

- :doc:`operators/anthropic` — full parameter reference for
  ``AnthropicBatchOperator``, ``AnthropicBatchSensor`` (poll an
  already-submitted batch without resubmitting on retry) and
  ``AnthropicAgentSessionOperator`` (Anthropic-hosted Managed Agent sessions).
- :doc:`connections` — the ``platform`` extra (bedrock, vertex, aws, foundry)
  and Workload Identity Federation for keyless auth.
- For interactive, single-call or agentic LLM workloads, prefer the
  vendor-agnostic ``apache-airflow-providers-common-ai`` provider with
  ``model="anthropic:claude-opus-4-8"``; this provider focuses on the
  batch/async surface and direct SDK access that the agent abstraction does
  not model.
