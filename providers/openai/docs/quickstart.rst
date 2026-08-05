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

Go from zero to a generated model response in three steps: install the
provider, configure a connection, and write a Dag.

1. Install
----------

.. code-block:: bash

    pip install apache-airflow-providers-openai

2. Configure the connection
----------------------------

Every call goes through an OpenAI connection (``conn_type`` ``openai``,
default connection id ``openai_default``). Specify your OpenAI API key in
the password field. See :ref:`howto/connection:openai` for the full
reference, including workload identity authentication (Kubernetes, Azure,
GCP, or a custom token provider) if you'd rather exchange short-lived
identity tokens than store a long-lived API key.

The quickest way to set one up is an environment variable:

.. code-block:: bash

    export AIRFLOW_CONN_OPENAI_DEFAULT='{"conn_type": "openai", "password": "sk-..."}'

Or add it through the Airflow UI (``Admin > Connections``) or the CLI (``airflow connections add``).

3. Write your first Dag
------------------------

The :class:`~airflow.providers.openai.operators.openai.OpenAIResponseOperator`
generates a model response with the OpenAI Responses API and returns the
response's aggregated output text:


.. exampleinclude:: /../../openai/src/airflow/providers/openai/example_dags/example_response.py
    :language: python
    :start-after: [START quickstart_response]
    :end-before: [END quickstart_response]

Run it like any other Dag (``airflow dags test quickstart_openai``) and the
``generate_response`` task pushes the aggregated output text to XCom.

Where to go next
-----------------

- :doc:`operators/openai` — the full operator set: embeddings with
  ``OpenAIEmbeddingOperator``, batch jobs with ``OpenAITriggerBatchOperator``,
  and the Responses/Conversations ``OpenAIHook`` methods for use inside
  ``@task`` functions.
- :ref:`howto/connection:openai` — the full connection reference, including
  workload identity authentication.
