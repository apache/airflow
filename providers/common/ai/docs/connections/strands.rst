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

.. _howto/connection:strands-gemini:

Strands Agents Connection
=========================

The `Strands Agents <https://strandsagents.com/>`__ connection types configure
access to LLM agents via the Strands Agents SDK. Use these connections with
:class:`~airflow.providers.common.ai.operators.agent.AgentOperator` or
``@task.agent``; the connection ``conn_type`` selects the Strands model backend.

Install the optional dependency before using a Strands connection:

.. code-block:: bash

    pip install 'apache-airflow-providers-common-ai[strands]'

Supported connection types
--------------------------

.. list-table::
   :header-rows: 1
   :widths: 25 75

   * - ``conn_type``
     - Backend
   * - ``strands-gemini``
     - Google Gemini via :class:`~airflow.providers.common.ai.hooks.strands_ai.StrandsGeminiHook`

Google Gemini (``strands-gemini``)
-----------------------------------

Default Connection IDs
~~~~~~~~~~~~~~~~~~~~~~

:class:`~airflow.providers.common.ai.hooks.strands_ai.StrandsGeminiHook` uses
``strands_default`` by default.

Configuring the Connection
~~~~~~~~~~~~~~~~~~~~~~~~~~

Model
    Gemini model identifier (for example ``gemini-2.5-flash``). Stored in
    ``extra["model"]`` and overridable via the hook/operator ``model_id`` parameter.

API Key (Password field)
    Google AI API key. When empty, the hook delegates to the ``GOOGLE_API_KEY``
    environment variable.

Model Parameters (extra JSON)
    Optional generation parameters forwarded to Strands ``GeminiModel.params``
    (for example ``temperature``, ``max_output_tokens``, ``top_p``, ``top_k``).

Example Connection
~~~~~~~~~~~~~~~~~~

.. code-block:: json

    {
        "conn_id": "strands_default",
        "conn_type": "strands-gemini",
        "password": "<GOOGLE_AI_API_KEY>",
        "extra": {
            "model": "gemini-2.5-flash",
            "params": {
                "temperature": 0.7,
                "max_output_tokens": 2048
            }
        }
    }

Using with AgentOperator
------------------------

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_strands.py
    :language: python
    :start-after: [START howto_operator_strands_basic]
    :end-before: [END howto_operator_strands_basic]

.. seealso::
    :ref:`Strands Agents hook <howto/hook:strands_ai>`
    Example DAGs in ``example_strands.py``
