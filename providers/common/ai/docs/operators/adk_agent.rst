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

.. _howto/operator:adk_agent:

``AdkAgentOperator`` & ``@task.adk_agent``
==========================================

Use :class:`~airflow.providers.common.ai.operators.adk_agent.AdkAgentOperator`
or the ``@task.adk_agent`` decorator to run an agent backed by Google's
`Agent Development Kit <https://google.github.io/adk-docs/>`_.  The agent
reasons about the prompt, calls tools in a multi-turn loop, and returns a
final answer using Gemini models.

Install the extra dependency first:

.. code-block:: bash

    pip install 'apache-airflow-providers-common-ai[adk]'

.. seealso::
    :ref:`Connection configuration <howto/connection:adk>`


ADK Agent with Tools
--------------------

Tools are plain Python callables whose **docstrings** are sent to the LLM —
no special base class is needed.

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_adk_agent.py
    :language: python
    :start-after: [START howto_operator_adk_agent]
    :end-before: [END howto_operator_adk_agent]

.. note::

    ``AdkAgentOperator`` does **not** require ``llm_conn_id``; instead it
    relies on ``model_id`` and environment-level authentication
    (``GOOGLE_API_KEY`` or Application Default Credentials).  You can
    optionally provide ``llm_conn_id`` to store the API key and model in an
    Airflow connection.


TaskFlow Decorator
------------------

The ``@task.adk_agent`` decorator wraps ``AdkAgentOperator``.  The function
returns the prompt string; all other parameters are passed to the operator.

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_adk_agent.py
    :language: python
    :start-after: [START howto_decorator_adk_agent]
    :end-before: [END howto_decorator_adk_agent]


Parameters
----------

- ``prompt``: The prompt to send to the agent (operator) or the return value
  of the decorated function (decorator).
- ``model_id``: Model identifier (e.g. ``"gemini-2.5-flash"``). **Required**.
- ``llm_conn_id``: Airflow connection ID (optional).  When provided, API key
  and model can be read from the connection.
- ``system_prompt``: System-level instructions for the agent.  Supports Jinja
  templating.
- ``output_type``: Expected output type (default: ``str``).  Set to a Pydantic
  ``BaseModel`` for structured output.
- ``tools``: List of plain Python callables whose docstrings are exposed to
  the LLM as tool descriptions.
- ``agent_params``: Additional keyword arguments passed to the ADK ``Agent``
  constructor.
