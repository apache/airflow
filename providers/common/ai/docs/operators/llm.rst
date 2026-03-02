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

.. _howto/operator:llm:

``LLMOperator``
===============

Use :class:`~airflow.providers.common.ai.operators.llm.LLMOperator` for
general-purpose LLM calls — summarization, extraction, classification,
structured output, or any prompt-based task.

The operator sends a prompt to an LLM via
:class:`~airflow.providers.common.ai.hooks.pydantic_ai.PydanticAIHook` and
returns the output as XCom.

.. seealso::
    :ref:`Connection configuration <howto/connection:pydantic_ai>`

Basic Usage
-----------

Provide a ``prompt`` and the operator returns the LLM's response as a string:

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_llm.py
    :language: python
    :start-after: [START howto_operator_llm_basic]
    :end-before: [END howto_operator_llm_basic]

Structured Output
-----------------

Set ``output_type`` to a Pydantic ``BaseModel`` subclass. The LLM is instructed
to return structured data, and the result is serialized via ``model_dump()``
for XCom:

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_llm.py
    :language: python
    :start-after: [START howto_operator_llm_structured]
    :end-before: [END howto_operator_llm_structured]

Agent Parameters
----------------

Pass additional keyword arguments to the pydantic-ai ``Agent`` constructor
via ``agent_params`` — for example, ``retries``, ``model_settings``, or ``tools``.
See the `pydantic-ai Agent docs <https://ai.pydantic.dev/api/agent/>`__ for
the full list of supported parameters.

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_llm.py
    :language: python
    :start-after: [START howto_operator_llm_agent_params]
    :end-before: [END howto_operator_llm_agent_params]

TaskFlow Decorator
------------------

The ``@task.llm`` decorator wraps ``LLMOperator``. The function returns the
prompt string; all other parameters are passed to the operator:

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_llm.py
    :language: python
    :start-after: [START howto_decorator_llm]
    :end-before: [END howto_decorator_llm]

With structured output:

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_llm.py
    :language: python
    :start-after: [START howto_decorator_llm_structured]
    :end-before: [END howto_decorator_llm_structured]

Classification with ``Literal``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Set ``output_type`` to a ``Literal`` to constrain the LLM to a fixed set of
labels — useful for classification tasks:

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_llm_classification.py
    :language: python
    :start-after: [START howto_decorator_llm_classification]
    :end-before: [END howto_decorator_llm_classification]

Multi-task pipeline with dynamic mapping
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Combine ``@task.llm`` with upstream and downstream tasks. Use ``.expand()``
to process a list of items in parallel:

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_llm_analysis_pipeline.py
    :language: python
    :start-after: [START howto_decorator_llm_pipeline]
    :end-before: [END howto_decorator_llm_pipeline]

Parameters
----------

- ``prompt``: The prompt to send to the LLM (operator) or the return value of the
  decorated function (decorator).
- ``llm_conn_id``: Airflow connection ID for the LLM provider.
- ``model_id``: Model identifier (e.g. ``"openai:gpt-5"``). Overrides the connection's extra field.
- ``system_prompt``: System-level instructions for the agent. Supports Jinja templating.
- ``output_type``: Expected output type (default: ``str``). Set to a Pydantic ``BaseModel``
  for structured output.
- ``agent_params``: Additional keyword arguments passed to the pydantic-ai ``Agent``
  constructor (e.g. ``retries``, ``model_settings``, ``tools``). Supports Jinja templating.
