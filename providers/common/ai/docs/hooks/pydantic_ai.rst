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

.. _howto/hook:pydantic_ai:

PydanticAIHook
==============

Use :class:`~airflow.providers.common.ai.hooks.pydantic_ai.PydanticAIHook` to interact
with LLM providers via `pydantic-ai <https://ai.pydantic.dev/>`__.

The hook manages API credentials from an Airflow connection and creates pydantic-ai
``Model`` and ``Agent`` objects. It supports any provider that pydantic-ai supports.

.. seealso::
    :ref:`Connection configuration <howto/connection:pydanticai>`

Basic Usage
-----------

Use the hook in a ``@task`` function to call an LLM:

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_pydantic_ai_hook.py
    :language: python
    :start-after: [START howto_hook_pydantic_ai_basic]
    :end-before: [END howto_hook_pydantic_ai_basic]

Overriding the Model
--------------------

The model can be specified at three levels (highest priority first):

1. ``model_id`` parameter on the hook
2. ``model`` key in the connection's extra JSON
3. (No default — raises an error if neither is set)

.. code-block:: python

    # Use model from the connection's extra JSON
    hook = PydanticAIHook(llm_conn_id="my_llm")

    # Override with a specific model
    hook = PydanticAIHook(llm_conn_id="my_llm", model_id="anthropic:claude-opus-4-6")

Structured Output
-----------------

Pydantic-ai's structured output works naturally through the hook.
Define a Pydantic model for the expected output shape, then pass it as ``output_type``:

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_pydantic_ai_hook.py
    :language: python
    :start-after: [START howto_hook_pydantic_ai_structured_output]
    :end-before: [END howto_hook_pydantic_ai_structured_output]

Loading Agent Config from a Spec File
--------------------------------------

Instead of hard-coding model name, instructions, and settings in Python, you can
store them in a YAML or JSON `AgentSpec
<https://ai.pydantic.dev/agents/#agent-spec>`__ file and pass its path via
``spec_file``.  This keeps prompt engineering separate from Dag logic and lets
you version-control agent configs independently.

.. code-block:: yaml
   :caption: agent_spec.yaml

   model: openai:gpt-4o-mini
   instructions: >
     You are a concise summarizer. Given any text, respond with a single
     paragraph that captures the key points.
   model_settings:
     temperature: 0.3
   retries: 2

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_pydantic_ai_hook.py
    :language: python
    :start-after: [START howto_hook_pydantic_ai_spec_file]
    :end-before: [END howto_hook_pydantic_ai_spec_file]

The model declared in the spec file is used unless ``model_id`` or the
connection's ``model`` extra is set, in which case the hook model takes
precedence. Passing ``instructions`` to ``create_agent`` when a ``spec_file`` is
also given appends additional instructions to the file value.
