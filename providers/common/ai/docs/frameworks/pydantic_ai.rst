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

.. _howto/frameworks:pydantic_ai:

Pydantic AI
===========

This provider's operators run `Pydantic AI <https://ai.pydantic.dev/>`__ agents, and
:class:`~airflow.providers.common.ai.operators.agent.AgentOperator` builds one for you
from a connection, a prompt and a list of toolsets. If you already have a Pydantic AI
agent, with its own instructions, output types, capabilities or history processing, you
do not have to rebuild it as an ``AgentOperator``. Run it in a ``@task`` and give it what
Airflow has: a model from a connection, and toolsets bound to your connections.

Run your own agent in a task
----------------------------

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_pydantic_ai_agent.py
    :language: python
    :start-after: [START example_pydantic_ai_agent]
    :end-before: [END example_pydantic_ai_agent]

``PydanticAIHook.get_hook`` returns the hook for the connection's type, and its
:meth:`~airflow.providers.common.ai.hooks.pydantic_ai.PydanticAIHook.get_conn` returns
the Pydantic AI model the connection describes, with the same vendor prefixes, fallback
connections and self-hosted endpoints as ``AgentOperator``; see :doc:`../model_providers`.
Every toolset this provider ships is a Pydantic AI toolset, so it goes into
``toolsets=`` as it is, next to any toolsets and tools of your own.

For the OpenTelemetry spans ``AgentOperator`` emits, build the agent with the hook's
:meth:`~airflow.providers.common.ai.hooks.pydantic_ai.PydanticAIHook.create_agent`
instead of ``Agent(...)``. It takes the same arguments, and under ``[common.ai]
otel_export_enabled`` it sends the agent's spans through Airflow's tracing, without prompt
text unless ``capture_content`` is on; see :doc:`../observability`. Pydantic AI's own
instrumentation records prompt and completion text by default.

What you get, and what you do not
---------------------------------

The SQL, hook, object storage, DataFusion, MCP, sandbox and managed-agent toolsets behave
as they do inside ``AgentOperator``: SQL validation, ``allowed_tables``, result bounds,
object-storage path checks, and the secret masker on everything a tool returns, including
the text of a failure the model is asked to correct. Their calls are counted in the
``common_ai.tool_calls`` metric described in :doc:`../observability`. The Agent Skills
toolset is the exception: its results are masked only inside ``AgentOperator``, and its
calls are not counted.

``AgentOperator`` adds things on top of the agent that a task running your own agent
does not get:

- Durable replay of model and tool steps across task retries (``durable=True``).
- Human review of the output (``enable_hitl_review``), and a pause for a person to
  approve marked tool calls before they run (:doc:`../tool_approval`).
- Masking of the results of the Agent Skills toolset and of toolsets you wrote yourself.
  There is no public masking wrapper, so run the agent through ``AgentOperator`` if their
  results can carry a secret.
- Rendering of templated connection IDs in toolsets, such as ``SQLToolset("{{ ... }}")``.
  In your own task, pass the connection ID itself.
- Tool call logging, and the task's identity (``airflow.dag_id``, ``airflow.task_id``
  and the rest) on the agent's spans.

If you find yourself rebuilding one of these, that is a sign ``AgentOperator`` fits:
its ``agent_params`` passes any other argument through to the Pydantic AI ``Agent``.
