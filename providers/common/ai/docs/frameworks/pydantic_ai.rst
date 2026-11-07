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

For the OpenTelemetry spans and secret masking ``AgentOperator`` gets, build the agent
with the hook's
:meth:`~airflow.providers.common.ai.hooks.pydantic_ai.PydanticAIHook.create_agent`
instead of ``Agent(...)``. It takes the same arguments, and under ``[common.ai]
otel_export_enabled`` it sends the agent's spans through Airflow's tracing, without prompt
text unless ``capture_content`` is on; see :doc:`../observability`. Pydantic AI's own
instrumentation records prompt and completion text by default. It also passes what every
tool returns or raises through Airflow's secret masker, whichever toolset or capability
supplied the tool. To get that masking on an agent you build with ``Agent(...)``, add
:class:`~airflow.providers.common.ai.utils.toolset_base.MaskingCapability` last in its
``capabilities``:

.. code-block:: python

    from airflow.providers.common.ai.utils.toolset_base import MaskingCapability

    agent = Agent(model, toolsets=[my_toolset], capabilities=[MaskingCapability()])

Pydantic AI 2.43 and later print a first-run banner to stderr when Airflow was started from
a coding agent such as Claude Code, Cursor or Codex, and a task's stderr lands in its log
as ``ERROR`` lines. Importing ``PydanticAIHook``, as the example above does, turns the
banner off for the task process; for an agent run without the hook, set
``PYDANTIC_AI_NO_BANNER=1`` in the worker environment.

What you get, and what you do not
---------------------------------

The SQL, hook, object storage, DataFusion, MCP, sandbox and managed-agent toolsets behave
as they do inside ``AgentOperator``: SQL validation, ``allowed_tables``, result bounds,
object-storage path checks, and the secret masker on everything a tool returns, including
the text of a failure the model is asked to correct. Their calls are counted in the
``common_ai.tool_calls`` metric described in :doc:`../observability`. The Agent Skills
toolset is the exception: its results are masked only in an agent built with
``create_agent`` or given ``MaskingCapability``, and its calls are not counted.

``AgentOperator`` adds things on top of the agent that a task running your own agent
does not get:

- Durable replay of model and tool steps across task retries (``durable=True``).
- Human review of the output (``enable_hitl_review``), and a pause for a person to
  approve marked tool calls before they run (:doc:`../tool_approval`).
- Rendering of templated connection IDs in toolsets, such as ``SQLToolset("{{ ... }}")``.
  In your own task, pass the connection ID itself.
- Tool call logging, and the task's identity (``airflow.dag_id``, ``airflow.task_id``
  and the rest) on the agent's spans.

If you find yourself rebuilding one of these, that is a sign ``AgentOperator`` fits:
its ``agent_params`` passes any other argument through to the Pydantic AI ``Agent``.
