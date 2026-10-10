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

.. _capabilities:

Capabilities and guardrails
===========================

A pydantic-ai `capability <https://ai.pydantic.dev/capabilities/>`__ adds a behavior to an agent
in one declaration: tools, instructions, model settings, and hooks that run around each model
request or tool call. ``Thinking`` turns on the model's reasoning at a chosen effort level,
``WebSearch`` and ``WebFetch`` give the model the web through its provider's native tool, and
guardrail packages such as ``pydantic-ai-shields`` check inputs and outputs. For the full
catalog, see the pydantic-ai documentation and the
`pydantic-ai-harness capability matrix <https://github.com/pydantic/pydantic-ai-harness#capability-matrix>`__.

Pass capabilities to ``AgentOperator`` or ``@task.agent`` with ``capabilities=``:

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_agent_capabilities.py
    :language: python
    :start-after: [START howto_operator_agent_capabilities_thinking]
    :end-before: [END howto_operator_agent_capabilities_thinking]

Capabilities and toolsets work together: the agent gets the tools from both.

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_agent_capabilities.py
    :language: python
    :start-after: [START howto_operator_agent_capabilities_composed]
    :end-before: [END howto_operator_agent_capabilities_composed]

pydantic-ai wraps hooks in list order, the first capability outermost, so a guard listed first
sees a request before the capabilities after it. A capability can declare its own position
(for example, always outermost), which takes precedence over the list.

Guardrails
----------

A guardrail is a capability that checks what goes into or comes out of the agent and stops the
run when a check fails. This example uses ``InputGuard`` from ``pydantic-ai-shields`` to reject a
prompt before the agent run starts.

.. note::

    Experimental: the ``shields`` extra can change or be removed in a minor release of this
    provider.
    See :ref:`howto/stability`.

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_agent_capabilities.py
    :language: python
    :start-after: [START howto_operator_agent_capabilities_input_guard]
    :end-before: [END howto_operator_agent_capabilities_input_guard]

Toolsets as capabilities
------------------------

pydantic-ai's ``Toolset`` capability holds a toolset, so any toolset from this provider can be
passed that way. The connection IDs of ``SQLToolset``, ``MCPToolset`` and ``HookToolset`` are
templated inside a ``Toolset`` capability the same way as in ``toolsets=``:

.. code-block:: python

    from pydantic_ai.capabilities import Toolset

    AgentOperator(
        task_id="analyst",
        prompt="How many orders shipped yesterday?",
        llm_conn_id="pydanticai_default",
        capabilities=[Toolset(SQLToolset(db_conn_id="warehouse_{{ var.value.environment }}"))],
    )

A ``Toolset`` capability built from a function is resolved when the run starts, so its
connection IDs are not templated.

Tool results from a ``Toolset`` capability are masked like those from ``toolsets=``.
When ``enable_tool_logging=True`` (the default), ``AgentOperator`` logs calls to
these toolsets and other tools contributed by capabilities, including MCP
toolsets. Output tools and provider-native tools, including native MCP, are not
covered. See :doc:`toolsets/logging` for configuration details and the complete
limitations. Put a toolset in the capability list when you need to order it
against another capability, such as a guardrail.

With durable execution
----------------------

With ``durable=True``, a retry replays completed steps from the cache instead of running them
again. Whether a capability's work is replayed depends on where it runs:

.. list-table::
    :header-rows: 1

    * - Capability
      - On retry
    * - ``Thinking``, and ``WebSearch``, ``WebFetch`` or ``ImageGeneration`` when the model's
        provider runs the tool natively
      - Replayed with the cached model response.
    * - ``WebSearch``, ``WebFetch`` or ``ImageGeneration`` falling back to a local tool, for a
        provider without the native one
      - The local tool runs again.
    * - ``Toolset`` holding a toolset
      - Tool results are replayed.
    * - ``MCP``, ``PrefixTools``, ``CombinedCapability``, a ``Toolset`` built from a function,
        and capabilities from an agent spec file
      - Tools run again. Pass tools you need replayed in ``toolsets=`` instead.
    * - pydantic-ai-harness ``CodeMode``
      - Not allowed: the operator raises ``ValueError``. This includes a ``CodeMode`` inside a
        ``CombinedCapability`` or a wrapper such as ``PrefixTools``, but not one a capability
        function builds when the run starts.

See :doc:`durable_execution` for how the cache works.

Serialization
-------------

Capabilities passed with ``capabilities=`` are not stored in the serialized Dag. The worker
builds them from the Dag file when the task runs, so a capability can hold functions and
clients that do not serialize.

``capabilities`` inside ``agent_params`` is still accepted and reaches the agent the same way.
``agent_params`` is a template field, though, so Airflow stores each capability's repr in the
serialized Dag. For a capability holding a function, such as the ``InputGuard`` above, that
repr includes the function's memory address, which can differ from one parse to the next and
change the serialized Dag with it. Prefer ``capabilities=``. Passing both fails the task with a
``ValueError``, since there would be no single order to run the hooks in.

A mapped task (``AgentOperator.partial(...).expand(...)`` or a mapped ``@task.agent``) is the
exception: Airflow stores every argument given to ``partial``, so there ``capabilities`` is
serialized as a repr too, as ``toolsets`` is.
