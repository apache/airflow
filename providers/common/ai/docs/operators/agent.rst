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

.. _howto/operator:agent:

``AgentOperator`` & ``@task.agent``
===================================

Use :class:`~airflow.providers.common.ai.operators.agent.AgentOperator` or
the ``@task.agent`` decorator to run an LLM agent with **tools** — the agent
reasons about the prompt, calls tools (database queries, API calls, etc.) in
a multi-turn loop, and returns a final answer.

This is different from
:class:`~airflow.providers.common.ai.operators.llm.LLMOperator`, which sends
a single prompt and returns the output. ``AgentOperator`` manages a stateful
tool-call loop where the LLM decides which tools to call and when to stop.

.. seealso::
    :ref:`Connection configuration <howto/connection:pydantic_ai>`


SQL Agent
---------

The most common pattern: give an agent access to a database so it can answer
questions by writing and executing SQL.

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_agent.py
    :language: python
    :start-after: [START howto_operator_agent_sql]
    :end-before: [END howto_operator_agent_sql]

The ``SQLToolset`` provides four tools to the agent:

.. list-table::
   :header-rows: 1
   :widths: 20 50

   * - Tool
     - Description
   * - ``list_tables``
     - Lists available table names (filtered by ``allowed_tables`` if set)
   * - ``get_schema``
     - Returns column names and types for a table
   * - ``query``
     - Executes a SQL query and returns rows as JSON
   * - ``check_query``
     - Validates SQL syntax without executing it


Hook-based Tools
----------------

Wrap any Airflow Hook's methods as agent tools using ``HookToolset``. Only
methods you explicitly list are exposed — there is no auto-discovery.

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_agent.py
    :language: python
    :start-after: [START howto_operator_agent_hook]
    :end-before: [END howto_operator_agent_hook]


TaskFlow Decorator
------------------

The ``@task.agent`` decorator wraps ``AgentOperator``. The function returns
the prompt string; all other parameters are passed to the operator.

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_agent.py
    :language: python
    :start-after: [START howto_decorator_agent]
    :end-before: [END howto_decorator_agent]


Structured Output
-----------------

Set ``output_type`` to a Pydantic ``BaseModel`` subclass to get structured
data back. The result is serialized via ``model_dump()`` for XCom.

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_agent.py
    :language: python
    :start-after: [START howto_decorator_agent_structured]
    :end-before: [END howto_decorator_agent_structured]


Chaining with Downstream Tasks
-------------------------------

The agent's output is pushed to XCom like any other operator, so downstream
tasks can consume it.

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_agent.py
    :language: python
    :start-after: [START howto_agent_chain]
    :end-before: [END howto_agent_chain]


Parameters
----------

- ``prompt``: The prompt to send to the agent (operator) or the return value
  of the decorated function (decorator).
- ``llm_conn_id``: Airflow connection ID for the LLM provider.
- ``model_id``: Model identifier (e.g. ``"openai:gpt-5"``). Overrides the
  connection's extra field.
- ``system_prompt``: System-level instructions for the agent. Supports Jinja
  templating.
- ``output_type``: Expected output type (default: ``str``). Set to a Pydantic
  ``BaseModel`` for structured output.
- ``toolsets``: List of pydantic-ai toolsets (``SQLToolset``, ``HookToolset``,
  etc.).
- ``enable_tool_logging``: Wrap each toolset in
  :class:`~airflow.providers.common.ai.toolsets.logging.LoggingToolset` so that
  every tool call is logged in real time. Default ``True``.
- ``agent_params``: Additional keyword arguments passed to the pydantic-ai
  ``Agent`` constructor (e.g. ``retries``, ``model_settings``).


Logging
-------

All AI operators automatically log a post-run summary after ``run_sync()``
completes. ``AgentOperator`` additionally wraps toolsets for real-time
per-tool-call logging (controlled by ``enable_tool_logging``).

**Real-time tool call logging** (AgentOperator only) — each tool call is
logged as it happens:

.. code-block:: text

    INFO - Tool call: list_tables
    INFO - Tool list_tables returned in 0.12s
    INFO - Tool call: get_schema
    INFO - Tool get_schema returned in 0.08s
    INFO - Tool call: query
    INFO - Tool query returned in 0.34s

Tool arguments are logged at DEBUG level to avoid leaking sensitive data at
the default log level.

**Post-run summary** (all operators) — after the LLM run finishes, a summary
is logged with model name, token usage, and the full tool call sequence:

.. code-block:: text

    INFO - LLM run complete: model=gpt-5, requests=4, tool_calls=3, input_tokens=2847, output_tokens=512, total_tokens=3359
    INFO - Tool call sequence: list_tables -> get_schema -> query

At DEBUG level, the LLM output is also logged (truncated to 500 characters).

Both layers use Airflow's ``::group::`` / ``::endgroup::`` log markers, which
render as collapsible sections in the Airflow UI task log viewer.

To disable real-time tool logging while keeping the post-run summary:

.. code-block:: python

    AgentOperator(
        task_id="my_agent",
        prompt="...",
        llm_conn_id="my_llm",
        toolsets=[SQLToolset(db_conn_id="my_db")],
        enable_tool_logging=False,
    )


Security
--------

.. seealso::
    :ref:`Toolsets — Security <howto/toolsets>` for defense layers,
    ``allowed_tables`` limitations, ``HookToolset`` guidelines, recommended
    configurations, and the production checklist.
