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

Agents with tools: ``AgentOperator`` and ``@task.agent``
========================================================

Use :class:`~airflow.providers.common.ai.operators.agent.AgentOperator` or
the ``@task.agent`` decorator to run an LLM agent with **tools**: the agent
reasons about the prompt, calls tools (database queries, API calls, etc.) in
a multi-turn loop, and returns a final answer.

This is different from
:class:`~airflow.providers.common.ai.operators.llm.LLMOperator`, which sends
a single prompt and returns the output. ``AgentOperator`` manages a stateful
tool-call loop where the LLM decides which tools to call and when to stop.

.. seealso::
    :ref:`Connection configuration <howto/connection:pydanticai>`

SQL agent
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

Hook-based tools
----------------

Wrap any Airflow Hook's methods as agent tools using ``HookToolset``. Only
methods you explicitly list are exposed; there is no auto-discovery.

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_agent.py
    :language: python
    :start-after: [START howto_operator_agent_hook]
    :end-before: [END howto_operator_agent_hook]

TaskFlow decorator
------------------

The ``@task.agent`` decorator wraps ``AgentOperator``. The function returns
the prompt string; all other parameters are passed to the operator.

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_agent.py
    :language: python
    :start-after: [START howto_decorator_agent]
    :end-before: [END howto_decorator_agent]

.. _howto/operator:agent-multimodal:

Multimodal prompts
^^^^^^^^^^^^^^^^^^

The decorated callable may also return a ``Sequence[UserContent]`` -- for
example, a list mixing strings with ``ImageUrl``, ``BinaryContent``, or other
pydantic-ai user-content types -- to send vision, audio, or document inputs
to the model. This mirrors the input types accepted by pydantic-ai's
``Agent.run_sync``.

.. code-block:: python

    from pydantic_ai.messages import ImageUrl


    @task.agent(llm_conn_id="pydanticai_default", system_prompt="You are an image analyst.")
    def analyze_review(image_url: str):
        return ["Describe what you see:", ImageUrl(url=image_url)]

.. note::

    Combining a non-string prompt with ``enable_hitl_review=True`` is not
    currently supported -- the HITL session model stores the prompt as a
    string, so a ``Sequence`` prompt will raise at the review boundary.

Structured output
-----------------

Set ``output_type`` to a Pydantic ``BaseModel`` subclass to get structured data
back. The model instance is pushed to XCom unchanged so downstream tasks can
type-hint the class directly (``def downstream(result: MyModel)``) and use
attribute access (``result.field``).

:doc:`../structured_output` explains the XCom deserialization rules, the cross-Dag gap and
``serialize_output``.

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_agent.py
    :language: python
    :start-after: [START howto_decorator_agent_structured_output_class]
    :end-before: [END howto_decorator_agent_structured_output_class]

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_agent.py
    :language: python
    :start-after: [START howto_decorator_agent_structured]
    :end-before: [END howto_decorator_agent_structured]

Chaining with downstream tasks
------------------------------

The agent's output is pushed to XCom like any other operator, so downstream
tasks can consume it.

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_agent.py
    :language: python
    :start-after: [START howto_agent_chain]
    :end-before: [END howto_agent_chain]

.. _howto/operator:agent-dynamic-system-prompt:

Dynamic system prompt
---------------------

``system_prompt`` is a templated field, so instead of a static string it
can be a Jinja expression that reads a value an earlier task already
computed -- for example, tailoring the agent's instructions to a
classification produced upstream.

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_agent.py
    :language: python
    :start-after: [START howto_agent_dynamic_system_prompt]
    :end-before: [END howto_agent_dynamic_system_prompt]

Open the **Rendered Template** tab on the task instance to see the
substituted ``system_prompt`` after Jinja fills in ``classify``'s XCom
values.

Agent features
--------------

Four features have pages of their own:

- :doc:`../message_history`: pass ``message_history`` to carry a conversation across runs.
- :doc:`../durable_execution`: set ``durable=True`` to replay completed model and tool steps
  on retry instead of paying for them again.
- :doc:`../guardrails`: pass pydantic-ai capabilities and ``pydantic-ai-shields`` guardrails
  through ``agent_params``.
- :doc:`../code_mode`: set ``code_mode=True`` to collapse the agent's tools into a single
  ``run_code`` tool the model drives by writing Python.

.. _agent-durable-execution:

Durable execution
^^^^^^^^^^^^^^^^^

Moved to :doc:`../durable_execution`.

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
  ``AgentSkillsToolset`` for :ref:`agent-skills`, etc.).
- ``enable_tool_logging``: Wrap each toolset in
  :class:`~airflow.providers.common.ai.toolsets.logging.LoggingToolset` so that
  every tool call is logged in real time. Default ``True``.
- ``agent_params``: Additional keyword arguments passed to the pydantic-ai
  ``Agent`` constructor (e.g. ``retries``, ``model_settings``, ``capabilities``).
  See :ref:`capabilities-passthrough` for how to enable pydantic-ai capabilities
  such as ``Thinking``, ``WebSearch``, and ``ImageGeneration``.
- ``usage_limits``: Optional pydantic-ai ``UsageLimits`` enforced on every
  agent run (initial run, durable replay, and HITL regeneration), or a
  ``dict`` of the same fields -- the dict form is templated via Jinja, then
  coerced per field type, failing the task with a ``ValueError`` naming the
  field if a rendered value doesn't parse. Use it to cap requests, tokens, or
  tool calls per task -- agents are particularly prone to runaway tool loops,
  so ``tool_calls_limit`` is a useful guardrail. It also supports a per-run
  USD ``cost_limit``; see :ref:`howto/operator:llm` for the caveats (not a
  hard guarantee; not enforced for models pydantic-ai can't price, which log
  a warning instead of failing the run) and an example. Default ``None``.

  .. warning::
     With ``durable=True``, a task retry replays cached model steps instead of
     re-calling the model -- but pydantic-ai still adds each replayed step's
     cost to the retry's own usage total, since it cannot distinguish a replay
     from a live call. A ``cost_limit`` therefore counts already-paid-for
     replayed cost against every retry's fresh budget, leaving less headroom
     for the new calls the retry actually makes. And if the limit is lowered
     between attempts -- easy to do by accident when ``usage_limits`` is
     templated as a dict -- a retry can exceed it with zero new model calls.
     The ``LLM run cost`` line in the task log reports the run's cumulative
     cost for the same reason, not what this attempt actually spent.
- ``durable``: When ``True``, enables step-level caching of model responses and
  tool results. On retry, cached steps are replayed instead of re-executing
  expensive LLM calls. On Airflow >= 3.3 the cache uses the task state store (no
  configuration needed); on older cores it requires the ``[common.ai]
  durable_cache_path`` config option to be set. Default ``False``.
- ``code_mode``: When ``True``, wraps the agent's tools in a single ``run_code``
  tool that the model drives by writing Python, executed in the Monty sandbox.
  Requires the ``code-mode`` extra. Default ``False``. See :ref:`code-mode`.
- ``message_history``: Prior conversation to seed a multi-turn session, as a list
  of pydantic-ai ``ModelMessage`` objects or their JSON form (``str`` / ``bytes``).
  When set, the post-run transcript is pushed to XCom under the key
  ``message_history`` for the next run to resume. Default ``None`` (single-turn).
  See :doc:`../message_history`.
- ``serialize_output``: If ``True`` and ``output_type`` is a Pydantic
  ``BaseModel`` subclass, the model instance is dumped to a ``dict`` via
  ``model_dump()`` before being pushed to XCom. Default ``False`` -- the
  Pydantic instance flows through XCom unchanged. Set to ``True`` when a
  downstream consumer needs the dict shape.

**HITL review parameters**: ``enable_hitl_review``, ``max_hitl_iterations``,
``hitl_timeout`` and ``hitl_poll_interval`` turn on and bound the iterative review
loop, which needs the ``hitl_review`` plugin. :doc:`../hitl_review` documents each
parameter and the review workflow.

Logging
-------

All AI operators automatically log a post-run summary after ``run_sync()``
completes. ``AgentOperator`` additionally wraps toolsets for real-time
per-tool-call logging (controlled by ``enable_tool_logging``).

**Real-time tool call logging** (AgentOperator only): each tool call is
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

**Post-run summary** (all operators): after the LLM run finishes, a summary
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
    :doc:`../agent_security` for defense layers,
    ``allowed_tables`` limitations, ``HookToolset`` guidelines, recommended
    configurations, and the production checklist.
