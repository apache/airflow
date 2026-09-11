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
    :ref:`Connection configuration <howto/connection:pydanticai>`


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
    Widening HITL review to multimodal prompts is tracked as a follow-up.


Structured Output
-----------------

Set ``output_type`` to a Pydantic ``BaseModel`` subclass to get structured data
back. The model instance is pushed to XCom unchanged so downstream tasks can
type-hint the class directly (``def downstream(result: MyModel)``) and use
attribute access (``result.field``).

The declared ``output_type`` (and any ``BaseModel`` reachable from
``Union``/``Optional``/``list`` shapes) is registered for XCom deserialization by
the worker when it loads the Dag, before any task runs. The Pydantic class must
be defined at **module scope** and bound to an attribute matching its
``__name__``. Same-Dag downstream tasks need no configuration. The UI's XCom
viewer renders the value via the ``stringify`` path (no configuration needed;
see the ``LLMOperator`` guide for the exact representation). Cross-Dag
``xcom_pull`` consumers still need the class ``qualname`` added to
``[core] allowed_deserialization_classes``.

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_agent.py
    :language: python
    :start-after: [START howto_decorator_agent_structured_output_class]
    :end-before: [END howto_decorator_agent_structured_output_class]

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


.. _howto/operator:agent-dynamic-system-prompt:

Dynamic System Prompt
----------------------

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


Multi-turn Sessions
-------------------

By default each agent run is a cold, single-turn conversation. To carry a
conversation across runs -- a chat or iterative agent where "and the third one?"
must resolve against an earlier answer -- pass ``message_history``.

When ``message_history`` is set, the operator seeds the run with those prior
turns and, after the run, pushes the full updated transcript
(``result.all_messages()``) to XCom under the key ``message_history``. The next
run reads it back to resume the conversation. ``None`` (the default) keeps the
single-turn behavior unchanged.

The operator does **not** decide *where* a session is stored -- that keying is
deployment-specific. The pattern is three tasks: load the prior transcript for
the session, run the agent, store the updated transcript. The example keys a
JSON file in object storage by ``session_id`` (use ``s3://`` / ``gs://`` in a
deployment); the first run starts from an empty ``"[]"``.

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_agent.py
    :language: python
    :start-after: [START howto_agent_session]
    :end-before: [END howto_agent_session]

``message_history`` accepts a list of pydantic-ai ``ModelMessage`` objects or
their JSON form (``str`` / ``bytes``), so the value emitted to XCom feeds
straight back in on the next run. When pulling it via a template, pass
``default='[]'`` (as above) so the first run -- which has no XCom yet -- starts a
fresh session instead of trying to parse the string ``"None"``.

The transcript is **cumulative**: each turn appends to it, so it grows for the
life of the session. For long sessions, configure an object-storage XCom backend
or trim older turns before the next run rather than feeding the whole history
back unbounded.

.. note::

    ``message_history`` cannot be combined with ``enable_hitl_review`` -- the
    operator raises at construction. The post-review (human-approved) transcript
    is not recoverable today, so emitting the pre-review transcript would
    silently drop the reviewed turns.


Durable Execution
-----------------

Agent tasks can involve multiple LLM calls and tool invocations. If a task
fails mid-run (network error, timeout, transient API failure), a plain retry
re-executes every LLM call and tool call from scratch -- repeating work that
already succeeded and incurring additional cost.

Setting ``durable=True`` caches each LLM response and tool result as it
completes. On retry, completed steps are replayed from the cache and only the
remaining steps run against the live model and tools. The cache is deleted
after successful completion.

Durable execution only helps when the task has retries configured. Without
retries there is nothing to replay.

**Configuration**

On **Airflow >= 3.3** the cache is stored in the
:doc:`task state store <apache-airflow:core-concepts/task-state-store>`,
scoped to the task instance. No configuration is required; the store handles
persistence across retries.

By default each cached step is written to the Airflow metadata database. Model
responses and large tool results can be sizable, so for agents with large
payloads configure ``[workers] state_store_backend`` to offload step values to
external storage (e.g. object storage) instead of the metadata database; the
provider then stores only a reference in the database.

On **Airflow < 3.3** the cache is persisted to ObjectStorage and the location
must be set in ``airflow.cfg``. The task raises ``ValueError`` at runtime if
``durable=True`` and the option is missing.

.. code-block:: ini

    [common.ai]
    # Local filesystem -- suitable for development
    durable_cache_path = file:///tmp/airflow_durable_cache

The value is an ObjectStorage URI, so any supported backend works. For
production, use a shared store so retries on a different worker can read the
cache:

.. code-block:: ini

    [common.ai]
    durable_cache_path = s3://my-bucket/airflow/durable-cache

**Operator example**

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_agent_durable.py
    :language: python
    :start-after: [START howto_operator_agent_durable]
    :end-before: [END howto_operator_agent_durable]

**Decorator example**

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_agent_durable.py
    :language: python
    :start-after: [START howto_decorator_agent_durable]
    :end-before: [END howto_decorator_agent_durable]

**How it works**

1. On first execution, each LLM response and tool result is saved as the agent
   progresses, together with a fingerprint of the request that produced it
   (model, message history, settings, and tools for LLM steps; tool name,
   arguments, and call id for tool steps).
2. If the task fails and Airflow retries it, completed steps are loaded from
   the cache and returned without calling the model or tool. Steps not yet in
   the cache proceed normally.
3. Before a step is replayed, its stored fingerprint is compared against the
   current request. If anything changed between attempts -- the system
   prompt, the model, the toolset, model settings, or the conversation so
   far -- the stale entry is discarded, a warning is logged, and the step
   re-runs live. A divergence also invalidates the steps after it: re-running
   an LLM step produces fresh tool call ids, so tool results recorded under
   the old conversation no longer match. A changed agent costs a re-run; it
   never replays responses that belong to a different conversation.
4. After successful completion, the cached steps are deleted.

Replay verification compares the **requests** sent to models and tools, not
the code behind them. Editing a tool's implementation between attempts does
not invalidate an already-cached result for an identical call, and pointing
``llm_conn_id`` at a different endpoint serving the same model name does not
invalidate cached responses -- clear the cache to force a fully fresh run.

After the run, a single INFO summary line reports how many steps were
replayed vs executed fresh. Per-step detail is available at DEBUG level.

The cache is scoped to a single task instance (Dag id, run id, task id, and
map index), so each run replays only its own steps. On Airflow >= 3.3 the cache
lives in the task state store and is removed when the Dag run is cleaned up; on
Airflow < 3.3 it is a JSON file named ``{dag_id}_{task_id}_{run_id}.json`` (with
``_{map_index}`` appended for mapped tasks) under the configured
``durable_cache_path``.

.. note::

    Runs that fail permanently (exhaust all retries) leave their cached steps
    behind. These do not affect future Dag runs (each run is scoped separately).
    On Airflow >= 3.3 they are reclaimed when the Dag run is removed; on Airflow
    < 3.3 the orphaned JSON files consume storage until cleaned up, so add a
    lifecycle policy to the storage backend or remove them periodically.

**Side effects and idempotency**

Durable execution caches **return values**, not side effects. When a step is
replayed, the tool's code does not run -- only the stored return value is
returned. Two things follow from this:

- If a tool completed successfully and its result was cached, the tool will
  **not** run again on retry. Any side effect it produced (writing a file,
  sending a message) already happened during the original run and is not
  repeated.
- If a tool fails *before* its result is cached, it **will** run again on
  retry. A tool that partially completed (e.g. sent an email then raised an
  exception) may produce the side effect a second time.

All built-in toolsets (``SQLToolset`` with ``allow_writes=False``,
``HookToolset`` in read-only mode) are read-only and replay safely. For custom
tools with non-idempotent side effects, design the tool to be idempotent. For
example, check whether the operation already completed before acting, or
use database constraints to prevent duplicate writes.

Tool results must be JSON-serializable to be cached. If a tool returns a
non-serializable value (e.g. ``BinaryContent`` from MCP tools), that step is
skipped with a warning and will re-execute on retry instead of replaying from
cache. The task itself still succeeds.


.. _capabilities-passthrough:

Capabilities (pydantic-ai)
--------------------------

pydantic-ai `capabilities <https://ai.pydantic.dev/capabilities/>`__ bundle
tools, lifecycle hooks, instructions, and model settings into composable units.
Common ones include ``Thinking`` (reasoning at a configurable effort level),
``WebSearch``, ``WebFetch``, ``ImageGeneration``, and ``MCP``.
For the current capability catalog and package-specific installation notes, see
the pydantic-ai documentation and the
`pydantic-ai-harness capability matrix <https://github.com/pydantic/pydantic-ai-harness#capability-matrix>`__.

``AgentOperator`` does not yet expose a first-class ``capabilities=`` kwarg,
but anything passed through ``agent_params`` is forwarded to the underlying
``Agent(...)`` constructor.

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_agent_capabilities.py
    :language: python
    :start-after: [START howto_operator_agent_capabilities_thinking]
    :end-before: [END howto_operator_agent_capabilities_thinking]

Capabilities compose with toolsets -- pydantic-ai merges tools from both.

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_agent_capabilities.py
    :language: python
    :start-after: [START howto_operator_agent_capabilities_composed]
    :end-before: [END howto_operator_agent_capabilities_composed]

Guardrail capabilities use the same passthrough pattern. This example uses
``InputGuard`` from ``pydantic-ai-shields`` to reject a prompt before the agent
run starts.

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_agent_capabilities.py
    :language: python
    :start-after: [START howto_operator_agent_capabilities_input_guard]
    :end-before: [END howto_operator_agent_capabilities_input_guard]

.. warning::

    ``agent_params`` is a templated field, which Airflow serializes by calling
    ``str()`` on values it doesn't natively understand. Capability instances
    are not yet round-trip-safe through Dag serialization, so the examples
    below construct them inside the ``@dag`` function -- not at module level.
    First-class ``capabilities=`` support on ``AgentOperator`` (with proper
    serializer hooks) is tracked as a follow-up.


.. _code-mode:

Code Mode (Monty sandbox)
-------------------------

Set ``code_mode=True`` to collapse the agent's tools into a single ``run_code``
tool powered by the `Monty <https://github.com/pydantic/monty>`__ sandbox (via
pydantic-ai-harness). Instead of one model round-trip per tool call, the model
writes a single Python snippet that calls the tools as functions -- with loops,
conditionals, and ``asyncio.gather`` -- in one turn. For multi-tool workflows
this cuts round-trips and token use.

The generated code runs in Monty's deny-by-default sandbox: it cannot read the
filesystem, the network, or environment variables. It can only call the tools
you registered. Code mode therefore does not widen what the agent can reach --
the tools it calls still run in the worker -- it only changes how the model
invokes them. See :ref:`Toolsets security <howto/toolsets>` for the tool
boundary.

When to use it
^^^^^^^^^^^^^^

Code mode pays off for **orchestration-heavy, computation-light** workflows:
calling several tools, looping over their results, filtering, and combining them.
Collapsing many sequential tool calls into one turn is where the round-trip and
token savings come from -- the example above answers a per-customer question in a
single ``run_code`` block instead of one model round-trip per customer.

It is **not a general-purpose code runtime**. The generated code is only the glue
between tool calls; every real capability must come from a tool. Monty runs a
subset of Python and **cannot import third-party libraries** (pandas, numpy,
requests, boto3, ...) and has no filesystem or network access. If a task needs to
crunch data inline with a library, you have two options, both better than code
mode:

- **Push the work into a tool.** Do the aggregation in SQL (``SQLToolset``), or
  expose a hook method that returns the processed result (``HookToolset``). The
  tool runs in the full worker environment with all its dependencies, and code
  mode just orchestrates it.
- **Use a container-based execution environment** (e.g. Docker or E2B via
  pydantic-ai-harness) instead of the in-process Monty sandbox. These support
  third-party packages but pay a per-run container cost and a larger security
  surface, so reach for them only when inline library code is genuinely required.

Requires the ``code-mode`` extra::

    pip install "apache-airflow-providers-common-ai[code-mode]"

.. exampleinclude:: /../../ai/src/airflow/providers/common/ai/example_dags/example_agent.py
    :language: python
    :start-after: [START howto_operator_agent_code_mode]
    :end-before: [END howto_operator_agent_code_mode]

Unlike passing a capability through ``agent_params`` (see
:ref:`capabilities-passthrough`), ``code_mode`` is a plain boolean and is
serialization-safe: the ``CodeMode`` capability is built at execution time, not
stored on the serialized operator.

.. note::

    Monty is pre-1.0. The ``code-mode`` extra is opt-in so its dependency churn
    never affects the base provider install.


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
  agent run (initial run, durable replay, and HITL regeneration). Use it to
  cap requests, tokens, or tool calls per task -- agents are particularly
  prone to runaway tool loops, so ``tool_calls_limit`` is a useful guardrail.
  See :ref:`howto/operator:llm` for an example. Default ``None``.
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
  See `Multi-turn Sessions`_.
- ``serialize_output``: If ``True`` and ``output_type`` is a Pydantic
  ``BaseModel`` subclass, the model instance is dumped to a ``dict`` via
  ``model_dump()`` before being pushed to XCom. Default ``False`` -- the
  Pydantic instance flows through XCom unchanged. Set to ``True`` when a
  downstream consumer needs the dict shape.

**HITL Review parameters** (requires the ``hitl_review`` plugin -- see
:doc:`../hitl_review` for the full review workflow):

- ``enable_hitl_review``: When ``True``, the operator enters an iterative
  review loop after the first generation. A human reviewer can approve,
  reject, or request changes via the plugin's REST API at ``/hitl-review``
  or through the **HITL Review** extra link on the task instance. Default
  ``False``.
- ``max_hitl_iterations``: Maximum outputs shown to the reviewer (1 = initial
  output). When the reviewer requests changes at iteration >= this limit, the
  task fails with ``HITLMaxIterationsError`` without calling the LLM. E.g. 5
  allows changes at iterations 1-4. Default ``5``.
- ``hitl_timeout``: Maximum wall-clock time to wait for all review rounds
  combined. ``None`` means no timeout (the operator blocks until a terminal
  action).
- ``hitl_poll_interval``: Seconds between XCom polls while waiting for a
  human response. Default ``10``.


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
